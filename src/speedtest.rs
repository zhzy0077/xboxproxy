use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use futures::{stream, StreamExt};
use http::header::{HOST, RANGE, USER_AGENT};
use http_body_util::BodyExt;
use hyper::{Request, Uri};
use tokio::sync::Notify;

use crate::db::SpeedTestRecord;
use crate::metrics::MetricsStore;
use crate::proxy::{request_with_ip, ProxyClient};
use crate::selector::HostDef;
use crate::selector::{now_unix, Selector};

#[derive(Debug, Clone)]
pub struct SpeedTestConfig {
    pub interval_sec: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub test_chunk_bytes: u64,
    pub warmup_passes: usize,
    pub failure_penalty_sec: u64,
    pub max_concurrency: usize,
    /// How long to wait before retrying a pass that was postponed because the
    /// proxy was serving downloads.
    pub busy_retry_sec: u64,
}

impl Default for SpeedTestConfig {
    fn default() -> Self {
        Self {
            interval_sec: 3600,
            connect_timeout_ms: 2_000,
            read_timeout_ms: 10_000,
            test_chunk_bytes: 10 * 1024 * 1024,
            warmup_passes: 1,
            failure_penalty_sec: 60,
            max_concurrency: 64,
            busy_retry_sec: 60,
        }
    }
}

/// A speedtest pass is never started while the proxy has served a request
/// within this window, so active Xbox downloads are not starved and
/// measurements are not skewed by concurrent transfers.
pub const ACTIVITY_GRACE: Duration = Duration::from_secs(60);

pub const AUTO_SPEEDTEST_ENV: &str = "XBOXPROXY_AUTO_SPEEDTEST";

/// Automatic hourly/warmup passes. Unset or empty defaults to on; `0` / `false`
/// / `no` / `off` disables them. Manual dashboard runs still work.
pub fn auto_from_env() -> bool {
    parse_auto_speedtest(std::env::var(AUTO_SPEEDTEST_ENV).ok().as_deref())
}

pub fn parse_auto_speedtest(raw: Option<&str>) -> bool {
    match raw {
        None => true,
        Some(v) => {
            let v = v.trim().to_ascii_lowercase();
            v.is_empty() || !matches!(v.as_str(), "0" | "false" | "no" | "off")
        }
    }
}

/// Shared handle for triggering speedtest passes on demand (dashboard button)
/// and for reporting whether a pass is currently running.
#[derive(Clone, Default)]
pub struct SpeedTestControl {
    inner: Arc<SpeedTestControlInner>,
}

#[derive(Default)]
struct SpeedTestControlInner {
    notify: Notify,
    running: AtomicBool,
    /// When `Some`, the next manual pass uses this URL and updates only the
    /// matching endpoint group. `None` tests every group with its bundled URL.
    pending_url: Mutex<Option<String>>,
}

impl SpeedTestControl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake the speedtest loop. `url` restricts the pass to the group that
    /// owns that host; `None` tests every group.
    pub fn trigger(&self, url: Option<String>) {
        *self.inner.pending_url.lock().unwrap() = url;
        self.inner.notify.notify_one();
    }

    fn take_url(&self) -> Option<String> {
        self.inner.pending_url.lock().unwrap().take()
    }

    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::SeqCst)
    }
}

pub async fn run_forever(
    cfg: SpeedTestConfig,
    selector: Selector,
    metrics: MetricsStore,
    client: ProxyClient,
    control: SpeedTestControl,
    auto: bool,
) {
    if auto {
        tracing::info!(interval_sec = cfg.interval_sec, "speedtest loop started");
        let warmup = cfg.warmup_passes.max(1);
        for i in 0..warmup {
            wait_until_idle(&cfg, &metrics).await;
            run_controlled_pass(&cfg, &selector, &metrics, &client, &control, false, None).await;
            if i + 1 < warmup {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    } else {
        tracing::info!("automatic speedtest disabled; waiting for dashboard trigger");
    }
    loop {
        let forced = if auto {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(cfg.interval_sec)) => {
                    tracing::info!("scheduled speedtest pass due");
                    false
                }
                _ = control.inner.notify.notified() => {
                    tracing::info!("manual speedtest requested (forced)");
                    true
                }
            }
        } else {
            control.inner.notify.notified().await;
            tracing::info!("manual speedtest requested (forced)");
            true
        };
        let url = if forced { control.take_url() } else { None };
        if forced {
            // Manual trigger: run immediately, even if the proxy is serving
            // downloads, and do not abort mid-pass because of traffic.
            run_controlled_pass(
                &cfg,
                &selector,
                &metrics,
                &client,
                &control,
                true,
                url.as_deref(),
            )
            .await;
        } else {
            wait_until_idle(&cfg, &metrics).await;
            run_controlled_pass(
                &cfg,
                &selector,
                &metrics,
                &client,
                &control,
                false,
                url.as_deref(),
            )
            .await;
        }
    }
}

/// Block until the proxy is not serving downloads (polling every
/// `busy_retry_sec`), so a pass never runs while a download is active.
async fn wait_until_idle(cfg: &SpeedTestConfig, metrics: &MetricsStore) {
    while metrics.is_busy(ACTIVITY_GRACE) {
        tracing::info!(
            active_requests = metrics.active_requests(),
            "proxy busy serving downloads; postponing speedtest pass"
        );
        tokio::time::sleep(Duration::from_secs(cfg.busy_retry_sec)).await;
    }
}

async fn run_controlled_pass(
    cfg: &SpeedTestConfig,
    selector: &Selector,
    metrics: &MetricsStore,
    client: &ProxyClient,
    control: &SpeedTestControl,
    forced: bool,
    url_override: Option<&str>,
) {
    control.inner.running.store(true, Ordering::SeqCst);
    run_pass(cfg, selector, metrics, client, forced, url_override).await;
    control.inner.running.store(false, Ordering::SeqCst);
}

/// Endpoint groups to test. A URL override selects the single group that
/// owns that host (e.g. `http://assets1.xboxlive.cn/Z/XXXX` → xbox-assets).
fn target_defs(
    selector: &Selector,
    url_override: Option<&str>,
) -> anyhow::Result<(Vec<HostDef>, Option<Uri>)> {
    match url_override {
        None => Ok((selector.defs(), None)),
        Some(raw) => {
            let uri: Uri = raw
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid test URL: {e}"))?;
            let host = uri
                .host()
                .ok_or_else(|| anyhow::anyhow!("test URL has no host"))?;
            let def = selector.host_for(host).cloned().ok_or_else(|| {
                anyhow::anyhow!("test URL host {host:?} is not a managed Xbox CDN domain")
            })?;
            Ok((vec![def], Some(uri)))
        }
    }
}

pub async fn run_pass(
    cfg: &SpeedTestConfig,
    selector: &Selector,
    metrics: &MetricsStore,
    client: &ProxyClient,
    forced: bool,
    url_override: Option<&str>,
) {
    let (defs, override_uri) = match target_defs(selector, url_override) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "speedtest pass skipped");
            return;
        }
    };
    let total: usize = defs.iter().map(|d| d.ips.len()).sum();
    tracing::info!(
        hosts = defs.len(),
        candidates = total,
        url = url_override.unwrap_or("bundled"),
        "running speedtest pass"
    );

    let mut tasks = Vec::new();
    for def in &defs {
        let test_url: Uri = if let Some(u) = &override_uri {
            u.clone()
        } else {
            match def.test_url.parse() {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(host = %def.name, error = %e, "invalid endpoint test_url");
                    continue;
                }
            }
        };
        for ip in &def.ips {
            let def = def.clone();
            let ip = *ip;
            let cfg = cfg.clone();
            let test_url = test_url.clone();
            let client = client.clone();
            tasks.push(async move {
                let result = measure(&cfg, &def, &test_url, ip, &client).await;
                (def, ip, result)
            });
        }
    }

    // Process in waves of `max_concurrency`, checking for live downloads
    // between waves (unless forced): if a download starts mid-pass, the
    // remaining tests are aborted so the download is not starved.
    let mut waves = stream::iter(tasks).chunks(cfg.max_concurrency.max(1));
    while let Some(wave) = waves.next().await {
        if !forced && metrics.is_busy(ACTIVITY_GRACE) {
            tracing::info!("download traffic detected; aborting speedtest pass");
            break;
        }
        let results: Vec<_> = stream::iter(wave)
            .buffer_unordered(cfg.max_concurrency.max(1))
            .collect()
            .await;
        for (def, ip, r) in results {
            selector
                .update_result(&def.name, ip, r.latency_ms, r.speed_kbps, r.success)
                .await;
            metrics.record_speedtest(SpeedTestRecord {
                ts: now_unix() as i64,
                host: def.name.clone(),
                ip: ip.to_string(),
                latency_ms: r.latency_ms,
                speed_kbps: r.speed_kbps,
                success: r.success,
            });
        }
    }
}

struct MeasureResult {
    latency_ms: Option<u64>,
    speed_kbps: Option<f64>,
    success: bool,
}

async fn measure(
    cfg: &SpeedTestConfig,
    _def: &HostDef,
    test_url: &Uri,
    ip: IpAddr,
    client: &ProxyClient,
) -> MeasureResult {
    // Mirror the reference: grab a random 10 MiB window of a 30 MiB file.
    let total_size = 30u64 * 1024 * 1024;
    let chunk = cfg.test_chunk_bytes.clamp(1, total_size);
    let max_start = total_size.saturating_sub(chunk);
    let range_from = if max_start == 0 {
        0
    } else {
        rand::random::<u64>() % (max_start + 1)
    };
    let range_to = range_from + chunk - 1;

    let req = Request::builder()
        .method(http::Method::GET)
        .uri(test_url.clone())
        .header(HOST, test_url.host().unwrap_or_default())
        .header(RANGE, format!("bytes={range_from}-{range_to}"))
        .header(
            USER_AGENT,
            format!("xboxproxy/{}", env!("CARGO_PKG_VERSION")),
        )
        .body(Body::empty())
        .expect("build speedtest request");

    let start = Instant::now();
    // A server that accepts the TCP connection but never sends response
    // headers would otherwise stall the request forever (there is no built-in
    // header timeout in the HTTP client). That would wedge the whole speedtest
    // pass, since buffer_unordered waits for every task. Cap connect + headers
    // explicitly; the body read loop below has its own timeout.
    let request_timeout = Duration::from_millis(cfg.connect_timeout_ms + cfg.read_timeout_ms);
    let (result, _chosen) =
        match tokio::time::timeout(request_timeout, request_with_ip(client, req, Some(ip))).await {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!(%ip, "speedtest request timed out waiting for response");
                return MeasureResult {
                    latency_ms: None,
                    speed_kbps: None,
                    success: false,
                };
            }
        };
    let resp = match result {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(%ip, error = %e, "speedtest connect/request failed");
            return MeasureResult {
                latency_ms: None,
                speed_kbps: None,
                success: false,
            };
        }
    };
    let latency_ms = start.elapsed().as_millis() as u64;

    if !resp.status().is_success() {
        let hdrs: Vec<String> = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|s| format!("{}: {}", k.as_str(), s)))
            .collect();
        tracing::warn!(
            %ip,
            status = %resp.status().as_u16(),
            response_headers = ?hdrs.as_slice(),
            "speedtest got non-success status"
        );
        return MeasureResult {
            latency_ms: Some(latency_ms),
            speed_kbps: None,
            success: false,
        };
    }

    let mut body = resp.into_body().into_data_stream();
    let dl_start = Instant::now();
    let mut bytes = 0u64;
    let timeout = Duration::from_millis(cfg.read_timeout_ms);
    loop {
        if bytes >= chunk {
            break;
        }
        let elapsed = dl_start.elapsed();
        if elapsed >= timeout {
            break;
        }
        let remaining = timeout - elapsed;
        match tokio::time::timeout(remaining, body.next()).await {
            Ok(Some(Ok(data))) => bytes = bytes.saturating_add(data.len() as u64).min(chunk),
            Ok(Some(Err(_))) => break,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let elapsed = dl_start.elapsed().as_secs_f64();
    let speed_kbps = if elapsed > 0.0 {
        bytes as f64 * 8.0 / 1024.0 / elapsed
    } else {
        0.0
    };
    // Match the reference implementation: an IP is usable as soon as any data
    // was downloaded (partial transfers from throttled or mid-body-dropped
    // connections still count, ranked by their speed estimate). Requiring the
    // full chunk here marks perfectly usable IPs as failed whenever the CDN
    // throttles concurrent transfers or drops a connection mid-body.
    MeasureResult {
        latency_ms: Some(latency_ms),
        speed_kbps: Some(speed_kbps),
        success: bytes > 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::UpstreamConnector;
    use crate::selector::Selector;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_config() -> SpeedTestConfig {
        SpeedTestConfig {
            interval_sec: 1,
            connect_timeout_ms: 500,
            read_timeout_ms: 500,
            test_chunk_bytes: 4,
            warmup_passes: 1,
            failure_penalty_sec: 60,
            max_concurrency: 1,
            busy_retry_sec: 60,
        }
    }

    async fn test_client() -> (ProxyClient, tokio::net::TcpListener, SocketAddr) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let selector = Selector::new(Vec::new(), Duration::from_secs(60));
        let connector = UpstreamConnector {
            selector,
            connect_timeout: Duration::from_secs(1),
        };
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(connector);
        (client, listener, addr)
    }

    #[tokio::test]
    async fn non_success_response_is_a_failed_measurement() {
        let (client, listener, addr) = test_client().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\n\r\nnope")
                .await
                .unwrap();
        });
        let url = format!("http://test.invalid:{}/file", addr.port());
        let def = HostDef {
            name: "test".into(),
            domain_map: vec![("test.invalid".into(), "test.invalid".into())],
            ips: vec![addr.ip()],
            upstream_port: addr.port(),
            test_url: url.clone(),
        };
        let cfg = test_config();
        let result = measure(&cfg, &def, &url.parse().unwrap(), addr.ip(), &client).await;
        assert!(!result.success);
        assert_eq!(result.speed_kbps, None);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn partial_body_still_counts_as_usable() {
        let (client, listener, addr) = test_client().await;
        // Serve only half of the requested chunk: the reference implementation
        // treats any downloaded bytes as a usable (ranked) candidate.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-1/1048576\r\nContent-Length: 2\r\n\r\nok",
                )
                .await
                .unwrap();
        });
        let url = format!("http://test.invalid:{}/file", addr.port());
        let def = HostDef {
            name: "test".into(),
            domain_map: vec![("test.invalid".into(), "test.invalid".into())],
            ips: vec![addr.ip()],
            upstream_port: addr.port(),
            test_url: url.clone(),
        };
        let cfg = test_config();
        let result = measure(&cfg, &def, &url.parse().unwrap(), addr.ip(), &client).await;
        assert!(result.success);
        assert!(result.speed_kbps.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn silent_server_times_out_and_fails() {
        let (client, listener, addr) = test_client().await;
        // Server accepts the connection, reads the request, and then never
        // sends a response. The measurement must fail instead of hanging.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
            let _ = &mut stream;
        });
        let url = format!("http://test.invalid:{}/file", addr.port());
        let def = HostDef {
            name: "test".into(),
            domain_map: vec![("test.invalid".into(), "test.invalid".into())],
            ips: vec![addr.ip()],
            upstream_port: addr.port(),
            test_url: url.clone(),
        };
        let cfg = test_config();
        let result = measure(&cfg, &def, &url.parse().unwrap(), addr.ip(), &client).await;
        assert!(!result.success);
        assert!(result.latency_ms.is_none());
        assert!(result.speed_kbps.is_none());
        server.abort();
    }

    #[test]
    fn parse_auto_speedtest_defaults_on() {
        assert!(parse_auto_speedtest(None));
        assert!(parse_auto_speedtest(Some("")));
        assert!(parse_auto_speedtest(Some("true")));
        assert!(!parse_auto_speedtest(Some("0")));
        assert!(!parse_auto_speedtest(Some("false")));
        assert!(!parse_auto_speedtest(Some("OFF")));
    }

    fn two_groups() -> Selector {
        Selector::new(
            vec![
                HostDef {
                    name: "xbox-assets".into(),
                    domain_map: vec![
                        ("assets1.xboxlive.cn".into(), "assets1.xboxlive.cn".into()),
                        ("assets2.xboxlive.cn".into(), "assets2.xboxlive.cn".into()),
                        ("d1.xboxlive.cn".into(), "assets1.xboxlive.cn".into()),
                        ("d2.xboxlive.cn".into(), "assets2.xboxlive.cn".into()),
                    ],
                    ips: vec!["1.1.1.1".parse().unwrap()],
                    upstream_port: 80,
                    test_url: "http://assets1.xboxlive.cn/default".into(),
                },
                HostDef {
                    name: "xbox-content".into(),
                    domain_map: vec![
                        ("dlassets.xboxlive.cn".into(), "dlassets.xboxlive.cn".into()),
                        (
                            "dlassets2.xboxlive.cn".into(),
                            "dlassets2.xboxlive.cn".into(),
                        ),
                    ],
                    ips: vec!["2.2.2.2".parse().unwrap()],
                    upstream_port: 80,
                    test_url: "http://dlassets.xboxlive.cn/default".into(),
                },
            ],
            Duration::from_secs(60),
        )
    }

    #[test]
    fn test_url_selects_only_the_matching_group() {
        let s = two_groups();
        let (defs, uri) = target_defs(&s, Some("http://assets1.xboxlive.cn/Z/XXXX")).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "xbox-assets");
        assert_eq!(uri.unwrap().path(), "/Z/XXXX");

        let (defs, _) = target_defs(&s, Some("http://dlassets2.xboxlive.cn/public/foo")).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "xbox-content");

        let (defs, uri) = target_defs(&s, None).unwrap();
        assert_eq!(defs.len(), 2);
        assert!(uri.is_none());

        assert!(target_defs(&s, Some("http://example.com/x")).is_err());
        assert!(target_defs(&s, Some("not a url")).is_err());
    }
}
