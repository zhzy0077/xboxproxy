use std::net::IpAddr;
use std::time::{Duration, Instant};

use axum::body::Body;
use futures::{stream, StreamExt};
use http::header::{HOST, RANGE, USER_AGENT};
use http_body_util::BodyExt;
use hyper::{Request, Uri};

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
}

impl Default for SpeedTestConfig {
    fn default() -> Self {
        Self {
            interval_sec: 300,
            connect_timeout_ms: 2_000,
            read_timeout_ms: 10_000,
            test_chunk_bytes: 10 * 1024 * 1024,
            warmup_passes: 1,
            failure_penalty_sec: 60,
            max_concurrency: 64,
        }
    }
}

pub async fn run_forever(
    cfg: SpeedTestConfig,
    selector: Selector,
    metrics: MetricsStore,
    client: ProxyClient,
) {
    tracing::info!(interval_sec = cfg.interval_sec, "speedtest loop started");
    let warmup = cfg.warmup_passes.max(1);
    for i in 0..warmup {
        run_pass(&cfg, &selector, &metrics, &client).await;
        if i + 1 < warmup {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    loop {
        tokio::time::sleep(Duration::from_secs(cfg.interval_sec)).await;
        run_pass(&cfg, &selector, &metrics, &client).await;
    }
}

pub async fn run_pass(
    cfg: &SpeedTestConfig,
    selector: &Selector,
    metrics: &MetricsStore,
    client: &ProxyClient,
) {
    let defs = selector.defs();
    let total: usize = defs.iter().map(|d| d.ips.len()).sum();
    tracing::info!(
        hosts = defs.len(),
        candidates = total,
        "running speedtest pass"
    );

    let mut tasks = Vec::new();
    for def in &defs {
        let test_url: Uri = match def.test_url.parse() {
            Ok(u) => u,
            Err(e) => {
                tracing::error!(host = %def.name, error = %e, "invalid endpoint test_url");
                continue;
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

    let results: Vec<_> = stream::iter(tasks)
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
    let (result, _chosen) = request_with_ip(client, req, Some(ip)).await;
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
    let mut body_failed = false;
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
            Ok(Some(Err(_))) => {
                body_failed = true;
                break;
            }
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
    MeasureResult {
        latency_ms: Some(latency_ms),
        speed_kbps: Some(speed_kbps),
        success: bytes == chunk && !body_failed,
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
}
