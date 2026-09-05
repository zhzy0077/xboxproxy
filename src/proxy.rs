use std::cell::Cell;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::State;
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use futures::Stream;
use http::header::{HeaderMap, HeaderName, HeaderValue, HOST};
use http::{Method, StatusCode, Uri, Version};
use http_body_util::BodyExt;
use hyper_util::client::legacy::{Client, Error as LegacyClientError};
use hyper_util::rt::TokioIo;
use tower::Service;

use crate::db::RequestRecord;
use crate::metrics::MetricsStore;
use crate::selector::{now_unix, Selector, CURRENT_IP};
use crate::speedtest::SpeedTestControl;

pub type ProxyClient = Client<UpstreamConnector, Body>;

/// Maximum time to wait for the upstream to send response headers after the
/// connection is established. Some CDN IPs accept TCP connections and then
/// never respond; without this cap a single dead IP would hang proxy requests
/// (and the speedtest pass) indefinitely.
const UPSTREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(30);

async fn request_upstream(
    client: &ProxyClient,
    req: axum::http::Request<Body>,
    pre: Option<IpAddr>,
) -> Result<(Response<hyper::body::Incoming>, Option<IpAddr>), String> {
    let (result, chosen) =
        tokio::time::timeout(UPSTREAM_HEADER_TIMEOUT, request_with_ip(client, req, pre))
            .await
            .map_err(|_| "upstream response header timeout".to_string())?;
    result.map(|r| (r, chosen)).map_err(|e| e.to_string())
}

#[derive(Clone)]
pub struct AppState {
    pub selector: Selector,
    pub metrics: MetricsStore,
    pub client: ProxyClient,
    pub db_path: PathBuf,
    pub speedtest: SpeedTestControl,
    /// When true, candidate IPs were pinned via env and speedtest is off.
    pub pinned: bool,
    /// When false, the hourly/warmup loop is off; dashboard URL runs still work.
    pub auto_speedtest: bool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", any(proxy_handler))
        .route("/{*path}", any(proxy_handler))
        .with_state(state)
}

/// Opens a TCP connection to the chosen upstream IP while keeping the original
/// Host header on the request.
#[derive(Clone)]
pub struct UpstreamConnector {
    pub selector: Selector,
    pub connect_timeout: std::time::Duration,
}

impl Service<Uri> for UpstreamConnector {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let selector = self.selector.clone();
        let timeout = self.connect_timeout;
        Box::pin(async move {
            let host = uri.host().unwrap_or_default().to_string();
            let port = selector
                .upstream_port(&host)
                .unwrap_or_else(|| uri.port_u16().unwrap_or(80));
            let addr = selector.resolve(&host, port).await?;
            let stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "upstream connect timeout")
                })??;
            stream.set_nodelay(true).ok();
            CURRENT_IP.try_with(|c| c.set(Some(addr.ip()))).ok();
            Ok(TokioIo::new(stream))
        })
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_tokens(headers: &HeaderMap) -> HashSet<String> {
    headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect()
}

fn should_strip_header(name: &HeaderName, connection: &HashSet<String>) -> bool {
    is_hop_by_hop(name.as_str()) || connection.contains(name.as_str())
}

fn host_name_from_header(host_header: &str) -> Option<String> {
    let authority: Uri = format!("http://{host_header}/").parse().ok()?;
    if authority.path() != "/" || authority.query().is_some() {
        return None;
    }
    if authority.authority()?.as_str().contains('@') {
        return None;
    }
    let host = authority
        .host()?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.');
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

/// Send `req` to the upstream, overriding the resolved IP with `pre` (when
/// `Some`). Returns the upstream response plus the IP that was actually used
/// (the connector records the selected address for metrics).
pub(crate) async fn request_with_ip(
    client: &ProxyClient,
    req: axum::http::Request<Body>,
    pre: Option<IpAddr>,
) -> (
    Result<Response<hyper::body::Incoming>, LegacyClientError>,
    Option<IpAddr>,
) {
    CURRENT_IP
        .scope(Cell::new(pre), async move {
            let result = client.request(req).await;
            let chosen = CURRENT_IP.with(|c| c.get());
            (result, chosen)
        })
        .await
}

#[derive(Debug, Clone)]
pub struct RequestMeta {
    pub client_ip: String,
    pub host: String,
    pub path: String,
    pub method: String,
    pub status: u16,
    pub upstream_ip: Option<String>,
    pub passthrough: bool,
    pub ttfb_ms: u64,
}

pub struct RecordState {
    pub metrics: MetricsStore,
    pub meta: RequestMeta,
    pub started: Instant,
    pub bytes: AtomicU64,
    pub recorded: AtomicBool,
    pub error: Mutex<Option<String>>,
}

pub fn finalize(state: &RecordState) {
    if state.recorded.swap(true, Ordering::SeqCst) {
        return;
    }
    let bytes = state.bytes.load(Ordering::Relaxed);
    let duration = state.started.elapsed();
    let duration_ms = duration.as_millis() as u64;
    let speed_kbps = if duration.as_secs_f64() > 0.0 {
        bytes as f64 * 8.0 / 1024.0 / duration.as_secs_f64()
    } else {
        0.0
    };
    let error = state.error.lock().unwrap().clone();
    let rec = RequestRecord {
        ts: now_unix() as i64,
        client_ip: state.meta.client_ip.clone(),
        host: state.meta.host.clone(),
        path: state.meta.path.clone(),
        method: state.meta.method.clone(),
        status: state.meta.status,
        bytes,
        duration_ms,
        ttfb_ms: state.meta.ttfb_ms,
        throughput_kbps: speed_kbps,
        upstream_ip: state.meta.upstream_ip.clone(),
        passthrough: state.meta.passthrough,
        error,
    };
    state.metrics.record_request(rec);
}

pub struct CountingStream<S> {
    pub inner: S,
    pub state: Arc<RecordState>,
}

impl<S, E> Stream for CountingStream<S>
where
    S: Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    type Item = Result<bytes::Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.state
                    .bytes
                    .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.state.error.lock().unwrap() = Some(e.to_string());
                finalize(&this.state);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                finalize(&this.state);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for CountingStream<S> {
    fn drop(&mut self) {
        finalize(&self.state);
    }
}

fn bad_request(msg: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(msg.to_string()))
        .unwrap()
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from("host is not managed by xboxproxy"))
        .unwrap()
}

fn service_unavailable(msg: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(msg))
        .unwrap()
}

fn record_and_unavailable(state: &RecordState, msg: String) -> Response<Body> {
    finalize(state);
    tracing::warn!(host = %state.meta.host, "{msg}");
    service_unavailable(msg)
}

pub async fn proxy_handler(
    State(state): State<AppState>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
) -> Response<Body> {
    let started = Instant::now();
    let client_ip = client_addr.ip().to_string();

    let host_header = match req.headers().get(HOST).and_then(|v| v.to_str().ok()) {
        Some(h) => h.trim().to_string(),
        None => return bad_request("missing Host header"),
    };
    let host_name = match host_name_from_header(&host_header) {
        Some(host) => host,
        None => return bad_request("invalid Host header"),
    };

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let method = req.method().clone();
    let is_rewritten = state.selector.host_for(&host_name).is_some();
    if !is_rewritten {
        return not_found();
    }
    let _activity = state.metrics.begin_request();
    let upstream_host = state
        .selector
        .upstream_host_for(&host_name)
        .unwrap_or(&host_name)
        .to_string();

    let uri_str = format!("http://{upstream_host}{path_and_query}");
    let uri: Uri = match uri_str.parse() {
        Ok(u) => u,
        Err(_) => return bad_request("invalid request URI"),
    };

    // Forward client headers (hop-by-hop stripped, Host handled by hyper).
    let mut fwd = HeaderMap::new();
    let request_connection = connection_tokens(req.headers());
    for (k, v) in req.headers() {
        if should_strip_header(k, &request_connection)
            || k == HOST
            || ((method == Method::GET || method == Method::HEAD)
                && (k == http::header::CONTENT_LENGTH || k == http::header::TRANSFER_ENCODING))
        {
            continue;
        }
        fwd.insert(k, v.clone());
    }
    let fwd_client_ip =
        HeaderValue::from_str(&client_ip).unwrap_or_else(|_| HeaderValue::from_static(""));
    fwd.insert(
        HeaderName::from_static("x-forwarded-for"),
        fwd_client_ip.clone(),
    );
    fwd.insert(HeaderName::from_static("x-real-ip"), fwd_client_ip);
    let forwarded_headers: Vec<String> = fwd
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| format!("{}: {}", k.as_str(), s)))
        .collect();
    let forwarded_headers = std::sync::Arc::new(forwarded_headers);
    let fwd = std::sync::Arc::new(fwd);

    tracing::debug!(
        %host_name, %method, uri = %uri,
        upstream_headers = ?forwarded_headers.as_slice(),
        "forwarded request ready for upstream"
    );

    let build_req = |body: Body| {
        let mut up_req = axum::http::Request::builder()
            .method(method.clone())
            .uri(uri.clone())
            .version(Version::HTTP_11)
            .body(body)
            .map_err(|e| e.to_string())?;
        *up_req.headers_mut() = (*fwd).clone();
        Ok::<_, String>(up_req)
    };

    let retryable = matches!(method, Method::GET | Method::HEAD);
    let max_attempts: u8 = if retryable { 3 } else { 1 };

    let mut resp: Option<Response<hyper::body::Incoming>> = None;
    let mut ttfb_ms: u64 = 0;
    let mut upstream_ip: Option<IpAddr> = None;
    let mut upstream_status: Option<u16> = None;
    let mut last_err: Option<String> = None;
    let mut attempts: u8 = 0;

    if retryable {
        for attempt in 0..max_attempts {
            attempts = attempt + 1;
            let ip = state.selector.best_ip(&host_name).await;
            let up_req = match build_req(Body::empty()) {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(e);
                    tracing::warn!(
                        %host_name, %method, attempt, requested_ip = ?ip,
                        error = last_err.as_deref().unwrap_or(""),
                        "upstream request build failed"
                    );
                    break;
                }
            };
            let t0 = Instant::now();
            match request_upstream(&state.client, up_req, ip).await {
                Ok((r, chosen)) => {
                    let up_status = r.status().as_u16();
                    let response_headers: Vec<String> = r
                        .headers()
                        .iter()
                        .filter_map(|(k, v)| {
                            v.to_str().ok().map(|s| format!("{}: {}", k.as_str(), s))
                        })
                        .collect();
                    ttfb_ms = t0.elapsed().as_millis() as u64;
                    upstream_ip = chosen;
                    upstream_status = Some(up_status);
                    tracing::debug!(
                        %host_name, %method, attempt, requested_ip = ?ip,
                        upstream_ip = ?chosen, upstream_status = up_status,
                        response_headers = ?response_headers.as_slice(),
                        "upstream responded"
                    );
                    resp = Some(r);
                    break;
                }
                Err(e) => {
                    let err = e;
                    last_err = Some(err.clone());
                    tracing::debug!(
                        %host_name, %method, attempt, requested_ip = ?ip,
                        error = %err, "upstream attempt failed"
                    );
                    if let Some(ip) = ip {
                        state.selector.mark_failure(&host_name, ip).await;
                    }
                }
            }
        }
    } else {
        attempts = 1;
        let ip = state.selector.best_ip(&host_name).await;
        let body = if matches!(method, Method::GET | Method::HEAD) {
            Body::empty()
        } else {
            Body::from_stream(req.into_body().into_data_stream())
        };
        let up_req = match build_req(body) {
            Ok(r) => r,
            Err(e) => {
                let msg = format!("upstream request build failed: {e}");
                tracing::warn!(
                    %host_name, %method, requested_ip = ?ip, error = %msg, "upstream request build failed"
                );
                return service_unavailable(msg);
            }
        };
        let t0 = Instant::now();
        match request_upstream(&state.client, up_req, ip).await {
            Ok((r, chosen)) => {
                let up_status = r.status().as_u16();
                let response_headers: Vec<String> = r
                    .headers()
                    .iter()
                    .filter_map(|(k, v)| v.to_str().ok().map(|s| format!("{}: {}", k.as_str(), s)))
                    .collect();
                ttfb_ms = t0.elapsed().as_millis() as u64;
                upstream_ip = chosen;
                upstream_status = Some(up_status);
                tracing::debug!(
                    %host_name, %method, requested_ip = ?ip, upstream_ip = ?chosen,
                    upstream_status = up_status,
                    response_headers = ?response_headers.as_slice(),
                    "upstream responded"
                );
                resp = Some(r);
            }
            Err(e) => {
                let err = e;
                last_err = Some(err.clone());
                tracing::debug!(
                    %host_name, %method, requested_ip = ?ip,
                    error = %err, "upstream attempt failed"
                );
                if let Some(ip) = ip {
                    state.selector.mark_failure(&host_name, ip).await;
                }
            }
        }
    }

    let reply_status: u16 = upstream_status.unwrap_or(502);
    let status: u16 = reply_status;
    tracing::info!(
        %method, %host_name, path = %path_and_query,
        attempts, upstream_ip = ?upstream_ip, upstream_status = ?upstream_status,
        reply_status, ttfb_ms,
        "proxy request completed"
    );

    let meta = RequestMeta {
        client_ip,
        host: host_name,
        path: path_and_query,
        method: method.as_str().to_string(),
        status,
        upstream_ip: upstream_ip.map(|i| i.to_string()),
        passthrough: false,
        ttfb_ms,
    };

    let record_state = Arc::new(RecordState {
        metrics: state.metrics.clone(),
        meta,
        started,
        bytes: AtomicU64::new(0),
        recorded: AtomicBool::new(false),
        error: Mutex::new(last_err.clone()),
    });

    match resp {
        Some(r) => {
            let mut resp_builder = Response::builder()
                .status(r.status())
                .version(Version::HTTP_11);
            let response_connection = connection_tokens(r.headers());
            for (k, v) in r.headers() {
                if should_strip_header(k, &response_connection) {
                    continue;
                }
                resp_builder = resp_builder.header(k, v);
            }
            let stream = CountingStream {
                inner: r.into_body().into_data_stream(),
                state: record_state.clone(),
            };
            resp_builder
                .body(Body::from_stream(stream))
                .unwrap_or_else(|e| service_unavailable(format!("response build failed: {e}")))
        }
        None => {
            let msg = match last_err {
                Some(e) => format!("upstream unreachable: {e}"),
                None => "upstream unreachable".to_string(),
            };
            record_and_unavailable(&record_state, msg)
        }
    }
}

impl fmt::Debug for UpstreamConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamConnector").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv6_and_port_in_host_header() {
        assert_eq!(
            host_name_from_header("[2001:db8::1]:8080"),
            Some("2001:db8::1".to_string())
        );
        assert_eq!(
            host_name_from_header("CDN.Example.com."),
            Some("cdn.example.com".to_string())
        );
        assert!(host_name_from_header("not a host").is_none());
        assert!(host_name_from_header("user@cdn.example.com").is_none());
        assert!(host_name_from_header("cdn.example.com/path").is_none());
    }

    #[test]
    fn strips_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", HeaderValue::from_static("keep-alive, x-test"));
        headers.insert("x-test", HeaderValue::from_static("private"));
        let tokens = connection_tokens(&headers);
        assert!(should_strip_header(
            &HeaderName::from_static("x-test"),
            &tokens
        ));
        assert!(should_strip_header(
            &HeaderName::from_static("connection"),
            &tokens
        ));
        assert!(!should_strip_header(
            &HeaderName::from_static("x-safe"),
            &tokens
        ));
    }

    #[tokio::test]
    async fn rejects_unmanaged_hosts_without_dns_resolution() {
        let selector = Selector::new(Vec::new(), std::time::Duration::from_secs(60));
        let connector = UpstreamConnector {
            selector: selector.clone(),
            connect_timeout: std::time::Duration::from_secs(1),
        };
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(connector);
        let (tx, _rx) = std::sync::mpsc::channel();
        let state = AppState {
            selector,
            metrics: MetricsStore::new(tx),
            client,
            db_path: std::path::PathBuf::from("/tmp/xboxproxy-test.db"),
            speedtest: SpeedTestControl::new(),
            pinned: false,
            auto_speedtest: true,
        };
        let req = axum::http::Request::builder()
            .uri("/")
            .header(HOST, "unmanaged.example")
            .body(Body::empty())
            .unwrap();

        let response = proxy_handler(
            State(state),
            ConnectInfo("127.0.0.1:12345".parse().unwrap()),
            req,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
