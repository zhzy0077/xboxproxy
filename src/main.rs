mod dashboard;
mod db;
mod endpoints;
mod metrics;
mod proxy;
mod selector;
mod speedtest;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use tracing_subscriber::EnvFilter;

use crate::metrics::MetricsStore;
use crate::proxy::{router as proxy_router, AppState, UpstreamConnector};
use crate::selector::Selector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let loaded = endpoints::load()?;
    let host_defs = loaded.hosts;
    tracing::info!(
        hosts = host_defs.len(),
        candidates = host_defs.iter().map(|d| d.ips.len()).sum::<usize>(),
        pinned = loaded.pinned,
        "loaded host definitions"
    );
    if loaded.pinned {
        for def in &host_defs {
            tracing::info!(
                endpoint = %def.name,
                ips = ?def.ips,
                "using pinned CDN IPs; speedtest disabled"
            );
        }
    }

    // SQLite writer thread + handle
    let db_path = PathBuf::from("data/xboxproxy.db");
    let db = db::Db::start(db_path.clone())?;
    let db_tx = db.tx();

    // Retention cleanup job.
    {
        let db_tx = db_tx.clone();
        let retention_days = 7_u32;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3600));
            loop {
                ticker.tick().await;
                let cutoff = selector::now_unix() as i64 - (retention_days as i64) * 86400;
                if db_tx.send(db::DbCommand::Cleanup(cutoff)).is_err() {
                    break;
                }
            }
        });
    }

    // In-memory + DB metrics sink.
    let metrics = MetricsStore::new(db_tx);

    // Best-CDN selector.
    let speedtest_cfg = speedtest::SpeedTestConfig::default();
    let selector = Selector::new(
        host_defs,
        Duration::from_secs(speedtest_cfg.failure_penalty_sec),
    );

    // Upstream client that connects to selected CDN IPs while preserving Host.
    let connector = UpstreamConnector {
        selector: selector.clone(),
        connect_timeout: Duration::from_millis(speedtest_cfg.connect_timeout_ms),
    };
    // Connections must not be pooled: the pool is keyed by host, so a pooled
    // connection would keep serving the first IP even after the selector picks
    // a faster candidate. Open a fresh connection per request instead (the
    // reference XboxDownload client disables pooling for the same reason).
    let client: proxy::ProxyClient =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_max_idle_per_host(0)
            .build(connector);

    // Periodic upstream speed/latency measurement. The control handle is also
    // used by the dashboard "Run now" button. Pinning IPs fully disables this.
    // XBOXPROXY_AUTO_SPEEDTEST=false skips warmup/hourly passes but still
    // accepts a dashboard URL trigger for a single group.
    let speedtest_control = speedtest::SpeedTestControl::new();
    let auto_speedtest = speedtest::auto_from_env();
    if loaded.pinned {
        tracing::info!("speedtest disabled because CDN IPs are pinned");
    } else {
        tokio::spawn(speedtest::run_forever(
            speedtest_cfg,
            selector.clone(),
            metrics.clone(),
            client.clone(),
            speedtest_control.clone(),
            auto_speedtest,
        ));
    }

    let app_state = AppState {
        selector,
        metrics,
        client,
        db_path,
        speedtest: speedtest_control,
        pinned: loaded.pinned,
        auto_speedtest: auto_speedtest && !loaded.pinned,
    };

    let bind_addr: SocketAddr = "0.0.0.0:80".parse()?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let app: Router = proxy_router(app_state.clone()).merge(dashboard::router(app_state));
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    );
    tracing::info!("proxy and dashboard listening on http://{bind_addr}");
    serve.await?;
    Ok(())
}
