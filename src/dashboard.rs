use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::db;
use crate::proxy::AppState;
use crate::selector::now_unix;

const DASHBOARD_HTML: &str = include_str!("../templates/dashboard.html");

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/manage", get(index))
        .route("/manage/", get(index))
        .route("/manage/api/summary", get(summary))
        .route("/manage/api/requests", get(requests))
        .route("/manage/api/speedtests", get(speedtests))
        .route("/manage/api/speedtests/run", post(run_speedtest))
        .route("/manage/api/hosts", get(hosts))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

fn open_db(state: &AppState) -> anyhow::Result<rusqlite::Connection> {
    rusqlite::Connection::open(&state.db_path).map_err(anyhow::Error::from)
}

fn to_500(e: anyhow::Error) -> Response {
    tracing::error!(error = %e, "api error");
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
    since: Option<i64>,
}

async fn summary(State(state): State<AppState>) -> Response {
    let now = now_unix() as i64;
    let conn = match open_db(&state) {
        Ok(c) => c,
        Err(e) => return to_500(e),
    };
    let hour = db::stats_since(&conn, now - 3600, 3600);
    let day = db::stats_since(&conn, now - 86400, 86400);
    let series = db::series_since(&conn, now - 3600, 60);
    let status = db::status_counts(&conn, now - 86400);
    let hosts = state.selector.snapshot().await;
    let recent = db::recent_requests(&conn, 50, Some(now - 3600));
    let recent_st = db::recent_speedtests(&conn, 20);
    match (hour, day, series, status, recent, recent_st) {
        (Ok(hour), Ok(day), Ok(series), Ok(status), Ok(recent), Ok(recent_st)) => Json(json!({
            "now": now,
            "last_hour": hour,
            "last_24h": day,
            "series": series,
            "status_counts": status,
            "hosts": hosts,
            "recent": recent,
            "recent_speedtests": recent_st,
            "speedtest_running": state.speedtest.is_running(),
            "pinned": state.pinned,
            "speedtest_disabled": state.pinned,
            "auto_speedtest": state.auto_speedtest,
        }))
        .into_response(),
        _ => to_500(anyhow::anyhow!("query failed")),
    }
}

#[derive(Deserialize, Default)]
struct RunSpeedtestBody {
    #[serde(default)]
    url: Option<String>,
}

async fn run_speedtest(
    State(state): State<AppState>,
    Json(body): Json<RunSpeedtestBody>,
) -> Response {
    if state.pinned {
        return Json(json!({ "started": false, "reason": "pinned" })).into_response();
    }
    if state.speedtest.is_running() {
        return Json(json!({ "started": false, "reason": "already_running" })).into_response();
    }
    let url = body
        .url
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(raw) = url.as_deref() {
        let uri: axum::http::Uri = match raw.parse() {
            Ok(u) => u,
            Err(_) => {
                return Json(json!({ "started": false, "reason": "invalid_url" })).into_response();
            }
        };
        let host = uri.host().unwrap_or("");
        let Some(def) = state.selector.host_for(host) else {
            return Json(json!({ "started": false, "reason": "unknown_host" })).into_response();
        };
        tracing::info!(url = raw, group = %def.name, "manual speedtest requested via dashboard");
    } else {
        tracing::info!("manual speedtest requested via dashboard (all groups)");
    }
    state.speedtest.trigger(url);
    Json(json!({ "started": true, "reason": null })).into_response()
}

async fn requests(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let conn = match open_db(&state) {
        Ok(c) => c,
        Err(e) => return to_500(e),
    };
    match db::recent_requests(&conn, limit, q.since) {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => to_500(e),
    }
}

async fn speedtests(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let conn = match open_db(&state) {
        Ok(c) => c,
        Err(e) => return to_500(e),
    };
    match db::recent_speedtests(&conn, limit) {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => to_500(e),
    }
}

async fn hosts(State(state): State<AppState>) -> Response {
    let snapshot = state.selector.snapshot().await;
    Json(snapshot).into_response()
}
