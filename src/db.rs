use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RequestRecord {
    pub ts: i64,
    pub client_ip: String,
    pub host: String,
    pub path: String,
    pub method: String,
    pub status: u16,
    pub bytes: u64,
    pub duration_ms: u64,
    pub ttfb_ms: u64,
    pub throughput_kbps: f64,
    pub upstream_ip: Option<String>,
    pub passthrough: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeedTestRecord {
    pub ts: i64,
    pub host: String,
    pub ip: String,
    pub latency_ms: Option<u64>,
    pub speed_kbps: Option<f64>,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub enum DbCommand {
    InsertRequest(RequestRecord),
    InsertSpeedTest(SpeedTestRecord),
    Cleanup(i64),
}

#[derive(Clone)]
pub struct Db {
    tx: mpsc::Sender<DbCommand>,
}

impl Db {
    /// Start the writer thread owning a single SQLite connection and return a handle.
    pub fn start(path: impl Into<PathBuf>) -> anyhow::Result<Db> {
        let path = path.into();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let (tx, rx) = mpsc::channel::<DbCommand>();
        let conn = Connection::open(&path)?;
        init(&conn)?;
        thread::Builder::new()
            .name("db-writer".to_string())
            .spawn(move || writer_loop(conn, rx))?;
        Ok(Db { tx })
    }

    pub fn tx(&self) -> mpsc::Sender<DbCommand> {
        self.tx.clone()
    }
}

fn init(conn: &Connection) -> anyhow::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.busy_timeout(std::time::Duration::from_millis(2000))?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            client_ip TEXT NOT NULL,
            host TEXT NOT NULL,
            path TEXT NOT NULL,
            method TEXT NOT NULL,
            status INTEGER NOT NULL,
            bytes INTEGER NOT NULL DEFAULT 0,
            duration_ms INTEGER NOT NULL DEFAULT 0,
            ttfb_ms INTEGER NOT NULL DEFAULT 0,
            throughput_kbps REAL NOT NULL DEFAULT 0,
            upstream_ip TEXT,
            passthrough INTEGER NOT NULL DEFAULT 0,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts);
        CREATE TABLE IF NOT EXISTS speedtests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            host TEXT NOT NULL,
            ip TEXT NOT NULL,
            latency_ms INTEGER,
            speed_kbps REAL,
            success INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_speedtests_ts ON speedtests(ts);
        "#,
    )?;
    Ok(())
}

fn writer_loop(conn: Connection, rx: mpsc::Receiver<DbCommand>) {
    let mut insert_req = conn
        .prepare(
            "INSERT INTO requests (ts, client_ip, host, path, method, status, bytes, duration_ms, ttfb_ms, throughput_kbps, upstream_ip, passthrough, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        )
        .expect("prepare insert request");
    let mut insert_st = conn
        .prepare(
            "INSERT INTO speedtests (ts, host, ip, latency_ms, speed_kbps, success)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .expect("prepare insert speedtest");
    let mut del_req = conn
        .prepare("DELETE FROM requests WHERE ts < ?1")
        .expect("prepare delete requests");
    let mut del_st = conn
        .prepare("DELETE FROM speedtests WHERE ts < ?1")
        .expect("prepare delete speedtests");

    while let Ok(cmd) = rx.recv() {
        let res = match cmd {
            DbCommand::InsertRequest(r) => insert_req.execute(rusqlite::params![
                r.ts,
                r.client_ip,
                r.host,
                r.path,
                r.method,
                i64::from(r.status),
                r.bytes as i64,
                r.duration_ms as i64,
                r.ttfb_ms as i64,
                r.throughput_kbps,
                r.upstream_ip,
                r.passthrough as i64,
                r.error
            ]),
            DbCommand::InsertSpeedTest(s) => insert_st.execute(rusqlite::params![
                s.ts,
                s.host,
                s.ip,
                s.latency_ms.map(|v| v as i64),
                s.speed_kbps,
                s.success as i64
            ]),
            DbCommand::Cleanup(cutoff) => {
                let a = del_req.execute([cutoff]);
                let b = del_st.execute([cutoff]);
                match (a, b) {
                    (Ok(a), Ok(b)) => {
                        tracing::info!(
                            deleted_requests = a,
                            deleted_speedtests = b,
                            "retention cleanup done"
                        );
                        Ok(a)
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                }
            }
        };
        if let Err(e) = res {
            tracing::warn!(error = %e, "failed to persist metric");
        }
    }
}

// ---- Read helpers (used by the dashboard API) ----

#[derive(Debug, Default, Serialize)]
pub struct TimeWindowStats {
    pub requests: i64,
    pub throughput_kbps: f64,
    pub ttfb_ms: f64,
    pub errors: i64,
    pub bytes: i64,
}

pub fn stats_since(conn: &Connection, since: i64) -> anyhow::Result<TimeWindowStats> {
    let mut st = conn.prepare(
        "SELECT COUNT(*), COALESCE(AVG(throughput_kbps),0), COALESCE(AVG(ttfb_ms),0),
                COALESCE(SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END),0), COALESCE(SUM(bytes),0)
         FROM requests WHERE ts >= ?1",
    )?;
    let row = st.query_row([since], |r| {
        Ok(TimeWindowStats {
            requests: r.get(0)?,
            throughput_kbps: r.get(1)?,
            ttfb_ms: r.get(2)?,
            errors: r.get(3)?,
            bytes: r.get(4)?,
        })
    })?;
    Ok(row)
}

#[derive(Debug, Serialize)]
pub struct BucketStat {
    pub ts: i64,
    pub requests: i64,
    pub throughput_kbps: f64,
    pub ttfb_ms: f64,
    pub errors: i64,
}

pub fn series_since(conn: &Connection, since: i64) -> anyhow::Result<Vec<BucketStat>> {
    let mut st = conn.prepare(
        "SELECT (ts / 60) * 60 AS bucket, COUNT(*), COALESCE(AVG(throughput_kbps),0),
                COALESCE(AVG(ttfb_ms),0), COALESCE(SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END),0)
         FROM requests WHERE ts >= ?1 GROUP BY bucket ORDER BY bucket",
    )?;
    let rows = st.query_map([since], |r| {
        Ok(BucketStat {
            ts: r.get(0)?,
            requests: r.get(1)?,
            throughput_kbps: r.get(2)?,
            ttfb_ms: r.get(3)?,
            errors: r.get(4)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

#[derive(Debug, Serialize)]
pub struct StatusCount {
    pub status: i64,
    pub count: i64,
}

pub fn status_counts(conn: &Connection, since: i64) -> anyhow::Result<Vec<StatusCount>> {
    let mut st = conn.prepare(
        "SELECT status, COUNT(*) AS cnt FROM requests WHERE ts >= ?1 GROUP BY status ORDER BY cnt DESC",
    )?;
    let rows = st.query_map([since], |r| {
        Ok(StatusCount {
            status: r.get(0)?,
            count: r.get(1)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn recent_requests(
    conn: &Connection,
    limit: i64,
    since: Option<i64>,
) -> anyhow::Result<Vec<RequestRecord>> {
    let limit = limit.clamp(0, 500);
    fn row_to_request(r: &rusqlite::Row<'_>) -> rusqlite::Result<RequestRecord> {
        Ok(RequestRecord {
            ts: r.get(0)?,
            client_ip: r.get(1)?,
            host: r.get(2)?,
            path: r.get(3)?,
            method: r.get(4)?,
            status: r.get::<_, i64>(5)? as u16,
            bytes: r.get::<_, i64>(6)? as u64,
            duration_ms: r.get::<_, i64>(7)? as u64,
            ttfb_ms: r.get::<_, i64>(8)? as u64,
            throughput_kbps: r.get(9)?,
            upstream_ip: r.get(10)?,
            passthrough: r.get::<_, i64>(11)? != 0,
            error: r.get(12)?,
        })
    }
    const SELECT: &str =
        "SELECT ts, client_ip, host, path, method, status, bytes, duration_ms, ttfb_ms,
                    throughput_kbps, upstream_ip, passthrough, error FROM requests";
    let sql = if since.is_some() {
        format!("{SELECT} WHERE ts >= ?1 ORDER BY id DESC LIMIT ?2")
    } else {
        format!("{SELECT} ORDER BY id DESC LIMIT ?1")
    };
    let mut st = conn.prepare(&sql)?;
    let rows = if let Some(s) = since {
        st.query_map(rusqlite::params![s, limit], row_to_request)?
    } else {
        st.query_map([limit], row_to_request)?
    };
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn recent_speedtests(conn: &Connection, limit: i64) -> anyhow::Result<Vec<SpeedTestRecord>> {
    let limit = limit.clamp(0, 500);
    let mut st = conn.prepare(
        "SELECT ts, host, ip, latency_ms, speed_kbps, success FROM speedtests ORDER BY id DESC LIMIT ?1",
    )?;
    let rows = st.query_map([limit], |r| {
        Ok(SpeedTestRecord {
            ts: r.get(0)?,
            host: r.get(1)?,
            ip: r.get(2)?,
            latency_ms: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
            speed_kbps: r.get(4)?,
            success: r.get::<_, i64>(5)? != 0,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}
