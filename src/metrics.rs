use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use crate::db::{DbCommand, RequestRecord, SpeedTestRecord};
use crate::selector::now_unix;

/// Metrics sink: pushes records to the SQLite writer thread and tracks live
/// proxy activity so the speedtester stays out of the way of downloads.
#[derive(Clone)]
pub struct MetricsStore {
    db_tx: Sender<DbCommand>,
    active_requests: Arc<AtomicU64>,
    last_activity: Arc<AtomicU64>,
}

impl MetricsStore {
    pub fn new(db_tx: Sender<DbCommand>) -> Self {
        MetricsStore {
            db_tx,
            active_requests: Arc::new(AtomicU64::new(0)),
            last_activity: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Mark a managed proxy request as in-flight. The returned guard
    /// decrements the counter and stamps the activity time when the request
    /// completes, including on early returns.
    pub fn begin_request(&self) -> ActivityGuard {
        self.active_requests.fetch_add(1, Ordering::SeqCst);
        self.last_activity.store(now_unix(), Ordering::SeqCst);
        ActivityGuard {
            metrics: self.clone(),
        }
    }

    pub fn active_requests(&self) -> u64 {
        self.active_requests.load(Ordering::SeqCst)
    }

    pub fn last_activity(&self) -> u64 {
        self.last_activity.load(Ordering::SeqCst)
    }

    /// True while a request is in flight or one finished within `grace`.
    /// Speedtests should not run during this window so active downloads are
    /// not starved and measurements are not skewed by concurrent transfers.
    pub fn is_busy(&self, grace: Duration) -> bool {
        self.active_requests() > 0
            || now_unix().saturating_sub(self.last_activity()) < grace.as_secs()
    }

    pub fn record_request(&self, rec: RequestRecord) {
        let _ = self.db_tx.send(DbCommand::InsertRequest(rec));
    }

    pub fn record_speedtest(&self, rec: SpeedTestRecord) {
        let _ = self.db_tx.send(DbCommand::InsertSpeedTest(rec));
    }
}

/// Decrements the in-flight request counter when dropped.
pub struct ActivityGuard {
    metrics: MetricsStore,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.metrics.active_requests.fetch_sub(1, Ordering::SeqCst);
        self.metrics
            .last_activity
            .store(now_unix(), Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_tracks_inflight_requests_and_grace_window() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let m = MetricsStore::new(tx);
        assert!(!m.is_busy(Duration::from_secs(60)));

        let guard = m.begin_request();
        assert!(m.is_busy(Duration::from_secs(60)));
        assert_eq!(m.active_requests(), 1);

        drop(guard);
        assert_eq!(m.active_requests(), 0);
        // Still busy for the grace window after the request ends...
        assert!(m.is_busy(Duration::from_secs(60)));
        // ...but not with zero grace.
        assert!(!m.is_busy(Duration::ZERO));
    }
}
