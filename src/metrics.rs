use std::sync::mpsc::Sender;

use crate::db::{DbCommand, RequestRecord, SpeedTestRecord};

/// Metrics sink: pushes records to the SQLite writer thread.
#[derive(Clone)]
pub struct MetricsStore {
    db_tx: Sender<DbCommand>,
}

impl MetricsStore {
    pub fn new(db_tx: Sender<DbCommand>) -> Self {
        MetricsStore { db_tx }
    }

    pub fn record_request(&self, rec: RequestRecord) {
        let _ = self.db_tx.send(DbCommand::InsertRequest(rec));
    }

    pub fn record_speedtest(&self, rec: SpeedTestRecord) {
        let _ = self.db_tx.send(DbCommand::InsertSpeedTest(rec));
    }
}
