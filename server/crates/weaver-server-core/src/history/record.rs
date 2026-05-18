#[derive(Debug, Clone)]
pub struct JobHistoryRow {
    pub job_id: u64,
    pub job_hash: Option<Vec<u8>>,
    pub name: String,
    pub status: String,
    pub error_message: Option<String>,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub health: u32,
    pub category: Option<String>,
    pub output_dir: Option<String>,
    pub nzb_path: Option<String>,
    pub created_at: i64,
    pub completed_at: i64,
    pub metadata: Option<String>,
    pub last_diagnostic_id: Option<String>,
    pub last_diagnostic_uploaded_at_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IntegrationEventRow {
    pub id: i64,
    pub timestamp: i64,
    pub kind: String,
    pub item_id: Option<u64>,
    pub payload_json: String,
}
