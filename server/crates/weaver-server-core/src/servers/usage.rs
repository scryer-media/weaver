use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDownloadUsage {
    pub server_id: u32,
    pub lifetime_bytes: u64,
    pub quota_baseline_bytes: u64,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl ServerDownloadUsage {
    pub fn empty(server_id: u32) -> Self {
        Self {
            server_id,
            lifetime_bytes: 0,
            quota_baseline_bytes: 0,
            window_start: None,
            window_end: None,
            updated_at: Utc::now(),
        }
    }

    pub fn used_bytes(&self) -> u64 {
        self.lifetime_bytes
            .saturating_sub(self.quota_baseline_bytes)
    }
}
