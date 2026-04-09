#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandwidthUsageMinuteBucket {
    pub bucket_epoch_minute: i64,
    pub payload_bytes: u64,
}
