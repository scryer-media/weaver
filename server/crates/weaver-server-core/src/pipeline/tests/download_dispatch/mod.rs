use super::*;

/// Seconds since the epoch, as the NZB's `date` attribute carries it.
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// A one-file job whose files carry `posted_at` (or no date at all).
fn posted_job_spec(name: &str, posted_at: Option<u64>) -> JobSpec {
    let mut spec = standalone_job_spec(name, &[("queued.bin".to_string(), 512u32)]);
    for file in &mut spec.files {
        file.posted_at_epoch = posted_at;
    }
    spec
}

mod emits_download_pipeline_drained;
mod gapless_lanes;
mod lane_depth_seeding;
mod lane_failure_visibility;
mod owned_download_lane_pool;
mod propagation_delay;
mod stall_retry;
