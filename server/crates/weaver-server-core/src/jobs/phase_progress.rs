use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JobPhase {
    Downloading,
    Repairing,
    Extracting,
    Moving,
}

#[derive(Debug, Default)]
#[repr(align(64))]
pub struct PhaseCounters {
    pub completed_bytes: AtomicU64,
    pub total_bytes: AtomicU64,
}

#[derive(Debug)]
pub struct PhaseAttemptCounters {
    phase: Arc<PhaseCounters>,
    attempted_bytes: AtomicU64,
}

impl PhaseAttemptCounters {
    pub fn new(phase: Arc<PhaseCounters>) -> Self {
        Self {
            phase,
            attempted_bytes: AtomicU64::new(0),
        }
    }

    pub fn record_completed(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.attempted_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.phase
            .completed_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn commit(&self) -> u64 {
        self.attempted_bytes.swap(0, Ordering::Relaxed)
    }

    pub fn rollback(&self) -> u64 {
        let bytes = self.attempted_bytes.swap(0, Ordering::Relaxed);
        if bytes == 0 {
            return 0;
        }
        let _ = self.phase.completed_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
        bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct JobPhaseProgress {
    pub phase: JobPhase,
    pub completed_bytes: u64,
    pub total_bytes: u64,
    pub progress_percent: f32,
    pub rate_bps: Option<u64>,
    pub estimated_remaining_ms: Option<u64>,
    pub started_at_epoch_ms: f64,
    pub updated_at_epoch_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_attempt_rollback_removes_uncommitted_bytes() {
        let phase = Arc::new(PhaseCounters::default());
        phase.completed_bytes.store(10, Ordering::Relaxed);

        let attempt = PhaseAttemptCounters::new(Arc::clone(&phase));
        attempt.record_completed(5);
        attempt.record_completed(7);

        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 22);
        assert_eq!(attempt.rollback(), 12);
        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(attempt.rollback(), 0);
        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn phase_attempt_commit_prevents_later_rollback() {
        let phase = Arc::new(PhaseCounters::default());
        let attempt = PhaseAttemptCounters::new(Arc::clone(&phase));

        attempt.record_completed(9);

        assert_eq!(attempt.commit(), 9);
        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 9);
        assert_eq!(attempt.rollback(), 0);
        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 9);
    }
}
