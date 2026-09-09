//! Native verification accounting, independent of recovery availability.

use super::*;
use crate::operations::instrumentation::{JobStageKind, VerificationOutcomeKind};
use par3_rs::session::RepairStatus;
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    sources: u64,
    sets: usize,
    outcome: VerificationOutcomeKind,
}

#[derive(Clone, Copy)]
pub(super) struct Receipt {
    stamp: Stamp,
    elapsed: Duration,
}

impl Coordinator {
    // Consume one settled native assessment. The stamp deliberately excludes
    // recovery packets and assessment-cache hits: neither verifies new bytes.
    fn take_verification(&mut self, job_id: JobId) -> Option<Receipt> {
        let job = self.jobs.get_mut(&job_id)?;
        if job.installing
            || job.ticket.is_some()
            || !job.pending.is_empty()
            || !job.errors.is_empty()
            || !job.dirty.is_empty()
        {
            return None;
        }
        let runtime = job.runtime.as_ref()?;
        if runtime.sets.is_empty() {
            return None;
        }
        let mut receipt = Receipt {
            stamp: Stamp {
                sources: 0,
                sets: runtime.sets.len(),
                outcome: VerificationOutcomeKind::Intact,
            },
            elapsed: Duration::ZERO,
        };
        for set in runtime.sets.values() {
            let view = set.view.as_ref()?;
            if view.status == RepairStatus::IncompleteMetadata || view.files.is_empty() {
                return None;
            }
            receipt.stamp.sources = receipt.stamp.sources.saturating_add(set.verification_runs);
            receipt.elapsed = receipt.elapsed.saturating_add(set.verification_elapsed);
            for file in &view.files {
                if !file.complete {
                    if file.source.is_none() {
                        receipt.stamp.outcome = VerificationOutcomeKind::Missing;
                    } else if receipt.stamp.outcome != VerificationOutcomeKind::Missing {
                        receipt.stamp.outcome = VerificationOutcomeKind::Damaged;
                    }
                }
            }
        }
        if job
            .verification
            .is_some_and(|previous| previous.stamp == receipt.stamp)
        {
            return None;
        }
        let previous = job.verification.replace(receipt);
        receipt.elapsed = receipt
            .elapsed
            .saturating_sub(previous.map_or(Duration::ZERO, |p| p.elapsed));
        Some(receipt)
    }
}

impl Pipeline {
    pub(in crate::pipeline) fn note_par3_verification(&mut self, job_id: JobId) {
        let Some(receipt) = self
            .par3_runtime
            .as_mut()
            .and_then(|runtime| runtime.take_verification(job_id))
        else {
            return;
        };
        self.jobs_with_verification_outcome.insert(job_id);
        self.metrics
            .job_lifecycle
            .note_verification(receipt.stamp.outcome);
        if !receipt.elapsed.is_zero() {
            self.metrics
                .job_lifecycle
                .note_stage_duration(JobStageKind::Verify, receipt.elapsed);
        }
        let _ = self.event_tx.send(
            crate::events::model::PipelineEvent::Par3VerificationComplete {
                job_id,
                passed: receipt.stamp.outcome == VerificationOutcomeKind::Intact,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use par3_rs::source::MemorySourceAccess;

    fn publish(runtime: &mut Par3Job, index: usize, damaged: bool) {
        let (name, mut bytes) = match index {
            0 => (
                "a.bin",
                (0..5000u32).map(|i| (i * 7 + 3) as u8).collect::<Vec<_>>(),
            ),
            1 => ("b.txt", b"qrstuvwxyz".to_vec()),
            _ => (
                "sub/c.bin",
                (0..4000u32).map(|i| (i * 13 + 1) as u8).collect::<Vec<_>>(),
            ),
        };
        if damaged {
            bytes[2300] ^= 1;
        }
        let source = SourceId(index as u64);
        let len = bytes.len() as u64;
        let mut access = MemorySourceAccess::default();
        access.insert(source, 1, Arc::from(bytes));
        runtime
            .publish_access(
                source,
                Arc::new(access),
                name.into(),
                std::iter::once(0..len).collect(),
                None,
            )
            .unwrap();
    }

    #[test]
    fn verification_receipts_ignore_recovery_and_replay_but_count_changed_sources() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("set.par3");
        let recovery = root.path().join("set.vol0+1.par3");
        std::fs::write(&index, include_bytes!("../../backend/fixtures/set.par3")).unwrap();
        std::fs::write(
            &recovery,
            include_bytes!("../../backend/fixtures/set.vol0+1.par3"),
        )
        .unwrap();
        let mut coordinator = Coordinator::default();
        let id = JobId(1);
        coordinator.admit(id).unwrap();
        assert!(coordinator.take_verification(id).is_none());
        let runtime = coordinator
            .jobs
            .get_mut(&id)
            .unwrap()
            .runtime
            .as_mut()
            .unwrap();
        runtime.scan_file(SourceId(99), index, None).unwrap();
        runtime.assess().unwrap();
        let missing = coordinator.take_verification(id).unwrap();
        assert_eq!(missing.stamp.outcome, VerificationOutcomeKind::Missing);
        assert_eq!(missing.elapsed, Duration::ZERO);
        assert!(coordinator.take_verification(id).is_none());
        let runtime = coordinator
            .jobs
            .get_mut(&id)
            .unwrap()
            .runtime
            .as_mut()
            .unwrap();
        for index in 0..3 {
            publish(runtime, index, index == 0);
        }
        runtime.assess().unwrap();
        let damaged = coordinator.take_verification(id).unwrap();
        assert_eq!(damaged.stamp.outcome, VerificationOutcomeKind::Damaged);
        assert!(!damaged.elapsed.is_zero());
        for _ in 0..3 {
            let runtime = coordinator
                .jobs
                .get_mut(&id)
                .unwrap()
                .runtime
                .as_mut()
                .unwrap();
            runtime
                .scan_file(SourceId(98), recovery.clone(), None)
                .unwrap();
            let reads = runtime.options.diagnostics.source_io().read_bytes;
            runtime.assess().unwrap();
            assert_eq!(runtime.options.diagnostics.source_io().read_bytes, reads);
            assert!(coordinator.take_verification(id).is_none());
        }
        for _ in 0..2 {
            let runtime = coordinator
                .jobs
                .get_mut(&id)
                .unwrap()
                .runtime
                .as_mut()
                .unwrap();
            publish(runtime, 0, false);
            runtime.assess().unwrap();
            let intact = coordinator.take_verification(id).unwrap();
            assert_eq!(intact.stamp.outcome, VerificationOutcomeKind::Intact);
            assert!(!intact.elapsed.is_zero());
            assert!(coordinator.take_verification(id).is_none());
        }
    }
}
