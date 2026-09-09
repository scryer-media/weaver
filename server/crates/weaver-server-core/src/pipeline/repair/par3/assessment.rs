//! Bounded native assessment views returned by blocking workers.

use super::*;
use crate::pipeline::repair::backend::RepairBackend;
use par3_rs::ingest::{IngestedPacket, MergeEffect};
use par3_rs::session::{AssessedFile, RecoveryRequirement, RepairAssessment, RepairStatus};
use std::sync::atomic::{AtomicUsize, Ordering};

// Scheduling views and queued publications share a process-wide host budget,
// separate from the native engine budget. Publication leases follow their
// ranges into retained source state instead of expiring at worker dispatch.
const VIEW_LIMIT: usize = 16 << 20;
static VIEW_BYTES: AtomicUsize = AtomicUsize::new(0);

pub(super) struct ViewReservation(usize);

impl ViewReservation {
    pub fn acquire(bytes: usize) -> EngineResult<Self> {
        VIEW_BYTES
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= VIEW_LIMIT)
            })
            .map_err(|_| EngineError::ResourceLimit("PAR3 host state"))?;
        Ok(Self(bytes))
    }
}

impl Drop for ViewReservation {
    fn drop(&mut self) {
        VIEW_BYTES.fetch_sub(self.0, Ordering::AcqRel);
    }
}

pub(in crate::pipeline) struct AssessmentView {
    pub status: RepairStatus,
    pub files: Vec<AssessedFile>,
    pub requirements: Vec<RecoveryRequirement>,
    pub(super) verified_sources: std::collections::BTreeSet<SourceId>,
    _reservation: ViewReservation,
}

impl AssessmentView {
    fn capture(assessment: &RepairAssessment) -> EngineResult<Self> {
        let cost = assessment.files.iter().try_fold(512usize, |bytes, file| {
            bytes
                .checked_add(320)?
                .checked_add(file.path.len())?
                .checked_add(file.unresolved.len().checked_mul(32)?)
        });
        let cost = assessment.requirements.iter().try_fold(
            cost.ok_or(EngineError::ResourceLimit("PAR3 assessment view size"))?,
            |bytes, requirement| {
                bytes
                    .checked_add(256)?
                    .checked_add(requirement.available.len().checked_mul(16)?)
            },
        );
        let reservation = ViewReservation::acquire(
            cost.ok_or(EngineError::ResourceLimit("PAR3 assessment view size"))?,
        )?;
        Ok(Self {
            status: assessment.status,
            files: assessment.files.clone(),
            requirements: assessment.requirements.clone(),
            verified_sources: assessment
                .files
                .iter()
                .filter(|file| file.complete)
                .filter_map(|file| file.source)
                .collect(),
            _reservation: reservation,
        })
    }
}

pub(super) struct SetSession {
    pub native: par3_rs::Par3RepairSession,
    pub view: Option<AssessmentView>,
}

impl SetSession {
    pub fn new(
        id: par3_rs::InputSetId,
        sources: PublishedSources,
        options: ExecutionOptions,
    ) -> EngineResult<Self> {
        Ok(Self {
            native: par3_rs::Par3RepairSession::new(id, Arc::new(sources), options)?,
            view: None,
        })
    }

    pub fn merge(&mut self, packet: IngestedPacket) -> EngineResult<()> {
        // Clear the actor's old answer even when native admission fails partway.
        let previous = self.view.take();
        let effect = self.native.merge(packet)?;
        if matches!(effect, MergeEffect::Replay) {
            self.view = previous;
        }
        Ok(())
    }

    pub fn assess(&mut self) -> EngineResult<()> {
        self.view = None;
        self.view = Some(AssessmentView::capture(self.native.assess()?)?);
        Ok(())
    }

    pub fn invalidate(&mut self, source: SourceId) {
        self.view = None;
        self.native.invalidate(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_view_reservation_fails_without_changing_accounting() {
        assert!(matches!(
            ViewReservation::acquire(VIEW_LIMIT + 1),
            Err(EngineError::ResourceLimit(_))
        ));
        // An independent small reservation still succeeds and releases on drop.
        drop(ViewReservation::acquire(1).unwrap());
    }
}
