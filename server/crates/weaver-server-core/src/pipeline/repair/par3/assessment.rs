//! Bounded native assessment views returned by blocking workers.

use super::*;
use crate::pipeline::repair::backend::RepairBackend;
use par3_rs::ingest::{IngestedPacket, MergeEffect};
use par3_rs::session::{AssessedFile, RecoveryRequirement, RepairAssessment, RepairStatus};
pub(super) struct ViewReservation {
    _reservation: super::budget::Reservation,
}

impl ViewReservation {
    pub fn acquire(bytes: usize) -> EngineResult<Self> {
        super::budget::budgets()
            .metadata
            .acquire(bytes)
            .map(|reservation| Self {
                _reservation: reservation,
            })
    }

    /// Retained host bytes this lease charges, for the queue-depth gauge.
    pub fn bytes(&self) -> usize {
        self._reservation.bytes()
    }
}

pub(in crate::pipeline) struct AssessmentView {
    pub status: RepairStatus,
    pub files: Vec<AssessedFile>,
    pub(super) output_lengths: Vec<u64>,
    pub requirements: Vec<RecoveryRequirement>,
    pub(super) block_size: u64,
    pub(super) embedded_source: Option<SourceId>,
    pub(super) verified_sources: std::collections::BTreeSet<SourceId>,
    _reservation: ViewReservation,
}

impl AssessmentView {
    fn capture(
        assessment: &RepairAssessment,
        layout: Option<&par3_rs::layout::BlockLayout>,
    ) -> EngineResult<Self> {
        let cost = assessment.files.iter().try_fold(512usize, |bytes, file| {
            bytes
                .checked_add(328)?
                .checked_add(file.path.len())?
                .checked_add(file.unresolved.len().checked_mul(32)?)
        });
        let cost = assessment.requirements.iter().try_fold(
            cost.ok_or(budget::host_limit("PAR3 assessment view size"))?,
            |bytes, requirement| bytes.checked_add(requirement_view_cost(requirement)?),
        );
        let reservation =
            ViewReservation::acquire(cost.ok_or(budget::host_limit("PAR3 assessment view size"))?)?;
        let mut files = assessment.files.clone();
        for file in &mut files {
            if file.source == Some(bindings::RETIRED_SOURCE) {
                file.source = None;
            }
        }
        Ok(Self {
            status: assessment.status,
            output_lengths: layout
                .map(|layout| layout.files().iter().map(|file| file.len).collect())
                .unwrap_or_default(),
            requirements: assessment.requirements.clone(),
            block_size: layout.map_or(0, |layout| layout.block_size()),
            embedded_source: layout
                .filter(|layout| {
                    layout.files().len() == 1
                        && layout.files()[0].extents.iter().any(|extent| {
                            matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected)
                        })
                })
                .and_then(|_| files.first().and_then(|file| file.source)),
            verified_sources: files
                .iter()
                .filter(|file| file.complete)
                .filter_map(|file| file.source)
                .collect(),
            files,
            _reservation: reservation,
        })
    }
}

/// Retained host bytes one requirement costs the view that holds it.
///
/// Every vector the host keeps a copy of is charged here. The requirement
/// itself is cloned into the view, so `available` and `next_indices` are held
/// twice over — once in the engine's answer and once here. `recovery_indices`
/// is a range, not a vector, so it costs nothing beyond the struct.
///
/// The cohort plan built from this view (see [`super::cohorts::CohortPlan`])
/// copies `next_indices` again and a subset of `available` into each window,
/// and has no reservation of its own: those copies live exactly as long as the
/// view they were derived from, so the view is what pays for them.
fn requirement_view_cost(requirement: &RecoveryRequirement) -> Option<usize> {
    const INDEX_BYTES: usize = std::mem::size_of::<u64>();
    let available = requirement.available.len();
    let next = requirement.next_indices.len();
    256usize
        .checked_add(available.checked_mul(16)?)?
        .checked_add(next.checked_mul(INDEX_BYTES)?)?
        // The cohort window's own `next` and `held`, where `held` is at worst
        // all of `available`.
        .checked_add(64)?
        .checked_add(next.checked_mul(INDEX_BYTES)?)?
        .checked_add(available.checked_mul(INDEX_BYTES)?)
}

pub(super) struct SetSession {
    pub native: par3_rs::Par3RepairSession,
    pub view: Option<AssessmentView>,
    pub cauchy_matrix: Option<par3_rs::Fingerprint>,
    pub verification_elapsed: std::time::Duration,
    pub verification_runs: u64,
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
            cauchy_matrix: None,
            verification_elapsed: std::time::Duration::ZERO,
            verification_runs: 0,
        })
    }

    pub fn merge(&mut self, packet: IngestedPacket) -> EngineResult<()> {
        // Clear the actor's old answer even when native admission fails partway.
        let previous = self.view.take();
        let cauchy_matrix = packet
            .metadata()
            .filter(|packet| matches!(packet.body(), par3_rs::packet::PacketBody::CauchyMatrix(_)))
            .map(|packet| packet.hash());
        let effect = self.native.merge(packet)?;
        // Complete protected data has no recovery requirements. Preserve the
        // authenticated matrix identity for explicit carrier-only replacement.
        if self.cauchy_matrix.is_none() {
            self.cauchy_matrix = cauchy_matrix;
        }
        if matches!(effect, MergeEffect::Replay) {
            self.view = previous;
        }
        Ok(())
    }

    pub fn assess(&mut self) -> EngineResult<()> {
        self.view = None;
        let layout = self.native.layout()?;
        let before = self.native.diagnostics().source_verifications;
        let started = std::time::Instant::now();
        self.view = Some(AssessmentView::capture(
            self.native.assess()?,
            layout.as_deref(),
        )?);
        if self.native.diagnostics().source_verifications != before {
            self.verification_runs = self.verification_runs.saturating_add(1);
            self.verification_elapsed = self.verification_elapsed.saturating_add(started.elapsed());
        }
        Ok(())
    }

    pub fn source_arrived(
        &mut self,
        source: SourceId,
        options: &ExecutionOptions,
    ) -> EngineResult<()> {
        self.view = None;
        let before = options.diagnostics.source_io().read_bytes;
        let started = std::time::Instant::now();
        self.native.source_arrived(source)?;
        if options.diagnostics.source_io().read_bytes != before {
            self.verification_runs = self.verification_runs.saturating_add(1);
            self.verification_elapsed = self.verification_elapsed.saturating_add(started.elapsed());
        }
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
        assert!(
            ViewReservation::acquire(usize::MAX)
                .as_ref()
                .err()
                .is_some_and(budget::is_limit)
        );
        // An independent small reservation still succeeds and releases on drop.
        drop(ViewReservation::acquire(1).unwrap());
    }

    fn requirement_with(available: &[u64], next_indices: &[u64]) -> RecoveryRequirement {
        RecoveryRequirement {
            matrix: [0u8; 16],
            cohort: 0,
            cohorts: 1,
            recovery_indices: 0..64,
            lost: next_indices.len() as u64,
            available: available.to_vec(),
            additional: next_indices.len() as u64,
            in_flight: 0,
            outstanding: next_indices.len() as u64,
            next_indices: next_indices.to_vec(),
        }
    }

    /// Every index vector the host retains is paid for. A requirement naming
    /// the indices still to fetch costs more than one naming none, and a
    /// requirement holding more available indices costs more again — nothing
    /// the view and the cohort windows keep is free.
    #[test]
    fn the_view_cost_grows_with_every_retained_index_vector() {
        let bare = requirement_view_cost(&requirement_with(&[], &[])).unwrap();
        let with_next = requirement_view_cost(&requirement_with(&[], &[1, 3, 5])).unwrap();
        let with_both = requirement_view_cost(&requirement_with(&[2, 4], &[1, 3, 5])).unwrap();

        assert!(
            with_next > bare,
            "the indices still to fetch are retained twice and must be charged"
        );
        assert!(
            with_both > with_next,
            "available indices are retained by the cohort windows as well"
        );
    }
}
