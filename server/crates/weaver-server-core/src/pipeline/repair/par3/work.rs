//! Retained ownership and bounded dispatch for PAR3 blocking work.

use super::*;
use crate::operations::metrics::{
    PAR3_MEMORY_CATEGORIES, Par3EngineNarrowing, Par3EngineRefusal, Par3Phase, Par3Stage,
    PipelineMetrics,
};
use crate::pipeline::RepairWorkDone;
use par3_rs::runtime::CancellationToken;
use tokio::sync::mpsc;

const MAX_JOBS: usize = 256;
const MAX_PENDING: usize = 4096;

/// Engine counters sampled once at dispatch and once at handback. The deltas
/// between the two are the only PAR3 engine telemetry weaver folds into its
/// own metrics: nothing in this crate counts per byte, per block or per stripe.
#[derive(Debug, Default, Clone, Copy)]
struct EngineCounters {
    source_read_bytes: u64,
    source_reads: u64,
    stage_calls: [u64; Par3Stage::COUNT],
    stage_millis: [u64; Par3Stage::COUNT],
    stage_completed: [u64; Par3Stage::COUNT],
    file_sync_calls: u64,
    file_sync_millis: u64,
    donor_read_bytes: u64,
    donor_time_cap_hits: u64,
    reader_cache_hits: u64,
    reader_cache_evictions: u64,
    reserved_bytes: u64,
    reserved_peak_bytes: u64,
    retained_bytes: u64,
    packets_authenticated: u64,
    packets_rejected: u64,
    ranges_unavailable: u64,
    /// The engine's categorised ledger: current and peak reservations per
    /// category. Absolute values, so these are published rather than folded.
    ledger_bytes: [u64; PAR3_MEMORY_CATEGORIES],
    ledger_peak_bytes: [u64; PAR3_MEMORY_CATEGORIES],
    /// The widths the engine last admitted. Also absolute by the engine's own
    /// definition: each field holds the most recent admission.
    admitted: [u64; ADMITTED_WIDTHS],
    /// Cumulative refusals by cause, narrowings by width, and the bytes a
    /// bounded working set pushed onto the I/O layer. These fold in as deltas.
    refusals: [u64; Par3EngineRefusal::COUNT],
    narrowed: [u64; Par3EngineNarrowing::COUNT],
    reread_bytes: u64,
    reconstructed_bytes: u64,
    /// What the engine's admission caches currently hold. Absolute.
    cache_entries: u64,
    cache_bytes: u64,
    /// Transform and coefficient work the engine's codecs performed.
    /// Cumulative, so these fold in as deltas.
    codec: [u64; CODEC_COUNTERS],
    /// Carrier bytes the scanner read and could not authenticate. Cumulative
    /// per job, so this folds in as a delta.
    damaged_bytes: u64,
}

/// Admitted widths captured per handback, in the order the fields below are
/// published. Fixed so the capture allocates nothing.
const ADMITTED_WIDTHS: usize = 6;

/// Codec work counters captured per handback, in the order they are published.
const CODEC_COUNTERS: usize = 6;

/// The engine stage each tracked class maps to.
const TRACKED_STAGES: [(Par3Stage, par3_rs::runtime::Stage); Par3Stage::COUNT] = [
    (Par3Stage::Scan, par3_rs::runtime::Stage::Scan),
    (Par3Stage::Metadata, par3_rs::runtime::Stage::Metadata),
    (Par3Stage::Verify, par3_rs::runtime::Stage::Verify),
    (Par3Stage::Assess, par3_rs::runtime::Stage::Assess),
    (Par3Stage::Placement, par3_rs::runtime::Stage::Placement),
    (Par3Stage::Repair, par3_rs::runtime::Stage::Repair),
    (Par3Stage::Checkpoint, par3_rs::runtime::Stage::Checkpoint),
];

fn as_millis(elapsed: std::time::Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

/// The phase an engine stage puts a work slot in.
fn engine_phase(stage: par3_rs::runtime::Stage) -> Par3Phase {
    use par3_rs::runtime::Stage;
    match stage {
        Stage::Scan => Par3Phase::ScanningCarriers,
        Stage::Metadata | Stage::Container | Stage::Checkpoint => Par3Phase::ResolvingMetadata,
        Stage::Verify => Par3Phase::Verifying,
        Stage::Assess => Par3Phase::Assessing,
        Stage::Placement => Par3Phase::DonorSearch,
        Stage::Create | Stage::Carrier | Stage::Repair | Stage::Encode | Stage::Decode => {
            Par3Phase::Repairing
        }
    }
}

/// The phase a queued work unit puts its slot in before the engine speaks.
fn pending_phase(input: &PendingInput) -> Par3Phase {
    match input {
        PendingInput::Assess => Par3Phase::Assessing,
        PendingInput::Donors => Par3Phase::DonorSearch,
        PendingInput::Readback(_) => Par3Phase::Readback,
        PendingInput::Repair { .. } => Par3Phase::Repairing,
        PendingInput::Carrier { .. } | PendingInput::Embedded { .. } => Par3Phase::ScanningCarriers,
        PendingInput::Virtual { .. }
        | PendingInput::CompleteFile { .. }
        | PendingInput::File { .. } => Par3Phase::ResolvingMetadata,
    }
}

impl EngineCounters {
    fn capture(runtime: &Par3Job) -> Self {
        let diagnostics = &runtime.options.diagnostics;
        let source = diagnostics.source_io();
        let sync = diagnostics.file_sync();
        let (reader_cache_hits, reader_cache_evictions) = runtime.virtual_readers.counters();
        let mut counters = Self {
            source_read_bytes: source.read_bytes,
            source_reads: source.read_calls,
            file_sync_calls: sync.calls,
            file_sync_millis: as_millis(sync.elapsed),
            donor_read_bytes: runtime.donor_search.read_bytes,
            donor_time_cap_hits: runtime.donor_search.time_cap_hits,
            reader_cache_hits,
            reader_cache_evictions,
            reserved_bytes: runtime.options.memory.used() as u64,
            reserved_peak_bytes: runtime.options.memory.peak() as u64,
            retained_bytes: super::budget::budgets().host_used(),
            packets_authenticated: runtime.packets_authenticated,
            packets_rejected: runtime.packets_rejected,
            ranges_unavailable: runtime.ranges_unavailable,
            admitted: {
                let admission = diagnostics.admission();
                [
                    admission.stripe_bytes,
                    admission.stripe_buffers,
                    admission.output_tile,
                    admission.verify_batch,
                    admission.workers,
                    admission.window_bytes,
                ]
            },
            refusals: {
                let refusals = diagnostics.refusals();
                [
                    refusals.exceeds_limit,
                    refusals.peer_contention,
                    refusals.unmeasured,
                ]
            },
            narrowed: {
                let waits = diagnostics.waits();
                [
                    waits.stripe_narrowed,
                    waits.workers_refused,
                    waits.batch_narrowed,
                ]
            },
            reread_bytes: diagnostics.amplification().reread_bytes,
            reconstructed_bytes: diagnostics.amplification().reconstructed_bytes,
            cache_entries: diagnostics.caches().entries,
            cache_bytes: diagnostics.caches().bytes,
            codec: {
                let codec = diagnostics.codec();
                [
                    codec.transform_calls,
                    codec.butterflies,
                    codec.butterflies_skipped,
                    codec.multiply_accumulates,
                    codec.factors_computed,
                    codec.factors_reused,
                ]
            },
            damaged_bytes: runtime.damaged_bytes(),
            ..Self::default()
        };
        // The ledger is read straight off the budget these diagnostics were
        // first used with; reading it allocates nothing and takes no lock.
        if let Some(ledger) = diagnostics.memory() {
            for (index, category) in par3_rs::runtime::MemoryCategory::ALL
                .into_iter()
                .enumerate()
            {
                let entry = ledger.category(category);
                counters.ledger_bytes[index] = entry.current;
                counters.ledger_peak_bytes[index] = entry.peak;
            }
        }
        for (tracked, stage) in TRACKED_STAGES {
            let snapshot = diagnostics.stage(stage);
            counters.stage_calls[tracked.index()] = snapshot.calls;
            counters.stage_millis[tracked.index()] = as_millis(snapshot.elapsed);
            counters.stage_completed[tracked.index()] = snapshot.completed;
        }
        counters
    }

    /// Fold this handback's deltas into the live metrics. One load per counter
    /// and one `fetch_add` per non-zero delta; never called from a work loop.
    fn apply(self, before: Self, metrics: &PipelineMetrics) {
        use std::sync::atomic::Ordering::Relaxed;
        let add = |counter: &std::sync::atomic::AtomicU64, delta: u64| {
            if delta != 0 {
                counter.fetch_add(delta, Relaxed);
            }
        };
        let par3 = &metrics.par3;
        add(
            &par3.source_read_bytes_total,
            self.source_read_bytes
                .saturating_sub(before.source_read_bytes),
        );
        add(
            &par3.source_reads_total,
            self.source_reads.saturating_sub(before.source_reads),
        );
        add(
            &par3.file_sync_calls_total,
            self.file_sync_calls.saturating_sub(before.file_sync_calls),
        );
        add(
            &par3.file_sync_ms_total,
            self.file_sync_millis
                .saturating_sub(before.file_sync_millis),
        );
        add(
            &par3.donor_read_bytes_total,
            self.donor_read_bytes
                .saturating_sub(before.donor_read_bytes),
        );
        add(
            &par3.donor_time_cap_hits_total,
            self.donor_time_cap_hits
                .saturating_sub(before.donor_time_cap_hits),
        );
        add(
            &par3.encrypted_reader_cache_hits_total,
            self.reader_cache_hits
                .saturating_sub(before.reader_cache_hits),
        );
        add(
            &par3.encrypted_reader_cache_evictions_total,
            self.reader_cache_evictions
                .saturating_sub(before.reader_cache_evictions),
        );
        for (tracked, _) in TRACKED_STAGES {
            let index = tracked.index();
            par3.note_stage_delta(
                tracked,
                self.stage_calls[index].saturating_sub(before.stage_calls[index]),
                self.stage_millis[index].saturating_sub(before.stage_millis[index]),
            );
        }
        // Repair stage units are output bytes, so its completed delta is the
        // reconstructed byte count without any per-byte counting here.
        let repair = Par3Stage::Repair.index();
        add(
            &par3.repair_bytes_reconstructed_total,
            self.stage_completed[repair].saturating_sub(before.stage_completed[repair]),
        );
        add(
            &par3.packets_authenticated_total,
            self.packets_authenticated
                .saturating_sub(before.packets_authenticated),
        );
        add(
            &par3.packets_rejected_total,
            self.packets_rejected
                .saturating_sub(before.packets_rejected),
        );
        add(
            &par3.carrier_ranges_unavailable_total,
            self.ranges_unavailable
                .saturating_sub(before.ranges_unavailable),
        );
        add(
            &par3.carrier_damaged_bytes_total,
            self.damaged_bytes.saturating_sub(before.damaged_bytes),
        );
        par3.reserved_bytes.store(self.reserved_bytes, Relaxed);
        par3.retained_bytes.store(self.retained_bytes, Relaxed);
        par3.reserved_peak_bytes
            .store(self.reserved_peak_bytes, Relaxed);
        par3.store_ledger(self.ledger_bytes, self.ledger_peak_bytes);
        for (field, value) in [
            (&par3.engine_admitted_stripe_bytes, self.admitted[0]),
            (&par3.engine_admitted_stripe_buffers, self.admitted[1]),
            (&par3.engine_admitted_output_tile, self.admitted[2]),
            (&par3.engine_admitted_verify_batch, self.admitted[3]),
            (&par3.engine_admitted_workers, self.admitted[4]),
            (&par3.engine_admitted_window_bytes, self.admitted[5]),
        ] {
            field.store(value, Relaxed);
        }
        par3.note_engine_refusals(std::array::from_fn(|index| {
            self.refusals[index].saturating_sub(before.refusals[index])
        }));
        par3.note_engine_narrowed(std::array::from_fn(|index| {
            self.narrowed[index].saturating_sub(before.narrowed[index])
        }));
        add(
            &par3.engine_reread_bytes_total,
            self.reread_bytes.saturating_sub(before.reread_bytes),
        );
        add(
            &par3.engine_reconstructed_bytes_total,
            self.reconstructed_bytes
                .saturating_sub(before.reconstructed_bytes),
        );
        par3.engine_cache_entries.store(self.cache_entries, Relaxed);
        par3.engine_cache_bytes.store(self.cache_bytes, Relaxed);
        // The codec counts arrive cumulative for the session's whole life, so
        // what this handback contributed is the difference against the
        // baseline the same work unit was dispatched with.
        for (index, field) in [
            &par3.engine_codec_transform_calls_total,
            &par3.engine_codec_butterflies_total,
            &par3.engine_codec_butterflies_skipped_total,
            &par3.engine_codec_multiply_accumulates_total,
            &par3.engine_codec_factors_computed_total,
            &par3.engine_codec_factors_reused_total,
        ]
        .into_iter()
        .enumerate()
        {
            add(field, self.codec[index].saturating_sub(before.codec[index]));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkKey {
    Source(SourceId),
    Assess,
    Donors,
    Repair(par3_rs::InputSetId),
    Readback,
}

enum WorkOutput {
    Published,
    Repaired(RepairCompletion),
    Readback(readback::ReadbackDone),
}

/// Keep output-path accounting alive through channel handback, partial-error
/// handling and asynchronous assembly/database reconciliation.
pub(super) struct RepairCompletion {
    pub result: EngineResult<par3_rs::session_repair::SessionRepairReport>,
    pub outputs: EngineResult<Vec<readback::VerifiedOutput>>,
    /// True only when the embedded repair path installed a replacement.
    pub embedded_replacement: bool,
    pub _reservation: Option<assessment::ViewReservation>,
}

enum PendingInput {
    Assess,
    Donors,
    Readback(Box<readback::Installation>),
    Virtual {
        image: virtual_source::VirtualInput,
        name: String,
    },
    Repair {
        set: par3_rs::InputSetId,
        path: PathBuf,
    },
    CompleteFile {
        path: PathBuf,
        name: String,
    },
    Embedded {
        path: PathBuf,
        name: String,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    },
    Carrier {
        path: PathBuf,
        ranges: Option<Vec<std::ops::Range<u64>>>,
    },
    File {
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    },
}

impl PendingInput {
    fn retained_cost(&self) -> EngineResult<usize> {
        let (path, extra) = match self {
            Self::Donors | Self::Assess => return Ok(1024),
            Self::Readback(_) => return Ok(readback::STRIPE_RESERVATION),
            Self::Virtual { name, .. } => {
                return name
                    .capacity()
                    .checked_mul(2)
                    .and_then(|cost| cost.checked_add(1024))
                    .ok_or(budget::host_limit("PAR3 virtual publication"));
            }
            Self::Repair { path, .. } => (path, Some(0)),
            Self::CompleteFile { path, name } => (path, name.capacity().checked_mul(2)),
            Self::Embedded {
                path, name, ranges, ..
            } => (
                path,
                name.capacity().checked_mul(2).and_then(|bytes| {
                    bytes.checked_add(
                        ranges
                            .as_ref()
                            .map_or(Some(0), |ranges| ranges.capacity().checked_mul(32))?,
                    )
                }),
            ),
            Self::Carrier { path, ranges } => (
                path,
                ranges
                    .as_ref()
                    .map_or(Some(0), |ranges| ranges.capacity().checked_mul(32)),
            ),
            Self::File { path, name, ranges } => (
                path,
                name.capacity()
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(ranges.capacity().checked_mul(32)?)),
            ),
        };
        1024usize
            .checked_add(
                path.capacity()
                    .checked_mul(2)
                    .ok_or(budget::host_limit("PAR3 queued path"))?,
            )
            .and_then(|bytes| bytes.checked_add(extra?))
            .ok_or(budget::host_limit("PAR3 queued publication"))
    }
}

struct QueuedInput {
    input: PendingInput,
    reservation: assessment::ViewReservation,
    queued_at: std::time::Instant,
}

impl QueuedInput {
    fn new(input: PendingInput, reservation: assessment::ViewReservation) -> Self {
        Self {
            input,
            reservation,
            queued_at: std::time::Instant::now(),
        }
    }
}

struct WorkTiming {
    job_id: JobId,
    key: WorkKey,
    queued_at: std::time::Instant,
    started: std::time::Instant,
}

impl Drop for WorkTiming {
    fn drop(&mut self) {
        tracing::info!(job_id = self.job_id.0, operation = ?self.key,
            queue_wait_us = self.started.duration_since(self.queued_at).as_micros() as u64,
            execution_us = self.started.elapsed().as_micros() as u64,
            "PAR3 native work finished");
    }
}

#[derive(Default)]
pub(super) struct Acquisition {
    pub batch: Option<RecoveryBatch>,
    pub prefetched: bool,
    /// Indices a drained window retracted while a worker held the engine
    /// session. The retraction lands on the session at its handback, followed
    /// by the reassessment that makes those indices askable again; dropping
    /// it would leave them counted in flight for the rest of the job.
    deferred_release: Vec<(par3_rs::InputSetId, par3_rs::Fingerprint, Vec<u64>)>,
}

pub(super) struct RecoveryBatch {
    pub articles: Vec<crate::jobs::ids::SegmentId>,
    /// Whether this window's articles have already been accounted for as
    /// arrivals or losses. Each admitted article is counted exactly once.
    settled: bool,
    job_id: JobId,
    cohorts: Vec<(par3_rs::InputSetId, par3_rs::Fingerprint, u64)>,
    /// Recovery indices this window declared to the engine as being acquired,
    /// so a reassessment taken while it is in flight does not ask for them
    /// again. Retracted verbatim when the window drains.
    declared: Vec<(par3_rs::InputSetId, par3_rs::Fingerprint, Vec<u64>)>,
    epoch: u64,
    assessment: u64,
    admitted_at: std::time::Instant,
    _reservation: assessment::ViewReservation,
}

impl Drop for RecoveryBatch {
    fn drop(&mut self) {
        tracing::debug!(job_id = self.job_id.0, articles = self.articles.len(),
            cohorts = ?self.cohorts, epoch = self.epoch, assessment = self.assessment,
            retained_us = self.admitted_at.elapsed().as_micros() as u64,
            "PAR3 acquisition window retired");
    }
}

struct KnownSource {
    carrier: bool,
    protected: bool,
    embedded_start: Option<u64>,
    complete_disk_image: bool,
    promoted: BTreeMap<u32, assessment::ViewReservation>,
    // Retained source, dirty and error bookkeeping outlives queued work,
    // including failed publications which never entered the engine.
    _reservation: assessment::ViewReservation,
}

struct MaterializedRanges {
    ranges: Vec<std::ops::Range<u64>>,
    _reservation: assessment::ViewReservation,
}

/// Committed availability held across a synchronous content-preserving rename.
/// Verification evidence is deliberately not part of this handoff.
pub(super) struct MaterializedSources(BTreeMap<SourceId, EngineResult<MaterializedRanges>>);

struct JobSlot {
    acquisition: Acquisition,
    repair_phase: bool,
    retry_serial: Option<WorkKey>,
    retry_repair: bool,
    runtime: Option<Par3Job>,
    sources: PublishedSources,
    epoch: u64,
    last_used: u64,
    known: BTreeMap<SourceId, KnownSource>,
    materialized: BTreeMap<SourceId, EngineResult<MaterializedRanges>>,
    dirty: std::collections::BTreeSet<SourceId>,
    pending: BTreeMap<WorkKey, QueuedInput>,
    ticket: Option<u64>,
    errors: BTreeMap<SourceId, EngineError>,
    donor_error: Option<EngineError>,
    spill: Option<SourceId>,
    /// The engine refusal that forced the spill, kept so the verdict can be
    /// classified against the numbers the engine actually measured rather
    /// than against the configured budget read back later.
    spill_limit: Option<par3_rs::runtime::ResourceLimit>,
    spill_disk: Option<budget::DiskReservation>,
    completed_repair: Option<RepairCompletion>,
    completed_readback: Option<EngineResult<readback::ReadbackDone>>,
    installing: bool,
    verification: Option<verification::Receipt>,
    /// Engine counters as of this job's last dispatch.
    engine_baseline: EngineCounters,
    /// When this job's current wait for PAR3 memory began.
    waiting_for_memory_since: Option<std::time::Instant>,
    /// The last typed verdict this job reached, for tests and diagnostics.
    last_outcome: Option<outcome::Par3Outcome>,
    /// Cohorts with losses named by the assessment the in-flight repair was
    /// dispatched against, credited only once that repair comes back whole.
    repair_cohorts: u64,
    /// Whether this job has already said its carriers were damaged. The
    /// summary is a job-level fact, not a per-handback one.
    damage_reported: bool,
    /// Whether this job has already reported the option packets its set
    /// carries that weaver does not apply.
    options_reported: bool,
}

impl Default for JobSlot {
    fn default() -> Self {
        let runtime = Par3Job::default();
        Self {
            sources: runtime.sources.clone(),
            acquisition: Acquisition::default(),
            repair_phase: false,
            retry_serial: None,
            retry_repair: false,
            runtime: Some(runtime),
            epoch: 0,
            last_used: 0,
            known: BTreeMap::new(),
            materialized: BTreeMap::new(),
            dirty: std::collections::BTreeSet::new(),
            pending: BTreeMap::new(),
            ticket: None,
            errors: BTreeMap::new(),
            donor_error: None,
            spill: None,
            spill_limit: None,
            spill_disk: None,
            completed_repair: None,
            completed_readback: None,
            installing: false,
            verification: None,
            engine_baseline: EngineCounters::default(),
            waiting_for_memory_since: None,
            last_outcome: None,
            repair_cohorts: 0,
            damage_reported: false,
            options_reported: false,
        }
    }
}

pub(crate) struct WorkDone {
    job_id: JobId,
    ticket: u64,
    epoch: u64,
    key: WorkKey,
    runtime: Option<Par3Job>,
    result: EngineResult<WorkOutput>,
}

/// Created only on PAR3 admission. Two job workers bound dispatch even
/// when many jobs arrive together. Retired tickets hold capacity until their
/// cancelled workers return; recreating a job cannot evade that bound.
pub(in crate::pipeline) struct Coordinator {
    jobs: BTreeMap<JobId, JobSlot>,
    in_flight: BTreeMap<u64, (JobId, CancellationToken)>,
    worker_allowances: BTreeMap<u64, usize>,
    contended: std::collections::BTreeSet<u64>,
    cpu_limit: usize,
    next_ticket: u64,
    last_job: Option<JobId>,
    tx: mpsc::Sender<RepairWorkDone>,
    metrics: Arc<PipelineMetrics>,
    #[cfg(test)]
    test_rx: Option<mpsc::Receiver<RepairWorkDone>>,
}

#[cfg(test)]
impl Default for Coordinator {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let mut coordinator = Self::new(tx, PipelineMetrics::new());
        coordinator.test_rx = Some(rx);
        coordinator
    }
}

impl Coordinator {
    pub(super) fn acquisition(&self, job_id: JobId) -> Option<&Acquisition> {
        self.jobs.get(&job_id).map(|job| &job.acquisition)
    }

    /// The articles of an acquisition window whose downloads have all
    /// finished, handed back once so no article is accounted for twice.
    pub(super) fn take_unsettled_articles(
        &mut self,
        job_id: JobId,
    ) -> Vec<crate::jobs::ids::SegmentId> {
        let Some(batch) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|job| job.acquisition.batch.as_mut())
        else {
            return Vec::new();
        };
        if std::mem::replace(&mut batch.settled, true) {
            return Vec::new();
        }
        batch.articles.clone()
    }

    pub(super) fn begin_recovery_batch(
        &mut self,
        job_id: JobId,
        articles: Vec<crate::jobs::ids::SegmentId>,
        prefetch: bool,
    ) -> EngineResult<()> {
        let cohort_count = self
            .assessments(job_id)
            .map(|(_, view)| view.requirements.len())
            .sum::<usize>();
        let reservation =
            assessment::ViewReservation::acquire(512 + articles.len() * 64 + cohort_count * 64)?;
        let cohorts: Vec<_> = self
            .assessments(job_id)
            .flat_map(|(set, view)| {
                view.requirements
                    .iter()
                    .filter(|need| need.additional != 0)
                    .map(move |need| (set, need.matrix, need.cohort))
            })
            .collect();
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("unknown PAR3 job"))?;
        job.acquisition.prefetched |= prefetch;
        job.acquisition.batch = Some(RecoveryBatch {
            articles,
            settled: false,
            job_id,
            cohorts,
            declared: Vec::new(),
            epoch: job.epoch,
            assessment: job.last_used,
            admitted_at: std::time::Instant::now(),
            _reservation: reservation,
        });
        Ok(())
    }

    /// Declare the recovery indices the window just admitted is expected to
    /// deliver, so a reassessment taken while it is in flight does not ask for
    /// them again.
    ///
    /// `carriers` pairs each selected carrier's advertised index span with
    /// how many of its articles this window admitted. Only indices the engine
    /// itself named as still wanted are declared, only where a selected
    /// carrier advertises them, and never more of one carrier's span than the
    /// window will actually fetch from it: a carrier whose name says nothing
    /// declares nothing, which merely means those indices stay askable.
    pub(in crate::pipeline) fn declare_recovery_in_flight(
        &mut self,
        job_id: JobId,
        carriers: &[(std::ops::Range<u64>, usize)],
    ) -> EngineResult<()> {
        if carriers.is_empty() {
            tracing::info!(
                job_id = job_id.0,
                "PAR3 recovery in-flight declaration skipped: no carrier span"
            );
            return Ok(());
        }
        let views = self.assessments(job_id).count();
        let wanted: usize = self
            .assessments(job_id)
            .flat_map(|(_, view)| view.requirements.iter().map(|need| need.next_indices.len()))
            .sum();
        let declared: Vec<(par3_rs::InputSetId, par3_rs::Fingerprint, Vec<u64>)> = self
            .assessments(job_id)
            .flat_map(|(set, view)| {
                view.requirements.iter().filter_map(move |need| {
                    let indices = super::cohorts::declarable_indices(
                        &need.next_indices,
                        carriers,
                        need.cohorts,
                    );
                    (!indices.is_empty()).then_some((set, need.matrix, indices))
                })
            })
            .collect();
        let count: usize = declared.iter().map(|(_, _, indices)| indices.len()).sum();
        let first = declared
            .iter()
            .flat_map(|(_, _, indices)| indices.first().copied())
            .min();
        tracing::info!(
            job_id = job_id.0,
            views,
            wanted,
            declared = count,
            first_index = first,
            carriers = ?carriers,
            "PAR3 recovery in-flight declared"
        );
        if declared.is_empty() {
            return Ok(());
        }
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return Ok(());
        };
        let Some(runtime) = job.runtime.as_mut() else {
            return Ok(());
        };
        for (set, matrix, indices) in &declared {
            if let Some(session) = runtime.sets.get_mut(set) {
                session.native.note_recovery_in_flight(*matrix, indices)?;
            }
        }
        if let Some(batch) = job.acquisition.batch.as_mut() {
            batch.declared = declared;
        }
        // The retained view still answers the assessment that produced these
        // indices; take a fresh one so the deficit this job publishes is what
        // is left to ask for rather than what was asked for. Sources have not
        // changed, so that assessment reads nothing.
        self.queue_reassessment(job_id)
    }

    /// Retract everything the drained window declared. The window is over, so
    /// nothing it named is still being acquired: an index that arrived is now
    /// the engine's own `available`, and one that did not must become askable
    /// again rather than sit in flight forever.
    pub(in crate::pipeline) fn forget_recovery_in_flight(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let declared = match job.acquisition.batch.as_mut() {
            Some(batch) => std::mem::take(&mut batch.declared),
            None => {
                tracing::info!(
                    job_id = job_id.0,
                    "PAR3 recovery in-flight release: no window"
                );
                return;
            }
        };
        let count: usize = declared.iter().map(|(_, _, indices)| indices.len()).sum();
        tracing::info!(
            job_id = job_id.0,
            released = count,
            "PAR3 recovery in-flight released"
        );
        // A window that declared nothing releases nothing, and must not cost
        // the job an assessment it did not need.
        if declared.is_empty() {
            return;
        }
        let Some(runtime) = job.runtime.as_mut() else {
            // A worker holds the session: the retraction waits for its
            // handback rather than being lost with the session absent.
            tracing::info!(
                job_id = job_id.0,
                released = count,
                "PAR3 recovery in-flight release deferred to the worker handback"
            );
            job.acquisition.deferred_release.extend(declared);
            return;
        };
        for (set, matrix, indices) in &declared {
            if let Some(session) = runtime.sets.get_mut(set) {
                session.native.forget_recovery_in_flight(*matrix, indices);
            }
        }
        // What was released has to become askable again, which only a fresh
        // assessment can say. A refusal here leaves the retained view stale
        // with the released indices still counted in flight, so it is never
        // silent.
        if let Err(error) = self.queue_reassessment(job_id) {
            tracing::warn!(
                job_id = job_id.0,
                %error,
                "PAR3 reassessment after an in-flight release could not be queued"
            );
        }
    }

    pub(in crate::pipeline) fn new(
        tx: mpsc::Sender<RepairWorkDone>,
        metrics: Arc<PipelineMetrics>,
    ) -> Self {
        Self {
            jobs: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            worker_allowances: BTreeMap::new(),
            contended: std::collections::BTreeSet::new(),
            cpu_limit: std::thread::available_parallelism()
                .map_or(1, usize::from)
                .saturating_sub(1)
                .max(1),
            next_ticket: 0,
            last_job: None,
            tx,
            metrics,
            #[cfg(test)]
            test_rx: None,
        }
    }

    /// Record this job's typed verdict, counting it once per distinct value.
    /// A job that keeps reaching the same verdict while it waits for more
    /// bytes is one verdict, not one per completion check.
    pub(in crate::pipeline) fn note_outcome(
        &mut self,
        job_id: JobId,
        outcome: outcome::Par3Outcome,
    ) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            if job.last_outcome.as_ref() == Some(&outcome) {
                return;
            }
            job.last_outcome = Some(outcome.clone());
        }
        self.metrics.par3.note_outcome(outcome.class());
    }

    /// Carriers whose scanner stopped short and still wants bytes it has not
    /// seen. A nonzero count means metadata discovery is not finished.
    pub(in crate::pipeline) fn carriers_awaiting_bytes(&self, job_id: JobId) -> u64 {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map_or(0, |runtime| {
                runtime
                    .carriers
                    .values()
                    .filter(|carrier| carrier.needed.is_some())
                    .count() as u64
            })
    }

    /// The vital packet family no carrier of this job produced a single
    /// authenticated copy of, if there is one. A set cannot be planned without
    /// a Start, a matrix and a Root, and a family with a zero count is a
    /// different complaint from a set that is merely still arriving.
    pub(in crate::pipeline) fn missing_vital_packet(
        &self,
        job_id: JobId,
    ) -> Option<super::carriers::Par3PacketKind> {
        let families = self
            .jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())?
            .authenticated_families();
        super::carriers::Par3PacketKind::VITAL
            .into_iter()
            .find(|kind| families[kind.index()] == 0)
    }

    /// What this job's carriers have found damaged so far.
    #[cfg(test)]
    pub(in crate::pipeline) fn carrier_damage(
        &self,
        job_id: JobId,
    ) -> Vec<super::carriers::CarrierDamage> {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map(super::Par3Job::damage_report)
            .unwrap_or_default()
    }

    /// This job's option-packet tally, the first time it is asked for.
    ///
    /// Options are reported, never applied, so one line per job is the whole
    /// obligation; later calls return nothing so a repeated completion check
    /// cannot repeat the line.
    pub(in crate::pipeline) fn take_option_packet_report(
        &mut self,
        job_id: JobId,
    ) -> Option<super::OptionPacketTally> {
        let job = self.jobs.get_mut(&job_id)?;
        let tally = job.runtime.as_ref()?.option_packet_tally();
        if tally.is_silent() || std::mem::replace(&mut job.options_reported, true) {
            return None;
        }
        Some(tally)
    }

    /// Whether another job currently owns a PAR3 work unit, and with it the
    /// share of the native budget this job's refusal collided with.
    pub(in crate::pipeline) fn peer_holds_par3_memory(&self, job_id: JobId) -> bool {
        self.in_flight.values().any(|(owner, _)| *owner != job_id)
    }

    /// Publish the queue depth, worker allowance and in-flight gauges.
    fn publish_dispatch_gauges(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let par3 = &self.metrics.par3;
        let (depth, bytes) = self
            .jobs
            .values()
            .fold((0usize, 0u64), |(depth, bytes), job| {
                (
                    depth + job.pending.len(),
                    job.pending.values().fold(bytes, |bytes, queued| {
                        bytes.saturating_add(queued.reservation.bytes() as u64)
                    }),
                )
            });
        par3.pending_work_depth.store(depth, Relaxed);
        par3.pending_work_bytes.store(bytes, Relaxed);
        par3.in_flight.store(self.in_flight.len(), Relaxed);
        par3.workers_admitted
            .store(self.worker_allowances.values().sum::<usize>(), Relaxed);
        // A job that owns no worker, has nothing queued, is not installing and
        // is not parked on memory is waiting on the pipeline, not on PAR3.
        // Releasing its slot here is the single place a phase returns to idle,
        // so no handback path can leave a stale phase behind and invent a
        // stall — and a parked job keeps its phase, so its wait keeps ageing
        // towards the stall threshold instead of being reset to idle.
        let now = self.metrics.now_ms();
        for (&job_id, job) in &self.jobs {
            if job.ticket.is_none()
                && job.pending.is_empty()
                && !job.installing
                && job.spill.is_none()
                && job.waiting_for_memory_since.is_none()
            {
                par3.store_phase(job_id.0, Par3Phase::Idle, now);
            }
        }
    }
    pub(in crate::pipeline) fn authenticated_set_count(&self, job_id: JobId) -> usize {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map_or(0, |runtime| {
                runtime.sets.len()
                    + runtime
                        .dormant_views
                        .keys()
                        .filter(|id| !runtime.sets.contains_key(id))
                        .count()
            })
    }

    /// Whether a worker currently owns the job's session, so a completion is
    /// owed on the repair channel. Narrower than [`Self::has_work`], which
    /// also counts work the next completion check has to dispatch or spill.
    #[cfg(test)]
    pub(in crate::pipeline) fn has_worker_in_flight(&self, job_id: JobId) -> bool {
        self.jobs
            .get(&job_id)
            .is_some_and(|job| job.ticket.is_some())
    }

    pub(in crate::pipeline) fn has_work(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|job| {
            job.installing
                || job.spill.is_some()
                || job.ticket.is_some()
                || !job.pending.is_empty()
                || !job.dirty.is_empty()
        })
    }

    #[cfg(test)]
    pub(super) fn enqueue(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_complete_carrier(job_id, source, path)
    }

    pub(super) fn enqueue_complete_carrier(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::Carrier { path, ranges: None })
    }

    pub(super) fn enqueue_carrier_ranges(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> EngineResult<()> {
        self.enqueue_input(
            job_id,
            source,
            PendingInput::Carrier {
                path,
                ranges: Some(ranges),
            },
        )
    }

    pub(super) fn enqueue_embedded(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    ) -> EngineResult<()> {
        self.enqueue_input(
            job_id,
            source,
            PendingInput::Embedded {
                path,
                name,
                ranges,
                start,
            },
        )
    }

    pub(super) fn embedded_start(&self, job_id: JobId, source: SourceId) -> Option<u64> {
        self.jobs.get(&job_id)?.known.get(&source)?.embedded_start
    }

    pub(super) fn has_complete_disk_image(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&source))
            .is_some_and(|known| known.complete_disk_image)
    }

    pub(super) fn enqueue_file(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::File { path, name, ranges })
    }

    pub(in crate::pipeline) fn contains_job(&self, job_id: JobId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    pub(super) fn enqueue_virtual(
        &mut self,
        job_id: JobId,
        source: SourceId,
        volume: crate::pipeline::direct_store::provider::VirtualVolume,
        name: String,
    ) -> EngineResult<()> {
        self.admit(job_id)?;
        if self.jobs[&job_id].spill.is_some() {
            return Ok(());
        }
        let result =
            virtual_source::VirtualInput::new(volume, &execution_options()).and_then(|image| {
                self.enqueue_input(job_id, source, PendingInput::Virtual { image, name })
            });
        match result {
            Err(error) if budget::is_host_pressure(&error) => {
                tracing::debug!(job_id = job_id.0, source = source.0, %error,
                    stage = "virtual_publication", "PAR3 source requires disk fallback");
                self.jobs
                    .get_mut(&job_id)
                    .expect("admitted job")
                    .spill
                    .get_or_insert(source);
                Ok(())
            }
            result => result,
        }
    }

    pub(in crate::pipeline) fn admit(&mut self, job_id: JobId) -> EngineResult<()> {
        if !self.jobs.contains_key(&job_id) && self.jobs.len() >= MAX_JOBS {
            return Err(budget::host_limit("PAR3 job count"));
        }
        self.jobs.entry(job_id).or_default();
        Ok(())
    }

    /// Publish actual disk bytes as candidates; this does not admit verification evidence.
    pub(super) fn enqueue_complete_file(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::CompleteFile { path, name })
    }

    pub(super) fn request_repair(
        &mut self,
        job_id: JobId,
        set: par3_rs::InputSetId,
        path: PathBuf,
    ) -> EngineResult<()> {
        let Some((_, view)) = self.assessments(job_id).find(|(id, view)| {
            *id == set
                && (view.status == par3_rs::session::RepairStatus::Ready
                    || (view.status == par3_rs::session::RepairStatus::Complete
                        && view
                            .embedded_source
                            .is_some_and(|source| self.embedded_start(job_id, source).is_some())))
        }) else {
            return Err(EngineError::InvalidState("PAR3 repair is not ready"));
        };
        // Reserve before execution can install anything. Include the native
        // report, partial-error temporary paths, and reconciliation copies.
        let replacing_carrier = view.status == par3_rs::session::RepairStatus::Complete;
        let outputs = view
            .files
            .iter()
            .filter(|file| replacing_carrier || !file.complete)
            .try_fold(2048usize, |bytes, file| {
                bytes
                    .checked_add(4096)?
                    .checked_add(path.capacity().checked_mul(16)?)?
                    .checked_add(file.path.len().checked_mul(16)?)
            });
        let outputs = outputs.ok_or(budget::host_limit("PAR3 repair result paths"))?;
        self.check_pending_capacity(job_id, WorkKey::Repair(set))?;
        let input = PendingInput::Repair { set, path };
        let reservation = assessment::ViewReservation::acquire(
            input
                .retained_cost()?
                .checked_add(outputs)
                .ok_or(budget::host_limit("PAR3 repair result paths"))?,
        )?;
        let cohorts = view
            .requirements
            .iter()
            .filter(|need| need.lost != 0)
            .count() as u64;
        let job = self.jobs.get_mut(&job_id).expect("assessed job");
        job.repair_cohorts = cohorts;
        job.pending
            .insert(WorkKey::Repair(set), QueuedInput::new(input, reservation));
        self.dispatch()
    }

    pub(super) fn is_installing(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|job| job.installing)
    }

    pub(in crate::pipeline) fn set_repair_phase(&mut self, job_id: JobId, owned: bool) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.repair_phase = owned;
        }
    }

    pub(in crate::pipeline) fn owns_repair_phase(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|job| job.repair_phase)
    }

    pub(super) fn queue_readback(
        &mut self,
        job_id: JobId,
        installation: Box<readback::Installation>,
    ) -> EngineResult<()> {
        self.check_pending_capacity(job_id, WorkKey::Readback)?;
        let reservation = assessment::ViewReservation::acquire(readback::STRIPE_RESERVATION)?;
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("missing PAR3 installation job"))?;
        if job.ticket.is_some() || job.pending.contains_key(&WorkKey::Readback) {
            return Err(EngineError::InvalidState("PAR3 readback already queued"));
        }
        job.installing = true;
        job.pending.insert(
            WorkKey::Readback,
            QueuedInput::new(PendingInput::Readback(installation), reservation),
        );
        self.dispatch()
    }

    pub(super) fn take_readback(
        &mut self,
        job_id: JobId,
    ) -> Option<EngineResult<readback::ReadbackDone>> {
        self.jobs.get_mut(&job_id)?.completed_readback.take()
    }

    pub(super) fn finish_installation(&mut self, job_id: JobId) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.installing = false;
            job.repair_phase = false;
        }
    }

    pub(super) fn take_repair_result(&mut self, job_id: JobId) -> Option<RepairCompletion> {
        self.jobs.get_mut(&job_id)?.completed_repair.take()
    }

    pub(super) fn take_repair_retry(&mut self, job_id: JobId) -> bool {
        self.jobs
            .get_mut(&job_id)
            .is_some_and(|job| std::mem::take(&mut job.retry_repair))
    }

    pub(super) fn queue_reassessment(&mut self, job_id: JobId) -> EngineResult<()> {
        let reservation = assessment::ViewReservation::acquire(1024)?;
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("unknown PAR3 job"))?;
        job.pending.insert(
            WorkKey::Assess,
            QueuedInput::new(PendingInput::Assess, reservation),
        );
        self.dispatch()
    }

    pub(super) fn error(&self, job_id: JobId) -> Option<&EngineError> {
        let job = self.jobs.get(&job_id)?;
        job.errors.values().next().or(job.donor_error.as_ref())
    }

    pub(super) fn take_spill(&mut self, job_id: JobId) -> Option<SourceId> {
        let job = self.jobs.get_mut(&job_id)?;
        if job.ticket.is_some() || job.installing || job.completed_repair.is_some() {
            return None;
        }
        job.spill.take()
    }

    /// The refusal that forced the pending spill, when the engine measured
    /// one. A host-side ceiling refuses without a native measurement, so the
    /// caller must still have a verdict for `None`.
    pub(super) fn take_spill_limit(
        &mut self,
        job_id: JobId,
    ) -> Option<par3_rs::runtime::ResourceLimit> {
        self.jobs.get_mut(&job_id)?.spill_limit.take()
    }

    pub(super) fn reserve_spill_disk(
        &mut self,
        job_id: JobId,
        bytes: u64,
        available: u64,
        reserve: u64,
    ) -> EngineResult<()> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("PAR3 spill job disappeared"))?;
        job.spill_disk = Some(budget::DiskReservation::acquire(bytes, available, reserve)?);
        Ok(())
    }

    pub(in crate::pipeline) fn take_spill_disk(
        &mut self,
        job_id: JobId,
    ) -> Option<budget::DiskReservation> {
        self.jobs.get_mut(&job_id)?.spill_disk.take()
    }

    /// Re-arm a refused spill so the peer's handback drives another attempt,
    /// and start or continue this job's wait for PAR3 memory. There is no
    /// timer here: the wait ends when a peer hands its work unit back and the
    /// completion check runs again.
    pub(in crate::pipeline) fn park_for_memory(
        &mut self,
        job_id: JobId,
        source: SourceId,
        limit: Option<par3_rs::runtime::ResourceLimit>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let now_ms = self.metrics.now_ms();
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        job.spill = Some(source);
        // The refusal is restored with the spill: the retry after the peer
        // hands back classifies against the same measurement.
        job.spill_limit = limit;
        if job.waiting_for_memory_since.is_none() {
            job.waiting_for_memory_since = Some(std::time::Instant::now());
            self.metrics
                .par3
                .waiting_for_memory_active
                .fetch_add(1, Relaxed);
        }
        self.metrics
            .par3
            .store_phase(job_id.0, Par3Phase::AwaitingMemory, now_ms);
    }

    /// Every job currently parked on PAR3 memory. A handback frees the share
    /// of the budget they collided with, so each of them is owed another
    /// completion check whether or not it is the job that handed back.
    pub(in crate::pipeline) fn jobs_awaiting_memory(&self) -> Vec<JobId> {
        self.jobs
            .iter()
            .filter(|(_, job)| job.waiting_for_memory_since.is_some())
            .map(|(&job_id, _)| job_id)
            .collect()
    }

    /// End this job's memory wait, crediting however long it lasted. Safe to
    /// call for a job that was never waiting.
    pub(in crate::pipeline) fn resume_from_memory(&mut self, job_id: JobId) {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let Some(since) = job.waiting_for_memory_since.take() else {
            return;
        };
        let par3 = &self.metrics.par3;
        par3.waiting_for_memory_active.fetch_sub(1, Relaxed);
        par3.waiting_for_memory_ms_total
            .fetch_add(as_millis(since.elapsed()), Relaxed);
    }

    /// Occupy a coordinator slot with one real carrier work unit, the way the
    /// dispatcher does, so a peer collision can be staged from the pipeline.
    #[cfg(test)]
    pub(in crate::pipeline) fn force_dispatch(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_complete_carrier(job_id, source, path)?;
        self.dispatch()
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn force_spill(
        &mut self,
        job_id: JobId,
        source: SourceId,
        limit: Option<par3_rs::runtime::ResourceLimit>,
    ) {
        self.admit(job_id).unwrap();
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.spill = Some(source);
        job.spill_limit = limit;
    }

    pub(super) fn release_spilled_images(
        &mut self,
        job_id: JobId,
        sources: &[SourceId],
    ) -> EngineResult<()> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return Ok(());
        };
        if job.ticket.is_some() {
            return Err(EngineError::InvalidState("PAR3 spill still has a reader"));
        }
        for &source in sources {
            job.sources.release_withdrawn_image(source)?;
            job.errors.remove(&source);
        }
        if let Some(runtime) = job.runtime.as_ref() {
            runtime.virtual_readers.clear()?;
        }
        Ok(())
    }

    /// Called only after the scheduler has exhausted available parity.
    pub(in crate::pipeline) fn request_donor_search(
        &mut self,
        job_id: JobId,
    ) -> EngineResult<bool> {
        let Some(job) = self.jobs.get(&job_id) else {
            return Ok(false);
        };
        let Some(runtime) = job.runtime.as_ref() else {
            return Ok(false);
        };
        if runtime.donor_search.exhaustive
            || runtime.donor_search.exhausted
            || job.pending.contains_key(&WorkKey::Donors)
        {
            return Ok(false);
        }
        self.check_pending_capacity(job_id, WorkKey::Donors)?;
        let reservation = assessment::ViewReservation::acquire(1024)?;
        self.jobs
            .get_mut(&job_id)
            .expect("known job")
            .pending
            .insert(
                WorkKey::Donors,
                QueuedInput::new(PendingInput::Donors, reservation),
            );
        self.dispatch()?;
        Ok(true)
    }

    pub(super) fn donor_search_exhausted(&self, job_id: JobId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .is_some_and(|runtime| runtime.donor_search.exhausted)
    }

    pub(in crate::pipeline) fn is_promoted(&self, job_id: JobId, file_index: u32) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&SourceId(u64::from(file_index))))
            .is_some_and(|source| !source.promoted.is_empty())
    }

    pub(in crate::pipeline) fn article_promoted(
        &self,
        job_id: JobId,
        file_index: u32,
        ordinal: u32,
    ) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&SourceId(u64::from(file_index))))
            .is_some_and(|source| source.promoted.contains_key(&ordinal))
    }

    pub(super) fn needed_offset(&self, job_id: JobId, file_index: u32) -> Option<u64> {
        self.jobs
            .get(&job_id)?
            .runtime
            .as_ref()?
            .carriers
            .get(&SourceId(u64::from(file_index)))?
            .needed
    }

    pub(super) fn promote(
        &mut self,
        job_id: JobId,
        file_index: u32,
        ordinal: u32,
    ) -> EngineResult<()> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("unknown PAR3 job"))?;
        let source = SourceId(u64::from(file_index));
        if !job.known.contains_key(&source) {
            if job.known.len() >= MAX_PENDING {
                return Err(budget::host_limit("PAR3 recovery candidates"));
            }
            job.known.insert(
                source,
                KnownSource {
                    carrier: true,
                    protected: false,
                    embedded_start: None,
                    complete_disk_image: false,
                    promoted: BTreeMap::new(),
                    _reservation: assessment::ViewReservation::acquire(512)?,
                },
            );
        }
        let known = job.known.get_mut(&source).expect("admitted source");
        if let std::collections::btree_map::Entry::Vacant(entry) = known.promoted.entry(ordinal) {
            entry.insert(assessment::ViewReservation::acquire(96)?);
        }
        Ok(())
    }

    pub(super) fn is_carrier(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&source))
            .is_some_and(|source| source.carrier)
    }

    /// Identity learned from an authenticated layout, not a verification claim.
    /// Ordinary publication changes invalidate evidence but preserve this binding.
    pub(super) fn protects_source(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&source))
            .is_some_and(|source| source.protected)
    }

    pub(super) fn knows_source(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .is_some_and(|job| job.known.contains_key(&source))
    }

    pub(super) fn dirty_sources(&self, job_id: JobId) -> Vec<SourceId> {
        self.jobs
            .get(&job_id)
            .filter(|job| job.ticket.is_none() && !job.installing)
            .map(|job| {
                job.dirty
                    .iter()
                    .filter(|source| !job.pending.contains_key(&WorkKey::Source(**source)))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn invalidate_source(
        &mut self,
        job_id: JobId,
        source: SourceId,
    ) -> EngineResult<()> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return Ok(());
        };
        if !job.known.contains_key(&source) {
            return Ok(());
        }
        let epoch = job
            .epoch
            .checked_add(1)
            .ok_or(budget::host_limit("PAR3 source epochs"))?;
        job.sources.withdraw(source)?;
        job.known
            .get_mut(&source)
            .expect("known source")
            .complete_disk_image = false;
        job.epoch = epoch;
        job.pending.remove(&WorkKey::Source(source));
        job.pending
            .retain(|key, _| !matches!(key, WorkKey::Repair(_)));
        job.dirty.insert(source);
        if let Some(runtime) = job.runtime.as_mut() {
            for set in runtime.sets.values_mut() {
                set.invalidate(source);
            }
            runtime.carriers.remove(&source);
        }
        Ok(())
    }

    pub(super) fn take_materialized_sources(
        &mut self,
        job_id: JobId,
    ) -> Option<MaterializedSources> {
        self.jobs
            .get_mut(&job_id)
            .map(|job| MaterializedSources(std::mem::take(&mut job.materialized)))
    }

    pub(super) fn restore_materialized_sources(
        &mut self,
        job_id: JobId,
        sources: MaterializedSources,
    ) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.materialized = sources.0;
        }
    }

    pub(super) fn invalidate_bindings(&mut self, job_id: JobId) -> EngineResult<()> {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.materialized.clear();
            for source in job.known.values_mut() {
                source.protected = false;
            }
        }
        let sources: Vec<_> = self
            .jobs
            .get(&job_id)
            .into_iter()
            .flat_map(|job| job.known.keys().copied())
            .collect();
        for source in sources {
            self.invalidate_source(job_id, source)?;
        }
        Ok(())
    }

    pub(in crate::pipeline) fn note_materialized_ranges(
        &mut self,
        job_id: JobId,
        source: SourceId,
        extents: &[(u64, u64)],
    ) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        job.materialized.remove(&source);
        let result = (|| {
            let bytes = extents
                .len()
                .checked_mul(32)
                .and_then(|bytes| bytes.checked_add(256))
                .ok_or(budget::host_limit("PAR3 materialized ranges"))?;
            let reservation = assessment::ViewReservation::acquire(bytes)?;
            let ranges = extents
                .iter()
                .filter(|(_, len)| *len != 0)
                .map(|&(offset, len)| {
                    offset
                        .checked_add(len)
                        .map(|end| offset..end)
                        .ok_or(budget::host_limit("PAR3 materialized offsets"))
                })
                .collect::<EngineResult<Vec<_>>>()?;
            Ok(MaterializedRanges {
                ranges,
                _reservation: reservation,
            })
        })();
        job.materialized.insert(source, result);
    }

    pub(super) fn materialized_ranges(
        &self,
        job_id: JobId,
        source: SourceId,
    ) -> EngineResult<Option<&[std::ops::Range<u64>]>> {
        match self
            .jobs
            .get(&job_id)
            .and_then(|job| job.materialized.get(&source))
        {
            Some(Ok(entry)) => Ok(Some(entry.ranges.as_slice())),
            Some(Err(_)) => Err(budget::host_limit("PAR3 materialized ranges")),
            None => Ok(None),
        }
    }

    pub(super) fn name_match_source(&self, job_id: JobId) -> Option<SourceId> {
        if self.has_work(job_id) {
            return None;
        }
        self.jobs
            .get(&job_id)
            .filter(|job| job.errors.is_empty())
            .and_then(|job| job.runtime.as_ref())
            .and_then(|runtime| runtime.name_search.source())
    }

    pub(super) fn take_name_match(
        &mut self,
        job_id: JobId,
    ) -> EngineResult<Option<placement::NameMatch>> {
        if self.has_work(job_id) {
            return Ok(None);
        }
        match self
            .jobs
            .get_mut(&job_id)
            .and_then(|job| job.runtime.as_mut())
        {
            Some(runtime) => runtime.take_name_match(),
            None => Ok(None),
        }
    }

    pub(in crate::pipeline) fn assessments(
        &self,
        job_id: JobId,
    ) -> impl Iterator<Item = (par3_rs::InputSetId, &assessment::AssessmentView)> {
        self.jobs
            .get(&job_id)
            .filter(|job| {
                !job.installing
                    && job.ticket.is_none()
                    && job.pending.is_empty()
                    && job.errors.is_empty()
                    && job.dirty.is_empty()
            })
            .and_then(|job| job.runtime.as_ref())
            .into_iter()
            .flat_map(|runtime| {
                runtime
                    .sets
                    .iter()
                    .filter_map(|(&id, set)| set.view.as_ref().map(|view| (id, view)))
                    .chain(runtime.dormant_views.iter().map(|(&id, view)| (id, view)))
            })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn source_verifications(&self, job_id: JobId) -> u64 {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map_or(0, |runtime| {
                runtime
                    .sets
                    .values()
                    .map(|set| set.native.diagnostics().source_verifications)
                    .sum()
            })
    }

    pub(in crate::pipeline) fn verified(&self, job_id: JobId) -> bool {
        let count = self.authenticated_set_count(job_id);
        count != 0
            && self
                .assessments(job_id)
                .filter(|(_, view)| view.status == par3_rs::session::RepairStatus::Complete)
                .count()
                == count
    }

    /// A terminal claim must name a bound protected source. A clean job does
    /// not establish evidence for its unprotected files or recovery carriers.
    pub(in crate::pipeline) fn verified_file(&self, job_id: JobId, source: SourceId) -> bool {
        self.verified(job_id) && self.source_verified(job_id, source)
    }

    /// Current native evidence for one bound source can survive damage in a
    /// sibling. Terminal delivery still requires the aggregate verdict above.
    pub(in crate::pipeline) fn source_verified(&self, job_id: JobId, source: SourceId) -> bool {
        self.assessments(job_id)
            .any(|(_, view)| view.verified_sources.contains(&source))
    }

    fn enqueue_input(
        &mut self,
        job_id: JobId,
        source: SourceId,
        input: PendingInput,
    ) -> EngineResult<()> {
        if !self.jobs.contains_key(&job_id) && self.jobs.len() >= MAX_JOBS {
            return Err(budget::host_limit("PAR3 job count"));
        }
        self.check_pending_capacity(job_id, WorkKey::Source(source))?;
        let reservation = assessment::ViewReservation::acquire(input.retained_cost()?)?;
        let job = self.jobs.entry(job_id).or_default();
        if !job.known.contains_key(&source) && job.known.len() >= MAX_PENDING {
            return Err(budget::host_limit("PAR3 known sources"));
        }
        let carrier = matches!(
            input,
            PendingInput::Carrier { .. } | PendingInput::Embedded { .. }
        );
        // This records byte availability, never a hash verdict. Explicit full
        // disk publications supersede article holes until a later source write
        // or identity rebind withdraws the publication.
        let complete_disk_image = matches!(
            input,
            PendingInput::CompleteFile { .. }
                | PendingInput::Carrier { ranges: None, .. }
                | PendingInput::Embedded { ranges: None, .. }
        );
        let embedded_start = match &input {
            PendingInput::Embedded { start, .. } => Some(*start),
            _ => None,
        };
        if let Some(known) = job.known.get_mut(&source) {
            known.carrier = carrier;
            known.embedded_start = embedded_start;
            known.complete_disk_image = complete_disk_image;
        } else {
            let reservation = assessment::ViewReservation::acquire(512)?;
            job.known.insert(
                source,
                KnownSource {
                    carrier,
                    protected: false,
                    embedded_start,
                    complete_disk_image,
                    promoted: BTreeMap::new(),
                    _reservation: reservation,
                },
            );
        }
        job.pending.insert(
            WorkKey::Source(source),
            QueuedInput::new(input, reservation),
        );
        Ok(())
    }

    fn check_pending_capacity(&self, job_id: JobId, key: WorkKey) -> EngineResult<()> {
        let already_pending = self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.pending.contains_key(&key));
        if !already_pending
            && self
                .jobs
                .values()
                .map(|job| job.pending.len())
                .sum::<usize>()
                >= MAX_PENDING
        {
            return Err(budget::host_limit("pending PAR3 work"));
        }
        Ok(())
    }

    /// Reclaim the least recently used recovery-waiting sessions before
    /// admitting another work unit. PAR2 budgets and policy are independent.
    fn evict_idle_sessions(&mut self, protected: JobId, headroom: usize) {
        let Some(memory) = self
            .jobs
            .get(&protected)
            .and_then(|job| job.runtime.as_ref())
            .map(|runtime| runtime.options.memory.clone())
        else {
            return;
        };
        while memory.available() < headroom {
            let victim = self
                .jobs
                .iter()
                .filter(|(id, job)| {
                    **id != protected
                        && job.ticket.is_none()
                        && !job.installing
                        && job.pending.is_empty()
                        && job.dirty.is_empty()
                        && job.errors.is_empty()
                        && job.completed_repair.is_none()
                        && job.completed_readback.is_none()
                        && job.runtime.as_ref().is_some_and(|runtime| {
                            !runtime.sets.is_empty()
                                && runtime.name_search.source().is_none()
                                && runtime.sets.values().all(|set| {
                                    set.view.as_ref().is_some_and(|view| {
                                        view.status == par3_rs::session::RepairStatus::NeedRecovery
                                    })
                                })
                        })
                })
                .min_by_key(|(id, job)| (job.last_used, **id))
                .map(|(&id, _)| id);
            let Some(victim) = victim else {
                break;
            };
            let before = memory.used();
            self.jobs
                .get_mut(&victim)
                .expect("idle victim")
                .runtime
                .as_mut()
                .expect("retained idle runtime")
                .evict_native_sessions();
            tracing::debug!(
                job_id = victim.0,
                released_bytes = before.saturating_sub(memory.used()),
                "evicted idle PAR3 sessions; current arrivals will reopen native analysis"
            );
        }
    }

    pub(super) fn dispatch(&mut self) -> EngineResult<()> {
        for _ in 0..2 {
            self.dispatch_one()?;
        }
        Ok(())
    }

    fn dispatch_one(&mut self) -> EngineResult<()> {
        self.publish_dispatch_gauges();
        let available = self
            .cpu_limit
            .saturating_sub(self.worker_allowances.values().sum::<usize>());
        let slots_full = self.in_flight.len() >= 2
            || self.in_flight.values().any(|(id, _)| {
                self.jobs
                    .get(id)
                    .is_some_and(|job| job.retry_serial.is_some())
            });
        if slots_full || available == 0 {
            // Only a job that actually has queued work is being held back; an
            // idle coordinator is not waiting for anything.
            let waiting: Vec<JobId> = self
                .jobs
                .iter()
                .filter(|(_, job)| job.ticket.is_none() && !job.pending.is_empty())
                .map(|(&id, _)| id)
                .collect();
            if !waiting.is_empty() {
                let counter = if slots_full {
                    &self.metrics.par3.dispatch_refused_slots_total
                } else {
                    &self.metrics.par3.dispatch_refused_cpu_total
                };
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let now = self.metrics.now_ms();
                for job_id in waiting {
                    self.metrics
                        .par3
                        .store_phase(job_id.0, Par3Phase::AwaitingCpuAllowance, now);
                }
            }
            return Ok(());
        }
        let ready = |job: &&JobSlot| {
            job.ticket.is_none()
                && (job.retry_serial.is_none() || self.in_flight.is_empty())
                && (job.spill.is_none() || job.installing)
                && if job.installing {
                    job.pending.contains_key(&WorkKey::Readback)
                } else {
                    !job.pending.is_empty()
                }
        };
        let next = self
            .jobs
            .iter()
            .filter(|(_, job)| ready(job))
            .find(|(id, _)| self.last_job.is_none_or(|last| **id > last))
            .or_else(|| self.jobs.iter().find(|(_, job)| ready(job)))
            .map(|(&id, _)| id);
        let Some(job_id) = next else {
            return Ok(());
        };
        if self.jobs[&job_id].retry_serial.is_some() {
            // A peer may have returned its worker while retaining idle readers.
            // Return those cache leases too before the isolated retry; checked-
            // out readers remain owned until they actually return.
            for (&id, job) in &self.jobs {
                if id != job_id
                    && job.ticket.is_none()
                    && let Some(runtime) = &job.runtime
                {
                    runtime.virtual_readers.clear()?;
                }
            }
        }
        let contenders = self
            .jobs
            .values()
            .filter(ready)
            .count()
            .min(2 - self.in_flight.len());
        let workers = (available / contenders.max(1)).max(1);
        self.evict_idle_sessions(job_id, budget::budgets().native.limit() / 2);
        let job = self.jobs.get_mut(&job_id).expect("selected ready job");
        let ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or(budget::host_limit("PAR3 worker tickets"))?;
        let Some(mut runtime) = job.runtime.take() else {
            return Err(EngineError::InvalidState(
                "PAR3 session already owned by a worker",
            ));
        };
        runtime.options.workers = workers;
        let stripe_bytes = runtime.options.stripe_bytes as u64;
        let serial_retry = job.retry_serial.is_some();
        // One load of the engine's own counters per dispatch. Every PAR3
        // number weaver publishes is a delta between this sample and the one
        // taken at handback, so no engine loop ever touches weaver's metrics.
        job.engine_baseline = EngineCounters::capture(&runtime);
        let metrics = Arc::clone(&self.metrics);
        runtime.options.progress.get_or_insert_with(move || {
            // At most two relaxed atomic operations per event, no lock, no log,
            // no allocation, and no path that can panic. The engine calls this
            // synchronously from its own bounded work units.
            par3_rs::runtime::ProgressCallback::new(move |event| {
                let now = metrics.now_ms();
                match event.phase {
                    par3_rs::runtime::ProgressPhase::Begin
                    | par3_rs::runtime::ProgressPhase::End => {
                        metrics
                            .par3
                            .note_engine_phase(job_id.0, engine_phase(event.stage), now);
                    }
                    // `completed` is cumulative within the scope, so adding it
                    // here would over-count. The bytes come from the Repair
                    // stage's own total at handback instead.
                    par3_rs::runtime::ProgressPhase::Advance => {
                        metrics.par3.note_progress(job_id.0, now);
                    }
                }
            })
        });
        for set in runtime.sets.values_mut() {
            if let Err(error) = set
                .native
                .set_execution_limits(workers, runtime.options.stripe_bytes)
            {
                job.runtime = Some(runtime);
                return Err(error);
            }
        }
        if !runtime.dormant_views.is_empty() {
            job.verification = None;
        }
        self.next_ticket = ticket;
        self.last_job = Some(job_id);
        let (key, input) = if job.installing {
            (
                WorkKey::Readback,
                job.pending
                    .remove(&WorkKey::Readback)
                    .expect("pending readback"),
            )
        } else {
            job.pending.pop_first().expect("pending input")
        };
        let epoch = job.epoch;
        let assess =
            job.pending.is_empty() && job.dirty.iter().all(|id| key == WorkKey::Source(*id));
        job.ticket = Some(ticket);
        self.in_flight
            .insert(ticket, (job_id, runtime.options.cancel.clone()));
        if self.in_flight.len() > 1 {
            self.contended.extend(self.in_flight.keys());
        }
        self.worker_allowances.insert(ticket, workers);
        {
            use std::sync::atomic::Ordering::Relaxed;
            let par3 = &self.metrics.par3;
            let now = self.metrics.now_ms();
            par3.store_phase(job_id.0, pending_phase(&input.input), now);
            par3.effective_stripe_bytes.store(stripe_bytes, Relaxed);
            par3.dispatch_wait_ms_total
                .fetch_add(as_millis(input.queued_at.elapsed()), Relaxed);
            if serial_retry {
                par3.verify_serial_fallback_total.fetch_add(1, Relaxed);
            }
            // Source publication is not counted here: the engine's own
            // read counters cover it and arrive as a delta at handback.
            let started = match key {
                WorkKey::Repair(_) => Some(&par3.repairs_started_total),
                WorkKey::Donors => Some(&par3.donor_searches_total),
                WorkKey::Assess => Some(&par3.reassessments_total),
                WorkKey::Readback => Some(&par3.readback_windows_total),
                WorkKey::Source(_) => None,
            };
            if let Some(counter) = started {
                counter.fetch_add(1, Relaxed);
            }
        }
        tracing::debug!(
            job_id = job_id.0,
            ticket,
            workers,
            active_jobs = self.in_flight.len(),
            "PAR3 worker admitted"
        );
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let _timing = WorkTiming {
                    job_id,
                    key,
                    queued_at: input.queued_at,
                    started: std::time::Instant::now(),
                };
                // Keep the queue lease live while the worker owns its input;
                // successful publication transfers it into retained state.
                if matches!(input.input, PendingInput::Donors | PendingInput::Assess) {
                    if matches!(input.input, PendingInput::Donors) {
                        runtime.donor_search.exhaustive = true;
                    }
                    let result = runtime.assess().map(|()| WorkOutput::Published);
                    return (runtime, result);
                }
                if let PendingInput::Readback(mut installation) = input.input {
                    let result = installation.read_unit(&runtime.sources, &runtime.options);
                    return (
                        runtime,
                        Ok(WorkOutput::Readback(readback::ReadbackDone {
                            installation,
                            result,
                            _reservation: input.reservation,
                        })),
                    );
                }
                if let PendingInput::Repair { set, path } = input.input {
                    let layout = runtime
                        .sets
                        .get_mut(&set)
                        .ok_or(EngineError::InvalidState("missing PAR3 repair set"))
                        .and_then(|set| set.native.layout())
                        .and_then(|layout| {
                            layout.ok_or(EngineError::InvalidState("missing PAR3 repair layout"))
                        });
                    let layout = match layout {
                        Ok(layout) => layout,
                        Err(error) => {
                            return (
                                runtime,
                                Ok(WorkOutput::Repaired(RepairCompletion {
                                    result: Err(error),
                                    outputs: Ok(Vec::new()),
                                    embedded_replacement: false,
                                    _reservation: Some(input.reservation),
                                })),
                            );
                        }
                    };
                    let result = runtime.repair(set, &path);
                    let installed = match &result {
                        Ok(report) => report.installed.as_slice(),
                        Err(EngineError::RepairInterrupted { installed, .. }) => {
                            installed.as_slice()
                        }
                        _ => &[],
                    };
                    let embedded_replacement = !installed.is_empty()
                        && layout.files().iter().any(|file| {
                            file.extents.iter().any(|extent| {
                                matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected)
                            })
                        });
                    let outputs = installed
                        .iter()
                        .map(|output| {
                            let file = layout
                                .files()
                                .iter()
                                .find(|file| path.join(&file.path) == output.path)
                                .ok_or(EngineError::InvalidState(
                                    "PAR3 output has no authenticated length",
                                ))?;
                            readback::VerifiedOutput::capture(
                                output.path.clone(),
                                file.len,
                                &runtime.options,
                            )
                        })
                        .collect();
                    return (
                        runtime,
                        Ok(WorkOutput::Repaired(RepairCompletion {
                            result,
                            outputs,
                            embedded_replacement,
                            _reservation: Some(input.reservation),
                        })),
                    );
                }
                let WorkKey::Source(source) = key else {
                    return (
                        runtime,
                        Err(EngineError::InvalidState("invalid PAR3 work key")),
                    );
                };
                let before = runtime.sources.revision(source).ok().flatten();
                let result = match input.input {
                    PendingInput::Virtual { image, name } => {
                        runtime.publish_virtual(source, image, name)
                    }
                    PendingInput::Repair { .. }
                    | PendingInput::Readback(_)
                    | PendingInput::Donors
                    | PendingInput::Assess => {
                        unreachable!("repair dispatched above")
                    }
                    PendingInput::CompleteFile { path, name } => std::fs::metadata(&path)
                        .map_err(EngineError::from)
                        .and_then(|metadata| {
                            let ranges = if metadata.len() == 0 {
                                Vec::new()
                            } else {
                                std::iter::once(0..metadata.len()).collect()
                            };
                            runtime.publish_file(source, path, name, ranges)
                        }),
                    PendingInput::Embedded {
                        path,
                        name,
                        ranges,
                        start,
                    } => runtime.scan_embedded(source, path, name, ranges, start),
                    PendingInput::Carrier { path, ranges } => {
                        runtime.scan_file(source, path, ranges)
                    }
                    PendingInput::File { path, name, ranges } => {
                        runtime.publish_file(source, path, name, ranges)
                    }
                };
                if let Ok(Some(after)) = runtime.sources.revision(source)
                    && Some(after) != before
                {
                    // Scanning can fail after publication succeeded. Keep its
                    // lease even on that exit; the ranges are still retained.
                    runtime.publication_memory.insert(source, input.reservation);
                }
                let result = result.and_then(|()| if assess { runtime.assess() } else { Ok(()) });
                (runtime, result.map(|()| WorkOutput::Published))
            })
            .await;
            let (runtime, result) = match result {
                Ok((runtime, result)) => (Some(runtime), result),
                Err(error) => (None, Err(std::io::Error::other(error).into())),
            };
            let _ = tx
                .send(RepairWorkDone::Par3(Box::new(WorkDone {
                    job_id,
                    ticket,
                    epoch,
                    key,
                    runtime,
                    result,
                })))
                .await;
        });
        Ok(())
    }

    #[cfg(test)]
    async fn recv(&mut self) -> Option<WorkDone> {
        match self
            .test_rx
            .as_mut()
            .expect("test coordinator receiver")
            .recv()
            .await?
        {
            RepairWorkDone::Par3(done) => Some(*done),
            RepairWorkDone::Par2(_) => panic!("PAR3 unit worker returned a PAR2 outcome"),
        }
    }

    /// Return the live job to recheck, or discard a forgotten/stale result.
    pub(super) fn settle(&mut self, done: WorkDone) -> Option<JobId> {
        let (job_id, _) = self.in_flight.get(&done.ticket)?;
        if *job_id != done.job_id {
            return None;
        }
        self.in_flight.remove(&done.ticket);
        self.worker_allowances.remove(&done.ticket);
        let contended = self.contended.remove(&done.ticket);
        let job = self.jobs.get_mut(&done.job_id)?;
        if job.ticket != Some(done.ticket) {
            return None;
        }
        job.ticket = None;
        job.last_used = done.ticket;
        let mut runtime = done.runtime.unwrap_or_default();
        {
            use std::sync::atomic::Ordering::Relaxed;
            let par3 = &self.metrics.par3;
            // The whole engine fold for this work unit: one capture, one set
            // of deltas, one pass of `fetch_add`. Nothing below runs per byte.
            let counters = EngineCounters::capture(&runtime);
            let read_bytes = counters
                .source_read_bytes
                .saturating_sub(job.engine_baseline.source_read_bytes);
            if done.key == WorkKey::Assess && read_bytes == 0 {
                par3.reassessments_zero_read_total.fetch_add(1, Relaxed);
            }
            if done.epoch != job.epoch {
                par3.reverify_generation_changed_total.fetch_add(1, Relaxed);
            }
            if done.key == WorkKey::Donors && runtime.donor_search.exhausted {
                par3.donor_search_exhausted_total.fetch_add(1, Relaxed);
            }
            if let Ok(WorkOutput::Readback(readback::ReadbackDone {
                result: Ok(readback::ReadbackUnit::Stripe(span)),
                ..
            })) = &done.result
            {
                par3.readback_bytes_total.fetch_add(span.len, Relaxed);
            }
            if matches!(done.key, WorkKey::Repair(_))
                && let Ok(WorkOutput::Repaired(completion)) = &done.result
            {
                if matches!(
                    completion.result,
                    Err(EngineError::Cancelled | EngineError::RepairInterrupted { .. })
                ) {
                    par3.repair_cancelled_total.fetch_add(1, Relaxed);
                } else if completion.result.is_ok() {
                    // The cohorts this repair was dispatched against are only
                    // processed once its report comes back whole.
                    let cohorts = std::mem::take(&mut job.repair_cohorts);
                    if cohorts != 0 {
                        par3.repair_cohorts_processed_total
                            .fetch_add(cohorts, Relaxed);
                    }
                }
            }
            counters.apply(job.engine_baseline, &self.metrics);
            let rejected_packets = counters
                .packets_rejected
                .saturating_sub(job.engine_baseline.packets_rejected);
            let unavailable_ranges = counters
                .ranges_unavailable
                .saturating_sub(job.engine_baseline.ranges_unavailable);
            let damaged_bytes = counters
                .damaged_bytes
                .saturating_sub(job.engine_baseline.damaged_bytes);
            job.engine_baseline = counters;
            if (rejected_packets, unavailable_ranges, damaged_bytes) != (0, 0, 0) {
                // Informational: it never fails the job on its own, but it is
                // the last verdict the job reached and worth reporting.
                let damage = outcome::Par3Outcome::CarrierDamage {
                    carriers: runtime.damage_report(),
                    rejected_packets,
                    unavailable_ranges,
                    damaged_bytes,
                };
                // One line per job, not one per handback: a job that keeps
                // scanning a damaged carrier says this once.
                if !std::mem::replace(&mut job.damage_reported, true) {
                    tracing::warn!(job_id = done.job_id.0, summary = %damage, "PAR3 carrier damage");
                }
                if job.last_outcome.as_ref() != Some(&damage) {
                    par3.note_outcome(damage.class());
                    job.last_outcome = Some(damage);
                }
            }
        }
        if done.epoch != job.epoch {
            // A write raced this operation. Keep capacity until this handback,
            // retain unrelated evidence, and require fresh publication before
            // any returned scheduling view can be consumed.
            if let WorkKey::Source(source) = done.key {
                job.dirty.insert(source);
            }
            for &source in &job.dirty {
                for set in runtime.sets.values_mut() {
                    set.invalidate(source);
                }
                runtime.carriers.remove(&source);
            }
        } else if let WorkKey::Source(source) = done.key {
            job.dirty.remove(&source);
        }
        if done.epoch == job.epoch {
            for view in runtime.sets.values().filter_map(|set| set.view.as_ref()) {
                for source in view.files.iter().filter_map(|file| file.source) {
                    if let Some(known) = job.known.get_mut(&source) {
                        known.protected = true;
                    }
                }
            }
        }
        let deferred_release = std::mem::take(&mut job.acquisition.deferred_release);
        for (set, matrix, indices) in &deferred_release {
            if let Some(session) = runtime.sets.get_mut(set) {
                session.native.forget_recovery_in_flight(*matrix, indices);
            }
        }
        job.sources = runtime.sources.clone();
        job.runtime = Some(runtime);
        if !deferred_release.is_empty() {
            // The window drained while this worker was out; only a fresh
            // assessment can put what it released back on offer.
            match assessment::ViewReservation::acquire(1024) {
                Ok(reservation) => {
                    job.pending.insert(
                        WorkKey::Assess,
                        QueuedInput::new(PendingInput::Assess, reservation),
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        job_id = done.job_id.0,
                        %error,
                        "PAR3 reassessment after a deferred in-flight release could not be queued"
                    );
                }
            }
        }
        if done.epoch != job.epoch {
            if matches!(done.key, WorkKey::Repair(_)) {
                // Installation may have finished before the racing write. The
                // caller must not certify its outputs against the newer epoch.
                job.completed_repair = Some(RepairCompletion {
                    result: Err(EngineError::InvalidState(
                        "PAR3 repair output changed before handback",
                    )),
                    outputs: Ok(Vec::new()),
                    embedded_replacement: false,
                    _reservation: None,
                });
            } else if done.key == WorkKey::Readback {
                job.completed_readback = Some(Err(EngineError::InvalidState(
                    "PAR3 readback changed before handback",
                )));
            }
            return Some(done.job_id);
        }
        let native_pressure = match &done.result {
            Err(error) => budget::is_native_pressure(error),
            Ok(WorkOutput::Repaired(completion)) => completion
                .result
                .as_ref()
                .err()
                .is_some_and(budget::is_native_pressure),
            _ => false,
        };
        // A repair retry can perform an assessment first. Only the operation
        // that needed isolation consumes the reservation; afterward this job
        // participates in ordinary shared scheduling again.
        if !contended && job.retry_serial == Some(done.key) {
            job.retry_serial = None;
        }
        if contended && native_pressure {
            job.retry_serial = Some(done.key);
            job.retry_repair = matches!(done.key, WorkKey::Repair(_));
            if let WorkKey::Source(source) = done.key {
                job.dirty.insert(source);
                job.errors.remove(&source);
                return Some(done.job_id);
            }
            if matches!(done.key, WorkKey::Assess | WorkKey::Donors) {
                let input = if done.key == WorkKey::Donors {
                    PendingInput::Donors
                } else {
                    PendingInput::Assess
                };
                match assessment::ViewReservation::acquire(1024) {
                    Ok(reservation) => {
                        job.pending
                            .insert(done.key, QueuedInput::new(input, reservation));
                    }
                    Err(error) => {
                        job.donor_error = Some(error);
                    }
                }
                return Some(done.job_id);
            }
            // Repair reports still pass through installation reconciliation.
        }
        // The refusal travels with the source it fenced: the verdict this
        // spill eventually reaches is classified from the engine's own
        // measurement, which is only in hand here.
        let refusal = match &done.result {
            Err(error)
                if matches!(
                    done.key,
                    WorkKey::Source(_) | WorkKey::Donors | WorkKey::Repair(_)
                ) =>
            {
                budget::error_pressure_source(error)
                    .or_else(|| match done.key {
                        WorkKey::Source(source) if budget::is_host_pressure(error) => Some(source),
                        _ => None,
                    })
                    .map(|source| (source, Some(error)))
            }
            Ok(WorkOutput::Repaired(completion)) => {
                completion.result.as_ref().err().and_then(|error| {
                    budget::error_pressure_source(error).map(|source| (source, Some(error)))
                })
            }
            _ => None,
        };
        let pressure = refusal.map(|(source, _)| source);
        if let Some((source, error)) = refusal {
            // Fence dispatch now, but preserve repair reports: installed files
            // must still pass reconciliation before the completion gate spills.
            job.spill.get_or_insert(source);
            if let Some(limit) = error.and_then(budget::engine_limit) {
                job.spill_limit.get_or_insert(limit);
            }
            let stage = match done.key {
                WorkKey::Donors => "donor_search",
                WorkKey::Repair(_) => "repair",
                _ => "assessment",
            };
            tracing::debug!(
                job_id = done.job_id.0,
                source = source.0,
                stage,
                "PAR3 virtual reader requires disk fallback"
            );
        }
        match (done.key, done.result) {
            (WorkKey::Donors | WorkKey::Assess, result) => {
                job.donor_error = if pressure.is_some() {
                    None
                } else {
                    result.err()
                };
            }
            (WorkKey::Readback, result) => {
                job.completed_readback = Some(result.and_then(|output| match output {
                    WorkOutput::Readback(done) => Ok(done),
                    _ => Err(EngineError::InvalidState("missing PAR3 readback result")),
                }));
            }
            (WorkKey::Repair(_), result) => {
                job.completed_repair = Some(match result {
                    Ok(WorkOutput::Repaired(completion)) => completion,
                    other => RepairCompletion {
                        result: Err(other
                            .err()
                            .unwrap_or(EngineError::InvalidState("missing PAR3 repair report"))),
                        outputs: Ok(Vec::new()),
                        embedded_replacement: false,
                        _reservation: None,
                    },
                });
            }
            (WorkKey::Source(source), Ok(_)) => {
                job.errors.remove(&source);
            }
            (WorkKey::Source(source), Err(error)) => {
                if pressure.is_some() {
                    job.errors.remove(&source);
                } else {
                    tracing::warn!(job_id = done.job_id.0, source = source.0, %error, "PAR3 carrier discovery incomplete");
                    job.errors.insert(source, error);
                }
            }
        }
        Some(done.job_id)
    }

    pub(in crate::pipeline) fn forget(&mut self, job_id: JobId) {
        self.resume_from_memory(job_id);
        self.metrics
            .par3
            .store_phase(job_id.0, Par3Phase::Idle, self.metrics.now_ms());
        self.jobs.remove(&job_id);
        for (owner, token) in self.in_flight.values() {
            if *owner == job_id {
                token.cancel();
            }
        }
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        for (_, token) in self.in_flight.values() {
            token.cancel();
        }
    }
}

mod verification;

#[cfg(test)]
mod pressure_tests;
#[cfg(test)]
mod tests;
