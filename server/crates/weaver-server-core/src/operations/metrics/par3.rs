//! PAR3 recovery telemetry: relaxed atomics only, fixed storage, no allocation.
//!
//! Every field here is written from either the pipeline actor or a blocking
//! PAR3 worker's handback. Nothing in this module may be touched from a
//! per-byte, per-block or per-stripe loop: the engine already counts those in
//! its own relaxed atomics, and this layer only folds in the deltas once a
//! work unit is handed back. The single exception is the progress handler,
//! which is capped at two relaxed atomic operations per event.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

/// Work slots the coordinator can keep busy at once. This mirrors the
/// coordinator's own in-flight bound; a third job waiting for a slot reuses
/// the least recently advanced entry rather than growing this array.
pub const PAR3_SLOTS: usize = 2;

/// A non-idle phase whose last progress is older than this counts as a stall.
pub const PAR3_STALL_THRESHOLD_MS: u64 = 30_000;

/// What a PAR3 job is currently waiting on or doing, as an exported code.
///
/// The numeric encoding is part of the `/metrics` contract: the exporter emits
/// one series per phase and the code is what a stored gauge round-trips
/// through. Never renumber an existing variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3Phase {
    #[default]
    Idle,
    ScanningCarriers,
    ResolvingMetadata,
    Verifying,
    Assessing,
    AwaitingRecoveryArticles,
    AwaitingMemory,
    AwaitingCpuAllowance,
    AwaitingDiskFallback,
    DonorSearch,
    Repairing,
    Readback,
    Installing,
}

impl Par3Phase {
    pub const ALL: [Self; 13] = [
        Self::Idle,
        Self::ScanningCarriers,
        Self::ResolvingMetadata,
        Self::Verifying,
        Self::Assessing,
        Self::AwaitingRecoveryArticles,
        Self::AwaitingMemory,
        Self::AwaitingCpuAllowance,
        Self::AwaitingDiskFallback,
        Self::DonorSearch,
        Self::Repairing,
        Self::Readback,
        Self::Installing,
    ];

    pub const fn as_code(self) -> usize {
        match self {
            Self::Idle => 0,
            Self::ScanningCarriers => 1,
            Self::ResolvingMetadata => 2,
            Self::Verifying => 3,
            Self::Assessing => 4,
            Self::AwaitingRecoveryArticles => 5,
            Self::AwaitingMemory => 6,
            Self::AwaitingCpuAllowance => 7,
            Self::AwaitingDiskFallback => 8,
            Self::DonorSearch => 9,
            Self::Repairing => 10,
            Self::Readback => 11,
            Self::Installing => 12,
        }
    }

    pub const fn from_code(code: usize) -> Self {
        match code {
            1 => Self::ScanningCarriers,
            2 => Self::ResolvingMetadata,
            3 => Self::Verifying,
            4 => Self::Assessing,
            5 => Self::AwaitingRecoveryArticles,
            6 => Self::AwaitingMemory,
            7 => Self::AwaitingCpuAllowance,
            8 => Self::AwaitingDiskFallback,
            9 => Self::DonorSearch,
            10 => Self::Repairing,
            11 => Self::Readback,
            12 => Self::Installing,
            _ => Self::Idle,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::ScanningCarriers => "scanning_carriers",
            Self::ResolvingMetadata => "resolving_metadata",
            Self::Verifying => "verifying",
            Self::Assessing => "assessing",
            Self::AwaitingRecoveryArticles => "awaiting_recovery_articles",
            Self::AwaitingMemory => "awaiting_memory",
            Self::AwaitingCpuAllowance => "awaiting_cpu_allowance",
            Self::AwaitingDiskFallback => "awaiting_disk_fallback",
            Self::DonorSearch => "donor_search",
            Self::Repairing => "repairing",
            Self::Readback => "readback",
            Self::Installing => "installing",
        }
    }

    pub const fn is_idle(self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Which admission budget refused a PAR3 reservation.
///
/// Indexes a fixed array; the order is the exported label order and must not
/// change once released.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3AdmissionReason {
    /// The engine's own retained native state (stripes, matrices, decoders).
    RetainedState,
    /// Authenticated metadata and layout storage.
    ResolvedMetadata,
    /// The host-side copy of one assessment.
    AssessmentView,
    /// Free space for the disk fallback of a refused in-memory image.
    DiskFallbackSpace,
    /// Per-job carrier count.
    CarrierCount,
    /// Per-job authenticated set count.
    SetCount,
    /// Any other resource ceiling the engine named.
    Other,
}

impl Par3AdmissionReason {
    pub const COUNT: usize = 7;

    pub const ALL: [Self; Self::COUNT] = [
        Self::RetainedState,
        Self::ResolvedMetadata,
        Self::AssessmentView,
        Self::DiskFallbackSpace,
        Self::CarrierCount,
        Self::SetCount,
        Self::Other,
    ];

    pub const fn index(self) -> usize {
        match self {
            Self::RetainedState => 0,
            Self::ResolvedMetadata => 1,
            Self::AssessmentView => 2,
            Self::DiskFallbackSpace => 3,
            Self::CarrierCount => 4,
            Self::SetCount => 5,
            Self::Other => 6,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetainedState => "retained_state",
            Self::ResolvedMetadata => "resolved_metadata",
            Self::AssessmentView => "assessment_view",
            Self::DiskFallbackSpace => "disk_fallback_space",
            Self::CarrierCount => "carrier_count",
            Self::SetCount => "set_count",
            Self::Other => "other",
        }
    }
}

/// Memory categories the engine's ledger is divided into. The count is the
/// engine's, so a new category is a compile error here rather than a silently
/// dropped series.
pub const PAR3_MEMORY_CATEGORIES: usize = par3_rs::runtime::MEMORY_CATEGORIES;

/// The engine's own stable category names, in ledger order. These are the
/// exported label values: the engine owns the vocabulary, weaver only carries
/// it, so a rename travels with the engine rather than being mirrored here.
pub fn par3_memory_category_names() -> [&'static str; PAR3_MEMORY_CATEGORIES] {
    par3_rs::runtime::MemoryCategory::ALL.map(|category| category.name())
}

/// How the engine classified an admission it refused.
///
/// Indexes a fixed array; the order is the exported label order and must not
/// change once released.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3EngineRefusal {
    /// The request does not fit this session's own ceiling.
    ExceedsLimit,
    /// The same request fits once another reservation releases.
    PeerContention,
    /// A ceiling that was never expressed in bytes.
    Unmeasured,
}

impl Par3EngineRefusal {
    pub const COUNT: usize = 3;

    pub const ALL: [Self; Self::COUNT] =
        [Self::ExceedsLimit, Self::PeerContention, Self::Unmeasured];

    pub const fn index(self) -> usize {
        match self {
            Self::ExceedsLimit => 0,
            Self::PeerContention => 1,
            Self::Unmeasured => 2,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExceedsLimit => "exceeds_limit",
            Self::PeerContention => "peer_contention",
            Self::Unmeasured => "unmeasured",
        }
    }
}

/// Which width had to give when a stage ran narrower than it was configured
/// to. These are not stalls: the engine never blocks on memory, it proceeds at
/// the width it could admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3EngineNarrowing {
    /// A codec stripe admitted below the configured stripe size.
    Stripe,
    /// A worker pool that could not be admitted, so the stage ran serially.
    Workers,
    /// A verification batch cut short because the next file was not admitted.
    VerifyBatch,
}

impl Par3EngineNarrowing {
    pub const COUNT: usize = 3;

    pub const ALL: [Self; Self::COUNT] = [Self::Stripe, Self::Workers, Self::VerifyBatch];

    pub const fn index(self) -> usize {
        match self {
            Self::Stripe => 0,
            Self::Workers => 1,
            Self::VerifyBatch => 2,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stripe => "stripe",
            Self::Workers => "workers",
            Self::VerifyBatch => "verify_batch",
        }
    }
}

/// Engine stages this layer folds back at handback. Deliberately a subset of
/// the engine's stage list: creation-only stages never run on the repair path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3Stage {
    Scan,
    Metadata,
    Verify,
    Assess,
    Placement,
    Repair,
    Checkpoint,
}

impl Par3Stage {
    pub const COUNT: usize = 7;

    pub const ALL: [Self; Self::COUNT] = [
        Self::Scan,
        Self::Metadata,
        Self::Verify,
        Self::Assess,
        Self::Placement,
        Self::Repair,
        Self::Checkpoint,
    ];

    pub const fn index(self) -> usize {
        match self {
            Self::Scan => 0,
            Self::Metadata => 1,
            Self::Verify => 2,
            Self::Assess => 3,
            Self::Placement => 4,
            Self::Repair => 5,
            Self::Checkpoint => 6,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Metadata => "metadata",
            Self::Verify => "verify",
            Self::Assess => "assess",
            Self::Placement => "placement",
            Self::Repair => "repair",
            Self::Checkpoint => "checkpoint",
        }
    }
}

/// Terminal classes a PAR3 job's assessment can reach. Indexes a fixed array;
/// the order is the exported label order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Par3OutcomeClass {
    Unrecoverable,
    NeedsRecovery,
    MetadataIncomplete,
    Unsupported,
    NotExecutable,
    WaitingForMemory,
    DoesNotFit,
    CarrierDamage,
    UnsafePath,
    NoOutputSpace,
}

impl Par3OutcomeClass {
    pub const COUNT: usize = 10;

    pub const ALL: [Self; Self::COUNT] = [
        Self::Unrecoverable,
        Self::NeedsRecovery,
        Self::MetadataIncomplete,
        Self::Unsupported,
        Self::NotExecutable,
        Self::WaitingForMemory,
        Self::DoesNotFit,
        Self::CarrierDamage,
        Self::UnsafePath,
        Self::NoOutputSpace,
    ];

    pub const fn index(self) -> usize {
        match self {
            Self::Unrecoverable => 0,
            Self::NeedsRecovery => 1,
            Self::MetadataIncomplete => 2,
            Self::Unsupported => 3,
            Self::NotExecutable => 4,
            Self::WaitingForMemory => 5,
            Self::DoesNotFit => 6,
            Self::CarrierDamage => 7,
            Self::UnsafePath => 8,
            Self::NoOutputSpace => 9,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unrecoverable => "unrecoverable",
            Self::NeedsRecovery => "needs_recovery",
            Self::MetadataIncomplete => "metadata_incomplete",
            Self::Unsupported => "unsupported",
            Self::NotExecutable => "not_executable",
            Self::WaitingForMemory => "waiting_for_memory",
            Self::DoesNotFit => "does_not_fit",
            Self::CarrierDamage => "carrier_damage",
            Self::UnsafePath => "unsafe_path",
            Self::NoOutputSpace => "no_output_space",
        }
    }
}

/// One PAR3 work slot's live phase: an opaque owner id beside an encoded
/// state.
#[derive(Debug, Default)]
pub struct Par3Slot {
    /// Owning job id, or zero when the slot is free.
    pub job_id: AtomicU64,
    /// [`Par3Phase`] code.
    pub phase: AtomicUsize,
    /// Milliseconds since `PipelineMetrics::start_time` at the phase change.
    pub phase_entered_ms: AtomicU64,
    /// Milliseconds since `PipelineMetrics::start_time` at the last progress.
    pub last_progress_ms: AtomicU64,
    /// Start of the stall in progress, or zero when this slot is not stalled.
    /// Written only by the snapshot tick.
    stall_started_ms: AtomicU64,
}

/// A slot's phase as read by the snapshot tick.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Par3SlotSnapshot {
    pub job_id: u64,
    pub phase: Par3Phase,
    pub phase_entered_ms: u64,
    pub last_progress_ms: u64,
    /// Time in the current phase as of this snapshot, whether or not the
    /// phase has reported progress.
    #[serde(default)]
    pub phase_age_ms: u64,
    /// Age of the stall in progress; zero when this slot is not stalled.
    pub current_stall_ms: u64,
}

/// Live PAR3 counters. Every field is a relaxed atomic; there is no lock, no
/// string, no allocation and no bucket search anywhere in this struct.
#[derive(Debug, Default)]
pub struct Par3Metrics {
    // ---- memory admission ------------------------------------------------
    admission_refused: [AtomicU64; Par3AdmissionReason::COUNT],
    pub waiting_for_memory_active: AtomicUsize,
    pub waiting_for_memory_ms_total: AtomicU64,
    pub spills_to_disk_total: AtomicU64,
    pub reserved_bytes: AtomicU64,
    pub retained_bytes: AtomicU64,
    pub reserved_peak_bytes: AtomicU64,
    /// Stripe size the coordinator asked the engine to use for the last work
    /// unit. The engine reports the width it actually admitted separately, in
    /// `engine_admitted_stripe_bytes`; the two differ under contention.
    pub effective_stripe_bytes: AtomicU64,

    // ---- engine memory ledger --------------------------------------------
    // Exported per category under the engine's own stable names. Written once
    // per work-unit handback, one relaxed store per category.
    ledger_bytes: [AtomicU64; PAR3_MEMORY_CATEGORIES],
    ledger_peak_bytes: [AtomicU64; PAR3_MEMORY_CATEGORIES],

    // ---- engine diagnostics ----------------------------------------------
    // The widths the engine last admitted, and what its own bounded working
    // sets cost. Admission and cache occupancy are last-observed values by the
    // engine's own definition; refusals, narrowings and amplification are
    // cumulative, so those fold in as deltas.
    pub engine_admitted_stripe_bytes: AtomicU64,
    pub engine_admitted_stripe_buffers: AtomicU64,
    pub engine_admitted_output_tile: AtomicU64,
    pub engine_admitted_verify_batch: AtomicU64,
    pub engine_admitted_workers: AtomicU64,
    pub engine_admitted_window_bytes: AtomicU64,
    engine_refusals: [AtomicU64; Par3EngineRefusal::COUNT],
    engine_narrowed: [AtomicU64; Par3EngineNarrowing::COUNT],
    pub engine_reread_bytes_total: AtomicU64,
    pub engine_reconstructed_bytes_total: AtomicU64,
    pub engine_cache_entries: AtomicU64,
    pub engine_cache_bytes: AtomicU64,
    // Transform and coefficient work the engine's codecs performed. Counted
    // once per call with that call's own totals, never per symbol, so these
    // are cheap enough for the engine to keep and cumulative here.
    pub engine_codec_transform_calls_total: AtomicU64,
    pub engine_codec_butterflies_total: AtomicU64,
    pub engine_codec_butterflies_skipped_total: AtomicU64,
    pub engine_codec_multiply_accumulates_total: AtomicU64,
    pub engine_codec_factors_computed_total: AtomicU64,
    pub engine_codec_factors_reused_total: AtomicU64,

    // ---- CPU and work slots ----------------------------------------------
    pub pending_work_depth: AtomicUsize,
    pub pending_work_bytes: AtomicU64,
    pub dispatch_refused_slots_total: AtomicU64,
    pub dispatch_refused_cpu_total: AtomicU64,
    pub dispatch_wait_ms_total: AtomicU64,
    pub workers_admitted: AtomicUsize,
    pub in_flight: AtomicUsize,

    // ---- recovery acquisition --------------------------------------------
    pub recovery_windows_admitted_total: AtomicU64,
    pub recovery_articles_requested_total: AtomicU64,
    pub recovery_articles_received_total: AtomicU64,
    pub recovery_articles_failed_total: AtomicU64,
    pub recovery_needed_bytes: AtomicU64,
    pub cohorts_with_deficit: AtomicUsize,
    pub metadata_incomplete_waits_total: AtomicU64,
    pub need_data_waits_total: AtomicU64,

    // ---- source reads ----------------------------------------------------
    pub source_read_bytes_total: AtomicU64,
    pub source_reads_total: AtomicU64,
    pub reassessments_total: AtomicU64,
    pub reassessments_zero_read_total: AtomicU64,
    pub reverify_generation_changed_total: AtomicU64,
    /// Work units the coordinator had to re-run alone because a shared-CPU
    /// attempt hit native pressure.
    pub verify_serial_fallback_total: AtomicU64,
    pub encrypted_reader_cache_hits_total: AtomicU64,
    pub encrypted_reader_cache_evictions_total: AtomicU64,

    // ---- donors ----------------------------------------------------------
    pub donor_searches_total: AtomicU64,
    pub donor_search_exhausted_total: AtomicU64,
    pub donor_read_bytes_total: AtomicU64,
    pub donor_time_cap_hits_total: AtomicU64,

    // ---- engine stages ---------------------------------------------------
    stage_calls: [AtomicU64; Par3Stage::COUNT],
    stage_ms: [AtomicU64; Par3Stage::COUNT],
    pub file_sync_calls_total: AtomicU64,
    pub file_sync_ms_total: AtomicU64,

    // ---- repair ----------------------------------------------------------
    pub repairs_started_total: AtomicU64,
    outcomes: [AtomicU64; Par3OutcomeClass::COUNT],
    pub repair_cohorts_processed_total: AtomicU64,
    pub repair_bytes_reconstructed_total: AtomicU64,
    pub repair_cancelled_total: AtomicU64,
    pub readback_windows_total: AtomicU64,
    pub readback_bytes_total: AtomicU64,
    pub readback_mismatch_total: AtomicU64,

    // ---- carriers --------------------------------------------------------
    pub packets_authenticated_total: AtomicU64,
    pub packets_rejected_total: AtomicU64,
    pub carrier_ranges_unavailable_total: AtomicU64,
    pub carrier_damaged_bytes_total: AtomicU64,

    // ---- stalls ----------------------------------------------------------
    slots: [Par3Slot; PAR3_SLOTS],
    pub stalls_total: AtomicU64,
    pub stall_duration_ms: AtomicU64,
    current_stall_ms: AtomicU64,
}

impl Par3Metrics {
    pub fn note_admission_refused(&self, reason: Par3AdmissionReason) {
        self.admission_refused[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn admission_refused(&self) -> [u64; Par3AdmissionReason::COUNT] {
        std::array::from_fn(|index| self.admission_refused[index].load(Ordering::Relaxed))
    }

    /// Publish the engine's categorised ledger. Called once per work-unit
    /// handback: two relaxed stores per category and nothing else.
    pub fn store_ledger(
        &self,
        current: [u64; PAR3_MEMORY_CATEGORIES],
        peak: [u64; PAR3_MEMORY_CATEGORIES],
    ) {
        for index in 0..PAR3_MEMORY_CATEGORIES {
            self.ledger_bytes[index].store(current[index], Ordering::Relaxed);
            self.ledger_peak_bytes[index].store(peak[index], Ordering::Relaxed);
        }
    }

    pub fn ledger_bytes(&self) -> [u64; PAR3_MEMORY_CATEGORIES] {
        std::array::from_fn(|index| self.ledger_bytes[index].load(Ordering::Relaxed))
    }

    pub fn ledger_peak_bytes(&self) -> [u64; PAR3_MEMORY_CATEGORIES] {
        std::array::from_fn(|index| self.ledger_peak_bytes[index].load(Ordering::Relaxed))
    }

    /// Fold one handback's refused admissions in, by cause.
    pub fn note_engine_refusals(&self, deltas: [u64; Par3EngineRefusal::COUNT]) {
        for cause in Par3EngineRefusal::ALL {
            let delta = deltas[cause.index()];
            if delta != 0 {
                self.engine_refusals[cause.index()].fetch_add(delta, Ordering::Relaxed);
            }
        }
    }

    pub fn engine_refusals(&self) -> [u64; Par3EngineRefusal::COUNT] {
        std::array::from_fn(|index| self.engine_refusals[index].load(Ordering::Relaxed))
    }

    /// Fold one handback's narrowed admissions in, by the width that gave.
    pub fn note_engine_narrowed(&self, deltas: [u64; Par3EngineNarrowing::COUNT]) {
        for width in Par3EngineNarrowing::ALL {
            let delta = deltas[width.index()];
            if delta != 0 {
                self.engine_narrowed[width.index()].fetch_add(delta, Ordering::Relaxed);
            }
        }
    }

    pub fn engine_narrowed(&self) -> [u64; Par3EngineNarrowing::COUNT] {
        std::array::from_fn(|index| self.engine_narrowed[index].load(Ordering::Relaxed))
    }

    pub fn note_outcome(&self, class: Par3OutcomeClass) {
        self.outcomes[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn outcomes(&self) -> [u64; Par3OutcomeClass::COUNT] {
        std::array::from_fn(|index| self.outcomes[index].load(Ordering::Relaxed))
    }

    /// Fold one engine stage delta in. Called once per work-unit handback,
    /// never inside the engine's own loops.
    pub fn note_stage_delta(&self, stage: Par3Stage, calls: u64, millis: u64) {
        if calls != 0 {
            self.stage_calls[stage.index()].fetch_add(calls, Ordering::Relaxed);
        }
        if millis != 0 {
            self.stage_ms[stage.index()].fetch_add(millis, Ordering::Relaxed);
        }
    }

    pub fn stage_calls(&self) -> [u64; Par3Stage::COUNT] {
        std::array::from_fn(|index| self.stage_calls[index].load(Ordering::Relaxed))
    }

    pub fn stage_ms(&self) -> [u64; Par3Stage::COUNT] {
        std::array::from_fn(|index| self.stage_ms[index].load(Ordering::Relaxed))
    }

    /// The slot this job already owns, otherwise a free one, otherwise the
    /// slot whose phase is oldest. Claiming never allocates and never blocks.
    fn slot_for(&self, job_id: u64) -> &Par3Slot {
        let mut free: Option<&Par3Slot> = None;
        let mut oldest: Option<&Par3Slot> = None;
        for slot in &self.slots {
            if slot.job_id.load(Ordering::Relaxed) == job_id {
                return slot;
            }
            if free.is_none() && slot.job_id.load(Ordering::Relaxed) == 0 {
                free = Some(slot);
            }
            let entered = slot.phase_entered_ms.load(Ordering::Relaxed);
            if oldest.is_none_or(|old| old.phase_entered_ms.load(Ordering::Relaxed) > entered) {
                oldest = Some(slot);
            }
        }
        free.or(oldest).unwrap_or(&self.slots[0])
    }

    /// Record a job's current phase. `now_ms` comes from
    /// [`super::PipelineMetrics::now_ms`] so every timestamp shares one origin.
    pub fn store_phase(&self, job_id: u64, phase: Par3Phase, now_ms: u64) {
        let slot = self.slot_for(job_id);
        if phase.is_idle() {
            if slot.job_id.load(Ordering::Relaxed) != job_id {
                return;
            }
            slot.job_id.store(0, Ordering::Relaxed);
        } else {
            slot.job_id.store(job_id, Ordering::Relaxed);
        }
        // Re-storing the phase a slot is already in is not progress: a wait
        // path that keeps announcing the same phase must not hide its own
        // stall, so only a real transition refreshes the progress mark.
        if slot.phase.swap(phase.as_code(), Ordering::Relaxed) != phase.as_code() {
            slot.phase_entered_ms.store(now_ms, Ordering::Relaxed);
            slot.last_progress_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// Note that a job's current phase made progress without changing phase.
    pub fn note_progress(&self, job_id: u64, now_ms: u64) {
        for slot in &self.slots {
            if slot.job_id.load(Ordering::Relaxed) == job_id {
                slot.last_progress_ms.store(now_ms, Ordering::Relaxed);
                return;
            }
        }
    }

    /// The progress handler's phase store: exactly two relaxed atomic writes
    /// on a slot this job already owns, and nothing at all otherwise. It takes
    /// no lock, logs nothing, allocates nothing and cannot panic.
    pub fn note_engine_phase(&self, job_id: u64, phase: Par3Phase, now_ms: u64) {
        for slot in &self.slots {
            if slot.job_id.load(Ordering::Relaxed) == job_id {
                // A phase is entered when it changes. Leaving the mark alone
                // here had each engine phase inherit the time the phase before
                // it began, so the phase a job is in reads as older than it is
                // — and the longer the job ran, the older. Progress inside one
                // phase is not a new entry, so the mark only moves on a real
                // transition, the same rule `store_phase` follows.
                if slot.phase.swap(phase.as_code(), Ordering::Relaxed) != phase.as_code() {
                    slot.phase_entered_ms.store(now_ms, Ordering::Relaxed);
                }
                slot.last_progress_ms.store(now_ms, Ordering::Relaxed);
                return;
            }
        }
    }

    /// Advance the stall model and return the per-slot view. Called once per
    /// snapshot tick; there is no timer thread and no background task.
    pub(super) fn observe(&self, now_ms: u64) -> [Par3SlotSnapshot; PAR3_SLOTS] {
        let mut current = 0u64;
        let view = std::array::from_fn(|index| {
            let slot = &self.slots[index];
            let phase = Par3Phase::from_code(slot.phase.load(Ordering::Relaxed));
            let last = slot.last_progress_ms.load(Ordering::Relaxed);
            let gap = now_ms.saturating_sub(last);
            let started = slot.stall_started_ms.load(Ordering::Relaxed);
            let stalled = !phase.is_idle() && gap >= PAR3_STALL_THRESHOLD_MS;
            let age = if stalled {
                if started == 0 {
                    // The gap crossed the threshold at this instant; credit the
                    // stall from the moment progress actually stopped.
                    let began = last.saturating_add(PAR3_STALL_THRESHOLD_MS).max(1);
                    slot.stall_started_ms.store(began, Ordering::Relaxed);
                    self.stalls_total.fetch_add(1, Ordering::Relaxed);
                    now_ms.saturating_sub(began)
                } else {
                    now_ms.saturating_sub(started)
                }
            } else {
                if started != 0 {
                    self.stall_duration_ms
                        .fetch_add(now_ms.saturating_sub(started), Ordering::Relaxed);
                    slot.stall_started_ms.store(0, Ordering::Relaxed);
                }
                0
            };
            current = current.max(age);
            let entered = slot.phase_entered_ms.load(Ordering::Relaxed);
            Par3SlotSnapshot {
                job_id: slot.job_id.load(Ordering::Relaxed),
                phase,
                phase_entered_ms: entered,
                last_progress_ms: last,
                phase_age_ms: now_ms.saturating_sub(entered),
                current_stall_ms: age,
            }
        });
        self.current_stall_ms.store(current, Ordering::Relaxed);
        view
    }

    pub(super) fn snapshot(&self, now_ms: u64) -> Par3MetricsSnapshot {
        let slots = self.observe(now_ms);
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let count = |value: &AtomicUsize| value.load(Ordering::Relaxed);
        Par3MetricsSnapshot {
            admission_refused: self.admission_refused(),
            waiting_for_memory_active: count(&self.waiting_for_memory_active),
            waiting_for_memory_ms_total: load(&self.waiting_for_memory_ms_total),
            spills_to_disk_total: load(&self.spills_to_disk_total),
            reserved_bytes: load(&self.reserved_bytes),
            retained_bytes: load(&self.retained_bytes),
            reserved_peak_bytes: load(&self.reserved_peak_bytes),
            effective_stripe_bytes: load(&self.effective_stripe_bytes),
            ledger_bytes: self.ledger_bytes(),
            ledger_peak_bytes: self.ledger_peak_bytes(),
            engine_admitted_stripe_bytes: load(&self.engine_admitted_stripe_bytes),
            engine_admitted_stripe_buffers: load(&self.engine_admitted_stripe_buffers),
            engine_admitted_output_tile: load(&self.engine_admitted_output_tile),
            engine_admitted_verify_batch: load(&self.engine_admitted_verify_batch),
            engine_admitted_workers: load(&self.engine_admitted_workers),
            engine_admitted_window_bytes: load(&self.engine_admitted_window_bytes),
            engine_refusals: self.engine_refusals(),
            engine_narrowed: self.engine_narrowed(),
            engine_reread_bytes_total: load(&self.engine_reread_bytes_total),
            engine_reconstructed_bytes_total: load(&self.engine_reconstructed_bytes_total),
            engine_cache_entries: load(&self.engine_cache_entries),
            engine_cache_bytes: load(&self.engine_cache_bytes),
            engine_codec_transform_calls_total: load(&self.engine_codec_transform_calls_total),
            engine_codec_butterflies_total: load(&self.engine_codec_butterflies_total),
            engine_codec_butterflies_skipped_total: load(
                &self.engine_codec_butterflies_skipped_total,
            ),
            engine_codec_multiply_accumulates_total: load(
                &self.engine_codec_multiply_accumulates_total,
            ),
            engine_codec_factors_computed_total: load(&self.engine_codec_factors_computed_total),
            engine_codec_factors_reused_total: load(&self.engine_codec_factors_reused_total),
            pending_work_depth: count(&self.pending_work_depth),
            pending_work_bytes: load(&self.pending_work_bytes),
            dispatch_refused_slots_total: load(&self.dispatch_refused_slots_total),
            dispatch_refused_cpu_total: load(&self.dispatch_refused_cpu_total),
            dispatch_wait_ms_total: load(&self.dispatch_wait_ms_total),
            workers_admitted: count(&self.workers_admitted),
            in_flight: count(&self.in_flight),
            recovery_windows_admitted_total: load(&self.recovery_windows_admitted_total),
            recovery_articles_requested_total: load(&self.recovery_articles_requested_total),
            recovery_articles_received_total: load(&self.recovery_articles_received_total),
            recovery_articles_failed_total: load(&self.recovery_articles_failed_total),
            recovery_needed_bytes: load(&self.recovery_needed_bytes),
            cohorts_with_deficit: count(&self.cohorts_with_deficit),
            metadata_incomplete_waits_total: load(&self.metadata_incomplete_waits_total),
            need_data_waits_total: load(&self.need_data_waits_total),
            source_read_bytes_total: load(&self.source_read_bytes_total),
            source_reads_total: load(&self.source_reads_total),
            reassessments_total: load(&self.reassessments_total),
            reassessments_zero_read_total: load(&self.reassessments_zero_read_total),
            reverify_generation_changed_total: load(&self.reverify_generation_changed_total),
            verify_serial_fallback_total: load(&self.verify_serial_fallback_total),
            encrypted_reader_cache_hits_total: load(&self.encrypted_reader_cache_hits_total),
            encrypted_reader_cache_evictions_total: load(
                &self.encrypted_reader_cache_evictions_total,
            ),
            donor_searches_total: load(&self.donor_searches_total),
            donor_search_exhausted_total: load(&self.donor_search_exhausted_total),
            donor_read_bytes_total: load(&self.donor_read_bytes_total),
            donor_time_cap_hits_total: load(&self.donor_time_cap_hits_total),
            stage_calls: self.stage_calls(),
            stage_ms: self.stage_ms(),
            file_sync_calls_total: load(&self.file_sync_calls_total),
            file_sync_ms_total: load(&self.file_sync_ms_total),
            repairs_started_total: load(&self.repairs_started_total),
            outcomes: self.outcomes(),
            repair_cohorts_processed_total: load(&self.repair_cohorts_processed_total),
            repair_bytes_reconstructed_total: load(&self.repair_bytes_reconstructed_total),
            repair_cancelled_total: load(&self.repair_cancelled_total),
            readback_windows_total: load(&self.readback_windows_total),
            readback_bytes_total: load(&self.readback_bytes_total),
            readback_mismatch_total: load(&self.readback_mismatch_total),
            packets_authenticated_total: load(&self.packets_authenticated_total),
            packets_rejected_total: load(&self.packets_rejected_total),
            carrier_ranges_unavailable_total: load(&self.carrier_ranges_unavailable_total),
            carrier_damaged_bytes_total: load(&self.carrier_damaged_bytes_total),
            slots,
            stalls_total: load(&self.stalls_total),
            stall_duration_ms: load(&self.stall_duration_ms),
            current_stall_ms: load(&self.current_stall_ms),
        }
    }

    /// Drive the stall model at a chosen instant, so a test can reach the
    /// stall threshold without sleeping for it.
    #[cfg(test)]
    pub fn observe_for_test(&self, now_ms: u64) -> [Par3SlotSnapshot; PAR3_SLOTS] {
        self.observe(now_ms)
    }

    /// Rewind a slot's last-progress mark so a test can reach the stall
    /// threshold without sleeping.
    #[cfg(test)]
    pub fn set_last_progress_for_test(&self, job_id: u64, now_ms: u64) {
        for slot in &self.slots {
            if slot.job_id.load(Ordering::Relaxed) == job_id {
                slot.last_progress_ms.store(now_ms, Ordering::Relaxed);
            }
        }
    }
}

/// Point-in-time PAR3 counters. Plain integers and fixed-size arrays only, so
/// the enclosing snapshot stays a fixed-size struct copy with no heap fields.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Par3MetricsSnapshot {
    pub admission_refused: [u64; Par3AdmissionReason::COUNT],
    pub waiting_for_memory_active: usize,
    pub waiting_for_memory_ms_total: u64,
    pub spills_to_disk_total: u64,
    pub reserved_bytes: u64,
    pub retained_bytes: u64,
    pub reserved_peak_bytes: u64,
    pub effective_stripe_bytes: u64,
    pub ledger_bytes: [u64; PAR3_MEMORY_CATEGORIES],
    pub ledger_peak_bytes: [u64; PAR3_MEMORY_CATEGORIES],
    pub engine_admitted_stripe_bytes: u64,
    pub engine_admitted_stripe_buffers: u64,
    pub engine_admitted_output_tile: u64,
    pub engine_admitted_verify_batch: u64,
    pub engine_admitted_workers: u64,
    pub engine_admitted_window_bytes: u64,
    pub engine_refusals: [u64; Par3EngineRefusal::COUNT],
    pub engine_narrowed: [u64; Par3EngineNarrowing::COUNT],
    pub engine_reread_bytes_total: u64,
    pub engine_reconstructed_bytes_total: u64,
    pub engine_cache_entries: u64,
    pub engine_cache_bytes: u64,
    pub engine_codec_transform_calls_total: u64,
    pub engine_codec_butterflies_total: u64,
    pub engine_codec_butterflies_skipped_total: u64,
    pub engine_codec_multiply_accumulates_total: u64,
    pub engine_codec_factors_computed_total: u64,
    pub engine_codec_factors_reused_total: u64,
    pub pending_work_depth: usize,
    pub pending_work_bytes: u64,
    pub dispatch_refused_slots_total: u64,
    pub dispatch_refused_cpu_total: u64,
    pub dispatch_wait_ms_total: u64,
    pub workers_admitted: usize,
    pub in_flight: usize,
    pub recovery_windows_admitted_total: u64,
    pub recovery_articles_requested_total: u64,
    pub recovery_articles_received_total: u64,
    pub recovery_articles_failed_total: u64,
    pub recovery_needed_bytes: u64,
    pub cohorts_with_deficit: usize,
    pub metadata_incomplete_waits_total: u64,
    pub need_data_waits_total: u64,
    pub source_read_bytes_total: u64,
    pub source_reads_total: u64,
    pub reassessments_total: u64,
    pub reassessments_zero_read_total: u64,
    pub reverify_generation_changed_total: u64,
    pub verify_serial_fallback_total: u64,
    pub encrypted_reader_cache_hits_total: u64,
    pub encrypted_reader_cache_evictions_total: u64,
    pub donor_searches_total: u64,
    pub donor_search_exhausted_total: u64,
    pub donor_read_bytes_total: u64,
    pub donor_time_cap_hits_total: u64,
    pub stage_calls: [u64; Par3Stage::COUNT],
    pub stage_ms: [u64; Par3Stage::COUNT],
    pub file_sync_calls_total: u64,
    pub file_sync_ms_total: u64,
    pub repairs_started_total: u64,
    pub outcomes: [u64; Par3OutcomeClass::COUNT],
    pub repair_cohorts_processed_total: u64,
    pub repair_bytes_reconstructed_total: u64,
    pub repair_cancelled_total: u64,
    pub readback_windows_total: u64,
    pub readback_bytes_total: u64,
    pub readback_mismatch_total: u64,
    pub packets_authenticated_total: u64,
    pub packets_rejected_total: u64,
    pub carrier_ranges_unavailable_total: u64,
    pub carrier_damaged_bytes_total: u64,
    pub slots: [Par3SlotSnapshot; PAR3_SLOTS],
    pub stalls_total: u64,
    pub stall_duration_ms: u64,
    /// Oldest stall in progress across the slots; zero when nothing is stalled.
    pub current_stall_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deliverable: the snapshot stays a fixed-size struct copy.
    ///
    /// `Copy` is the load-bearing bound: no `String`, `Vec`, `Box`, `Arc` or
    /// map can appear in a `Copy` type, so this fails to compile the moment a
    /// heap field is added. `needs_drop` pins the same property at runtime.
    #[test]
    fn the_snapshot_has_no_heap_fields() {
        fn requires_copy<T: Copy>(value: T) -> T {
            value
        }
        let snapshot = requires_copy(Par3MetricsSnapshot::default());
        assert!(!std::mem::needs_drop::<Par3MetricsSnapshot>());
        // A plain-integer struct is exactly as large as its fields; a heap
        // field would make this an eight-byte pointer instead.
        assert_eq!(
            size_of::<Par3MetricsSnapshot>(),
            size_of_val(&snapshot),
            "the snapshot must be a value, not a handle"
        );
        assert_eq!(snapshot.slots.len(), PAR3_SLOTS);
    }

    /// Deliverable: the progress handler allocates nothing.
    ///
    /// Both handler entry points are driven far more often than any real
    /// engine callback would be, against an owned slot and an unowned one, and
    /// the process allocation counter must not move at all.
    #[test]
    fn the_progress_handler_does_not_allocate() {
        let metrics = Par3Metrics::default();
        metrics.store_phase(7, Par3Phase::Repairing, 1_000);
        // Warm anything the first call might lazily build.
        metrics.note_progress(7, 1_001);
        metrics.note_engine_phase(7, Par3Phase::Verifying, 1_002);

        let before = crate::alloc_probe::allocations();
        for tick in 0..10_000u64 {
            metrics.note_progress(7, 2_000 + tick);
            metrics.note_engine_phase(7, Par3Phase::Repairing, 2_000 + tick);
            // A slot this job does not own must also be free of allocation.
            metrics.note_progress(9_999, 2_000 + tick);
            metrics.note_engine_phase(9_999, Par3Phase::Readback, 2_000 + tick);
        }
        assert_eq!(
            crate::alloc_probe::allocations(),
            before,
            "the progress handler allocated"
        );
    }

    /// Deliverable: a phase's age is its own. The engine's phase handler
    /// changes the phase a slot reports, so it has to stamp when that phase
    /// was entered; inheriting the previous phase's mark makes every later
    /// phase read as old as the job.
    #[test]
    fn an_engine_phase_change_stamps_its_own_entry() {
        let metrics = Par3Metrics::default();
        metrics.store_phase(11, Par3Phase::Verifying, 1_000);
        assert_eq!(metrics.observe(1_400)[0].phase_age_ms, 400);

        metrics.note_engine_phase(11, Par3Phase::Repairing, 1_500);
        let entered = metrics.observe(1_900);
        assert_eq!(entered[0].phase, Par3Phase::Repairing);
        assert_eq!(
            entered[0].phase_age_ms, 400,
            "the new phase is aged from when the engine entered it"
        );

        // Progress inside the same phase is not a new entry.
        metrics.note_engine_phase(11, Par3Phase::Repairing, 1_900);
        assert_eq!(
            metrics.observe(2_100)[0].phase_age_ms,
            600,
            "re-announcing a phase must not reset its age"
        );
    }

    /// Deliverable: the phase/stall model mirrors `download_pressure_*`.
    #[test]
    fn a_slot_stalls_once_and_credits_its_whole_duration_when_it_clears() {
        let metrics = Par3Metrics::default();
        metrics.store_phase(4, Par3Phase::AwaitingRecoveryArticles, 1_000);

        let fresh = metrics.observe(1_500);
        assert_eq!(fresh[0].job_id, 4);
        assert_eq!(fresh[0].phase, Par3Phase::AwaitingRecoveryArticles);
        assert_eq!(fresh[0].current_stall_ms, 0, "well inside the threshold");
        assert_eq!(
            fresh[0].phase_age_ms, 500,
            "phase age runs without progress"
        );
        assert_eq!(metrics.stalls_total.load(Ordering::Relaxed), 0);

        // One millisecond past the threshold starts exactly one stall, and the
        // age is counted from when progress stopped, not from this observation.
        let crossed = 1_000 + PAR3_STALL_THRESHOLD_MS + 1;
        let stalled = metrics.observe(crossed);
        assert_eq!(stalled[0].current_stall_ms, 1);
        assert_eq!(metrics.stalls_total.load(Ordering::Relaxed), 1);

        let grown = metrics.observe(crossed + 4_000);
        assert_eq!(grown[0].current_stall_ms, 4_001);
        assert_eq!(
            metrics.stalls_total.load(Ordering::Relaxed),
            1,
            "an ongoing stall is not a new stall"
        );

        // Progress clears the stall and credits its whole duration once.
        metrics.set_last_progress_for_test(4, crossed + 4_000);
        let cleared = metrics.observe(crossed + 4_100);
        assert_eq!(cleared[0].current_stall_ms, 0);
        assert_eq!(
            cleared[0].phase_age_ms,
            crossed + 3_100,
            "progress does not reset or freeze the phase age"
        );
        assert_eq!(metrics.stall_duration_ms.load(Ordering::Relaxed), 4_101);
        assert_eq!(metrics.current_stall_ms.load(Ordering::Relaxed), 0);
    }

    /// Re-announcing the same phase is not progress; a wait path that keeps
    /// saying the same thing must not be able to hide its own stall.
    #[test]
    fn repeating_a_phase_does_not_refresh_the_progress_mark() {
        let metrics = Par3Metrics::default();
        metrics.store_phase(11, Par3Phase::AwaitingMemory, 0);
        for tick in 0..50 {
            metrics.store_phase(11, Par3Phase::AwaitingMemory, tick * 1_000);
        }
        let view = metrics.observe(PAR3_STALL_THRESHOLD_MS + 500);
        assert_eq!(view[0].current_stall_ms, 500);
        assert_eq!(
            metrics.stalls_total.load(Ordering::Relaxed),
            1,
            "a wait that outlives the threshold is a stall, however often it \
             re-announces itself"
        );

        // A real transition does refresh it.
        metrics.store_phase(11, Par3Phase::Repairing, PAR3_STALL_THRESHOLD_MS + 500);
        let view = metrics.observe(PAR3_STALL_THRESHOLD_MS + 600);
        assert_eq!(view[0].current_stall_ms, 0);
    }

    /// An idle store releases the slot so a later job can claim it, and a job
    /// that does not own the slot cannot release someone else's.
    #[test]
    fn only_the_owner_releases_a_slot() {
        let metrics = Par3Metrics::default();
        metrics.store_phase(1, Par3Phase::Assessing, 0);
        metrics.store_phase(2, Par3Phase::Repairing, 0);
        metrics.store_phase(3, Par3Phase::Idle, 0);
        let view = metrics.observe(0);
        assert_eq!(
            view.iter().map(|slot| slot.job_id).collect::<Vec<_>>(),
            vec![1, 2],
            "a stranger's idle store must not evict an owner"
        );

        metrics.store_phase(1, Par3Phase::Idle, 10);
        let view = metrics.observe(10);
        assert_eq!(view[0].job_id, 0);
        assert_eq!(view[0].phase, Par3Phase::Idle);
    }

    /// Deliverable: an admission refusal lands in the slot the budget names.
    #[test]
    fn a_refusal_lands_in_its_own_slot_only() {
        let metrics = Par3Metrics::default();
        metrics.note_admission_refused(Par3AdmissionReason::RetainedState);
        metrics.note_admission_refused(Par3AdmissionReason::RetainedState);
        metrics.note_admission_refused(Par3AdmissionReason::DiskFallbackSpace);
        let refused = metrics.admission_refused();
        for reason in Par3AdmissionReason::ALL {
            let expected = match reason {
                Par3AdmissionReason::RetainedState => 2,
                Par3AdmissionReason::DiskFallbackSpace => 1,
                _ => 0,
            };
            assert_eq!(refused[reason.index()], expected, "{}", reason.as_str());
        }
    }

    /// Every exported code round-trips and every label is distinct, because
    /// both are part of the `/metrics` contract.
    #[test]
    fn the_exported_codes_and_labels_are_stable() {
        for phase in Par3Phase::ALL {
            assert_eq!(Par3Phase::from_code(phase.as_code()), phase);
        }
        assert_eq!(Par3Phase::ALL.len(), 13);
        assert_eq!(Par3Phase::from_code(usize::MAX), Par3Phase::Idle);
        let labels: std::collections::BTreeSet<_> =
            Par3Phase::ALL.iter().map(|phase| phase.as_str()).collect();
        assert_eq!(labels.len(), Par3Phase::ALL.len());

        let indexes: std::collections::BTreeSet<_> =
            Par3Stage::ALL.iter().map(|stage| stage.index()).collect();
        assert_eq!(indexes.len(), Par3Stage::COUNT);
        let classes: std::collections::BTreeSet<_> = Par3OutcomeClass::ALL
            .iter()
            .map(|class| class.index())
            .collect();
        assert_eq!(classes.len(), Par3OutcomeClass::COUNT);
    }
}
