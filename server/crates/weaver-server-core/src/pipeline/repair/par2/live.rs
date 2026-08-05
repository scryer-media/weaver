//! Advisory live PAR2 verification for ordinary assembled payload files.
//!
//! This deliberately has no direct-store or virtual-volume concepts.  It
//! receives bytes only after the normal assembler has committed them to disk,
//! and falls back to the existing verification pass whenever its evidence is
//! incomplete.  `par2-rs` owns slice hashing, sparse boundary buffering, and
//! overlap handling; Weaver owns file identity, disk reads, and lifecycle.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::OnceLock;

use par2_rs::{
    FeedDisposition, FeedOutcome, FileId, Packet, SliceEvidence, SliceEvidenceStrength,
    VerificationMemoryBudget, VerificationSession,
};

use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::DecodedChunk;

pub(crate) const LIVE_PAR2_ENV: &str = "WEAVER_LIVE_PAR2";
pub(crate) const PARTIAL_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const DISK_READ_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PRE_METADATA_RANGES: usize = 4096;

/// 0.7.9 is deliberately opt-in.  0.8 enables this by default, so keep the
/// parser explicit rather than overloading presence/absence semantics.
pub(crate) fn env_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| parse_enabled(std::env::var(LIVE_PAR2_ENV).ok().as_deref()))
}

fn parse_enabled(raw: Option<&str>) -> bool {
    matches!(
        raw.map(str::trim)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// A logical source-file range whose bytes have to be read after assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LiveRead {
    pub(crate) file_id: NzbFileId,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveMetrics {
    pub(crate) input_spans: u64,
    pub(crate) metadata_pending_spans: u64,
    pub(crate) metadata_range_overflows: u64,
    pub(crate) strongly_verified_slices: u64,
    pub(crate) invalid_slices: u64,
    pub(crate) partial_fallbacks: u64,
    pub(crate) overlap_fallbacks: u64,
    pub(crate) backfill_reads: u64,
    pub(crate) settle_reads: u64,
    pub(crate) disk_read_bytes: u64,
    pub(crate) disk_read_budget_exhausted: u64,
    pub(crate) full_verify_skips: u64,
}

/// Coalesced ranges retained only while metadata is unavailable.  They carry
/// no payload bytes, so activation can read existing committed bytes without
/// making delayed metadata a linear memory cost.
#[derive(Debug, Default)]
struct RangeSet {
    ranges: BTreeMap<u64, u64>,
}

impl RangeSet {
    fn insert(&mut self, start: u64, end: u64) -> bool {
        if end <= start {
            return true;
        }
        let mut start = start;
        let mut end = end;
        let overlapping = self
            .ranges
            .range(..=end)
            .filter_map(|(&range_start, &range_end)| {
                (range_end >= start).then_some((range_start, range_end))
            })
            .collect::<Vec<_>>();
        if self
            .ranges
            .len()
            .saturating_sub(overlapping.len())
            .saturating_add(1)
            > MAX_PRE_METADATA_RANGES
        {
            return false;
        }
        for (range_start, range_end) in overlapping {
            self.ranges.remove(&range_start);
            start = start.min(range_start);
            end = end.max(range_end);
        }
        self.ranges.insert(start, end);
        true
    }

    fn take(&mut self) -> Vec<(u64, u64)> {
        std::mem::take(&mut self.ranges).into_iter().collect()
    }
}

#[derive(Debug, Clone)]
struct LiveBinding {
    par2_file_id: FileId,
    length: u64,
    path: PathBuf,
    completed_bytes: Option<u64>,
    rejected: bool,
}

struct LiveJob {
    session: Option<VerificationSession>,
    bindings: HashMap<NzbFileId, LiveBinding>,
    pre_metadata_ranges: HashMap<NzbFileId, RangeSet>,
    pre_metadata_overflowed: HashSet<NzbFileId>,
    queued_reads: Vec<LiveRead>,
    queued_read_keys: HashSet<LiveRead>,
    disk_read_budget: u64,
}

impl Default for LiveJob {
    fn default() -> Self {
        Self {
            session: None,
            bindings: HashMap::new(),
            pre_metadata_ranges: HashMap::new(),
            pre_metadata_overflowed: HashSet::new(),
            queued_reads: Vec::new(),
            queued_read_keys: HashSet::new(),
            disk_read_budget: DISK_READ_BUDGET_BYTES,
        }
    }
}

/// Owns the caller-shareable boundary budget for all active jobs.
pub(crate) struct LivePar2Registry {
    enabled: bool,
    memory_budget: VerificationMemoryBudget,
    jobs: HashMap<JobId, LiveJob>,
    metrics: LiveMetrics,
}

impl Default for LivePar2Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl LivePar2Registry {
    pub(crate) fn new() -> Self {
        Self {
            enabled: env_enabled(),
            memory_budget: VerificationMemoryBudget::new(PARTIAL_BUDGET_BYTES),
            jobs: HashMap::new(),
            metrics: LiveMetrics::default(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    #[cfg(test)]
    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub(crate) fn metrics(&self) -> LiveMetrics {
        self.metrics
    }

    pub(crate) fn note_full_verify_skip(&mut self) {
        self.metrics.full_verify_skips = self.metrics.full_verify_skips.saturating_add(1);
        crate::runtime::perf_probe::record(
            "verify.live_par2.full_verify_skipped",
            std::time::Duration::ZERO,
        );
    }

    pub(crate) fn partial_bytes(&self) -> usize {
        self.memory_budget.buffered_bytes()
    }

    pub(crate) fn remove_job(&mut self, job_id: JobId) {
        self.jobs.remove(&job_id);
    }

    /// Whether live verification is running for this job (plan 138, M2).
    ///
    /// The adopted engine expresses "active" as holding a verification
    /// session: `activate` installs one, and the invalidation paths drop it.
    /// That is the same question 0.8.0 asked of its `JobLive::Active` variant,
    /// and it is what the retirement tests assert after CRC recovery and after
    /// an authoritative repair.
    pub(crate) fn is_active(&self, job_id: JobId) -> bool {
        self.jobs
            .get(&job_id)
            .is_some_and(|job| job.session.is_some())
    }

    /// Coverage recorded for a file before it was bound to a PAR2 description
    /// (plan 138, M2).
    ///
    /// 0.8.0 read this from its pre-binding buffers; the adopted engine keeps
    /// the same thing in `pre_metadata_ranges`. Plan 136's encrypted-overlay
    /// test uses it to prove a direct volume's posted cipher really reached
    /// live verification, so the answer must stay per-file and non-empty.
    pub(crate) fn recorded_ranges(&self, file_id: NzbFileId) -> Option<Vec<(u64, u64)>> {
        self.jobs
            .get(&file_id.job_id)?
            .pre_metadata_ranges
            .get(&file_id)
            .map(|set| {
                set.ranges
                    .iter()
                    .map(|(start, end)| (*start, *end))
                    .collect()
            })
    }

    pub(crate) fn invalidate_job(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        // The first authoritative PAR2 identity arrives after ordinary files
        // may already have been assembled. Preserve those pre-activation
        // ranges so activation can settle-read them. Once a session or stable
        // binding existed, invalidation is an identity change and the old
        // range records must not be reused by the replacement identity.
        let preserve_pre_activation_ranges = job.session.is_none() && job.bindings.is_empty();
        if !preserve_pre_activation_ranges {
            job.pre_metadata_ranges.clear();
            job.pre_metadata_overflowed.clear();
        }
        job.session = None;
        job.bindings.clear();
        job.queued_reads.clear();
        job.queued_read_keys.clear();
        job.disk_read_budget = DISK_READ_BUDGET_BYTES;
    }

    /// Start a session after the primary PAR2 is assembled.  Any earlier
    /// decoded data is represented by range records and backfilled after a
    /// stable identity binding is available.
    pub(crate) fn activate(&mut self, job_id: JobId, packets: &[Packet]) {
        if !self.enabled {
            return;
        }
        let job = self.jobs.entry(job_id).or_default();
        let mut session = VerificationSession::with_memory_budget(self.memory_budget.clone());
        session.add_par2_data(packets);
        if session.par2_set().is_some() {
            job.session = Some(session);
        }
    }

    pub(crate) fn merge_packets(&mut self, job_id: JobId, packets: &[Packet]) {
        let Some(session) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|job| job.session.as_mut())
        else {
            return;
        };
        session.add_par2_data(packets);
    }

    /// Bind exactly one pipeline file to exactly one PAR2 description.  The
    /// resolver owns ambiguity checks; a duplicate binding is rejected here
    /// defensively so a later evidence record cannot change identity.
    pub(crate) fn bind(
        &mut self,
        file_id: NzbFileId,
        par2_file_id: FileId,
        length: u64,
        path: PathBuf,
    ) -> bool {
        let Some(job) = self.jobs.get_mut(&file_id.job_id) else {
            return false;
        };
        let Some(session) = job.session.as_ref() else {
            return false;
        };
        let Some(desc) = session
            .par2_set()
            .and_then(|set| set.file_description(&par2_file_id))
        else {
            return false;
        };
        if job.pre_metadata_overflowed.contains(&file_id) {
            return false;
        }
        if let Some(existing) = job.bindings.get(&file_id) {
            return existing.par2_file_id == par2_file_id
                && existing.length == length
                && existing.path == path;
        }
        if desc.length != length
            || job
                .bindings
                .iter()
                .any(|(other, binding)| *other != file_id && binding.par2_file_id == par2_file_id)
        {
            return false;
        }

        job.bindings.insert(
            file_id,
            LiveBinding {
                par2_file_id,
                length,
                path,
                completed_bytes: None,
                rejected: false,
            },
        );
        let ranges = job
            .pre_metadata_ranges
            .get_mut(&file_id)
            .map(RangeSet::take)
            .unwrap_or_default();
        for (start, end) in ranges {
            Self::queue_read(
                &mut self.metrics,
                job,
                file_id,
                start,
                end.saturating_sub(start),
                true,
            );
        }
        true
    }

    pub(crate) fn note_file_complete(&mut self, file_id: NzbFileId, received_bytes: u64) {
        let Some(binding) = self
            .jobs
            .get_mut(&file_id.job_id)
            .and_then(|job| job.bindings.get_mut(&file_id))
        else {
            return;
        };
        binding.completed_bytes = Some(received_bytes);
        binding.rejected = binding.length != received_bytes;
    }

    /// Feed bytes which normal assembly has already committed.  Before
    /// metadata arrives retain only range shape, not bytes.
    pub(crate) fn note_segment(&mut self, file_id: NzbFileId, offset: u64, data: &DecodedChunk) {
        if !self.enabled || data.len_bytes() == 0 {
            return;
        }
        self.metrics.input_spans += 1;
        let job = self.jobs.entry(file_id.job_id).or_default();
        if job.pre_metadata_overflowed.contains(&file_id) {
            return;
        }
        let Some(session) = job.session.as_mut() else {
            let end = offset.saturating_add(data.len_bytes() as u64);
            let retained = job
                .pre_metadata_ranges
                .entry(file_id)
                .or_default()
                .insert(offset, end);
            if !retained {
                job.pre_metadata_ranges.remove(&file_id);
                job.pre_metadata_overflowed.insert(file_id);
                self.metrics.metadata_range_overflows =
                    self.metrics.metadata_range_overflows.saturating_add(1);
            }
            self.metrics.metadata_pending_spans += 1;
            return;
        };
        let Some(binding) = job.bindings.get(&file_id).cloned() else {
            let end = offset.saturating_add(data.len_bytes() as u64);
            let retained = job
                .pre_metadata_ranges
                .entry(file_id)
                .or_default()
                .insert(offset, end);
            if !retained {
                job.pre_metadata_ranges.remove(&file_id);
                job.pre_metadata_overflowed.insert(file_id);
                self.metrics.metadata_range_overflows =
                    self.metrics.metadata_range_overflows.saturating_add(1);
            }
            self.metrics.metadata_pending_spans += 1;
            return;
        };
        if binding.rejected {
            return;
        }

        let mut next_offset = offset;
        let mut outcomes = Vec::new();
        data.for_each_slice(|bytes| {
            outcomes.push(session.feed_range(&binding.par2_file_id, next_offset, bytes));
            next_offset = next_offset.saturating_add(bytes.len() as u64);
        });
        for outcome in outcomes {
            Self::record_outcome(&mut self.metrics, job, file_id, outcome, false);
        }
    }

    pub(crate) fn take_reads(&mut self, job_id: JobId) -> Vec<LiveRead> {
        self.jobs
            .get_mut(&job_id)
            .map(|job| std::mem::take(&mut job.queued_reads))
            .unwrap_or_default()
    }

    pub(crate) fn path_for_read(&self, read: LiveRead) -> Option<PathBuf> {
        self.jobs
            .get(&read.file_id.job_id)
            .and_then(|job| job.bindings.get(&read.file_id))
            .map(|binding| binding.path.clone())
    }

    /// Feed a completed backfill/settle read.  A short or failed read is left
    /// unresolved, which intentionally disables the optimization.
    pub(crate) fn apply_read(&mut self, read: LiveRead, bytes: &[u8]) {
        let Some(job) = self.jobs.get_mut(&read.file_id.job_id) else {
            return;
        };
        let Some(binding) = job.bindings.get(&read.file_id).cloned() else {
            return;
        };
        if binding.rejected || bytes.len() as u64 != read.len {
            return;
        }
        let Some(session) = job.session.as_mut() else {
            return;
        };
        self.metrics.disk_read_bytes = self
            .metrics
            .disk_read_bytes
            .saturating_add(bytes.len() as u64);
        let outcome = session.feed_range(&binding.par2_file_id, read.offset, bytes);
        Self::record_outcome(&mut self.metrics, job, read.file_id, outcome, true);
    }

    /// Schedule a full-slice settle pass for unresolved slices of files that
    /// are known complete.  This makes delayed metadata, partial boundaries,
    /// and conflicting/duplicate range handling fail safe without retaining
    /// decoded payloads in Weaver.
    pub(crate) fn schedule_settle_reads(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let Some(session) = job.session.as_ref() else {
            return;
        };
        let Some(set) = session.par2_set() else {
            return;
        };
        let strong = session
            .slice_evidence()
            .into_iter()
            .filter(|evidence| {
                evidence.is_valid() && evidence.strength() == SliceEvidenceStrength::Crc32AndMd5
            })
            .map(|evidence| (evidence.file_id(), evidence.slice_index()))
            .collect::<HashSet<_>>();
        let reads = job
            .bindings
            .iter()
            .filter_map(|(file_id, binding)| {
                (!binding.rejected
                    && binding.completed_bytes == Some(binding.length)
                    && set.file_description(&binding.par2_file_id).is_some())
                .then_some((*file_id, binding.clone()))
            })
            .flat_map(|(file_id, binding)| {
                let slices = set.slice_count_for_file(binding.length);
                let strong = &strong;
                (0..slices).filter_map(move |slice_index| {
                    (!strong.contains(&(binding.par2_file_id, slice_index))).then_some(LiveRead {
                        file_id,
                        offset: slice_index as u64 * set.slice_size,
                        len: (binding.length - slice_index as u64 * set.slice_size)
                            .min(set.slice_size),
                    })
                })
            })
            .collect::<Vec<_>>();
        for read in reads {
            Self::queue_read(
                &mut self.metrics,
                job,
                read.file_id,
                read.offset,
                read.len,
                false,
            );
        }
    }

    /// Pipeline-to-PAR2 bindings which are eligible to replace a full
    /// post-download scan.  This checks the same condition as exported slice
    /// evidence, without incrementing a skip counter merely for probing it.
    pub(crate) fn complete_bindings_if_strong(
        &self,
        job_id: JobId,
    ) -> Option<HashMap<NzbFileId, FileId>> {
        let job = self.jobs.get(&job_id)?;
        let session = job.session.as_ref()?;
        let set = session.par2_set()?;
        let strong = session
            .slice_evidence()
            .into_iter()
            .filter(|evidence| {
                evidence.is_valid() && evidence.strength() == SliceEvidenceStrength::Crc32AndMd5
            })
            .map(|evidence| (evidence.file_id(), evidence.slice_index()))
            .collect::<HashSet<_>>();
        let mut by_par2_file = HashMap::<FileId, NzbFileId>::new();
        for (file_id, binding) in &job.bindings {
            if binding.rejected
                || binding.completed_bytes != Some(binding.length)
                || by_par2_file
                    .insert(binding.par2_file_id, *file_id)
                    .is_some()
            {
                return None;
            }
        }
        for (par2_file_id, desc) in &set.files {
            let pipeline_file_id = *by_par2_file.get(par2_file_id)?;
            let binding = job.bindings.get(&pipeline_file_id)?;
            if binding.length != desc.length
                || !(0..set.slice_count_for_file(desc.length))
                    .all(|slice_index| strong.contains(&(*par2_file_id, slice_index)))
            {
                return None;
            }
        }
        Some(
            by_par2_file
                .into_iter()
                .map(|(par2_file_id, pipeline_file_id)| (pipeline_file_id, par2_file_id))
                .collect(),
        )
    }

    /// Strong, individually settled source slices are useful to a retained
    /// repair session even when another file remains corrupt or unresolved.
    /// The session still performs its normal scan for every other source.
    pub(crate) fn strong_evidence(&self, job_id: JobId) -> Vec<(PathBuf, SliceEvidence)> {
        let Some(job) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let Some(session) = job.session.as_ref() else {
            return Vec::new();
        };
        session
            .slice_evidence()
            .into_iter()
            .filter(|evidence| {
                evidence.is_valid() && evidence.strength() == SliceEvidenceStrength::Crc32AndMd5
            })
            .filter_map(|evidence| {
                job.bindings
                    .values()
                    .find(|binding| !binding.rejected && binding.par2_file_id == evidence.file_id())
                    .map(|binding| (binding.path.clone(), evidence))
            })
            .collect()
    }

    fn record_outcome(
        metrics: &mut LiveMetrics,
        job: &mut LiveJob,
        source_file_id: NzbFileId,
        outcome: FeedOutcome,
        from_disk: bool,
    ) {
        match outcome.disposition() {
            FeedDisposition::BudgetExhausted | FeedDisposition::NeedsSettleRead => {
                metrics.partial_fallbacks = metrics.partial_fallbacks.saturating_add(1);
            }
            FeedDisposition::ConflictingOverlap => {
                metrics.overlap_fallbacks = metrics.overlap_fallbacks.saturating_add(1);
            }
            _ => {}
        }
        for evidence in outcome.evidence() {
            if evidence.is_valid() && evidence.strength() == SliceEvidenceStrength::Crc32AndMd5 {
                metrics.strongly_verified_slices =
                    metrics.strongly_verified_slices.saturating_add(1);
            } else if !evidence.is_valid() {
                metrics.invalid_slices = metrics.invalid_slices.saturating_add(1);
            }
        }
        for read in outcome.settle_reads() {
            let Some((file_id, _)) = job
                .bindings
                .iter()
                .find(|(_, binding)| binding.par2_file_id == read.file_id())
            else {
                continue;
            };
            Self::queue_read(
                metrics,
                job,
                *file_id,
                read.offset(),
                read.length(),
                !from_disk,
            );
        }
        // `source_file_id` makes it explicit that the outcome must remain
        // tied to an assembled file rather than a free-floating PAR2 ID.
        let _ = source_file_id;
    }

    fn queue_read(
        metrics: &mut LiveMetrics,
        job: &mut LiveJob,
        file_id: NzbFileId,
        offset: u64,
        len: u64,
        backfill: bool,
    ) {
        let Some(end) = offset.checked_add(len) else {
            return;
        };
        let Some(binding) = job.bindings.get(&file_id) else {
            return;
        };
        if len == 0 || end > binding.length {
            return;
        }
        let read = LiveRead {
            file_id,
            offset,
            len,
        };
        if job.queued_read_keys.contains(&read) {
            return;
        }
        if len > job.disk_read_budget {
            metrics.disk_read_budget_exhausted =
                metrics.disk_read_budget_exhausted.saturating_add(1);
            return;
        }
        job.disk_read_budget -= len;
        job.queued_read_keys.insert(read);
        job.queued_reads.push(read);
        if backfill {
            metrics.backfill_reads = metrics.backfill_reads.saturating_add(1);
        } else {
            metrics.settle_reads = metrics.settle_reads.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_parser_is_opt_in() {
        for raw in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("off"),
            Some("no"),
        ] {
            assert!(!parse_enabled(raw));
        }
        for raw in [Some("1"), Some("true"), Some("YES"), Some(" on ")] {
            assert!(parse_enabled(raw));
        }
    }

    #[test]
    fn range_set_coalesces_overlapping_and_adjacent_ranges() {
        let mut ranges = RangeSet::default();
        assert!(ranges.insert(10, 20));
        assert!(ranges.insert(0, 10));
        assert!(ranges.insert(15, 30));
        assert_eq!(ranges.take(), vec![(0, 30)]);
    }

    #[test]
    fn pre_metadata_range_count_is_bounded() {
        let mut ranges = RangeSet::default();
        for index in 0..MAX_PRE_METADATA_RANGES as u64 {
            assert!(ranges.insert(index * 2, index * 2 + 1));
        }
        assert!(!ranges.insert(MAX_PRE_METADATA_RANGES as u64 * 2, u64::MAX));
        assert_eq!(ranges.ranges.len(), MAX_PRE_METADATA_RANGES);
    }

    #[test]
    fn pre_metadata_input_retains_ranges_not_payload_bytes() {
        let file_id = NzbFileId {
            job_id: JobId(42),
            file_index: 3,
        };
        let mut registry = LivePar2Registry::new();
        registry.set_enabled(true);
        registry.note_segment(file_id, 8, &DecodedChunk::from(vec![7; 32]));

        assert_eq!(registry.metrics().metadata_pending_spans, 1);
        assert_eq!(registry.partial_bytes(), 0);
        let ranges = registry
            .jobs
            .get_mut(&file_id.job_id)
            .unwrap()
            .pre_metadata_ranges
            .get_mut(&file_id)
            .unwrap();
        assert_eq!(ranges.take(), vec![(8, 40)]);
    }

    #[test]
    fn invalidation_preserves_ranges_before_first_activation() {
        let file_id = NzbFileId {
            job_id: JobId(44),
            file_index: 5,
        };
        let mut registry = LivePar2Registry::new();
        registry.set_enabled(true);
        registry.note_segment(file_id, 4, &DecodedChunk::from(vec![1; 12]));

        registry.invalidate_job(file_id.job_id);

        let ranges = &registry.jobs[&file_id.job_id].pre_metadata_ranges[&file_id].ranges;
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges.get(&4), Some(&16));
    }

    #[test]
    fn invalidation_clears_ranges_after_identity_binding() {
        let file_id = NzbFileId {
            job_id: JobId(45),
            file_index: 6,
        };
        let mut registry = LivePar2Registry::new();
        registry.set_enabled(true);
        registry.note_segment(file_id, 0, &DecodedChunk::from(vec![2; 8]));
        let job = registry.jobs.get_mut(&file_id.job_id).unwrap();
        job.pre_metadata_overflowed.insert(file_id);
        job.bindings.insert(
            file_id,
            LiveBinding {
                par2_file_id: FileId::from_bytes([3; 16]),
                length: 8,
                path: PathBuf::from("fixture.bin"),
                completed_bytes: None,
                rejected: false,
            },
        );

        registry.invalidate_job(file_id.job_id);

        let job = &registry.jobs[&file_id.job_id];
        assert!(job.pre_metadata_ranges.is_empty());
        assert!(job.pre_metadata_overflowed.is_empty());
    }

    #[test]
    fn disk_read_budget_deduplicates_ranges_and_falls_back_when_exhausted() {
        let file_id = NzbFileId {
            job_id: JobId(43),
            file_index: 4,
        };
        let mut registry = LivePar2Registry::new();
        let mut job = LiveJob::default();
        job.disk_read_budget = 8;
        job.bindings.insert(
            file_id,
            LiveBinding {
                par2_file_id: FileId::from_bytes([9; 16]),
                length: 16,
                path: PathBuf::from("fixture.bin"),
                completed_bytes: None,
                rejected: false,
            },
        );

        LivePar2Registry::queue_read(&mut registry.metrics, &mut job, file_id, 0, 6, true);
        LivePar2Registry::queue_read(&mut registry.metrics, &mut job, file_id, 0, 6, true);
        LivePar2Registry::queue_read(&mut registry.metrics, &mut job, file_id, 6, 3, false);

        assert_eq!(job.queued_reads.len(), 1);
        assert_eq!(job.disk_read_budget, 2);
        assert_eq!(registry.metrics.backfill_reads, 1);
        assert_eq!(registry.metrics.disk_read_budget_exhausted, 1);
    }
}

/// Best-effort ranged read: a short read (truncated or missing file) yields
/// fewer bytes rather than an error, leaving those blocks `Pending`.
pub(crate) fn read_range_best_effort(
    path: &std::path::Path,
    offset: u64,
    len: u64,
) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let Ok(len) = usize::try_from(len) else {
        return Ok(Vec::new());
    };
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; len];
    let mut read = 0usize;
    while read < len {
        match file.read(&mut bytes[read..])? {
            0 => break,
            n => read += n,
        }
    }
    bytes.truncate(read);
    Ok(bytes)
}

/// Same contract, same shape: a range the set never placed comes back short
/// rather than as an error, so those blocks stay `Pending` and the
/// authoritative pass owns them. The provider reports a hole as an error, which
/// is *stronger* than a short read — it distinguishes "not downloaded" from "the
/// disk is broken" — and this is the one caller that deliberately flattens the
/// two, because live verification is advisory and neither one is a verdict.
pub(crate) fn read_virtual_range_best_effort(
    provider: &crate::pipeline::direct_store::provider::HybridVolumeProvider,
    volume_index: u32,
    offset: u64,
    len: u64,
) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let Ok(len) = usize::try_from(len) else {
        return Ok(Vec::new());
    };
    let mut reader = provider.open(volume_index).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("direct-store volume {volume_index} is not registered"),
        )
    })?;
    reader.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; len];
    let mut read = 0usize;
    while read < len {
        match reader.read(&mut bytes[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(error) if crate::pipeline::direct_store::provider::is_hole(&error) => break,
            Err(error) => return Err(error),
        }
    }
    bytes.truncate(read);
    Ok(bytes)
}
