//! Live in-stream PAR2 block verification (plan 135, D5).
//!
//! PAR2 slice checksums are defined in source-file coordinates, which is
//! exactly the space a decoded segment's `file_offset` lives in. This module
//! hashes each block against its IFSC `SliceChecksum` while the download runs,
//! so a clean job finishes verification with its last article.
//!
//! Results are advisory: a fully matched, fully `Ok` job lets the completion
//! seam skip the post-download pass, and anything else falls through to the
//! existing pass unchanged.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use par2_rs::par2_set::Par2FileSet;

use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::DecodedChunk;

pub(crate) const LIVE_PAR2_ENV: &str = "WEAVER_LIVE_PAR2";

/// Ceiling on bytes held in boundary-block partial buffers, shared by every
/// file of every job. A per-file cap is a linear RSS term on large jobs, so
/// this budget is deliberately global (plan 135, D5).
const PARTIAL_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Per-job ceiling on bytes read back from disk for activation backfill and
/// settle. Live verification is advisory, so exhausting it leaves blocks
/// `Pending` — the existing verification pass then runs, exactly as today —
/// rather than paying unbounded extra I/O ahead of a pass that may run anyway.
const DISK_READ_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Largest single settle/backfill read handed to `spawn_blocking`.
const READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// Coalesced pre-activation ranges retained per file before the set installs.
/// Overflow drops the range record; those blocks fall to the settle pass.
const MAX_PRE_ACTIVATION_RANGES: usize = 4096;

/// Whether live PAR2 verification is compiled in and not disabled by the
/// operator kill switch. Read once, in the style of `e2e_failpoint`.
pub(crate) fn env_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| parse_enabled(std::env::var(LIVE_PAR2_ENV).ok().as_deref()))
}

fn parse_enabled(raw: Option<&str>) -> bool {
    // Convention: only the explicit off words disable. `WEAVER_LIVE_PAR2=""`
    // therefore reads as *enabled*, the same as an unset variable — an empty
    // value is not treated as "present, so off".
    let Some(value) = raw else {
        return true;
    };
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "off" | "no"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockState {
    Pending,
    Ok,
    Bad,
}

/// One block's byte range on the source file, queued for a read-back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveRead {
    pub(crate) file_id: NzbFileId,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

/// Binding of a pipeline file to the PAR2 file description it carries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LiveBinding {
    pub(crate) par2_file_id: par2_rs::FileId,
    pub(crate) length: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveMetrics {
    pub(crate) blocks_claimed_in_stream: u64,
    pub(crate) blocks_backfilled: u64,
    pub(crate) blocks_settled: u64,
    pub(crate) blocks_bad: u64,
    pub(crate) blocks_demoted: u64,
    pub(crate) partials_abandoned: u64,
    pub(crate) partial_bytes_peak: u64,
    pub(crate) spans_out_of_range: u64,
    pub(crate) full_verify_skips: u64,
}

/// Sorted, coalesced half-open ranges.
#[derive(Debug, Default, Clone)]
struct RangeSet {
    ranges: Vec<(u64, u64)>,
    overflowed: bool,
}

impl RangeSet {
    fn insert(&mut self, start: u64, end: u64) {
        if end <= start || self.overflowed {
            return;
        }
        let lo = self
            .ranges
            .partition_point(|(_, range_end)| *range_end < start);
        let mut hi = lo;
        let mut merged_start = start;
        let mut merged_end = end;
        while hi < self.ranges.len() && self.ranges[hi].0 <= merged_end {
            merged_start = merged_start.min(self.ranges[hi].0);
            merged_end = merged_end.max(self.ranges[hi].1);
            hi += 1;
        }
        self.ranges
            .splice(lo..hi, std::iter::once((merged_start, merged_end)));
        if self.ranges.len() > MAX_PRE_ACTIVATION_RANGES {
            self.overflowed = true;
            self.ranges.clear();
        }
    }

    fn covers(&self, start: u64, end: u64) -> bool {
        if end <= start {
            return true;
        }
        self.ranges
            .iter()
            .any(|(range_start, range_end)| *range_start <= start && *range_end >= end)
    }

    fn intersects(&self, start: u64, end: u64) -> bool {
        if end <= start {
            return false;
        }
        self.ranges
            .iter()
            .any(|(range_start, range_end)| *range_start < end && *range_end > start)
    }

    fn covered_bytes(&self) -> u64 {
        self.ranges
            .iter()
            .map(|(start, end)| end.saturating_sub(*start))
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

/// Bytes staged for a block that straddles segment boundaries.
struct PartialBlock {
    buf: Vec<u8>,
    covered: RangeSet,
    data_len: u64,
}

struct LiveFile {
    par2_file_id: par2_rs::FileId,
    length: u64,
    blocks: Vec<BlockState>,
    partials: HashMap<u32, PartialBlock>,
    /// Blocks in-stream feeding can never finish: part of their span landed
    /// before activation, or their partial buffer was abandoned under the
    /// global cap. Settle reads own them.
    settle_only: HashSet<u32>,
    settled: bool,
}

impl LiveFile {
    fn all_ok(&self) -> bool {
        self.blocks.iter().all(|state| *state == BlockState::Ok)
    }

    fn pending_blocks(&self) -> impl Iterator<Item = u32> + '_ {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, state)| **state == BlockState::Pending)
            .map(|(index, _)| index as u32)
    }
}

struct ActiveJob {
    set: Arc<Par2FileSet>,
    files: HashMap<NzbFileId, LiveFile>,
    /// Files whose bytes were recorded before the set installed and that have
    /// not been bound yet.
    awaiting_binding: HashMap<NzbFileId, RangeSet>,
    /// Files that cannot be verified live (no match, no IFSC data, length
    /// disagreement). Never retried.
    rejected: HashSet<NzbFileId>,
    disk_read_budget: u64,
    queued_reads: Vec<LiveRead>,
}

enum JobLive {
    /// The job's spec carries no PAR2 file, so there is nothing to verify
    /// against and no coverage worth recording. Kept as a marker so the
    /// pipeline seam answers the question once per job instead of per segment.
    Skipped,
    Recording(HashMap<NzbFileId, RangeSet>),
    Active(Box<ActiveJob>),
}

/// Per-pipeline owner of every job's live verification state.
pub(crate) struct LivePar2Registry {
    enabled: bool,
    jobs: HashMap<JobId, JobLive>,
    partial_bytes: u64,
    partial_budget: u64,
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
            jobs: HashMap::new(),
            partial_bytes: 0,
            partial_budget: PARTIAL_BUDGET_BYTES,
            metrics: LiveMetrics::default(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Test-only switch. Production uses the `WEAVER_LIVE_PAR2` kill switch,
    /// which is process-wide and read once; a differential test needs both
    /// arms inside one process.
    #[cfg(test)]
    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Test-only cap override so the overflow path is reachable without
    /// allocating the production budget.
    #[cfg(test)]
    pub(crate) fn set_partial_budget(&mut self, bytes: u64) {
        self.partial_budget = bytes;
    }

    /// Test-only read-budget override, for the same reason as the partial cap:
    /// the exhaustion paths need a fixture, not a 256 MiB one.
    #[cfg(test)]
    pub(crate) fn set_disk_read_budget(&mut self, job_id: JobId, bytes: u64) {
        if let Some(JobLive::Active(active)) = self.jobs.get_mut(&job_id) {
            active.disk_read_budget = bytes;
        }
    }

    #[cfg(test)]
    pub(crate) fn disk_read_budget(&self, job_id: JobId) -> Option<u64> {
        match self.jobs.get(&job_id) {
            Some(JobLive::Active(active)) => Some(active.disk_read_budget),
            _ => None,
        }
    }

    pub(crate) fn metrics(&self) -> LiveMetrics {
        self.metrics
    }

    pub(crate) fn note_full_verify_skip(&mut self) {
        self.metrics.full_verify_skips += 1;
        crate::runtime::perf_probe::record(
            "verify.live_par2.full_verify_skipped",
            std::time::Duration::from_nanos(1),
        );
    }

    pub(crate) fn remove_job(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.remove(&job_id) else {
            return;
        };
        if let JobLive::Active(active) = job {
            for file in active.files.values() {
                for partial in file.partials.values() {
                    self.partial_bytes =
                        self.partial_bytes.saturating_sub(partial.buf.len() as u64);
                }
            }
        }
    }

    pub(crate) fn is_active(&self, job_id: JobId) -> bool {
        matches!(self.jobs.get(&job_id), Some(JobLive::Active(_)))
    }

    /// Whether this job already has a verdict about being tracked at all. The
    /// caller only has to inspect the job spec when this is `false`.
    pub(crate) fn knows_job(&self, job_id: JobId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    /// Record that a job carries no PAR2 file, so its segments never allocate
    /// recording state. `activate` still upgrades it if a PAR2 set turns up
    /// anyway (an obfuscated PAR2 file the spec's roles did not name).
    pub(crate) fn skip_job(&mut self, job_id: JobId) {
        self.jobs.entry(job_id).or_insert(JobLive::Skipped);
    }

    /// A file finished downloading. Its decoded length is the only length that
    /// can be compared with the PAR2 description, so a disagreement retires
    /// the file's live state fail-safe: whatever its blocks hashed to, it is
    /// not the file the description names.
    pub(crate) fn note_file_complete(&mut self, file_id: NzbFileId, received_bytes: u64) {
        if !self.enabled {
            return;
        }
        let Some(JobLive::Active(active)) = self.jobs.get_mut(&file_id.job_id) else {
            return;
        };
        if active
            .files
            .get(&file_id)
            .is_none_or(|file| file.length == received_bytes)
        {
            return;
        }
        let Some(file) = active.files.remove(&file_id) else {
            return;
        };
        active.rejected.insert(file_id);
        for partial in file.partials.values() {
            self.partial_bytes = self.partial_bytes.saturating_sub(partial.buf.len() as u64);
        }
        crate::runtime::perf_probe::record(
            "verify.live_par2.file_length_mismatch",
            std::time::Duration::from_nanos(1),
        );
    }

    /// Whether the pipeline must resolve this file's PAR2 identity before the
    /// next span can be fed.
    pub(crate) fn needs_binding(&self, file_id: NzbFileId) -> bool {
        let Some(JobLive::Active(active)) = self.jobs.get(&file_id.job_id) else {
            return false;
        };
        !active.files.contains_key(&file_id) && !active.rejected.contains(&file_id)
    }

    /// Install the parsed set. Returns the files with recorded pre-activation
    /// coverage that now need binding.
    pub(crate) fn activate(&mut self, job_id: JobId, set: Arc<Par2FileSet>) -> Vec<NzbFileId> {
        if !self.enabled || matches!(self.jobs.get(&job_id), Some(JobLive::Active(_))) {
            return Vec::new();
        }
        let recorded = match self.jobs.remove(&job_id) {
            Some(JobLive::Recording(recorded)) => recorded,
            _ => HashMap::new(),
        };
        let mut awaiting: Vec<NzbFileId> = recorded.keys().copied().collect();
        awaiting.sort_by_key(|file_id| file_id.file_index);
        self.jobs.insert(
            job_id,
            JobLive::Active(Box::new(ActiveJob {
                set,
                files: HashMap::new(),
                awaiting_binding: recorded,
                rejected: HashSet::new(),
                disk_read_budget: DISK_READ_BUDGET_BYTES,
                queued_reads: Vec::new(),
            })),
        );
        awaiting
    }

    /// Bind a file to its PAR2 description, or reject it permanently.
    pub(crate) fn bind(&mut self, file_id: NzbFileId, binding: Option<LiveBinding>) {
        let Some(JobLive::Active(active)) = self.jobs.get_mut(&file_id.job_id) else {
            return;
        };
        let recorded = active.awaiting_binding.remove(&file_id);
        let Some(binding) = binding else {
            active.rejected.insert(file_id);
            return;
        };
        let slice_size = active.set.slice_size;
        let block_count = active.set.slice_count_for_file(binding.length) as usize;
        let checksums_ok = active
            .set
            .file_checksums(&binding.par2_file_id)
            .is_some_and(|checksums| checksums.len() == block_count);
        if slice_size == 0 || block_count == 0 || !checksums_ok {
            active.rejected.insert(file_id);
            return;
        }

        let mut file = LiveFile {
            par2_file_id: binding.par2_file_id,
            length: binding.length,
            blocks: vec![BlockState::Pending; block_count],
            partials: HashMap::new(),
            settle_only: HashSet::new(),
            settled: false,
        };

        // Pre-activation coverage retained no data, so blocks it touches are
        // read back from disk instead: fully covered blocks are backfilled
        // now, partially covered ones can never be finished in-stream and are
        // left to settle.
        if let Some(recorded) = recorded.filter(|recorded| !recorded.is_empty()) {
            let mut backfill = Vec::new();
            for index in 0..block_count as u32 {
                let start = index as u64 * slice_size;
                let end = (start + slice_size).min(binding.length);
                if recorded.covers(start, end) {
                    backfill.push(index);
                } else if recorded.intersects(start, end) {
                    file.settle_only.insert(index);
                }
            }
            let reads = coalesce_block_reads(file_id, &backfill, slice_size, binding.length);
            // Backfill is eager: it runs mid-download for bytes that a settle
            // read would cover anyway, and only when the job later qualifies.
            // Spending a partial budget here would starve the settle pass that
            // actually decides, so an over-budget backfill is skipped whole and
            // those blocks stay `Pending`.
            let backfill_bytes: u64 = reads.iter().map(|read| read.len).sum();
            if backfill_bytes <= active.disk_read_budget {
                active.disk_read_budget -= backfill_bytes;
                active.queued_reads.extend(reads);
            } else {
                crate::runtime::perf_probe::record(
                    "verify.live_par2.backfill_skipped_over_budget",
                    std::time::Duration::from_nanos(1),
                );
            }
        }

        active.files.insert(file_id, file);
    }

    /// Record a committed span.
    ///
    /// Ordering contract: callers invoke this only after the segment's disk
    /// write returned, so every byte handed here is already visible to a
    /// later read-back of the same range (plan 135, D5).
    pub(crate) fn note_segment(&mut self, file_id: NzbFileId, offset: u64, data: &DecodedChunk) {
        if !self.enabled {
            return;
        }
        let len = data.len_bytes() as u64;
        if len == 0 {
            return;
        }
        match self
            .jobs
            .entry(file_id.job_id)
            .or_insert_with(|| JobLive::Recording(HashMap::new()))
        {
            JobLive::Skipped => {}
            JobLive::Recording(recorded) => {
                recorded
                    .entry(file_id)
                    .or_default()
                    .insert(offset, offset.saturating_add(len));
            }
            JobLive::Active(active) => {
                if let Some(recorded) = active.awaiting_binding.get_mut(&file_id) {
                    recorded.insert(offset, offset.saturating_add(len));
                    return;
                }
                if !active.files.contains_key(&file_id) {
                    return;
                }
                let set = Arc::clone(&active.set);
                let Some(file) = active.files.get_mut(&file_id) else {
                    return;
                };
                let mut cursor = offset;
                let mut claimed = 0u64;
                let mut bad = 0u64;
                let mut demoted = 0u64;
                let mut abandoned = 0u64;
                let mut out_of_range = 0u64;
                let mut partial_bytes = self.partial_bytes;
                let partial_budget = self.partial_budget;
                data.for_each_slice(|slice| {
                    feed_contiguous(
                        file,
                        &set,
                        cursor,
                        slice,
                        &mut PartialBudget {
                            used: &mut partial_bytes,
                            cap: partial_budget,
                        },
                        &mut FeedCounters {
                            claimed: &mut claimed,
                            bad: &mut bad,
                            demoted: &mut demoted,
                            abandoned: &mut abandoned,
                            out_of_range: &mut out_of_range,
                        },
                    );
                    cursor = cursor.saturating_add(slice.len() as u64);
                });
                self.partial_bytes = partial_bytes;
                self.metrics.blocks_claimed_in_stream += claimed;
                self.metrics.blocks_bad += bad;
                self.metrics.blocks_demoted += demoted;
                self.metrics.partials_abandoned += abandoned;
                self.metrics.spans_out_of_range += out_of_range;
                if out_of_range > 0 {
                    crate::runtime::perf_probe::record_value(
                        "verify.live_par2.spans_out_of_range",
                        out_of_range,
                    );
                }
                if claimed > 0 {
                    crate::runtime::perf_probe::record_value(
                        "verify.live_par2.blocks_claimed_in_stream",
                        claimed,
                    );
                }
                if bad > 0 {
                    crate::runtime::perf_probe::record_value("verify.live_par2.blocks_bad", bad);
                }
                if self.partial_bytes > self.metrics.partial_bytes_peak {
                    self.metrics.partial_bytes_peak = self.partial_bytes;
                    crate::runtime::perf_probe::record_value(
                        "verify.live_par2.partial_bytes_peak",
                        self.partial_bytes,
                    );
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn partial_bytes(&self) -> u64 {
        self.partial_bytes
    }

    #[cfg(test)]
    pub(crate) fn partial_budget_bytes(&self) -> u64 {
        self.partial_budget
    }

    /// Drain the queued activation backfill reads for a job.
    pub(crate) fn take_queued_reads(&mut self, job_id: JobId) -> Vec<LiveRead> {
        let Some(JobLive::Active(active)) = self.jobs.get_mut(&job_id) else {
            return Vec::new();
        };
        std::mem::take(&mut active.queued_reads)
    }

    /// Read plan for a file's still-`Pending` blocks. The file is only marked
    /// settled once the budget granted every read the plan asked for, so a
    /// budget-truncated pass is retried by a later sweep instead of being
    /// mistaken for finished work.
    pub(crate) fn take_settle_reads(&mut self, file_id: NzbFileId) -> Vec<LiveRead> {
        let Some(JobLive::Active(active)) = self.jobs.get_mut(&file_id.job_id) else {
            return Vec::new();
        };
        let slice_size = active.set.slice_size;
        let Some(file) = active.files.get_mut(&file_id) else {
            return Vec::new();
        };
        let pending: Vec<u32> = file.pending_blocks().collect();
        let length = file.length;
        if pending.is_empty() {
            file.settled = true;
            return Vec::new();
        }
        let reads = coalesce_block_reads(file_id, &pending, slice_size, length);
        let mut granted = Vec::new();
        let mut fully_granted = true;
        for read in reads {
            if active.disk_read_budget < read.len {
                fully_granted = false;
                break;
            }
            active.disk_read_budget -= read.len;
            granted.push(read);
        }
        if let Some(file) = active.files.get_mut(&file_id) {
            file.settled = fully_granted;
        }
        granted
    }

    /// Files that still have `Pending` blocks and have not been settled.
    pub(crate) fn files_needing_settle(&self, job_id: JobId) -> Vec<NzbFileId> {
        let Some(JobLive::Active(active)) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let mut files: Vec<NzbFileId> = active
            .files
            .iter()
            .filter(|(_, file)| !file.settled && file.pending_blocks().next().is_some())
            .map(|(file_id, _)| *file_id)
            .collect();
        files.sort_by_key(|file_id| file_id.file_index);
        files
    }

    /// Feed bytes read back from disk. `offset` must be block-aligned; a short
    /// read simply leaves the uncovered blocks `Pending`.
    pub(crate) fn apply_read(
        &mut self,
        file_id: NzbFileId,
        offset: u64,
        bytes: &[u8],
        from_backfill: bool,
    ) {
        let Some(JobLive::Active(active)) = self.jobs.get_mut(&file_id.job_id) else {
            return;
        };
        let set = Arc::clone(&active.set);
        let slice_size = set.slice_size;
        let Some(file) = active.files.get_mut(&file_id) else {
            return;
        };
        if slice_size == 0 || !offset.is_multiple_of(slice_size) {
            return;
        }
        let Some(checksums) = set.file_checksums(&file.par2_file_id) else {
            return;
        };
        let mut claimed = 0u64;
        let mut bad = 0u64;
        let mut index = offset / slice_size;
        while (index as usize) < file.blocks.len() {
            let start = index * slice_size;
            if start >= file.length {
                break;
            }
            let local = (start - offset) as usize;
            if local >= bytes.len() {
                break;
            }
            let data_len = (file.length - start).min(slice_size);
            let Some(end) = local.checked_add(data_len as usize) else {
                break;
            };
            if end > bytes.len() {
                break;
            }
            let Some(expected) = checksums.get(index as usize) else {
                break;
            };
            let verdict = verify_block_bytes(&bytes[local..end], slice_size, expected);
            file.blocks[index as usize] = verdict;
            file.settle_only.remove(&(index as u32));
            if let Some(partial) = file.partials.remove(&(index as u32)) {
                self.partial_bytes = self.partial_bytes.saturating_sub(partial.buf.len() as u64);
            }
            match verdict {
                BlockState::Ok => claimed += 1,
                BlockState::Bad => bad += 1,
                BlockState::Pending => {}
            }
            index += 1;
        }
        if from_backfill {
            self.metrics.blocks_backfilled += claimed;
            crate::runtime::perf_probe::record_value("verify.live_par2.blocks_backfilled", claimed);
        } else {
            self.metrics.blocks_settled += claimed;
            crate::runtime::perf_probe::record_value("verify.live_par2.blocks_settled", claimed);
        }
        self.metrics.blocks_bad += bad;
        if bad > 0 {
            crate::runtime::perf_probe::record_value("verify.live_par2.blocks_bad", bad);
        }
    }

    /// Files proven fully `Ok`, keyed by the PAR2 file id they carry. `None`
    /// when the job has no active verifier at all.
    pub(crate) fn fully_verified_files(
        &self,
        job_id: JobId,
    ) -> Option<HashMap<par2_rs::FileId, NzbFileId>> {
        if !self.enabled {
            return None;
        }
        let JobLive::Active(active) = self.jobs.get(&job_id)? else {
            return None;
        };
        let mut verified = HashMap::new();
        for (file_id, file) in &active.files {
            if file.all_ok() {
                verified.insert(file.par2_file_id, *file_id);
            }
        }
        Some(verified)
    }

    #[cfg(test)]
    pub(crate) fn block_states(&self, file_id: NzbFileId) -> Option<Vec<BlockState>> {
        let JobLive::Active(active) = self.jobs.get(&file_id.job_id)? else {
            return None;
        };
        active.files.get(&file_id).map(|file| file.blocks.clone())
    }

    #[cfg(test)]
    pub(crate) fn recorded_ranges(&self, file_id: NzbFileId) -> Option<Vec<(u64, u64)>> {
        match self.jobs.get(&file_id.job_id)? {
            JobLive::Skipped => None,
            JobLive::Recording(recorded) => recorded.get(&file_id).map(|set| set.ranges.clone()),
            JobLive::Active(active) => active
                .awaiting_binding
                .get(&file_id)
                .map(|set| set.ranges.clone()),
        }
    }

    #[cfg(test)]
    pub(crate) fn recorded_bytes(&self, file_id: NzbFileId) -> u64 {
        self.recorded_ranges(file_id)
            .map(|ranges| {
                ranges
                    .iter()
                    .map(|(start, end)| end.saturating_sub(*start))
                    .sum()
            })
            .unwrap_or_default()
    }
}

struct FeedCounters<'a> {
    claimed: &'a mut u64,
    bad: &'a mut u64,
    demoted: &'a mut u64,
    abandoned: &'a mut u64,
    out_of_range: &'a mut u64,
}

/// The global partial-buffer allowance, borrowed for one feed.
struct PartialBudget<'a> {
    used: &'a mut u64,
    cap: u64,
}

impl PartialBudget<'_> {
    fn release(&mut self, bytes: u64) {
        *self.used = self.used.saturating_sub(bytes);
    }
}

fn feed_contiguous(
    file: &mut LiveFile,
    set: &Par2FileSet,
    offset: u64,
    bytes: &[u8],
    budget: &mut PartialBudget<'_>,
    counters: &mut FeedCounters<'_>,
) {
    let slice_size = set.slice_size;
    let Some(checksums) = set.file_checksums(&file.par2_file_id) else {
        return;
    };
    // The block vector is sized from the PAR2 description, not from the NZB's
    // declared totals, so a span can legitimately run past its end (a file
    // bound to a shorter description, a trailing segment). Everything past
    // `file.length` is ignored rather than indexed.
    if offset >= file.length || offset.saturating_add(bytes.len() as u64) > file.length {
        *counters.out_of_range += 1;
    }
    let mut cursor = offset;
    let mut rest = bytes;
    while !rest.is_empty() && cursor < file.length {
        let index = cursor / slice_size;
        let block_start = index * slice_size;
        let data_len = (file.length - block_start).min(slice_size);
        let block_end = block_start + data_len;
        if cursor >= block_end {
            break;
        }
        let take = ((block_end - cursor) as usize).min(rest.len());
        let chunk = &rest[..take];
        let Some(expected) = checksums.get(index as usize) else {
            break;
        };

        if cursor == block_start && take as u64 == data_len {
            // Whole block inside one contiguous decoded slice: verify without
            // copying anything.
            let verdict = verify_block_bytes(chunk, slice_size, expected);
            file.blocks[index as usize] = verdict;
            file.settle_only.remove(&(index as u32));
            if let Some(partial) = file.partials.remove(&(index as u32)) {
                budget.release(partial.buf.len() as u64);
            }
            match verdict {
                BlockState::Ok => *counters.claimed += 1,
                BlockState::Bad => *counters.bad += 1,
                BlockState::Pending => {}
            }
        } else {
            stage_partial(
                file,
                index as u32,
                data_len,
                cursor - block_start,
                chunk,
                slice_size,
                expected,
                budget,
                counters,
            );
        }

        cursor += take as u64;
        rest = &rest[take..];
    }
}

#[allow(clippy::too_many_arguments)]
fn stage_partial(
    file: &mut LiveFile,
    index: u32,
    data_len: u64,
    block_offset: u64,
    chunk: &[u8],
    slice_size: u64,
    expected: &par2_rs::SliceChecksum,
    budget: &mut PartialBudget<'_>,
    counters: &mut FeedCounters<'_>,
) {
    if file.blocks[index as usize] != BlockState::Pending {
        // A boundary re-feed of a block that already has a verdict: the bytes
        // on disk may have changed under it (a duplicate segment, a CRC-failed
        // decode that still committed), and a partial feed cannot re-verify on
        // its own the way the whole-block path does. The verdict is retired and
        // the settle read owns the block from here.
        file.blocks[index as usize] = BlockState::Pending;
        file.settle_only.insert(index);
        file.settled = false;
        if let Some(partial) = file.partials.remove(&index) {
            budget.release(partial.buf.len() as u64);
        }
        *counters.demoted += 1;
        crate::runtime::perf_probe::record(
            "verify.live_par2.block_demoted",
            std::time::Duration::from_nanos(1),
        );
        return;
    }
    if file.settle_only.contains(&index) {
        return;
    }

    if !file.partials.contains_key(&index) {
        let Ok(buf_len) = usize::try_from(data_len) else {
            file.settle_only.insert(index);
            return;
        };
        if budget.used.saturating_add(data_len) > budget.cap {
            // Over the global cap: abandon the partial and let the settle read
            // own this block rather than allocating.
            file.settle_only.insert(index);
            *counters.abandoned += 1;
            crate::runtime::perf_probe::record(
                "verify.live_par2.partial_abandoned",
                std::time::Duration::from_nanos(1),
            );
            return;
        }
        *budget.used = budget.used.saturating_add(data_len);
        file.partials.insert(
            index,
            PartialBlock {
                buf: vec![0u8; buf_len],
                covered: RangeSet::default(),
                data_len,
            },
        );
    }

    let Some(partial) = file.partials.get_mut(&index) else {
        return;
    };
    let start = block_offset as usize;
    let end = start + chunk.len();
    if end > partial.buf.len() {
        return;
    }
    partial.buf[start..end].copy_from_slice(chunk);
    partial
        .covered
        .insert(block_offset, block_offset + chunk.len() as u64);
    if partial.covered.covered_bytes() < partial.data_len {
        return;
    }

    let Some(partial) = file.partials.remove(&index) else {
        return;
    };
    budget.release(partial.buf.len() as u64);
    let verdict = verify_block_bytes(&partial.buf, slice_size, expected);
    file.blocks[index as usize] = verdict;
    match verdict {
        BlockState::Ok => *counters.claimed += 1,
        BlockState::Bad => *counters.bad += 1,
        BlockState::Pending => {}
    }
}

/// Full-strength block check: MD5 **and** CRC32 against the IFSC entry, with
/// the tail block zero-padded to `slice_size` exactly as `par2-rs` does.
/// v1 has no CRC-only fast path — plan 135 D5 leaves that as future work.
fn verify_block_bytes(
    data: &[u8],
    slice_size: u64,
    expected: &par2_rs::SliceChecksum,
) -> BlockState {
    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("verify.live_par2.block_hash");
    let mut state = par2_rs::SliceChecksumState::new();
    state.update(data);
    let (crc32, md5) = state.finalize(Some(slice_size));
    if crc32 == expected.crc32 && md5 == expected.md5 {
        BlockState::Ok
    } else {
        BlockState::Bad
    }
}

/// Turn a sorted block index list into read requests, merging adjacent blocks
/// up to [`READ_CHUNK_BYTES`].
fn coalesce_block_reads(
    file_id: NzbFileId,
    blocks: &[u32],
    slice_size: u64,
    length: u64,
) -> Vec<LiveRead> {
    let mut reads: Vec<LiveRead> = Vec::new();
    for index in blocks {
        let start = *index as u64 * slice_size;
        if start >= length {
            continue;
        }
        let len = (length - start).min(slice_size);
        match reads.last_mut() {
            Some(last) if last.offset + last.len == start && last.len + len <= READ_CHUNK_BYTES => {
                last.len += len;
            }
            _ => reads.push(LiveRead {
                file_id,
                offset: start,
                len,
            }),
        }
    }
    reads
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

/// [`read_range_best_effort`] over a direct set's virtual volume (plan 135, D5).
///
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

#[cfg(test)]
mod tests;
