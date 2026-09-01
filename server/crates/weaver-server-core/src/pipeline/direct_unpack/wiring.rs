//! The controller: deciding which 7z sets to chase, feeding the chase, and
//! ending it.
//!
//! # Where a chase begins
//!
//! A **split** set arms off its topology, which is built when one of its parts
//! finishes downloading. That is the earliest moment the *ordered part list*
//! exists, and the gated reader is nothing without it: the archive stream is
//! the concatenation of those parts in that order. The topology names every
//! part, complete or not, so the chase starts with parts still arriving.
//!
//! A **single** `.7z` has no order to discover, so it does not wait for a
//! topology at all — it arms as a one-part set the moment its first 32 bytes
//! are committed, which is the earliest anything can be known about it. Its
//! length is the header's word until the file finishes; completion settles it,
//! and a disagreement aborts the set rather than feeding the decoder a stream
//! that is not the archive the header described.
//!
//! Admission is retried, not latched, while the answer is merely "not yet" — no
//! bytes on part one, no topology. It latches permanently on a real refusal, so
//! a malformed archive is examined once and never again.
//!
//! # What the chase costs when it is off
//!
//! The watermark hook sits on the download's commit path, so its cost when the
//! feature is dark has to be indistinguishable from zero: one `is_empty` on a
//! map, and nothing else — no hashing, no allocation, no lock.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use super::coverage::SetCoverage;
use super::reader::GatedSplitReader;
use super::settings::{DirectUnpackGate, DirectUnpackSettings};
use super::start_header::{SIGNATURE_HEADER_LEN, StartHeader};
use crate::jobs::ids::JobId;
use crate::pipeline::FullSetExtractionOutcome;
use crate::pipeline::Pipeline;
use crate::pipeline::completion::finalize::extract::{
    SevenZipExtractionContext, extract_7z_stream,
};
use crate::pipeline::extraction::ExtractionRoot;

/// Buffer between the decoder and the gated reader.
///
/// The gated reader returns short reads at the download frontier — it serves
/// what is committed and no more — and an unbuffered decoder would turn that
/// into a syscall per fragment. 128 KiB is large enough to amortise that and
/// small enough that a park never sits on a mostly-empty buffer.
const CHASE_BUFFER_BYTES: usize = 128 * 1024;

/// Why a set will never be chased.
///
/// Every variant is permanent for that set: the conventional path still
/// extracts it, so a refusal costs nothing but the chase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalReason {
    /// The first part's opening bytes could not be read.
    HeaderUnreadable,
    /// The bytes are not a valid 7z signature header (bad magic, bad CRC).
    HeaderMalformed,
    /// The header declares no end header, so there is no entry table to decode.
    EmptyEndHeader,
    /// The declared end header is larger than the extraction memory budget
    /// would allow the decoder to buffer.
    EndHeaderTooLarge,
    /// The declared lengths do not describe a coherent archive.
    LengthOverflow,
    /// The chase could not get a staging directory or an extraction budget.
    BudgetUnavailable,
}

impl RefusalReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HeaderUnreadable => "header_unreadable",
            Self::HeaderMalformed => "header_malformed",
            Self::EmptyEndHeader => "empty_end_header",
            Self::EndHeaderTooLarge => "end_header_too_large",
            Self::LengthOverflow => "length_overflow",
            Self::BudgetUnavailable => "budget_unavailable",
        }
    }
}

/// Whether an aborted set may ever be chased again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortLatch {
    /// Never re-arm: the archive, the topology, or the job is gone.
    Permanent,
    /// May re-arm later. The bytes stopped for a reason that says nothing about
    /// the archive — a pause, say — so a later part completion can try again.
    Retryable,
}

/// Why an armed set stopped being chased.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemotionReason {
    /// The download ended before the chase could finish.
    DownloadEnded,
    /// A part file could not be opened or read — usually a rename that raced
    /// the chase.
    PartUnreadable,
    /// The decoder rejected the archive.
    DecodeFailed,
    /// PAR2 repair replaced bytes the chase had already read.
    RepairRewrote,
}

impl DemotionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DownloadEnded => "download_ended",
            Self::PartUnreadable => "part_unreadable",
            Self::DecodeFailed => "decode_failed",
            Self::RepairRewrote => "repair_rewrote",
        }
    }
}

/// What a finished chase produced.
///
/// Consumption installs these members instead of decoding the set a second
/// time, so the whole record is kept rather than reduced to a boolean: the
/// member list, the byte totals the Extracting phase needs, and the directory
/// to move from.
pub struct ChaseOutcome {
    pub result: Result<FullSetExtractionOutcome, String>,
    pub elapsed: Duration,
    pub staging_dir: PathBuf,
    /// Bytes the chase declared and wrote. Consumption attributes these to the
    /// job's Extracting phase, which is otherwise never told about work that
    /// happened before the phase began.
    pub total_bytes: u64,
    pub completed_bytes: u64,
    /// Repair rewrote a source file after this chase read it.
    pub tainted: bool,
}

impl ChaseOutcome {
    /// Break a usable outcome into the pieces consumption installs from.
    ///
    /// Only called on an outcome already known to be `Ok`, which is what
    /// [`ChaseDisposition::Ready`] means.
    pub(in crate::pipeline) fn into_installable(
        self,
    ) -> (FullSetExtractionOutcome, PathBuf, u64, u64) {
        let members = self
            .result
            .expect("a Ready disposition carries an Ok outcome");
        (
            members,
            self.staging_dir,
            self.total_bytes,
            self.completed_bytes,
        )
    }
}

/// Counters for the chase, by outcome.
#[derive(Debug, Default, Clone, Copy)]
pub struct DirectUnpackCounters {
    pub armed: u64,
    pub refused_header_unreadable: u64,
    pub refused_header_malformed: u64,
    pub refused_empty_end_header: u64,
    pub refused_end_header_too_large: u64,
    pub refused_length_overflow: u64,
    pub refused_budget_unavailable: u64,
    pub completed: u64,
    pub demoted_download_ended: u64,
    pub demoted_part_unreadable: u64,
    pub demoted_decode_failed: u64,
    pub demoted_repair_rewrote: u64,
    /// Chases whose members were installed instead of re-extracting.
    pub consumed: u64,
    /// Chases whose output was thrown away in favour of conventional extraction.
    pub discarded: u64,
}

/// Publish one direct-unpack event.
///
/// Every call site is a per-*set* decision — arming, refusing, finishing,
/// demoting, consuming — never a per-commit one. The watermark hook on the
/// download path deliberately records nothing: the standing rule is that
/// metrics never touch the hot path, and a counter there would be exactly that.
fn record_event(name: &str) {
    crate::runtime::perf_probe::record_owned(
        format!("direct_unpack.{name}"),
        std::time::Duration::from_nanos(1),
    );
}

impl DirectUnpackCounters {
    fn record_refusal(&mut self, reason: RefusalReason) {
        record_event(&format!("refused.{}", reason.as_str()));
        match reason {
            RefusalReason::HeaderUnreadable => self.refused_header_unreadable += 1,
            RefusalReason::HeaderMalformed => self.refused_header_malformed += 1,
            RefusalReason::EmptyEndHeader => self.refused_empty_end_header += 1,
            RefusalReason::EndHeaderTooLarge => self.refused_end_header_too_large += 1,
            RefusalReason::LengthOverflow => self.refused_length_overflow += 1,
            RefusalReason::BudgetUnavailable => self.refused_budget_unavailable += 1,
        }
    }

    fn record_demotion(&mut self, reason: DemotionReason) {
        record_event(&format!("demoted.{}", reason.as_str()));
        match reason {
            DemotionReason::DownloadEnded => self.demoted_download_ended += 1,
            DemotionReason::PartUnreadable => self.demoted_part_unreadable += 1,
            DemotionReason::DecodeFailed => self.demoted_decode_failed += 1,
            DemotionReason::RepairRewrote => self.demoted_repair_rewrote += 1,
        }
    }
}

/// One set currently being chased.
struct ArmedSet {
    coverage: Arc<SetCoverage>,
    staging_dir: PathBuf,
    handle: tokio::task::JoinHandle<Result<FullSetExtractionOutcome, String>>,
    started_at: Instant,
    /// The chase's own byte counters, detached from the job's phase display.
    /// Consumption copies these into the real phase so the Extracting bar shows
    /// the work that actually happened, attributed exactly once.
    counters: Arc<crate::jobs::PhaseCounters>,
}

/// A chase that has not finished yet, handed to the extraction context so it
/// can be awaited there rather than on the orchestrator loop.
pub(in crate::pipeline) struct PendingChase {
    pub(in crate::pipeline) handle:
        tokio::task::JoinHandle<Result<FullSetExtractionOutcome, String>>,
    pub(in crate::pipeline) staging_dir: PathBuf,
    pub(in crate::pipeline) counters: Arc<crate::jobs::PhaseCounters>,
}

/// What extraction should do about a set's chase.
pub(in crate::pipeline) enum ChaseDisposition {
    /// No usable chase; extract conventionally.
    None,
    /// A finished, untainted chase whose members are ready to install.
    Ready(Box<ChaseOutcome>),
    /// A chase still running. Every part is complete by now, so it is finishing
    /// at disk speed.
    Pending(PendingChase),
}

/// Per-pipeline direct-unpack state.
///
/// # A note on single-file 7z sets
///
/// Their topology is only built once the archive is fully downloaded, so a
/// chase admitted for one has nothing left to overlap. It is allowed rather
/// than special-cased: it costs one decode that the conventional path would
/// have done anyway, and it keeps the admission rule uniform. Making single-file
/// sets genuinely early would need a part list derived from the NZB's
/// classification instead of the topology, which is a larger change than this
/// work package.
#[derive(Default)]
pub(crate) struct DirectUnpackRuntime {
    /// Resolved once at pipeline construction and never re-read, so a set
    /// admitted under an enabled gate cannot find it disabled mid-chase.
    /// `None` in tests that build a runtime by hand, where the gate falls back
    /// to the all-defaults resolution: off.
    settings: Option<DirectUnpackSettings>,
    /// Sets currently being chased.
    armed: HashMap<(JobId, String), ArmedSet>,
    /// Files that are a bare `.7z` and have not been offered to arming yet.
    ///
    /// A split set arms off its topology, which appears when a part completes.
    /// A single file has no topology until the whole thing has landed, so its
    /// arming has to ride the commit path instead — and this set is what keeps
    /// that ride free: when it is empty, which is every job that carries no
    /// unsplit 7z, the hot path's added cost is one `is_empty`.
    pending_single_arm: HashSet<crate::jobs::ids::NzbFileId>,
    /// Sets held parked through a PAR2 repair rather than tainted, because
    /// every byte their decoder had already consumed was vouched for by the
    /// recovery set. Released when the repair reports success.
    parked_through_repair: HashSet<(JobId, String)>,
    /// Chases that have been woken with an abort and are awaiting a join. Kept
    /// apart from `armed` so an abort can be signalled synchronously from the
    /// paths that end a download, and joined later from the run loop.
    draining: Vec<((JobId, String), ArmedSet)>,
    /// Sets that will never arm again, with the reason they were refused.
    latched: HashMap<(JobId, String), &'static str>,
    /// `filename -> (set name, part index)` per job, for the watermark hook.
    /// Keyed so the hot path can look up by borrowed `&str` without allocating.
    watermark_targets: HashMap<JobId, HashMap<String, (String, usize)>>,
    /// Finished chases, awaiting a consumer.
    outcomes: HashMap<(JobId, String), ChaseOutcome>,
    counters: DirectUnpackCounters,
}

impl DirectUnpackRuntime {
    pub(crate) fn with_settings(settings: DirectUnpackSettings) -> Self {
        Self {
            settings: Some(settings),
            ..Self::default()
        }
    }

    pub(crate) fn settings(&self) -> DirectUnpackSettings {
        self.settings.unwrap_or_default()
    }

    pub(crate) fn gate(&self) -> DirectUnpackGate {
        self.settings().gate
    }

    /// Outcome counters, as a snapshot.
    ///
    /// The exported metric surface is the per-event `direct_unpack.*` records
    /// emitted at each decision; this snapshot exists so tests can assert on
    /// the same numbers without scraping them back out.
    #[cfg(test)]
    pub(crate) fn counters(&self) -> DirectUnpackCounters {
        self.counters
    }

    /// Whether the commit hook has nothing at all to do: nothing being chased,
    /// and no bare `.7z` waiting to arm.
    fn idle(&self) -> bool {
        self.armed.is_empty() && self.pending_single_arm.is_empty()
    }

    /// Whether the commit hook has any bare `.7z` waiting to arm.
    #[cfg(test)]
    pub(crate) fn no_pending_single_arm(&self) -> bool {
        self.pending_single_arm.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_armed(&self, job_id: JobId, set_name: &str) -> bool {
        self.armed.contains_key(&(job_id, set_name.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn latched_reason(&self, job_id: JobId, set_name: &str) -> Option<&'static str> {
        self.latched.get(&(job_id, set_name.to_string())).copied()
    }

    #[cfg(test)]
    pub(crate) fn outcome(&self, job_id: JobId, set_name: &str) -> Option<&ChaseOutcome> {
        self.outcomes.get(&(job_id, set_name.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn armed_coverage(&self, job_id: JobId, set_name: &str) -> Option<Arc<SetCoverage>> {
        self.armed
            .get(&(job_id, set_name.to_string()))
            .map(|set| Arc::clone(&set.coverage))
    }
}

impl Pipeline {
    /// Try to admit a 7z set to the chase.
    ///
    /// Called whenever a 7z topology is built or updated. Returns without
    /// latching when the answer is only "not yet" — no bytes on part one — so a
    /// later part's completion can try again.
    pub(in crate::pipeline) fn try_arm_direct_unpack(&mut self, job_id: JobId, set_name: &str) {
        if self.direct_unpack.gate() != DirectUnpackGate::Enabled {
            return;
        }
        let key = (job_id, set_name.to_string());
        if self.direct_unpack.armed.contains_key(&key)
            || self.direct_unpack.latched.contains_key(&key)
            || self.direct_unpack.outcomes.contains_key(&key)
        {
            return;
        }

        let Ok(paths) = self.sevenz_set_part_paths(job_id, set_name) else {
            return;
        };
        if paths.is_empty() {
            return;
        }
        self.arm_direct_unpack_with_paths(job_id, set_name, paths);
    }

    /// Arm a set over an explicit ordered part list.
    ///
    /// Split sets reach this through the topology, which is the only thing that
    /// knows their order. A bare `.7z` reaches it directly with a one-element
    /// list, because there is no order to discover and waiting for a topology
    /// would mean waiting for the whole file — which is the entire overlap.
    ///
    /// Callers are responsible for the gate and the already-armed checks; by
    /// here the decision to try is made.
    fn arm_direct_unpack_with_paths(&mut self, job_id: JobId, set_name: &str, paths: Vec<PathBuf>) {
        // The signature header lives in the first 32 bytes of part one. Read it
        // from the file rather than from any in-memory view: the file is what
        // the chase will read, and if those bytes are not there yet there is
        // nothing to decide on.
        let header_bytes = match read_signature_header(&paths[0]) {
            Ok(Some(bytes)) => bytes,
            // Fewer than 32 bytes so far: not a refusal, just early.
            Ok(None) => return,
            Err(error) => {
                debug!(
                    job_id = job_id.0,
                    set_name,
                    error = %error,
                    "direct unpack could not read the 7z signature header"
                );
                self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::HeaderUnreadable);
                return;
            }
        };

        let header = match StartHeader::parse(&header_bytes) {
            Ok(header) => header,
            Err(error) => {
                debug!(
                    job_id = job_id.0,
                    set_name,
                    error = %error,
                    "direct unpack refused a malformed 7z signature header"
                );
                self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::HeaderMalformed);
                return;
            }
        };

        // No entry table means nothing to decode.
        if header.next_header_size == 0 {
            self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::EmptyEndHeader);
            return;
        }

        let Ok(total_len) = header.total_len() else {
            self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::LengthOverflow);
            return;
        };

        let output_dir = self.direct_unpack_staging_dir(job_id, set_name);
        if let Err(error) = std::fs::create_dir_all(&output_dir) {
            warn!(
                job_id = job_id.0,
                set_name,
                path = %output_dir.display(),
                error = %error,
                "direct unpack could not create its staging directory"
            );
            self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::BudgetUnavailable);
            return;
        }
        let budget = match self.direct_unpack_budget(job_id, &paths, &output_dir) {
            Ok(budget) => budget,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name,
                    error = %error,
                    "direct unpack could not open an extraction budget"
                );
                self.latch_direct_unpack_refusal(
                    job_id,
                    set_name,
                    RefusalReason::BudgetUnavailable,
                );
                return;
            }
        };

        // `ArchiveReader::new` buffers the declared end header whole, and that
        // length came out of a file weaver fetched from a stranger. Bound it
        // against the same ceiling the conventional extractor reserves, BEFORE
        // any decoder sees it — checking after the allocation checks nothing.
        let end_header_ceiling = budget.max_memory_bytes();
        if header.next_header_size > end_header_ceiling {
            warn!(
                job_id = job_id.0,
                set_name,
                declared_end_header_bytes = header.next_header_size,
                ceiling = end_header_ceiling,
                "direct unpack refused an oversized 7z end header"
            );
            self.latch_direct_unpack_refusal(job_id, set_name, RefusalReason::EndHeaderTooLarge);
            return;
        }

        let coverage = Arc::new(SetCoverage::new(paths.len()));
        coverage.set_total_len(total_len);

        // Seed every part length already known, and build the filename lookup
        // the watermark hook uses. A length that is not known yet is not
        // invented: the reader parks on it until completion supplies it.
        let mut targets: HashMap<String, (String, usize)> = HashMap::new();
        for (index, path) in paths.iter().enumerate() {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                targets.insert(name.to_string(), (set_name.to_string(), index));
            }
            // Seed from the progress floor, never from the file's length on
            // disk.
            //
            // Under write-backlog pressure the decode path evicts parked
            // segments with `persist_out_of_order_segments`, which writes them
            // at their true offsets — beyond the contiguous floor, leaving
            // sparse holes behind. The file's length then describes the
            // furthest byte written, not the verified prefix, and a watermark
            // seeded from it would hand the decoder zero-filled holes as
            // committed bytes. For a 7z entry without a CRC that is silent
            // corruption. The floor is the only value that means "contiguous
            // and verified from zero", and because the persisted half of it is
            // also what a restart trusts, reading it here makes arming after a
            // restart correct for the same reason.
            if let Some(floor) = self.direct_unpack_progress_floor(job_id, path) {
                coverage.advance_watermark(index, floor);
            }
            if let Some(len) = self.direct_unpack_known_part_len(job_id, path) {
                coverage.note_part_len(index, len);
                coverage.mark_part_complete(index);
            }
        }

        let root = match ExtractionRoot::open(&output_dir) {
            Ok(root) => Arc::new(root),
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name,
                    error = %error,
                    "direct unpack could not open its staging root"
                );
                self.latch_direct_unpack_refusal(
                    job_id,
                    set_name,
                    RefusalReason::BudgetUnavailable,
                );
                return;
            }
        };

        let password = self.primary_archive_password_for_job(job_id);
        let boost_paths = paths.clone();
        let counters = Arc::new(crate::jobs::PhaseCounters::default());
        let handle = self.spawn_direct_unpack_worker(
            job_id,
            set_name.to_string(),
            paths,
            Arc::clone(&coverage),
            output_dir.clone(),
            root,
            budget,
            password,
            Arc::clone(&counters),
        );

        self.direct_unpack
            .watermark_targets
            .entry(job_id)
            .or_default()
            .extend(targets);
        self.direct_unpack.armed.insert(
            (job_id, set_name.to_string()),
            ArmedSet {
                coverage,
                staging_dir: output_dir,
                handle,
                started_at: Instant::now(),
                counters,
            },
        );
        self.direct_unpack.counters.armed += 1;
        record_event("armed");

        info!(
            job_id = job_id.0,
            set_name,
            total_bytes = total_len,
            "direct unpack armed"
        );

        self.boost_direct_unpack_tail_window(
            job_id,
            &boost_paths,
            total_len,
            header.next_header_size,
        );
    }

    /// Pull the archive's tail forward in the download queue.
    ///
    /// The decoder cannot list anything until it has the end header, which sits
    /// at the very end of the last part, so until those bytes land the chase is
    /// parked and the overlap has not begun. The birth-time boost covers the
    /// common case; this covers a slow job whose queue is still deep when the
    /// set arms.
    ///
    /// The window is `[total − W, total)` with `W = min(2·next_header_size +
    /// 1 MiB, 16 MiB)`: twice the declared end header, because a compressed or
    /// encrypted header is itself a packed stream sitting just before it, plus
    /// a megabyte of slack, capped so a preposterous declaration cannot boost
    /// the whole archive.
    ///
    /// Parts are mapped by their **NZB-declared** sizes — allowed here and
    /// nowhere else in this module, because this only reorders a queue. A
    /// declared size that turns out to be wrong boosts the wrong segments,
    /// which costs a little overlap and cannot cost correctness: the gated
    /// reader is driven by verified watermarks, never by anything computed
    /// here.
    ///
    /// Work already leased to a connection is unreachable by reprioritization.
    /// That is the same contract the identity head wave lives with, and the
    /// birth-time boost is what covers it.
    fn boost_direct_unpack_tail_window(
        &mut self,
        job_id: JobId,
        paths: &[PathBuf],
        total_len: u64,
        next_header_size: u64,
    ) {
        const TAIL_WINDOW_SLACK: u64 = 1024 * 1024;
        const TAIL_WINDOW_MAX: u64 = 16 * 1024 * 1024;

        let window = next_header_size
            .saturating_mul(2)
            .saturating_add(TAIL_WINDOW_SLACK)
            .min(TAIL_WINDOW_MAX);
        let window_start = total_len.saturating_sub(window);

        let mut targets: HashSet<crate::jobs::ids::SegmentId> = HashSet::new();
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let mut part_start = 0u64;
            for path in paths {
                let Some(file_id) = self.direct_unpack_file_id_for_part(job_id, path) else {
                    return;
                };
                let Some(file) = state.assembly.file(file_id) else {
                    return;
                };
                let part_end = part_start.saturating_add(file.total_bytes());

                if part_end > window_start {
                    let local_from = window_start.saturating_sub(part_start);
                    for segment_number in 0..file.total_segments() {
                        if let Some((_, to)) = file.segment_bounds(segment_number)
                            && to > local_from
                        {
                            targets.insert(crate::jobs::ids::SegmentId {
                                file_id,
                                segment_number,
                            });
                        }
                    }
                }
                part_start = part_end;
            }
        }

        if targets.is_empty() {
            return;
        }
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let boosted = state
            .download_queue
            .reprioritize_matching(|work| targets.contains(&work.segment_id).then_some(2));
        if boosted > 0 {
            debug!(
                job_id = job_id.0,
                boosted,
                window_bytes = window,
                "pulled the 7z tail window forward"
            );
        }
    }

    /// Publish a just-completed part to its chase, then try to arm the set.
    ///
    /// Completion is what supplies a part's exact length — the only moment the
    /// download knows it for certain — so this is where a chase learns where
    /// the next part begins.
    pub(in crate::pipeline) fn try_arm_direct_unpack_for_file(
        &mut self,
        job_id: JobId,
        file_id: crate::jobs::ids::NzbFileId,
    ) {
        if self.direct_unpack.gate() != DirectUnpackGate::Enabled {
            return;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        if file_asm.is_complete() {
            let filename = self.current_filename_for_file(job_id, file_asm);
            let received = file_asm.received_bytes();
            self.direct_unpack_note_commit(file_id, &filename, received, true);
            // Its grid verdicts are final now, so this is when damage becomes
            // knowable — and when the frontier has to stop short of it.
            self.cap_chased_part_at_damage(file_id, &filename);
        }

        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };
        // Only 7z sets are chased; the seam fires for every non-RAR archive.
        if !matches!(
            self.classified_role_for_file(job_id, file_asm),
            weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. }
        ) {
            return;
        }
        let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file_asm) else {
            return;
        };
        self.try_arm_direct_unpack(job_id, &set_name);
    }

    /// Register a job's bare `.7z` files as arming candidates.
    ///
    /// Called once at admission. Nothing is registered when the gate is off, so
    /// a dark pipeline keeps an empty set and the commit hook keeps costing one
    /// `is_empty`.
    pub(crate) fn register_direct_unpack_singles(&mut self, job_id: JobId, spec: &crate::JobSpec) {
        if self.direct_unpack.gate() != DirectUnpackGate::Enabled {
            return;
        }
        for (file_index, file) in spec.files.iter().enumerate() {
            if matches!(file.role, weaver_model::files::FileRole::SevenZipArchive) {
                self.direct_unpack
                    .pending_single_arm
                    .insert(crate::jobs::ids::NzbFileId {
                        job_id,
                        file_index: file_index as u32,
                    });
            }
        }
    }

    /// Try to arm a bare `.7z` from its opening bytes.
    ///
    /// Rides the commit path rather than the completion path: waiting for
    /// completion would mean waiting for the whole archive, which is exactly the
    /// overlap this exists to win. The candidate is retired from the pending set
    /// on any outcome that settles it — armed, refused, or no longer a single
    /// 7z — so the ride is paid for once.
    fn try_arm_single_sevenz(&mut self, file_id: crate::jobs::ids::NzbFileId, floor: u64) {
        if floor < SIGNATURE_HEADER_LEN {
            return;
        }
        let job_id = file_id.job_id;

        let Some(state) = self.jobs.get(&job_id) else {
            self.direct_unpack.pending_single_arm.remove(&file_id);
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            self.direct_unpack.pending_single_arm.remove(&file_id);
            return;
        };
        // Classification can move a file off `SevenZipArchive` — a rename, or a
        // set that turns out to be split after all. Either way it is no longer
        // this path's business.
        if !matches!(
            self.classified_role_for_file(job_id, file_asm),
            weaver_model::files::FileRole::SevenZipArchive
        ) {
            self.direct_unpack.pending_single_arm.remove(&file_id);
            return;
        }
        let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file_asm) else {
            return;
        };
        let filename = self.current_filename_for_file(job_id, file_asm);
        let Some(path) = self.resolve_job_input_path(job_id, &filename) else {
            self.direct_unpack.pending_single_arm.remove(&file_id);
            return;
        };

        let key = (job_id, set_name.clone());
        if self.direct_unpack.armed.contains_key(&key)
            || self.direct_unpack.latched.contains_key(&key)
            || self.direct_unpack.outcomes.contains_key(&key)
        {
            self.direct_unpack.pending_single_arm.remove(&file_id);
            return;
        }

        self.arm_direct_unpack_with_paths(job_id, &set_name, vec![path]);

        // Armed or refused, the candidate is settled either way. A refusal
        // latches, so leaving it pending would re-read the same 32 bytes on
        // every commit for the rest of the download.
        if self.direct_unpack.armed.contains_key(&key)
            || self.direct_unpack.latched.contains_key(&key)
        {
            self.direct_unpack.pending_single_arm.remove(&file_id);
        }
    }

    fn latch_direct_unpack_refusal(
        &mut self,
        job_id: JobId,
        set_name: &str,
        reason: RefusalReason,
    ) {
        self.direct_unpack
            .latched
            .insert((job_id, set_name.to_string()), reason.as_str());
        self.direct_unpack.counters.record_refusal(reason);
    }

    /// Where a chased set's members land.
    ///
    /// Deliberately not the conventional staging dir and not inside it: the
    /// conventional extractor's own output and the delivery scan both live
    /// there, and a demotion has to be able to `remove_dir_all` this without
    /// touching anything the conventional path will look at.
    pub(in crate::pipeline) fn direct_unpack_staging_dir(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> PathBuf {
        self.complete_dir
            .join(".weaver-direct-unpack")
            .join(job_id.0.to_string())
            .join(sanitize_set_dir_name(set_name))
    }

    /// An extraction budget for the chase, rooted at its own staging dir.
    ///
    /// Deliberately **not** [`Pipeline::extraction_budget`]. That one memoizes
    /// one budget per job and bakes the caller's staging path into it, so
    /// whichever caller arrives first decides where every later caller thinks
    /// the staging tree is. A chase that armed before conventional extraction
    /// ran would hand the conventional extractor a budget pointed at the
    /// chase's directory — a behaviour change to the path this feature is not
    /// allowed to touch. So the chase builds its own and keeps it to itself.
    /// The process-wide memory budget is still shared, which is what actually
    /// bounds concurrent decoders.
    fn direct_unpack_budget(
        &self,
        job_id: JobId,
        paths: &[PathBuf],
        staging: &std::path::Path,
    ) -> Result<Arc<crate::pipeline::extraction::JobExtractionBudget>, String> {
        let state = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {job_id:?} not found"))?;
        let part_names: HashSet<&std::ffi::OsStr> =
            paths.iter().filter_map(|path| path.file_name()).collect();
        // Sized against this set's own parts rather than every archive in the
        // job: the chase only ever decodes these.
        let declared_archive_bytes = state
            .assembly
            .files()
            .filter(|file| {
                part_names.contains(std::ffi::OsStr::new(
                    &self.current_filename_for_file(job_id, file),
                ))
            })
            .map(|file| file.total_bytes())
            .sum::<u64>()
            .max(1);
        let (initial_entries, initial_bytes) =
            ExtractionRoot::snapshot_usage(staging).unwrap_or((0, 0));

        crate::pipeline::extraction::JobExtractionBudget::new_with_process_memory(
            Arc::clone(&self.extraction_limits),
            Arc::clone(&self.process_memory_budget),
            staging.to_path_buf(),
            declared_archive_bytes,
            initial_entries,
            initial_bytes,
            Arc::clone(&self.metrics),
        )
    }

    /// The contiguous, verified prefix the download has committed for a part.
    ///
    /// The same value [`Pipeline::note_file_progress_floor`] maintains: the
    /// in-memory floor if there is one, else the persisted floor a restart
    /// would resume from.
    fn direct_unpack_progress_floor(&self, job_id: JobId, path: &std::path::Path) -> Option<u64> {
        let file_id = self.direct_unpack_file_id_for_part(job_id, path)?;
        Some(
            self.pending_file_progress
                .get(&file_id)
                .copied()
                .or_else(|| self.persisted_file_progress.get(&file_id).copied())
                .unwrap_or(0),
        )
    }

    /// The job file a part path belongs to, by its current name.
    fn direct_unpack_file_id_for_part(
        &self,
        job_id: JobId,
        path: &std::path::Path,
    ) -> Option<crate::jobs::ids::NzbFileId> {
        let filename = path.file_name()?.to_str()?;
        let state = self.jobs.get(&job_id)?;
        state
            .assembly
            .files()
            .find(|file| self.current_filename_for_file(job_id, file) == filename)
            .map(|file| file.file_id())
    }

    /// A part's exact decoded length, if it is already known for certain.
    ///
    /// Only a finished file has one: `received_bytes` is the sum of what was
    /// actually decoded and committed. The yEnc `=ybegin size=` header is
    /// available far earlier but is a *declaration* — the codebase treats it as
    /// evidence rather than truth, and posters misstate it — and a wrong length
    /// here would not merely be inaccurate, it would place every later part at
    /// the wrong archive offset. So an unfinished part gets no length, the
    /// reader parks at its boundary, and completion supplies the real one.
    fn direct_unpack_known_part_len(&self, job_id: JobId, path: &std::path::Path) -> Option<u64> {
        let filename = path.file_name()?.to_str()?;
        let state = self.jobs.get(&job_id)?;
        let file = state
            .assembly
            .files()
            .find(|file| self.current_filename_for_file(job_id, file) == filename)?;
        file.is_complete().then(|| file.received_bytes())
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_direct_unpack_worker(
        &self,
        job_id: JobId,
        set_name: String,
        paths: Vec<PathBuf>,
        coverage: Arc<SetCoverage>,
        output_dir: PathBuf,
        root: Arc<ExtractionRoot>,
        budget: Arc<crate::pipeline::extraction::JobExtractionBudget>,
        password: Option<String>,
        counters: Arc<crate::jobs::PhaseCounters>,
    ) -> tokio::task::JoinHandle<Result<FullSetExtractionOutcome, String>> {
        let pp_pool = self.pp_pool.clone();
        tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                let _memory_permit = budget.reserve_memory_wait(budget.max_memory_bytes())?;

                let pw = match password {
                    Some(ref value) => sevenz_rust2::Password::new(value),
                    None => sevenz_rust2::Password::empty(),
                };

                // The chase is invisible: its member events go to a channel
                // with no receivers, and its byte counters are its own rather
                // than the job's, so neither the phase display nor the event
                // stream learns that a decode is running mid-download.
                let (silent_events, _) = tokio::sync::broadcast::channel(1);
                let context = SevenZipExtractionContext {
                    job_id,
                    set_name,
                    output_dir,
                    root,
                    budget: Arc::clone(&budget),
                    password: pw,
                    event_tx: silent_events,
                    phase_counters: counters,
                };

                extract_7z_stream(&context, || {
                    GatedSplitReader::open(&paths, Arc::clone(&coverage))
                        .map(|reader| std::io::BufReader::with_capacity(CHASE_BUFFER_BYTES, reader))
                        .map_err(|error| format!("failed to open 7z direct-unpack reader: {error}"))
                })
            })
        })
    }

    /// Publish a part's committed watermark to any chase that wants it.
    ///
    /// On the download's commit path. When nothing is being chased this is a
    /// single `is_empty` and a return.
    pub(in crate::pipeline) fn direct_unpack_note_commit(
        &mut self,
        file_id: crate::jobs::ids::NzbFileId,
        filename: &str,
        committed_bytes: u64,
        complete: bool,
    ) {
        if self.direct_unpack.idle() {
            return;
        }
        let job_id = file_id.job_id;

        // A bare `.7z` waiting on its opening bytes arms here, because this is
        // the only place that learns the floor moved.
        if self.direct_unpack.pending_single_arm.contains(&file_id) {
            self.try_arm_single_sevenz(file_id, committed_bytes);
        }

        let Some(targets) = self.direct_unpack.watermark_targets.get(&job_id) else {
            return;
        };
        let Some((set_name, index)) = targets.get(filename) else {
            return;
        };
        let Some(armed) = self.direct_unpack.armed.get(&(job_id, set_name.clone())) else {
            return;
        };

        armed.coverage.advance_watermark(*index, committed_bytes);
        if complete {
            armed.coverage.mark_part_complete(*index);
        }
    }

    /// End every chase belonging to `job_id`.
    ///
    /// Every path that ends a download reaches this: without it a worker parks
    /// on bytes that are never coming and the thread leaks for the life of the
    /// process.
    /// End every chase belonging to `job_id`.
    ///
    /// Synchronous on purpose. Most of the paths that end a download are
    /// themselves synchronous — the job-removal seam, the per-file breaker —
    /// and an abort that needed `.await` could not be called from them. So this
    /// only *signals*: it wakes the worker and moves it to the draining list,
    /// and [`Self::reap_direct_unpack`] joins it from the run loop.
    pub(in crate::pipeline) fn direct_unpack_abort_job(
        &mut self,
        job_id: JobId,
        reason: &str,
        latch: AbortLatch,
    ) {
        if !self.direct_unpack.armed.is_empty() {
            let keys: Vec<(JobId, String)> = self
                .direct_unpack
                .armed
                .keys()
                .filter(|(armed_job, _)| *armed_job == job_id)
                .cloned()
                .collect();
            for (_, set_name) in keys {
                self.direct_unpack_abort_set(job_id, &set_name, reason, latch);
            }
        }
        if latch == AbortLatch::Permanent {
            self.direct_unpack.watermark_targets.remove(&job_id);
        }
    }

    /// End one chase: wake its worker and queue it for reaping.
    ///
    /// `latch` decides whether the set may ever be chased again. A pause is
    /// [`AbortLatch::Retryable`] — the bytes stopped for a reason that says
    /// nothing about the archive, and holding a blocking thread parked for the
    /// length of an indefinite pause is worse than starting over on resume.
    pub(in crate::pipeline) fn direct_unpack_abort_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
        reason: &str,
        latch: AbortLatch,
    ) {
        let Some(armed) = self
            .direct_unpack
            .armed
            .remove(&(job_id, set_name.to_string()))
        else {
            return;
        };

        // Wake the worker before anything else: it is parked on the coverage,
        // and a join that happened first would hang here instead of there.
        armed.coverage.abort(reason.to_string());

        self.direct_unpack
            .counters
            .record_demotion(DemotionReason::DownloadEnded);
        if latch == AbortLatch::Permanent {
            self.direct_unpack
                .latched
                .insert((job_id, set_name.to_string()), "download_ended");
        }
        info!(
            job_id = job_id.0,
            set_name,
            reason,
            retryable = latch == AbortLatch::Retryable,
            elapsed_ms = armed.started_at.elapsed().as_millis() as u64,
            "direct unpack aborted"
        );
        self.direct_unpack
            .draining
            .push(((job_id, set_name.to_string()), armed));
    }

    /// Abort every chase in the pipeline, then join them all. The shutdown and
    /// drain path, where leaving a blocking thread parked would hold the
    /// process open.
    pub(in crate::pipeline) async fn direct_unpack_shutdown(&mut self, reason: &str) {
        let jobs: Vec<JobId> = self
            .direct_unpack
            .armed
            .keys()
            .map(|(job_id, _)| *job_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        for job_id in jobs {
            self.direct_unpack_abort_job(job_id, reason, AbortLatch::Permanent);
        }
        while !self.direct_unpack.draining.is_empty() {
            let draining = std::mem::take(&mut self.direct_unpack.draining);
            for (key, armed) in draining {
                let _ = armed.handle.await;
                self.remove_direct_unpack_staging(&key, &armed.staging_dir);
            }
        }
    }

    /// Abort any chase whose set contains `filename`.
    ///
    /// The rename seam: a part renamed out from under the reader would become a
    /// `NotFound` on its next lazy open, so the chase is ended deliberately
    /// rather than failing obscurely later.
    pub(in crate::pipeline) fn direct_unpack_abort_sets_containing(
        &mut self,
        job_id: JobId,
        filename: &str,
        reason: &str,
    ) {
        if self.direct_unpack.armed.is_empty() {
            return;
        }
        let Some(set_name) = self
            .direct_unpack
            .watermark_targets
            .get(&job_id)
            .and_then(|targets| targets.get(filename))
            .map(|(set_name, _)| set_name.clone())
        else {
            return;
        };
        self.direct_unpack_abort_set(job_id, &set_name, reason, AbortLatch::Permanent);
    }

    fn remove_direct_unpack_staging(&self, key: &(JobId, String), staging_dir: &std::path::Path) {
        if let Err(error) = std::fs::remove_dir_all(staging_dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                job_id = key.0.0,
                set_name = %key.1,
                path = %staging_dir.display(),
                error = %error,
                "failed to remove direct-unpack staging"
            );
        }
    }

    /// Settle every chase for a job whose download has stopped producing bytes.
    ///
    /// A set whose parts all finished needs nothing — the coverage is already
    /// complete and the worker will run to the end. A set with a part that
    /// never finished will never get one, so it is aborted rather than left
    /// parked on bytes that are not coming.
    pub(in crate::pipeline) fn settle_direct_unpack_after_download(&mut self, job_id: JobId) {
        if self.direct_unpack.armed.is_empty() {
            return;
        }
        let sets: Vec<String> = self
            .direct_unpack
            .armed
            .keys()
            .filter(|(armed_job, _)| *armed_job == job_id)
            .map(|(_, set_name)| set_name.clone())
            .collect();

        for set_name in sets {
            let Ok(paths) = self.sevenz_set_part_paths(job_id, &set_name) else {
                continue;
            };
            let mut unsettled: Vec<String> = Vec::new();
            for (index, path) in paths.iter().enumerate() {
                // No more bytes are coming for this job, so whatever is on disk
                // is this part's final content. The assembly's own view is
                // preferred where it has one; where it does not — a set that
                // armed on the very last part's completion, which is when its
                // topology first existed, can reach here before every file has
                // been reconciled — the file's length on disk is just as final
                // and just as true. Only a part with no file at all is
                // genuinely incomplete.
                let len = self
                    .direct_unpack_known_part_len(job_id, path)
                    .or_else(|| std::fs::metadata(path).ok().map(|meta| meta.len()));
                match len {
                    Some(len) => {
                        if let Some(armed) =
                            self.direct_unpack.armed.get(&(job_id, set_name.clone()))
                        {
                            armed.coverage.advance_watermark(index, len);
                            armed.coverage.note_part_len(index, len);
                            armed.coverage.mark_part_complete(index);
                        }
                    }
                    None => unsettled.push(format!(
                        "{} (part {index})",
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("<unnamed>")
                    )),
                }
            }
            if !unsettled.is_empty() {
                // Name them. A set killed at the end of its download used to say
                // only that something was incomplete, which is the least useful
                // half of what it knew.
                warn!(
                    job_id = job_id.0,
                    set_name,
                    unsettled = %unsettled.join(", "),
                    "direct unpack has parts with no bytes at all after the download ended"
                );
                self.direct_unpack_abort_set(
                    job_id,
                    &set_name,
                    "download ended with parts incomplete",
                    AbortLatch::Permanent,
                );
            }
        }
    }

    /// Reap any chase that has finished on its own, recording the outcome.
    ///
    /// Polled rather than awaited: the controller must not block the
    /// orchestrator on a decode that is still chasing a live download.
    pub(in crate::pipeline) async fn reap_direct_unpack(&mut self) {
        // Aborted workers first: they were woken with an error and return
        // almost immediately, and their staging has to go.
        if !self.direct_unpack.draining.is_empty() {
            let mut still_running = Vec::new();
            for (key, armed) in std::mem::take(&mut self.direct_unpack.draining) {
                if armed.handle.is_finished() {
                    let _ = armed.handle.await;
                    self.remove_direct_unpack_staging(&key, &armed.staging_dir);
                } else {
                    still_running.push((key, armed));
                }
            }
            self.direct_unpack.draining = still_running;
        }

        if self.direct_unpack.armed.is_empty() {
            return;
        }
        let finished: Vec<(JobId, String)> = self
            .direct_unpack
            .armed
            .iter()
            .filter(|(_, armed)| armed.handle.is_finished())
            .map(|(key, _)| key.clone())
            .collect();

        for key in finished {
            let Some(armed) = self.direct_unpack.armed.remove(&key) else {
                continue;
            };
            let elapsed = armed.started_at.elapsed();
            let result = match armed.handle.await {
                Ok(result) => result,
                Err(error) => Err(format!("direct-unpack worker panicked: {error}")),
            };

            match &result {
                Ok(outcome) => {
                    self.direct_unpack.counters.completed += 1;
                    record_event("completed");
                    info!(
                        job_id = key.0.0,
                        set_name = %key.1,
                        members = outcome.extracted.len(),
                        elapsed_ms = elapsed.as_millis() as u64,
                        "direct unpack completed"
                    );
                }
                Err(error) => {
                    // A part that vanished under the reader is a rename racing
                    // the chase, not a broken archive.
                    let reason = if error.contains("failed to open 7z direct-unpack reader")
                        || error.contains("No such file or directory")
                    {
                        DemotionReason::PartUnreadable
                    } else {
                        DemotionReason::DecodeFailed
                    };
                    self.direct_unpack.counters.record_demotion(reason);
                    self.direct_unpack
                        .latched
                        .insert(key.clone(), reason.as_str());
                    if let Err(error) = std::fs::remove_dir_all(&armed.staging_dir)
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        warn!(
                            job_id = key.0.0,
                            set_name = %key.1,
                            error = %error,
                            "failed to remove direct-unpack staging after demotion"
                        );
                    }
                    warn!(
                        job_id = key.0.0,
                        set_name = %key.1,
                        reason = reason.as_str(),
                        error = %error,
                        elapsed_ms = elapsed.as_millis() as u64,
                        "direct unpack demoted"
                    );
                }
            }

            self.direct_unpack.outcomes.insert(
                key,
                ChaseOutcome {
                    result,
                    elapsed,
                    staging_dir: armed.staging_dir,
                    total_bytes: armed.counters.total_bytes.load(Ordering::Relaxed),
                    completed_bytes: armed.counters.completed_bytes.load(Ordering::Relaxed),
                    tainted: false,
                },
            );
        }
    }

    /// Decide what extraction should do with this set's chase, taking
    /// ownership of whatever it finds.
    ///
    /// Called from the extraction dispatch, which by then is downstream of PAR2
    /// verify and repair — so a chase that survived to here read the same bytes
    /// the conventional extractor would.
    pub(in crate::pipeline) fn take_direct_unpack_disposition(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> ChaseDisposition {
        let key = (job_id, set_name.to_string());

        if let Some(outcome) = self.direct_unpack.outcomes.remove(&key) {
            let usable = outcome.result.is_ok() && !outcome.tainted;
            if usable {
                // Counted here rather than after the move: the install itself
                // is a rename of files that already exist, and its rare
                // failures fall back to conventional extraction with a warning
                // rather than silently. `consumed` therefore means "a usable
                // chase was handed to extraction", which is the number worth
                // watching.
                self.direct_unpack.counters.consumed += 1;
                record_event("consumed");
                debug!(
                    job_id = job_id.0,
                    set_name,
                    chase_ms = outcome.elapsed.as_millis() as u64,
                    members = outcome
                        .result
                        .as_ref()
                        .map(|o| o.extracted.len())
                        .unwrap_or(0),
                    "installing a finished chase instead of extracting"
                );
                return ChaseDisposition::Ready(Box::new(outcome));
            }
            self.direct_unpack.counters.discarded += 1;
            record_event("discarded");
            self.remove_direct_unpack_staging(&key, &outcome.staging_dir);
            debug!(
                job_id = job_id.0,
                set_name,
                tainted = outcome.tainted,
                "discarding a chase outcome; extracting conventionally"
            );
            return ChaseDisposition::None;
        }

        // A tainted chase is never still armed: tainting ends it on the spot.
        let Some(armed) = self.direct_unpack.armed.remove(&key) else {
            return ChaseDisposition::None;
        };

        self.direct_unpack.counters.consumed += 1;
        record_event("consumed");
        ChaseDisposition::Pending(PendingChase {
            handle: armed.handle,
            staging_dir: armed.staging_dir,
            counters: armed.counters,
        })
    }

    /// Mark a set's chase unusable because repair replaced bytes it read.
    ///
    /// A running chase is aborted outright; a finished one is flagged so
    /// consumption throws it away. Called from every repair install site.
    pub(in crate::pipeline) fn taint_direct_unpack_set(&mut self, job_id: JobId, set_name: &str) {
        let key = (job_id, set_name.to_string());
        if let Some(outcome) = self.direct_unpack.outcomes.get_mut(&key) {
            if !outcome.tainted {
                outcome.tainted = true;
                self.direct_unpack
                    .counters
                    .record_demotion(DemotionReason::RepairRewrote);
                info!(
                    job_id = job_id.0,
                    set_name,
                    reason = DemotionReason::RepairRewrote.as_str(),
                    "direct unpack demoted — repair rewrote a chased set; its outcome is tainted"
                );
            }
            return;
        }
        // A running chase cannot recover from this: nothing it decodes after
        // the rewrite describes the file on disk, and consumption would throw
        // the result away regardless. So it ends here rather than at dispatch —
        // which releases its memory permit and its blocking thread now instead
        // of holding both, parked, until extraction finally gets around to it.
        if let Some(armed) = self.direct_unpack.armed.remove(&key) {
            armed
                .coverage
                .abort("repair rewrote the archive".to_string());
            self.direct_unpack
                .counters
                .record_demotion(DemotionReason::RepairRewrote);
            self.direct_unpack
                .latched
                .insert(key.clone(), DemotionReason::RepairRewrote.as_str());
            // "direct unpack demoted" is the canonical phrase, and `reason` the
            // canonical field: they are the only external evidence a demotion
            // ever produced, and a demotion logged in any other words is one
            // nothing downstream can see. This arm used to describe itself
            // instead, so a correct taint read as a missing one.
            info!(
                job_id = job_id.0,
                set_name,
                reason = DemotionReason::RepairRewrote.as_str(),
                elapsed_ms = armed.started_at.elapsed().as_millis() as u64,
                "direct unpack demoted — repair rewrote a set being chased; the chase is aborted"
            );
            self.direct_unpack.draining.push((key, armed));
        }
    }

    /// Hold a part's frontier below damage the recovery set has reported.
    ///
    /// Called when a chased part finishes downloading, which is when its grid
    /// verdicts are final. Computing them earlier would mean rebuilding the
    /// verdict map on every commit; computing them here costs one map build per
    /// chased file and still lands before repair, because repair does not run
    /// until the whole download has drained.
    ///
    /// Bytes the decoder consumed *before* this cap went up are not protected
    /// by it — that is what the repair-time vouching check is for.
    fn cap_chased_part_at_damage(&self, file_id: crate::jobs::ids::NzbFileId, filename: &str) {
        if self.direct_unpack.armed.is_empty() {
            return;
        }
        let job_id = file_id.job_id;
        let Some((set_name, index)) = self
            .direct_unpack
            .watermark_targets
            .get(&job_id)
            .and_then(|targets| targets.get(filename))
            .cloned()
        else {
            return;
        };
        let Some(armed) = self.direct_unpack.armed.get(&(job_id, set_name.clone())) else {
            return;
        };
        let Some(floor) = self.in_stream_damage_floor(file_id) else {
            return;
        };
        info!(
            job_id = job_id.0,
            set_name,
            part = index,
            damage_floor = floor,
            "holding a chased part below reported damage until repair"
        );
        armed.coverage.cap_at_damage(index, floor);
    }

    /// Decide, per chased set, whether a repair about to run can coexist with
    /// what the chase has already read.
    ///
    /// The vouching rule: every byte the decoder consumed must lie inside the
    /// contiguous run of blocks the recovery set positively found Intact. If it
    /// does, repair cannot rewrite anything the chase has folded into its
    /// output, so the chase is parked through the repair instead of thrown
    /// away. Anything less — a consumed byte past the vouched prefix, a file
    /// with no binding, a set whose parts cannot be resolved — falls back to
    /// the unconditional taint this replaced.
    pub(in crate::pipeline) fn decide_direct_unpack_before_repair(&mut self, job_id: JobId) {
        if self.direct_unpack.armed.is_empty() && self.direct_unpack.outcomes.is_empty() {
            return;
        }
        let sets: Vec<String> = self
            .direct_unpack
            .armed
            .keys()
            .chain(self.direct_unpack.outcomes.keys())
            .filter(|(key_job, _)| *key_job == job_id)
            .map(|(_, set_name)| set_name.clone())
            .collect();

        for set_name in sets {
            if self.direct_unpack_set_is_vouched(job_id, &set_name) {
                info!(
                    job_id = job_id.0,
                    set_name,
                    "every byte this chase consumed is vouched for; parking it through the repair"
                );
                self.direct_unpack
                    .parked_through_repair
                    .insert((job_id, set_name));
            } else {
                self.taint_direct_unpack_set(job_id, &set_name);
            }
        }
    }

    /// Whether the recovery set positively vouches for every byte this set's
    /// chase has consumed.
    fn direct_unpack_set_is_vouched(&self, job_id: JobId, set_name: &str) -> bool {
        // Every `false` below names itself. This decision is the difference
        // between a chase surviving a repair and being thrown away, it fires
        // once per repaired set, and its inputs are spread across the topology,
        // the assembly and the PAR2 runtime — so an unexplained refusal is a
        // gate run spent bisecting. It is a per-set decision on a cold path;
        // the logging costs nothing that matters.
        let refuse = |reason: &str, part: Option<usize>| {
            debug!(
                job_id = job_id.0,
                set_name, part, reason, "direct unpack cannot vouch this set against the repair"
            );
            false
        };

        let paths = match self.sevenz_set_part_paths(job_id, set_name) {
            Ok(paths) => paths,
            Err(error) => {
                debug!(
                    job_id = job_id.0,
                    set_name,
                    error = %error,
                    "direct unpack cannot vouch this set against the repair"
                );
                return false;
            }
        };
        if paths.is_empty() {
            return refuse("the set resolved to no part paths", None);
        }
        let key = (job_id, set_name.to_string());
        let coverage = self
            .direct_unpack
            .armed
            .get(&key)
            .map(|armed| Arc::clone(&armed.coverage));
        // A finished chase read its whole archive, so every byte of every part
        // has to be vouched for. A running one only has to answer for what it
        // actually took.
        let finished = coverage.is_none();

        for (index, path) in paths.iter().enumerate() {
            let Some(file_id) = self.direct_unpack_file_id_for_part(job_id, path) else {
                return refuse(
                    "no job file matches this part's current filename",
                    Some(index),
                );
            };
            let Some(intact_prefix) = self.in_stream_intact_prefix(file_id) else {
                return refuse(
                    "the recovery set does not bind this part, so nothing vouches for it",
                    Some(index),
                );
            };
            let consumed = match &coverage {
                Some(coverage) => coverage.consumed_high_water(index),
                None => match self.direct_unpack_known_part_len(job_id, path) {
                    Some(len) => len,
                    None => {
                        return refuse(
                            "a finished chase read this part but its final length is unknown",
                            Some(index),
                        );
                    }
                },
            };
            if consumed > intact_prefix {
                debug!(
                    job_id = job_id.0,
                    set_name,
                    part = index,
                    consumed,
                    intact_prefix,
                    finished,
                    "chase consumed past the vouched prefix; repair must taint it"
                );
                return false;
            }
        }
        true
    }

    /// Mark a set as parked through a repair, without going through the
    /// vouching decision. Lets a test exercise the release and failure paths
    /// without standing up a PAR2 binding and a populated grid.
    #[cfg(test)]
    pub(in crate::pipeline) fn park_direct_unpack_through_repair_for_test(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) {
        self.direct_unpack
            .parked_through_repair
            .insert((job_id, set_name.to_string()));
    }

    /// Settle the chases a repair was parked over, whichever way the repair
    /// ended.
    ///
    /// The single seam every exit from the repairer uses, so that adding an
    /// early return there cannot silently strand a parked chase: a set held
    /// under a damage cap is waiting on a frontier only this call advances, and
    /// nothing else in the pipeline will notice it is waiting.
    ///
    /// A no-op unless this was an actual repair — an analysis pass rewrites
    /// nothing, so it parks nothing.
    pub(in crate::pipeline) fn settle_direct_unpack_after_repair(
        &mut self,
        job_id: JobId,
        repair: bool,
        outcome: &Result<par2_rs::Par2RepairOutcome, String>,
    ) {
        if !repair {
            return;
        }
        match outcome {
            Ok(_) => self.release_direct_unpack_after_repair(job_id),
            Err(error) => self.fail_direct_unpack_after_repair(job_id, error),
        }
    }

    /// End the chases a repair was allowed to run underneath, because the
    /// repair did not finish.
    ///
    /// The symmetric half of [`Self::release_direct_unpack_after_repair`]. A
    /// set parked through a repair is parked at its damage cap, and only the
    /// repair's success was ever going to lift it — so a repair that fails
    /// leaves a blocking thread waiting on bytes nothing will now write. The
    /// job may well fail terminally straight after, which would abort it
    /// anyway, but it may also not: the failure could be one set's among
    /// several. This closes that gap without depending on what happens next.
    pub(in crate::pipeline) fn fail_direct_unpack_after_repair(
        &mut self,
        job_id: JobId,
        reason: &str,
    ) {
        if self.direct_unpack.parked_through_repair.is_empty() {
            return;
        }
        let stranded: Vec<String> = self
            .direct_unpack
            .parked_through_repair
            .iter()
            .filter(|(key_job, _)| *key_job == job_id)
            .map(|(_, set_name)| set_name.clone())
            .collect();

        for set_name in stranded {
            self.direct_unpack
                .parked_through_repair
                .remove(&(job_id, set_name.clone()));
            self.direct_unpack_abort_set(job_id, &set_name, reason, AbortLatch::Permanent);
        }
    }

    /// Reopen the chases a repair was allowed to run underneath.
    ///
    /// The parts on disk are now the repaired ones and the recovery set has
    /// vouched for everything already consumed, so the frontier opens to the
    /// whole file and the decoder finishes at disk speed.
    pub(in crate::pipeline) fn release_direct_unpack_after_repair(&mut self, job_id: JobId) {
        if self.direct_unpack.parked_through_repair.is_empty() {
            return;
        }
        let released: Vec<String> = self
            .direct_unpack
            .parked_through_repair
            .iter()
            .filter(|(key_job, _)| *key_job == job_id)
            .map(|(_, set_name)| set_name.clone())
            .collect();

        for set_name in released {
            self.direct_unpack
                .parked_through_repair
                .remove(&(job_id, set_name.clone()));
            let Ok(paths) = self.sevenz_set_part_paths(job_id, &set_name) else {
                continue;
            };
            let Some(armed) = self.direct_unpack.armed.get(&(job_id, set_name.clone())) else {
                // A finished chase has nothing to release: it was vouched for,
                // so its outcome stands as it is.
                continue;
            };
            let coverage = Arc::clone(&armed.coverage);
            for (index, path) in paths.iter().enumerate() {
                match std::fs::metadata(path) {
                    Ok(meta) => coverage.release_after_repair(index, meta.len()),
                    Err(error) => {
                        // The repair reported success and the part is not
                        // there. Skipping would leave this part's cap in place
                        // and the chase parked until job teardown; saying so
                        // ends it now, with a reason.
                        coverage.abort(format!("part {index} is unreadable after repair: {error}"));
                        break;
                    }
                }
            }
            info!(
                job_id = job_id.0,
                set_name, "repair finished; the parked chase resumes over the repaired bytes"
            );
            record_event("resumed_after_repair");
        }
    }

    /// Taint every chase whose set contains `filename`.
    ///
    /// Repair identifies its work by file, not by archive set, so this is the
    /// shape every repair site calls.
    pub(in crate::pipeline) fn taint_direct_unpack_for_file(
        &mut self,
        job_id: JobId,
        filename: &str,
    ) {
        if self.direct_unpack.armed.is_empty() && self.direct_unpack.outcomes.is_empty() {
            return;
        }
        let Some(set_name) = self
            .direct_unpack
            .watermark_targets
            .get(&job_id)
            .and_then(|targets| targets.get(filename))
            .map(|(set_name, _)| set_name.clone())
        else {
            return;
        };
        self.taint_direct_unpack_set(job_id, &set_name);
    }

    /// Drop every trace of a job, aborting anything still running.
    pub(crate) fn direct_unpack_forget_job(&mut self, job_id: JobId) {
        self.direct_unpack_abort_job(job_id, "job removed", AbortLatch::Permanent);
        self.direct_unpack
            .latched
            .retain(|(latched_job, _), _| *latched_job != job_id);
        self.direct_unpack
            .outcomes
            .retain(|(outcome_job, _), _| *outcome_job != job_id);
        self.direct_unpack.watermark_targets.remove(&job_id);
        self.direct_unpack
            .pending_single_arm
            .retain(|file_id| file_id.job_id != job_id);
        self.direct_unpack
            .parked_through_repair
            .retain(|(parked_job, _)| *parked_job != job_id);
    }
}

/// Move a finished chase's members into the conventional extraction staging
/// directory, preserving relative paths.
///
/// A rename when the two trees share a filesystem — which they do, both being
/// under `complete_dir` — and a copy when they do not, so an operator who has
/// mounted something unusual gets correct behaviour rather than an error. The
/// source tree is removed either way: after this the chase's directory has no
/// reason to exist.
pub(in crate::pipeline) fn install_chased_members(
    from: &std::path::Path,
    to: &std::path::Path,
) -> Result<(), String> {
    fn move_tree(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let source = entry.path();
            let destination = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                move_tree(&source, &destination)?;
                continue;
            }
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if std::fs::rename(&source, &destination).is_err() {
                // Cross-device, or a destination that already exists.
                std::fs::copy(&source, &destination)?;
                std::fs::remove_file(&source)?;
            }
        }
        Ok(())
    }

    move_tree(from, to).map_err(|error| {
        format!(
            "failed to install direct-unpack members from {} into {}: {error}",
            from.display(),
            to.display()
        )
    })?;
    let _ = std::fs::remove_dir_all(from);
    Ok(())
}

/// Read the 32-byte signature header, or `Ok(None)` if the file is still
/// shorter than that.
fn read_signature_header(path: &std::path::Path) -> std::io::Result<Option<[u8; 32]>> {
    use std::io::Read;

    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() < SIGNATURE_HEADER_LEN {
        return Ok(None);
    }
    let mut bytes = [0u8; 32];
    file.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

/// A set name is archive-derived, so it is not trusted as a path component.
fn sanitize_set_dir_name(set_name: &str) -> String {
    let cleaned: String = set_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('.');
    if trimmed.is_empty() {
        "set".to_string()
    } else {
        trimmed.to_string()
    }
}
