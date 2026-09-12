use super::*;
use crate::pipeline::direct_store::wiring::{DirectDamageResolution, DirectPar2Resolution};
use crate::runtime::fs as runtime_fs;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use weaver_model::files::{
    allocate_unique_download_filename, forget_reserved_download_filename,
    reserve_download_filename, sanitize_download_filename,
};

const PAR2_REPAIR_MEMORY_LIMIT_ENV: &str = "WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES";
// Sizes the transient streaming repair buffers (the decode matrix has its own
// budget floor inside par2-rs). 64 MiB measured within noise of far
// larger budgets on heavily damaged sets once streaming repair got its
// batched kernels, so the default stays small and repairs coexist with
// concurrent downloads; the env override remains for tuning.
const DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

const PAR2_IGNORE_EXTENSIONS_ENV: &str = "WEAVER_PAR2_IGNORE_EXTENSIONS";
// Metadata that travels with a post rather than being part of it. Damage to
// one of these never fails a job in either reference downloader: one clears its
// "has damaged files" flag for them outright, so ignorable-only damage reports
// as repair-not-needed even when the recovery set said repair was impossible;
// the other's quick check passes such a file both on a checksum mismatch and
// when it is missing entirely. This default is the union of the two lists,
// overridable through the env var above (comma- or semicolon-separated; an
// empty value disables the behaviour).
const DEFAULT_PAR2_IGNORE_EXTENSIONS: [&str; 4] = ["nfo", "sfv", "srr", "nzb"];

#[derive(Clone)]
struct Par2SessionEvidenceCandidate {
    file_id: NzbFileId,
    path: std::path::PathBuf,
    logical_name: String,
    expected_length: u64,
    full_md5: Option<[u8; 16]>,
    crc32: u32,
    contiguous_assembly_proven: bool,
    bound_file_id: Option<par2_rs::FileId>,
}

pub(crate) type RetainedPar2SessionOutcome = (par2_rs::Par2RepairOutcome, Vec<NzbFileId>, bool);

#[derive(Debug)]
pub(crate) struct RetainedPar2SessionFailure {
    message: String,
    file_descriptor_exhausted: bool,
}

impl RetainedPar2SessionFailure {
    fn other(message: String) -> Self {
        Self {
            message,
            file_descriptor_exhausted: false,
        }
    }

    fn from_session_error(error: par2_rs::Par2SessionError) -> Self {
        let file_descriptor_exhausted = error_chain_has_file_descriptor_exhaustion(&error);
        Self {
            message: format!("retained PAR2 session failed: {error}"),
            file_descriptor_exhausted,
        }
    }
}

pub(crate) type RetainedPar2SessionResult =
    Result<RetainedPar2SessionOutcome, RetainedPar2SessionFailure>;

/// What a detached damaged-path analysis hands back to the pipeline task.
///
/// The arms mirror the two ways the read can be run — through the retained
/// session, or through a one-shot repairer — and each carries back everything
/// the actor has to put away afterwards. The retained session in particular
/// travels *with* the ticket: it is taken out of the set runtime at submission,
/// which is also what stops a second pass opening a rival session for the same
/// set while the read is running, and it is restored when this lands.
pub(crate) enum Par2AnalysisTicketOutcome {
    Retained {
        session: Box<par2_rs::Par2RepairSession>,
        result: RetainedPar2SessionResult,
    },
    OneShot(Result<(par2_rs::Par2RepairOutcome, Option<Arc<par2_rs::ScanCarry>>), String>),
    /// The worker did not return a result at all.
    TaskFailed(String),
}

/// The work one damaged-path analysis ticket carries to its blocking worker.
///
/// Everything here is owned: no borrow of pipeline state crosses onto the
/// worker, which is what makes the read detachable in the first place.
enum Par2AnalysisWorkPlan {
    Retained {
        session: Box<par2_rs::Par2RepairSession>,
        candidates: Vec<Par2SessionEvidenceCandidate>,
        slice_evidence: Vec<(std::path::PathBuf, Vec<par2_rs::SliceEvidence>)>,
    },
    OneShot {
        options: Box<par2_rs::Par2RepairerOptions>,
    },
}

pub(in crate::pipeline) fn error_chain_has_file_descriptor_exhaustion(
    error: &(dyn std::error::Error + 'static),
) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(error) = source.downcast_ref::<std::io::Error>()
            && io_error_is_file_descriptor_exhaustion(error)
        {
            return true;
        }
        current = source.source();
    }
    false
}

fn io_error_is_file_descriptor_exhaustion(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE)
        )
    }
    #[cfg(windows)]
    {
        // ERROR_TOO_MANY_OPEN_FILES
        error.raw_os_error() == Some(4)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

fn committed_evidence_from_candidate(
    candidate: &Par2SessionEvidenceCandidate,
) -> Result<Option<par2_rs::CommittedFileEvidence>, String> {
    // Both evidence shapes assert the file's content; neither is meaningful
    // if the bytes on disk are not the length the PAR2 set describes (the
    // decoded length was already required to match when the candidate was
    // bound). One metadata stat per completed file, never a content read.
    // A stat-able file whose length disagrees with the PAR2 description can
    // never satisfy either evidence shape. A path that does not stat is NOT
    // rejected: direct sets deliberately keep no conventional file at this
    // path (the envelope + partials are the bytes), and a genuinely missing
    // conventional file fails downstream verification on its own.
    match std::fs::metadata(&candidate.path) {
        Ok(metadata) if metadata.len() != candidate.expected_length => return Ok(None),
        Ok(_) | Err(_) => {}
    }
    if let Some(md5) = candidate.full_md5 {
        return par2_rs::CommittedFileEvidence::from_full_md5_path(
            &candidate.path,
            &candidate.logical_name,
            candidate.expected_length,
            md5,
            candidate.bound_file_id,
        )
        .map(Some)
        .map_err(|error| format!("failed to capture PAR2 full-MD5 evidence: {error}"));
    }
    if !candidate.contiguous_assembly_proven {
        return Ok(None);
    }

    let mut first_16k = Vec::with_capacity(16 * 1024);
    File::open(&candidate.path)
        .and_then(|file| file.take(16 * 1024).read_to_end(&mut first_16k))
        .map_err(|error| {
            format!(
                "failed to capture first 16 KiB for retained PAR2 evidence {}: {error}",
                candidate.path.display()
            )
        })?;
    let proof = par2_rs::ContiguousAssemblyProof::try_new(
        candidate.expected_length,
        candidate.expected_length,
        candidate.expected_length,
        false,
        false,
        false,
        true,
    )
    .map_err(|error| format!("invalid contiguous PAR2 assembly proof: {error}"))?;
    par2_rs::CommittedFileEvidence::from_contiguous_assembly_path(
        &candidate.path,
        &candidate.logical_name,
        candidate.expected_length,
        candidate.crc32,
        par2_rs::checksum::md5(&first_16k),
        proof,
        candidate.bound_file_id,
    )
    .map(Some)
    .map_err(|error| format!("failed to capture PAR2 contiguous evidence: {error}"))
}

fn run_retained_par2_session(
    mut session: par2_rs::Par2RepairSession,
    candidates: Vec<Par2SessionEvidenceCandidate>,
    slice_evidence: Vec<(std::path::PathBuf, Vec<par2_rs::SliceEvidence>)>,
    repair: bool,
) -> (par2_rs::Par2RepairSession, RetainedPar2SessionResult) {
    // Per-slice evidence from the in-stream grid, alongside the whole-file
    // evidence below. The two answer different questions and neither subsumes
    // the other: committed evidence retires a file the pipeline can vouch for
    // end to end, while slice evidence places the individual blocks of a file
    // it can only vouch for in part — which is most of a damaged set. Seeding
    // both is what lets an authoritative pass over a damaged job read the
    // damaged files and nothing else.
    //
    // Path-keyed, because a session that finds its sources in a directory
    // refuses the `FileId`-keyed seat; the paths carry each file's effective
    // identity. A refusal here costs read savings and never correctness, so it
    // is counted and stepped over rather than failing the pass.
    for (path, evidence) in slice_evidence {
        for slice in evidence {
            if session.add_slice_evidence(path.clone(), slice).is_err() {
                crate::runtime::perf_probe::record(
                    "completion.par2_evidence.slice.rejected",
                    std::time::Duration::from_nanos(1),
                );
            }
        }
    }

    let mut admitted_file_ids = Vec::new();
    for candidate in candidates {
        let evidence = match committed_evidence_from_candidate(&candidate) {
            Ok(Some(evidence)) => evidence,
            Ok(None) | Err(_) => continue,
        };
        match session.add_committed_file(evidence) {
            Ok(()) => admitted_file_ids.push(candidate.file_id),
            Err(par2_rs::Par2SessionError::EvidenceDoesNotMatch { .. }) => {}
            Err(error) => {
                return (
                    session,
                    Err(RetainedPar2SessionFailure::other(format!(
                        "failed to add retained PAR2 evidence: {error}"
                    ))),
                );
            }
        }
    }

    let mut retried_source_change = false;
    let mut result = if repair {
        if session.assessment().is_err() {
            session.analyze().and_then(|_| session.repair())
        } else {
            session.repair()
        }
    } else {
        session.analyze()
    };
    if should_retry_par2_source_change(&result, retried_source_change) {
        // One retry gets a fresh unresolved-only analysis. A second change is
        // returned to the caller instead of repeatedly trusting a moving path.
        retried_source_change = true;
        admitted_file_ids.clear();
        session.invalidate_all_sources();
        result = session.analyze();
        if result.is_ok() && repair {
            result = session.repair();
        }
    }
    (
        session,
        result
            .map(|outcome| (outcome, admitted_file_ids, retried_source_change))
            .map_err(RetainedPar2SessionFailure::from_session_error),
    )
}

/// One damaged-path analysis ticket's blocking work, off the pipeline task.
///
/// Nothing here touches pipeline state: the plan owns the session, the evidence
/// and the options, and the actor picks the whole result up again in
/// `settle_par2_analysis_ticket`.
fn run_par2_analysis_work(plan: Par2AnalysisWorkPlan) -> Par2AnalysisTicketOutcome {
    match plan {
        Par2AnalysisWorkPlan::Retained {
            session,
            candidates,
            slice_evidence,
        } => {
            let (session, result) =
                run_retained_par2_session(*session, candidates, slice_evidence, false);
            Par2AnalysisTicketOutcome::Retained {
                session: Box::new(session),
                result,
            }
        }
        Par2AnalysisWorkPlan::OneShot { options } => {
            let repairer = par2_rs::Par2Repairer::new(*options);
            Par2AnalysisTicketOutcome::OneShot(
                repairer
                    .verify_or_repair_carrying()
                    .map_err(|error| format!("PAR2 repairer failed: {error}"))
                    .and_then(|(outcome, scan_carry)| {
                        ensure_par2_repair_completed(&outcome, false)?;
                        Ok((outcome, scan_carry))
                    }),
            )
        }
    }
}

fn should_retry_par2_source_change<T>(
    result: &Result<T, par2_rs::Par2SessionError>,
    already_retried: bool,
) -> bool {
    !already_retried && matches!(result, Err(par2_rs::Par2SessionError::SourceChanged { .. }))
}

/// The operator-facing form of a resource-limited PAR2 verdict.
///
/// A refusal names either a transient workspace budget in bytes or a PAR2
/// format cap on how many slices a set may address, and only the first is
/// tunable. The reason already distinguishes them and already carries the
/// numbers, so the tail this adds says which knob exists and what it applies to
/// rather than promising that setting it will help.
///
/// Without this the failure read as an internal limit with no stated remedy,
/// which is how a tunable refusal ends up looking like a dead end.
fn par2_resource_limit_message(reason: &str) -> String {
    format!(
        "PAR2 verification resource limit exceeded: {reason}. A workspace budget is raised by \
         setting {PAR2_REPAIR_MEMORY_LIMIT_ENV} above the byte figure named above; PAR2 \
         slice-count caps are fixed by the format and cannot be raised."
    )
}

/// Close one accepted-repair stage and open the next.
///
/// The tail that follows a successful repair is a sequence of whole-job passes
/// — a re-read of what was installed, deobfuscation, placement, reconciliation,
/// a digest refresh — and until they were stamped, "the repair took much longer
/// than the repair" was an argument rather than a measurement. Naming each one
/// turns it into a number.
///
/// Low-frequency by construction: this runs a handful of times per job, after a
/// repair that has already spent seconds or minutes on the payload, and never
/// on an article path — the same class as the lifecycle metric the same
/// function records. The probe costs nothing at all unless hot-path profiling
/// is switched on.
fn note_par2_repair_stage(
    job_id: JobId,
    stage: &'static str,
    started: std::time::Instant,
) -> std::time::Instant {
    let elapsed = started.elapsed();
    crate::runtime::perf_probe::record(stage, elapsed);
    debug!(
        job_id = job_id.0,
        stage,
        stage_ms = elapsed.as_millis() as u64,
        "PAR2 repair stage"
    );
    std::time::Instant::now()
}

fn ensure_par2_repair_completed(
    outcome: &par2_rs::Par2RepairOutcome,
    repair: bool,
) -> Result<(), String> {
    if !repair {
        return Ok(());
    }
    match outcome.status {
        par2_rs::Par2RepairStatus::Verified | par2_rs::Par2RepairStatus::Repaired => Ok(()),
        // Carries the verdict's own reason rather than just the status name:
        // this is the one arm whose refusal an operator can act on, and the
        // status alone says nothing about which limit was hit.
        par2_rs::Par2RepairStatus::ResourceLimited => Err(match &outcome.verification.repairable {
            par2_rs::verify::Repairability::ResourceLimited { reason } => {
                par2_resource_limit_message(reason)
            }
            _ => par2_resource_limit_message("the PAR2 repairer stopped at a resource limit"),
        }),
        par2_rs::Par2RepairStatus::RepairPossible | par2_rs::Par2RepairStatus::Insufficient => {
            Err(format!(
                "PAR2 repairer did not complete repair: {:?}",
                outcome.status
            ))
        }
    }
}

pub(in crate::pipeline) fn bounded_repair_evidence_covers_assessment(
    verification: &par2_rs::VerificationResult,
    slice_evidence: &[par2_rs::SliceEvidence],
) -> bool {
    let available = slice_evidence
        .iter()
        .filter(|evidence| evidence.is_valid())
        .map(|evidence| (evidence.file_id(), evidence.slice_index()))
        .collect::<HashSet<_>>();
    verification.files.iter().all(|file| {
        file.valid_slices
            .iter()
            .enumerate()
            .all(|(slice_index, valid)| {
                !*valid
                    || u32::try_from(slice_index)
                        .ok()
                        .is_some_and(|slice_index| available.contains(&(file.file_id, slice_index)))
            })
    })
}

pub(in crate::pipeline) fn run_file_descriptor_bounded_par2_repair(
    working_dir: std::path::PathBuf,
    par2_set: par2_rs::Par2FileSet,
    placement_overrides: HashMap<par2_rs::FileId, String>,
    slice_evidence: Vec<par2_rs::SliceEvidence>,
    memory_limit: usize,
    cancellation: par2_rs::CancellationToken,
    progress: Option<par2_rs::ProgressCallback>,
) -> Result<par2_rs::Par2RepairOutcome, String> {
    // The path-backed repairer keeps every source file open for the repair
    // lifetime. Large RAR sets can exhaust a process's descriptor budget even
    // though only a few slices are damaged. The access-backed session does not
    // scan paths, so its caller must first prove the in-stream grid names every
    // intact input slice the authoritative assessment will use.
    let source_access = std::sync::Arc::new(par2_rs::PlacementFileAccess::new(
        working_dir.clone(),
        &par2_set,
        placement_overrides,
    ));
    let mut options =
        par2_rs::Par2RepairSessionOptions::from_set(working_dir, par2_set, source_access);
    options.memory_limit = Some(memory_limit);
    options.cancel = Some(cancellation);
    options.progress = progress;

    let mut session = par2_rs::Par2RepairSession::open(options)
        .map_err(|error| format!("bounded filesystem PAR2 fallback failed to open: {error}"))?;
    for evidence in slice_evidence {
        session
            .add_slice_evidence_for_file(evidence)
            .map_err(|error| format!("bounded filesystem PAR2 evidence was rejected: {error}"))?;
    }
    session
        .analyze()
        .map_err(|error| format!("bounded filesystem PAR2 analysis failed: {error}"))?;
    let outcome = session
        .repair()
        .map_err(|error| format!("bounded filesystem PAR2 repair failed: {error}"))?;
    ensure_par2_repair_completed(&outcome, true)?;
    Ok(outcome)
}

fn default_par2_repair_memory_limit_bytes() -> usize {
    DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
}

/// Missing blocks [`Pipeline::apply_direct_damage_adjustments`] moved out of (or
/// deliberately left in) a PAR2 verdict's damage count.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DamageAdjustments {
    /// Eagerly-deleted, CRC-verified RAR volumes excused as `Complete`.
    pub(crate) skipped_blocks: u32,
    /// Eagerly-deleted volumes that were *not* excused, because something still
    /// holds them suspect.
    pub(crate) retained_suspect_blocks: u32,
    /// Source volumes of a finalized direct set, excused as `Complete`.
    pub(crate) forgiven_direct_blocks: u32,
}

impl DamageAdjustments {
    pub(crate) fn any(self) -> bool {
        self.skipped_blocks > 0
            || self.retained_suspect_blocks > 0
            || self.forgiven_direct_blocks > 0
    }
}

pub(crate) fn configured_par2_repair_memory_limit_bytes() -> usize {
    parse_par2_repair_memory_limit_bytes(
        std::env::var(PAR2_REPAIR_MEMORY_LIMIT_ENV).ok().as_deref(),
    )
}

fn parse_par2_repair_memory_limit_bytes(raw: Option<&str>) -> usize {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return default_par2_repair_memory_limit_bytes();
    };
    match value.parse::<usize>() {
        Ok(bytes) if bytes > 0 => bytes,
        _ => {
            let default_bytes = default_par2_repair_memory_limit_bytes();
            warn!(
                env = PAR2_REPAIR_MEMORY_LIMIT_ENV,
                value, default_bytes, "invalid PAR2 repair memory limit; using default"
            );
            default_bytes
        }
    }
}

/// The extension list [`par2_damage_ignorable`] matches against, as read from
/// the environment once per verdict.
pub(in crate::pipeline) fn configured_par2_ignore_extensions() -> Vec<String> {
    parse_par2_ignore_extensions(std::env::var(PAR2_IGNORE_EXTENSIONS_ENV).ok().as_deref())
}

/// Parse the override list. Unset takes the baked defaults; an explicitly empty
/// value turns the behaviour off entirely, which is the only way to get the old
/// "every described file must be whole" rule back.
///
/// Entries are separated by `,` or `;`, may carry a leading dot, and are
/// compared case-insensitively.
fn parse_par2_ignore_extensions(raw: Option<&str>) -> Vec<String> {
    let Some(value) = raw else {
        return DEFAULT_PAR2_IGNORE_EXTENSIONS
            .iter()
            .map(|extension| (*extension).to_string())
            .collect();
    };
    let mut extensions: Vec<String> = value
        .split([',', ';'])
        .map(|entry| {
            entry
                .trim()
                .trim_start_matches('.')
                .trim()
                .to_ascii_lowercase()
        })
        .filter(|entry| !entry.is_empty())
        .collect();
    extensions.sort();
    extensions.dedup();
    extensions
}

/// Whether damage to this file is the kind both reference downloaders refuse to
/// fail a job over.
///
/// The rule is by extension, not by size or by role: an `.nfo`, an `.sfv` and
/// their relatives are furniture posted alongside the payload, and shipping one
/// with a hole in it is strictly better for the user than refusing the payload
/// it describes. Matching is on the file's own extension so a payload that
/// merely *mentions* one of these names is unaffected.
pub(in crate::pipeline) fn par2_damage_ignorable(
    filename: &str,
    ignore_extensions: &[String],
) -> bool {
    if ignore_extensions.is_empty() {
        return false;
    }
    let Some(extension) = std::path::Path::new(filename)
        .extension()
        .and_then(|extension| extension.to_str())
    else {
        return false;
    };
    let extension = extension.to_ascii_lowercase();
    ignore_extensions.contains(&extension)
}

/// A clean PAR2 verdict reached without the authoritative pass, plus the two
/// strings that name which fast path produced it — one labelling the
/// reconciliation for any failure it classifies, one for the retry log.
struct CleanPar2Verification {
    verification: par2_rs::VerificationResult,
    placement_plan: par2_rs::PlacementPlan,
    slice_size: u64,
    verification_mode: CleanPar2VerificationMode,
    reconcile_context: &'static str,
    retry_message: &'static str,
}

#[derive(Default)]
pub(in crate::pipeline) struct Par2DeobfuscationOutcome {
    pub(in crate::pipeline) renamed: usize,
    canonical_description_file_ids: HashMap<par2_rs::RecoverySetId, HashSet<par2_rs::FileId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum CleanPar2VerificationMode {
    Grid,
    FileCrc,
    QuickDigest,
    StrongDecode,
    Authoritative,
}

impl CleanPar2VerificationMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Grid => "grid",
            Self::FileCrc => "file_crc",
            Self::QuickDigest => "quick_digest",
            Self::StrongDecode => "strong_decode",
            Self::Authoritative => "authoritative",
        }
    }
}

/// Which zero-read arm of the quick pass actually decided a set.
///
/// Ordered weakest-last, and the set is named for the weakest arm that
/// contributed: a verdict is only as strong as the thinnest evidence any one of
/// its files rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum QuickPar2Evidence {
    Grid,
    FileCrc,
    Digest,
}

impl QuickPar2Evidence {
    fn verification_mode(self) -> CleanPar2VerificationMode {
        match self {
            Self::Grid => CleanPar2VerificationMode::Grid,
            Self::FileCrc => CleanPar2VerificationMode::FileCrc,
            Self::Digest => CleanPar2VerificationMode::QuickDigest,
        }
    }
}

/// What the quick pass concluded, now that an incomplete answer is still an
/// answer.
///
/// `Full` is the historical shape: every described file proven without a
/// read. `Inconclusive` is every shape where the evidence is positively
/// distrusted — an in-stream damage verdict, a measured digest contradicting
/// a match, ambiguous matches — or where nothing was proven at all; the
/// authoritative pass owns those, reading everything, which is exactly what
/// distrusted evidence deserves. `Partial` is the shape that used to be
/// thrown away: some recovery files proven by zero-read evidence, the rest
/// merely unproven — no taint, just absence — so the gate reads ONLY the
/// unproven remainder and stands the proven entries in, the same carry
/// discipline every selective pass in this file already follows.
enum QuickPar2Outcome {
    Full(
        par2_rs::VerificationResult,
        par2_rs::PlacementPlan,
        QuickPar2Evidence,
    ),
    Partial(QuickPar2PartialEvidence),
    Inconclusive,
}

struct QuickPar2PartialEvidence {
    /// Zero-read `Complete` entries for every proven described file.
    proven: Vec<par2_rs::verify::FileVerification>,
    /// Placement the evidence itself established for the proven files.
    proven_plan: par2_rs::PlacementPlan,
    /// Recovery-set members the evidence could not speak for; the selective
    /// read covers exactly these. Non-recovery descriptions without evidence
    /// are simply not read — the authoritative pass never read them either.
    unproven_recovery: Vec<par2_rs::FileId>,
    /// Disk filenames the proven matches already own, withheld from the
    /// placement proposal for the unproven remainder.
    claimed_disk_names: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum Par2SetSettlementReason {
    Clean {
        slice_size: u64,
        verification_mode: CleanPar2VerificationMode,
    },
    Repaired,
    AbsentUnboundPayload,
}

/// Fold one description's per-slice IFSC CRC32s into a CRC32 over the whole
/// file *as PAR2 checksums it* — every slice padded out to the full slice size,
/// including the short final one.
///
/// `None` when the set carries no checksum table for this description, or when
/// the table is shorter than the description's own slice count: a partial fold
/// would be a CRC32 over a prefix and would be compared as though it covered
/// the file.
fn par2_description_padded_file_crc32(
    par2_set: &par2_rs::Par2FileSet,
    file_id: &par2_rs::FileId,
    length: u64,
    slice_size: u64,
) -> Option<u32> {
    if slice_size == 0 {
        return None;
    }
    let slice_count = par2_set.slice_count_for_file(length) as usize;
    if slice_count == 0 {
        // No slices is no evidence; "the CRCs agree" would be vacuously true.
        return None;
    }
    let checksums = par2_set.file_checksums(file_id)?;
    if checksums.len() < slice_count {
        return None;
    }
    let combine = weaver_yenc::Crc32Combine::new(slice_size);
    let mut folded = checksums[0].crc32;
    for checksum in &checksums[1..slice_count] {
        // Every slice's IFSC CRC32 covers exactly `slice_size` bytes in the
        // padded domain, so one operator serves the whole fold.
        folded = combine.combine(folded, checksum.crc32);
    }
    Some(folded)
}

/// Carry a CRC32 measured over `length` real bytes into the padded domain the
/// PAR2 slice checksums live in, by extending it with the zeros PAR2 pads the
/// final slice with.
fn pad_measured_file_crc32_to_slice_grid(
    measured_crc32: u32,
    length: u64,
    slice_count: u64,
    slice_size: u64,
) -> u32 {
    let padded_length = slice_count.saturating_mul(slice_size);
    let padding = padded_length.saturating_sub(length);
    if padding == 0 {
        return measured_crc32;
    }
    weaver_yenc::crc32_combine(
        measured_crc32,
        crate::pipeline::integrity::crc32_of_zeros(padding),
        padding,
    )
}

/// Descriptions of one set indexed by the pair a streamed whole-file CRC32 is
/// looked up on: the described length, and the description's CRC32 in the
/// padded domain PAR2's slice checksums live in.
type PaddedFileCrcLookup = HashMap<(u64, u32), Vec<(par2_rs::FileId, String)>>;

/// Every description of a set, indexed by the pair a streamed whole-file CRC32
/// can be looked up on.
///
/// Length is half the key on purpose. CRC32 alone is a 32-bit binding and the
/// arm that consumes this acts on a *unique* hit, so the cheapest available
/// discriminator is folded into the key rather than left to chance; a candidate
/// has to agree on both before it is even a candidate.
fn par2_padded_file_crc_lookup(par2_set: &par2_rs::Par2FileSet) -> PaddedFileCrcLookup {
    let mut lookup = PaddedFileCrcLookup::new();
    for file_id in par2_set
        .recovery_file_ids
        .iter()
        .chain(par2_set.non_recovery_file_ids.iter())
    {
        let Some(description) = par2_set.file_description(file_id) else {
            continue;
        };
        let Some(folded) = par2_description_padded_file_crc32(
            par2_set,
            file_id,
            description.length,
            par2_set.slice_size,
        ) else {
            continue;
        };
        let entry = lookup.entry((description.length, folded)).or_default();
        if entry.iter().any(|(known, _)| known == file_id) {
            continue;
        }
        entry.push((*file_id, sanitize_download_filename(&description.filename)));
    }
    lookup
}

fn log_clean_par2_verification_source(
    job_id: JobId,
    set_id: par2_rs::RecoverySetId,
    slice_size: u64,
    verification_mode: CleanPar2VerificationMode,
) {
    let verification_mode = verification_mode.as_str();
    info!(
        job_id = job_id.0,
        recovery_set_id = %set_id,
        slice_size,
        verification_mode,
        "PAR2 clean set verification source"
    );
}

/// The current recovery set's answer to one trip through the completion gate.
///
/// A waiting or running set owns the next re-entry.  A settled or failed set
/// lets the driver advance in deterministic index order; failures are retained
/// until every other set has had the same chance to repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::pipeline) enum SetGateOutcome {
    Settled,
    Waiting,
    #[allow(dead_code)]
    RepairRunning,
    Failed(String),
}

/// What reconciling one PAR2 verification against the assembly established.
///
/// A bare count cannot tell "nothing needed doing" apart from "a repaired,
/// re-verified file bound to nothing and is still sitting incomplete", and the
/// veto downstream has to tell those apart to decide whether it is looking at a
/// download failure or at our own reconciliation failing. Every way a binding
/// can fail is therefore carried out rather than swallowed.
/// What a job still has outstanding after a PAR2 pass reconciled, and whether
/// any of it is bad enough to refuse the job over.
#[derive(Debug)]
pub(in crate::pipeline) struct Par2IncompleteReport {
    /// Operator-facing description of everything left standing.
    pub(in crate::pipeline) message: String,
    /// PAR2-protected files left incomplete whose verified bytes could not be
    /// found. The one case that still fails: delivering would ship a hole under
    /// a verification that claims otherwise.
    pub(in crate::pipeline) unproven_protected: usize,
}

#[derive(Debug, Default)]
pub(in crate::pipeline) struct Par2Reconciliation {
    /// Assembly files this pass promoted to complete.
    pub(in crate::pipeline) completed: usize,
    /// Descriptions a `Complete`/`Renamed` verdict vouched for that bound to no
    /// assembly file at all.
    pub(in crate::pipeline) unbound: Vec<String>,
    /// Bindings refused because more than one file answered to them.
    pub(in crate::pipeline) contested: Vec<String>,
    /// Bound and verified, but the file at the canonical name is absent or is
    /// not the length PAR2 describes.
    pub(in crate::pipeline) length_mismatch: Vec<String>,
}

impl Par2Reconciliation {
    /// Whether anything about this pass needs reporting to the operator, as
    /// opposed to a clean bind-and-promote.
    fn has_failures(&self) -> bool {
        !self.unbound.is_empty() || !self.contested.is_empty() || !self.length_mismatch.is_empty()
    }

    /// The specific identity/reconciliation error, for a job that still has
    /// incomplete data files after this pass ran.
    fn failure_detail(&self) -> String {
        let mut parts = Vec::new();
        if !self.contested.is_empty() {
            parts.push(format!("contested bindings: {}", self.contested.join(", ")));
        }
        if !self.unbound.is_empty() {
            parts.push(format!(
                "verified files bound to no NZB entry: {}",
                self.unbound.join(", ")
            ));
        }
        if !self.length_mismatch.is_empty() {
            parts.push(format!(
                "verified files not installed at their described length: {}",
                self.length_mismatch.join(", ")
            ));
        }
        parts.join("; ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanPar2IntegrityGate {
    None,
    WeakTransform,
    StrongDecode,
}

#[derive(Debug, Clone, Copy, Default)]
struct PromotedRecoveryPipelineState {
    download_queue_len: usize,
    download_queue_has_recovery: bool,
    download_queue_promoted_recovery: usize,
    recovery_queue_len: usize,
    parked_promoted_recovery: usize,
    promoted_par2_files: usize,
    incomplete_promoted_par2_files: usize,
    active_promoted_downloads: usize,
    pending_promoted_retries: usize,
    pending_promoted_decode: usize,
    active_promoted_decodes: usize,
    write_buffered_promoted_recovery: usize,
    unavailable_promoted_recovery_segments: usize,
}

fn reserve_identity_filenames(
    identity: &crate::jobs::record::ActiveFileIdentity,
    occupied_filenames: &mut HashSet<String>,
) {
    reserve_download_filename(&identity.source_filename, occupied_filenames);
    reserve_download_filename(&identity.current_filename, occupied_filenames);
    if let Some(canonical) = identity.canonical_filename.as_ref() {
        reserve_download_filename(canonical, occupied_filenames);
    }
}

fn forget_identity_filenames(
    identity: &crate::jobs::record::ActiveFileIdentity,
    occupied_filenames: &mut HashSet<String>,
) {
    forget_reserved_download_filename(&identity.source_filename, occupied_filenames);
    forget_reserved_download_filename(&identity.current_filename, occupied_filenames);
    if let Some(canonical) = identity.canonical_filename.as_ref() {
        forget_reserved_download_filename(canonical, occupied_filenames);
    }
}

/// The entry names a directory holds right now, or nothing if it cannot be
/// read. Used to bracket a repair so its artefacts can be named by difference.
fn directory_entry_names(dir: &Path) -> HashSet<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return HashSet::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect()
}

fn reserve_directory_filenames(dir: &Path, occupied_filenames: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if let Some(filename) = entry.file_name().to_str() {
            reserve_download_filename(filename, occupied_filenames);
        }
    }
}

impl PromotedRecoveryPipelineState {
    fn has_pending_work(self) -> bool {
        self.download_queue_promoted_recovery > 0
            || self.active_promoted_downloads > 0
            || self.pending_promoted_retries > 0
            || self.pending_promoted_decode > 0
            || self.active_promoted_decodes > 0
            || self.write_buffered_promoted_recovery > 0
    }
}

fn par2_verification_needs_repair(verification: &par2_rs::VerificationResult) -> bool {
    verification.needs_repair()
}

/// Why waiting for targeted recovery is waiting for nothing, if it is.
///
/// The branch below hands a short repair back to `Downloading` on the promise
/// that more recovery is on its way. Three things can keep that promise: a
/// promotion that just put articles on the wire, work still moving through the
/// pipeline, and a promoted volume's segments parked for the gate to hand back
/// on its next entry. With none of them, nothing between now and the next entry
/// can change a single number this branch just read — the one pass that could,
/// the read-back of volumes that can no longer complete, already ran on the way
/// in. So the job would return here forever, and the honest answer is the
/// terminal one.
///
/// Reaching this at all means the shortfall gate above declined to fire, which
/// means the targeted total is counting blocks the available count cannot see.
/// That is an accounting disagreement rather than an ordinary shortfall, so it
/// is said out loud: a regression here should cost a log line, not a hung job.
fn par2_unreachable_recovery_failure(
    job_id: JobId,
    blocks_needed: u32,
    recovery_now: u32,
    targeted_total: u32,
    promoted_blocks: u32,
    recovery_still_settling: bool,
    parked_promoted_recovery: usize,
) -> Option<String> {
    if promoted_blocks > 0 || recovery_still_settling || parked_promoted_recovery > 0 {
        return None;
    }
    warn!(
        job_id = job_id.0,
        blocks_needed,
        recovery_now,
        targeted_total,
        "targeted PAR2 recovery total counts blocks this repair can never reach"
    );
    Some(format!(
        "not repairable: {blocks_needed} damaged slices, only {recovery_now} recovery blocks \
         reachable and no further recovery can arrive ({targeted_total} targeted)"
    ))
}

/// Whether the only work a verdict leaves outstanding is placement.
///
/// Nothing damaged, nothing missing, no slice to reconstruct — every described
/// file's content is on disk and whole, and some of it is sitting under a name
/// that is not its own. That is a verdict `needs_repair()` reports as true and
/// a repairer has nothing to do about: moving a file is what the placement plan
/// is for.
fn par2_verification_is_placement_only(verification: &par2_rs::VerificationResult) -> bool {
    verification.total_missing_blocks == 0
        && !verification.files.iter().any(|file| {
            matches!(
                file.status,
                par2_rs::verify::FileStatus::Damaged(_) | par2_rs::verify::FileStatus::Missing
            )
        })
}

/// Why a post-repair verification should fail the repair, if it should.
///
/// Damage and misplacement both make a verification "need repair", and only one
/// of them is a failure. A file that is intact but sitting under the wrong name
/// verifies as `Renamed` — not `Complete`, so `needs_repair()` is true — and
/// that same status is what the placement plan turns into a rename entry. A
/// repair rejected on it is refused for the one thing the placement about to
/// run would fix, and the message can only report zero damaged slices, because
/// nothing was damaged. Misplacement is judged after placement instead, by
/// re-reading the set where it then sits.
fn par2_post_repair_damage_failure(verification: &par2_rs::VerificationResult) -> Option<String> {
    if par2_verification_is_placement_only(verification) {
        return None;
    }
    let damaged = verification
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.status,
                par2_rs::verify::FileStatus::Damaged(_) | par2_rs::verify::FileStatus::Missing
            )
        })
        .count();
    let misplaced = verification
        .files
        .iter()
        .filter(|file| matches!(file.status, par2_rs::verify::FileStatus::Renamed(_)))
        .count();
    Some(format!(
        "PAR2 repair completed but {} damaged slice(s) across {} file(s) remain \
         ({} file(s) still to be placed)",
        verification.total_missing_blocks, damaged, misplaced
    ))
}

/// What an authoritative PAR2 pass is asked to read.
enum Par2PassScope {
    /// Every recovery file, over a plan observed by a fresh directory scan.
    /// The conventional pass, unchanged.
    WholeSet,
    /// Only these file IDs, read at their canonical names. The caller owns the
    /// merge with whatever it is standing in for, and the placement plan it
    /// returns is derived from that merged result rather than scanned.
    Selected(Vec<par2_rs::FileId>),
    /// Only these file IDs — the quick pass's unproven remainder — read
    /// through a 16 KiB-prefix placement proposal built inside the pass (see
    /// [`build_prefix_placement_proposal`]), never a scan. The second field
    /// names the disk files the quick pass already matched, so the proposal
    /// never offers a proven file as a candidate for an unproven description.
    /// The caller owns the merge with the quick pass's proven entries, exactly
    /// as `Selected` callers own their carry, and the strict verify that reads
    /// through the proposal is what proves each proposed placement — a
    /// proposal the bytes contradict comes back `Missing` or `Damaged` rather
    /// than becoming a wrong rename.
    SelectedProposed(Vec<par2_rs::FileId>, HashSet<String>),
}

/// Run the read the scope asks for. All arms share par2-rs's selected-file
/// verifier. The canonical-name scope also includes non-recovery descriptions:
/// without IFSC packets the verifier proves those by strict full-file MD5,
/// which is the identity proof a content rename needs. The selective arm alone
/// opts into the crate's fast-verify mode.
///
/// # Why the selective arm verifies from slice proof
///
/// par2-rs hashes at two tiers: a whole-file MD5, which is one message and
/// therefore one inherently serial chain, and per-slice MD5s, which are
/// independent messages fed several at a time through the crate's multi-buffer
/// SIMD engine. The strict pipeline reaches its `Complete` verdict from the
/// **whole-file** digest, so a strict pass over one large intact file is a
/// single serial MD5 chain. Measured on the crate's `single_file_verify_shape`
/// bench over one intact 128 MiB file: 149 ms strict, against 12 ms for the
/// same bytes proved from their per-slice IFSC checksums and a 7.8 ms
/// read-only floor — the serial digest is roughly 95% of the strict pass.
///
/// The whole-file rule exists to establish the *identity* of files the
/// library scans: a found file's full digest is what says it is the described
/// file at all. The selective arm reads nothing whose identity is open —
/// identity here is fixed by name, not discovered by digest. Each file in its
/// list is read at the canonical name its description gives it, where the
/// repairer just installed it after verifying the staged bytes against their
/// IFSC checksums (or, in the placement corners, confirmed it complete with
/// its own scan). Proving those bytes again from the same per-slice checksums
/// answers the question this pass is asking — did the rewrite land intact — at
/// read speed, and it is the same proof class the repairer's install readback
/// and the in-stream grid already stand a verdict on. The mode self-guards:
/// it engages only when the on-disk length matches the description (always
/// true for a fresh install) and falls back to the strict pipeline otherwise,
/// so the verdicts are byte-identical either way. The conventional whole-set
/// arm keeps the strict default because it *is* the scan that establishes
/// identity, and the canonical-name whole-set arm keeps it because it stands
/// alone for the whole set: it is the one pass behind that verdict, so it
/// establishes each file's identity rather than inheriting it from a pass that
/// just installed the bytes.
fn verify_in_scope(
    scope: &Par2PassScope,
    par2_set: &par2_rs::Par2FileSet,
    access: &dyn par2_rs::FileAccess,
) -> par2_rs::VerificationResult {
    match scope {
        Par2PassScope::WholeSet => par2_rs::verify_all(par2_set, access),
        Par2PassScope::Selected(file_ids) | Par2PassScope::SelectedProposed(file_ids, _) => {
            par2_rs::verify_selected_file_ids_with_options(
                par2_set,
                access,
                file_ids,
                &selective_pass_verify_options(),
            )
        }
    }
}

/// The options the selective post-repair arm verifies with: fast-verify on,
/// nothing else. Factored out of [`verify_in_scope`] so a test can pin that
/// the selective pass asks for slice proof — the verdicts are identical in
/// both modes by design, so no downstream observation could.
///
/// Shared with the direct-store side, whose post-repair read-back is the same
/// pass over virtual volumes and wants the same terms; see
/// [`crate::pipeline::Pipeline::verify_direct_sets_quietly`].
pub(in crate::pipeline) fn selective_pass_verify_options() -> par2_rs::VerifyOptions {
    let mut options = par2_rs::VerifyOptions::default();
    options.fast_verify = true;
    options
}

/// A repairer scan carry built from the authoritative whole-set pass this
/// module just ran, so the damaged path's repairer analysis does not re-read
/// bytes that pass already hashed.
///
/// Fingerprints are captured at each description's sanitized name — the same
/// resolution the pass read through when its plan proposed no moves, which is
/// the only shape the caller builds a carry for. Any file this cannot account
/// for refuses the whole carry rather than shipping a partial attestation:
/// a `Renamed` verdict means the layout is not the canonical one the carry
/// describes, and a present-attested file that fails to stat means the bytes
/// are not where the attestation says. par2-rs refusing the finished carry is
/// logged and dropped — the analysis then scans exactly as it always did.
pub(in crate::pipeline) fn build_host_verification_carry(
    verify_dir: &std::path::Path,
    par2_set: &par2_rs::Par2FileSet,
    verification: &par2_rs::VerificationResult,
) -> Option<std::sync::Arc<par2_rs::ScanCarry>> {
    let mut fingerprints: HashMap<par2_rs::FileId, par2_rs::FileStatFingerprint> = HashMap::new();
    for file in &verification.files {
        match &file.status {
            par2_rs::verify::FileStatus::Missing => continue,
            par2_rs::verify::FileStatus::Renamed(_) => return None,
            par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Damaged(_) => {}
        }
        let description = par2_set.file_description(&file.file_id)?;
        let path = verify_dir.join(sanitize_download_filename(&description.filename));
        let fingerprint = par2_rs::FileStatFingerprint::capture_path(&path)?;
        fingerprints.insert(file.file_id, fingerprint);
    }
    match par2_rs::ScanCarry::from_verification(verify_dir, par2_set, verification, &fingerprints) {
        Ok(carry) => Some(std::sync::Arc::new(carry)),
        Err(error) => {
            debug!(
                error = %error,
                "host verification carry refused — the repairer will scan normally"
            );
            None
        }
    }
}

/// The files a PAR2 repair rewrote, taken from the verification that decided
/// the repair was needed.
///
/// A repair writes a file when that file is not already complete at its
/// canonical path: par2-rs stages exactly the recoverable files that fail
/// `is_canonical_complete`, reconstructs or copies them into a staging
/// directory, reads them back against their IFSC slice checksums, and installs
/// them over their canonical targets. `Complete` therefore means untouched —
/// the repair had nothing to write — while `Damaged`, `Missing` and `Renamed`
/// are the three verdicts that put a file in the write set.
///
/// Untouchedness is the ordinary case, not the guarantee. A set holding two
/// files with identical content can have the placement scan's first-match-wins
/// rename move an exactly-placed file away, after which the repairer's own
/// scanner finds that content under the other name and copies it back. What
/// licenses carrying a `Complete` verdict is not that the bytes were left alone
/// but that they are *content-invariant*: the verdict and the digest it vouches
/// are statements about content matching the description, every install the
/// repair makes is read back against the set's IFSC checksums before it lands,
/// and content that matches the description matches it whichever file wrote it.
/// A `Complete` entry therefore stays true across a repair that did touch it.
///
/// `Renamed` is read too, because the repair acts on it. A file whose content
/// matches a description it is not named for is not complete at its canonical
/// path either, so the repair copies those bytes onto that path — that copy is
/// what `bytes_copied` counts on a run that reconstructed no slice at all — and
/// moves whatever held the name aside as `<name>.N`. Carrying the pre-repair
/// entry instead would report the file as still misplaced after the repair had
/// already placed it, and hand the placement step that follows a rename onto a
/// name the repair has just filled.
///
/// What is left carried is therefore exactly the files that were already intact
/// at their canonical names before the repair ran, which is the only set whose
/// pre-repair verdict still describes the disk afterwards.
/// What [`Pipeline::register_verified_par2_rar_outputs`] adopted.
///
/// The set names travel with the count because adopting a rebuilt volume
/// invalidates its set's derived plan: the plan was computed from the headers of
/// the volumes that were present, and this is a volume that was not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Par2RarOutputRegistration {
    /// Volumes whose facts this call newly persisted or changed.
    pub(crate) registered: usize,
    /// The RAR sets those volumes belong to.
    pub(crate) set_names: BTreeSet<String>,
    /// Split 7z parts the NZB never carried, adopted into their set's
    /// topology. Counted apart from `registered` because nothing about them
    /// is a RAR plan to invalidate: the topology *is* their plan.
    pub(crate) sevenz_parts: usize,
}

pub(in crate::pipeline) fn par2_repair_write_set(
    verification: &par2_rs::VerificationResult,
) -> Vec<par2_rs::FileId> {
    verification
        .files
        .iter()
        .filter(|file| !matches!(file.status, par2_rs::verify::FileStatus::Complete))
        .map(|file| file.file_id)
        .collect()
}

/// How many slices the repair actually had to reconstruct.
///
/// Counted from the *pre-repair* verdict, because that is the only place the
/// number survives. `Par2RepairOutcome::recovery_blocks_used` is derived from
/// the verification the repairer finishes with — `total_missing_blocks` capped
/// by the recovery block count — and a repair that succeeded finishes with zero
/// missing blocks. So that field reads zero on exactly the runs that did the
/// work, which is the one case anybody wants the number for.
fn par2_repair_slices_repaired(pre_repair: &par2_rs::VerificationResult) -> u32 {
    pre_repair.total_missing_blocks
}

/// The placement plan a fresh directory scan would produce for `verification`,
/// derived from the verdicts instead of re-read from disk.
///
/// A file that verified `Complete` was read at the name its description gives
/// it, so it is exactly placed; one that is still `Damaged` or `Missing` has no
/// disk file standing for it, which is what `unresolved` means. `Renamed`
/// carries its physical path and becomes the rename that moves it home.
///
/// `swaps` is necessarily empty: a swap is two files each sitting on the
/// other's name, and neither can then be `Complete` at its own name nor
/// `Renamed` to a name the plan is also moving something onto. Whatever
/// pairwise displacement existed was already normalized by the plan the
/// pre-repair pass produced and this job applied before repairing. `conflicts`
/// is empty for the same reason it is fatal elsewhere — the verification has
/// one entry per file ID, so no two disk files can be claiming one description
/// here.
/// Placement by proposal instead of proof: which disk file plausibly stands
/// for which description, decided from the description's own 16 KiB prefix
/// hash and length — never from a full-file digest.
///
/// This replaces the library placement scan wherever a strict verify follows,
/// which in this gate is everywhere: the scan confirmed each candidate by
/// computing its FULL-file MD5, so a whole-set pass paid two complete reads —
/// one to place the files and one to prove them — when the second read alone
/// answers both questions. A proposal from the 16 KiB hash is exactly the
/// identification tier the recovery format itself defines for finding files;
/// the verify that reads through the proposed plan is the proof, and a
/// proposal the bytes contradict surfaces as `Missing`/`Damaged` rather than
/// as a wrong rename.
///
/// The one capability deliberately given up: two same-length files sharing a
/// 16 KiB prefix but diverging later were disambiguated by the scan's full
/// digests. Here they are left `unresolved` — the verify then reads the
/// description at its own name and the damage path owns the outcome — because
/// paying a whole-set read on every pass to break a tie that pathological is
/// the wrong trade.
///
/// `restrict` limits the proposal to those descriptions (the quick pass's
/// unproven remainder); `claimed_names` are disk files another arm already
/// matched, never offered as candidates here.
fn build_prefix_placement_proposal(
    dir: &std::path::Path,
    par2_set: &par2_rs::Par2FileSet,
    restrict: Option<&HashSet<par2_rs::FileId>>,
    claimed_names: &HashSet<String>,
) -> par2_rs::PlacementPlan {
    let mut described: Vec<par2_rs::FileId> = par2_set
        .recovery_file_ids
        .iter()
        .chain(par2_set.non_recovery_file_ids.iter())
        .copied()
        .filter(|file_id| restrict.is_none_or(|only| only.contains(file_id)))
        .collect();
    described.sort_unstable_by_key(|file_id| *file_id.as_bytes());
    described.dedup();

    let mut exact = Vec::new();
    let mut renames: Vec<par2_rs::PlacementEntry> = Vec::new();
    let mut unresolved = Vec::new();
    let mut claimed: HashSet<String> = claimed_names.clone();
    let mut needs_candidate: Vec<(par2_rs::FileId, String, u64, [u8; 16])> = Vec::new();

    let prefix_matches = |path: &std::path::Path, length: u64, hash_16k: &[u8; 16]| -> bool {
        let window = length.min(16 * 1024) as usize;
        let mut prefix = vec![0u8; window];
        std::fs::File::open(path)
            .and_then(|mut file| {
                use std::io::Read;
                file.read_exact(&mut prefix)
            })
            .is_ok()
            && par2_rs::checksum::md5(&prefix) == *hash_16k
    };

    for file_id in described {
        let Some(desc) = par2_set.file_description(&file_id) else {
            continue;
        };
        let described_name = sanitize_download_filename(&desc.filename);
        // Presence at the described name is a proposal only when the bytes
        // agree: length and 16 KiB prefix. A file merely SITTING at the name
        // — a swapped pair is the canonical case — must fall through to the
        // candidate pool, or a pure placement problem masquerades as
        // whole-set damage when the verify reads the wrong bytes through an
        // `exact` entry.
        let path = dir.join(&described_name);
        let at_name_matches = std::fs::metadata(&path)
            .map(|metadata| metadata.is_file() && metadata.len() == desc.length)
            .unwrap_or(false)
            && prefix_matches(&path, desc.length, &desc.hash_16k);
        if at_name_matches {
            claimed.insert(described_name);
            exact.push(file_id);
        } else {
            needs_candidate.push((file_id, described_name, desc.length, desc.hash_16k));
        }
    }

    if !needs_candidate.is_empty() {
        // One directory listing, one 16 KiB read per length-matched candidate.
        // The prefix hash is cached per disk file so N misplaced descriptions
        // cost each candidate one read, not N.
        let mut prefix_hash_by_name: HashMap<String, Option<[u8; 16]>> = HashMap::new();
        let mut length_by_name: HashMap<String, u64> = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                if !file_type.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if claimed.contains(&name) {
                    continue;
                }
                if let Ok(metadata) = entry.metadata() {
                    length_by_name.insert(name, metadata.len());
                }
            }
        }
        for (file_id, described_name, length, hash_16k) in needs_candidate {
            let mut matched: Vec<String> = Vec::new();
            for (name, disk_len) in &length_by_name {
                if *disk_len != length || claimed.contains(name) {
                    continue;
                }
                let prefix_hash = prefix_hash_by_name.entry(name.clone()).or_insert_with(|| {
                    let window = length.min(16 * 1024) as usize;
                    let mut prefix = vec![0u8; window];
                    std::fs::File::open(dir.join(name))
                        .and_then(|mut file| {
                            use std::io::Read;
                            file.read_exact(&mut prefix)
                        })
                        .ok()
                        .map(|_| par2_rs::checksum::md5(&prefix))
                });
                if *prefix_hash == Some(hash_16k) {
                    matched.push(name.clone());
                }
            }
            match matched.as_slice() {
                [candidate] => {
                    claimed.insert(candidate.clone());
                    renames.push(par2_rs::PlacementEntry {
                        file_id,
                        current_name: candidate.clone(),
                        correct_name: described_name,
                    });
                }
                _ => unresolved.push(file_id),
            }
        }
    }

    // Two renames whose names cross are one swap: the rename applier must see
    // them as a pair or the first move finds its target occupied.
    let mut swaps = Vec::new();
    let mut swapped: HashSet<par2_rs::FileId> = HashSet::new();
    let by_current: HashMap<String, usize> = renames
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.current_name.clone(), index))
        .collect();
    for index in 0..renames.len() {
        let entry = &renames[index];
        if swapped.contains(&entry.file_id) {
            continue;
        }
        if let Some(&other_index) = by_current.get(&entry.correct_name) {
            let other = &renames[other_index];
            if other_index != index
                && other.correct_name == entry.current_name
                && !swapped.contains(&other.file_id)
            {
                swapped.insert(entry.file_id);
                swapped.insert(other.file_id);
                swaps.push((renames[index].clone(), renames[other_index].clone()));
            }
        }
    }
    let renames: Vec<par2_rs::PlacementEntry> = renames
        .into_iter()
        .filter(|entry| !swapped.contains(&entry.file_id))
        .collect();

    par2_rs::PlacementPlan {
        exact,
        swaps,
        renames,
        unresolved,
        conflicts: Vec::new(),
    }
}

/// The merge base for a partial quick verdict: the proven entries where the
/// evidence spoke, `Missing` placeholders where it did not.
///
/// `merge_verification_results` iterates the BASE's files — an updated entry
/// with no base counterpart is dropped — so the base must enumerate every
/// recovery member, in the recovery set's own order, for the merged result to
/// be shaped exactly as a whole-set pass's would have been. The placeholders
/// never survive into a settled verdict on their own: every placeholder id is
/// in the selective read's list, so the fresh entry replaces it, and a fresh
/// read that could not run leaves the placeholder saying exactly what is true
/// — this file is unproven and unlocated.
fn quick_partial_base_verification(
    par2_set: &par2_rs::Par2FileSet,
    partial: &QuickPar2PartialEvidence,
) -> par2_rs::VerificationResult {
    let proven_by_id: HashMap<par2_rs::FileId, &par2_rs::verify::FileVerification> = partial
        .proven
        .iter()
        .map(|file| (file.file_id, file))
        .collect();
    let files: Vec<par2_rs::verify::FileVerification> = par2_set
        .recovery_file_ids
        .iter()
        .filter_map(|file_id| {
            if let Some(proven) = proven_by_id.get(file_id) {
                return Some((*proven).clone());
            }
            let desc = par2_set.file_description(file_id)?;
            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            Some(par2_rs::verify::FileVerification {
                file_id: *file_id,
                filename: sanitize_download_filename(&desc.filename),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false; slice_count],
                missing_slice_count: slice_count as u32,
            })
        })
        .collect();
    let mut total_missing_blocks = 0u32;
    for file in &files {
        total_missing_blocks = total_missing_blocks.saturating_add(file.missing_slice_count);
    }
    par2_rs::VerificationResult {
        files,
        recovery_blocks_available: par2_set.recovery_block_count(),
        total_missing_blocks,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

/// One plan from the two halves of a partial pass: the placement the quick
/// evidence established for the proven files, and the proposal the selective
/// read just proved for the remainder. The two halves are disjoint by
/// construction — the proposal never offers a claimed disk name — so this is
/// concatenation, not reconciliation.
fn merge_partial_placement_plan(
    proven: par2_rs::PlacementPlan,
    fresh: par2_rs::PlacementPlan,
) -> par2_rs::PlacementPlan {
    let mut merged = proven;
    merged.exact.extend(fresh.exact);
    merged.swaps.extend(fresh.swaps);
    merged.renames.extend(fresh.renames);
    merged.unresolved.extend(fresh.unresolved);
    merged.conflicts.extend(fresh.conflicts);
    merged
}

fn placement_plan_from_verification(
    verification: &par2_rs::VerificationResult,
) -> par2_rs::PlacementPlan {
    let mut exact = Vec::new();
    let mut renames = Vec::new();
    let mut unresolved = Vec::new();
    for file in &verification.files {
        match &file.status {
            par2_rs::verify::FileStatus::Complete => exact.push(file.file_id),
            par2_rs::verify::FileStatus::Renamed(path) => {
                let Some(current_name) = path.file_name().map(|name| name.to_string_lossy()) else {
                    unresolved.push(file.file_id);
                    continue;
                };
                renames.push(par2_rs::PlacementEntry {
                    file_id: file.file_id,
                    current_name: current_name.into_owned(),
                    // Match the quick verifier and download identity layer;
                    // PAR2 descriptions can contain non-portable path names.
                    correct_name: sanitize_download_filename(&file.filename),
                });
            }
            par2_rs::verify::FileStatus::Damaged(_) | par2_rs::verify::FileStatus::Missing => {
                unresolved.push(file.file_id);
            }
        }
    }
    par2_rs::PlacementPlan {
        exact,
        swaps: Vec::new(),
        renames,
        unresolved,
        conflicts: Vec::new(),
    }
}

fn summarize_rar_set_phase(
    set_name: &str,
    set_state: &crate::pipeline::archive::rar_state::RarSetState,
) -> String {
    let phase = set_state
        .plan
        .as_ref()
        .map(|plan| plan.phase)
        .unwrap_or(set_state.phase);
    let mut ready_members = set_state
        .plan
        .as_ref()
        .map(|plan| {
            plan.ready_members
                .iter()
                .map(|member| member.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ready_members.sort();
    let waiting_on_volumes = set_state
        .plan
        .as_ref()
        .map(|plan| {
            let mut waiting = plan.waiting_on_volumes.iter().copied().collect::<Vec<_>>();
            waiting.sort_unstable();
            waiting
        })
        .unwrap_or_default();
    let mut in_flight_members = set_state
        .in_flight_members
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    in_flight_members.sort();
    let mut suspect_volumes = set_state
        .verified_suspect_volumes
        .iter()
        .copied()
        .collect::<Vec<_>>();
    suspect_volumes.sort_unstable();

    format!(
        "{set_name}: phase={phase:?} workers={} ready={ready_members:?} waiting={waiting_on_volumes:?} inflight={in_flight_members:?} suspect={suspect_volumes:?}",
        set_state.active_workers,
    )
}

impl Pipeline {
    fn current_rar_set_names_for_job(&self, job_id: JobId) -> HashSet<String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashSet::new();
        };

        let mut set_names: HashSet<String> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .and_then(|identity| identity.classification)
                    .and_then(|classification| {
                        matches!(
                            classification.kind,
                            crate::jobs::assembly::DetectedArchiveKind::Rar
                        )
                        .then_some(classification.set_name)
                    })
            })
            .collect();

        if set_names.is_empty() {
            set_names.extend(state.assembly.archive_topologies().iter().filter_map(
                |(set_name, topology)| {
                    matches!(
                        topology.archive_type,
                        crate::jobs::assembly::ArchiveType::Rar
                    )
                    .then_some(set_name.clone())
                },
            ));
        }

        set_names
    }

    fn job_has_idle_startable_rar_work(&self, job_id: JobId) -> bool {
        self.rar_sets
            .iter()
            .filter(|((rar_job_id, _), _)| *rar_job_id == job_id)
            .any(|((_, set_name), set_state)| {
                set_state.active_workers == 0
                    && set_state.in_flight_members.is_empty()
                    && set_state.plan.as_ref().is_some_and(|plan| {
                        plan.ready_members.iter().any(|ready_member| {
                            self.rar_ready_member_is_startable_for_batch_extraction(
                                job_id,
                                set_name,
                                &ready_member.name,
                            )
                        })
                    })
            })
    }

    pub(crate) fn job_has_live_rar_waiting_for_missing_volumes(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
                *rar_job_id == job_id
                    && (current_set_names.is_empty() || current_set_names.contains(set_name))
                    && set_state.plan.as_ref().is_some_and(|plan| {
                        matches!(
                            plan.phase,
                            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
                                | crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair
                        )
                    })
            })
    }

    /// Like [`Self::job_has_live_rar_waiting_for_missing_volumes`], but true
    /// only when the wait can no longer be answered by anything except
    /// recovery data — the distinction PAR2 repair-readiness needs and the
    /// phase alone cannot make.
    ///
    /// `WaitingForVolumes` is reported for two different situations. A volume
    /// that is genuinely absent — no parsed facts, no file on disk — is the
    /// repair case: nothing but recovery blocks can produce it. But a set mid
    /// swap-correction waits on volume *numbers* while every actual volume is
    /// present (the topology parsed them under mismatched numbering), and
    /// that wait is answered by the cached-header retry, not by PAR2. Treating
    /// the second as the first sent a swap job into damaged-path analysis,
    /// promoted 12 recovery blocks it had no use for, and emitted verification
    /// events its fixture forbids — phantom damage, wasted downloads, and a
    /// repair-first detour on a job the retry frontier was already fixing.
    ///
    /// So `WaitingForVolumes` qualifies only when some waited volume is truly
    /// absent *and* no waited volume is present — a non-empty
    /// [`present_waiting_rar_volumes`] means the volume-0 retry is still owed
    /// its chance to relink them — *and* the download pipeline is quiet.
    /// Absence only means "not coming" once nothing is en route: mid-download,
    /// a waited volume is absent simply because it has not arrived yet, and a
    /// just-demoted direct set is absent because its refetch was queued
    /// milliseconds ago. Both fired this predicate 10 seconds into a job and
    /// sent it through damaged-path analysis — phantom damage, 12 promoted
    /// recovery blocks with no use, and verification events the fixture
    /// forbids. The arm's design case — an interior volume the NZB never
    /// carried — is only *observable* at pipeline-quiet anyway, so the
    /// condition costs it nothing. (`job_has_pending_download_pipeline_work`
    /// deliberately excludes the parked recovery pool, so a job whose only
    /// remaining work is parked recovery still qualifies — that pool drains
    /// only through the promotion this predicate gates.)
    ///
    /// `AwaitingRepair` qualifies unconditionally: there the extraction
    /// machinery itself has concluded only repair moves the set forward, and
    /// its waiting list is legitimately empty.
    pub(crate) fn job_has_live_rar_waiting_for_absent_volumes(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
                if *rar_job_id != job_id
                    || !(current_set_names.is_empty() || current_set_names.contains(set_name))
                {
                    return false;
                }
                let Some(plan) = set_state.plan.as_ref() else {
                    return false;
                };
                match plan.phase {
                    crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair => true,
                    crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes => {
                        if self.job_has_pending_download_pipeline_work(job_id) {
                            return false;
                        }
                        let volume_paths = self.volume_paths_for_rar_set(job_id, set_name);
                        let some_volume_absent = plan.waiting_on_volumes.iter().any(|volume| {
                            !set_state.facts.contains_key(volume)
                                && !volume_paths.contains_key(volume)
                        });
                        some_volume_absent
                            && crate::pipeline::archive::topology::present_waiting_rar_volumes(
                                plan,
                                &set_state.facts,
                                &volume_paths,
                            )
                            .is_empty()
                    }
                    _ => false,
                }
            })
    }

    pub(crate) fn job_has_pending_rar_refresh_for_current_sets(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_refresh_state
            .iter()
            .any(|((refresh_job_id, set_name), refresh_state)| {
                *refresh_job_id == job_id
                    && (current_set_names.is_empty() || current_set_names.contains(set_name))
                    && (refresh_state.in_flight.is_some() || refresh_state.queued.is_some())
            })
    }

    pub(crate) fn job_has_incoherent_rar_waiting_state(&self, job_id: JobId) -> bool {
        let current_set_names = self.current_rar_set_names_for_job(job_id);

        self.rar_sets
            .iter()
            .any(|((rar_job_id, set_name), set_state)| {
                *rar_job_id == job_id
                    && (current_set_names.is_empty() || current_set_names.contains(set_name))
                    && set_state.active_workers == 0
                    && set_state.in_flight_members.is_empty()
                    && set_state.plan.as_ref().is_some_and(|plan| {
                        matches!(
                            plan.phase,
                            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
                        ) && plan.waiting_on_volumes.is_empty()
                            && plan.ready_members.is_empty()
                    })
            })
    }

    /// Whether a split 7z set of this job is waiting on a part the NZB never
    /// carried.
    ///
    /// The 7z counterpart of [`Self::job_has_live_rar_waiting_for_absent_volumes`].
    /// A 7z split set has no header chain to learn its hole from, so absence is
    /// read from two places. Either the topology's numbering has a gap — an
    /// interior `.7z.NNN` no NZB file was ever registered for — or the served
    /// recovery set describes a part of this set under a name the assembly has
    /// never heard of and no such file is on disk, which is how a withheld
    /// *last* part looks: the topology counted the parts it saw and believes the
    /// set is whole. Both are the shape only recovery blocks can move. Without
    /// naming it, the job either waits on readiness forever (interior) or
    /// extracts a truncated set, fails, and never asks PAR2 for the part (last),
    /// because the strong-decode fast path settles the set as clean first.
    ///
    /// Unlike the RAR predicate, this one does not wait for pipeline-quiet.
    /// The RAR arm needs quiet because its absence is read from *arrivals*:
    /// mid-download, a waited volume is absent simply because it has not landed
    /// yet. Both 7z readings are structural instead. The topology's
    /// `volume_map` is built from every file the NZB carries, complete or not,
    /// so a numbering gap in it is a part the posting never had; and the
    /// described-part reading excludes every name the map knows. A part still
    /// arriving is in the map and so is never mistaken for a hole. Deferring
    /// to quiet here was a live defect: the data parts of a small posting all
    /// landed while its recovery file was still in flight, this answered
    /// `false`, the strong-decode fast path settled the set as clean, and the
    /// extraction that followed opened a truncated set and failed the job.
    pub(crate) fn job_has_sevenz_set_waiting_for_absent_volumes(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let served_set = self
            .par2_served_set_id(job_id)
            .and_then(|set_id| self.par2_set_for(job_id, set_id));
        state
            .assembly
            .archive_topologies()
            .iter()
            .filter(|(_, topology)| {
                topology.archive_type == crate::jobs::assembly::ArchiveType::SevenZip
            })
            .any(|(set_name, topology)| {
                let numbered: HashSet<u32> = topology.volume_map.values().copied().collect();
                let interior_hole = topology.expected_volume_count.is_some_and(|expected| {
                    (0..expected).any(|volume| !numbered.contains(&volume))
                });
                if interior_hole {
                    return true;
                }
                served_set.is_some_and(|par2_set| {
                    !Self::absent_described_sevenz_parts(
                        &state.working_dir,
                        set_name,
                        topology,
                        par2_set,
                    )
                    .is_empty()
                })
            })
    }

    /// The parts of one split 7z set that the recovery set describes, the
    /// set's topology does not list, and the working directory does not hold —
    /// `(sanitized filename, volume number)`, in volume order.
    ///
    /// Names are compared sanitized on both sides because that is the form
    /// `volume_map` keys take (they are current download filenames) and the
    /// form the repairer's outputs land under.
    fn absent_described_sevenz_parts(
        working_dir: &Path,
        set_name: &str,
        topology: &crate::jobs::assembly::ArchiveTopology,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Vec<(String, u32)> {
        let set_key = sanitize_download_filename(set_name);
        let known: HashSet<String> = topology
            .volume_map
            .keys()
            .map(|name| sanitize_download_filename(name))
            .collect();
        let mut absent: Vec<(String, u32)> = par2_set
            .files
            .values()
            .filter_map(|description| {
                let filename = sanitize_download_filename(&description.filename);
                let role = weaver_model::files::FileRole::from_filename(&filename);
                let weaver_model::files::FileRole::SevenZipSplit { number } = role else {
                    return None;
                };
                let base = weaver_model::files::archive_base_name(&filename, &role)?;
                if sanitize_download_filename(&base) != set_key || known.contains(&filename) {
                    return None;
                }
                (!working_dir.join(&filename).is_file()).then_some((filename, number))
            })
            .collect();
        absent.sort_unstable_by_key(|(_, number)| *number);
        absent
    }

    /// Whether a job currently owns live download-stage work for queue presentation.
    /// Queued articles and delayed retries deliberately do not count: they are the
    /// scheduler's backlog, not current transfer activity.
    pub(crate) fn job_has_current_download_activity(&self, job_id: JobId) -> bool {
        self.hot_dispatch_job == Some(job_id)
            || self
                .active_download_connections_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .active_decodes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .pending_released_download_results_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .pending_decode
                .iter()
                .any(|work| work.segment_id.file_id.job_id == job_id)
            || self
                .write_buffers
                .iter()
                .any(|(file_id, buffer)| file_id.job_id == job_id && buffer.buffered_len() > 0)
    }

    /// Everything the download stage still owes this job.
    ///
    /// A health probe is deliberately *not* on this list. A probe is an
    /// estimate sampled alongside the download, never work the job owes: it
    /// moves no payload byte and cannot change the outcome of a job whose
    /// recovery set already covers the damage. Counting it here made the
    /// completion checkpoint — and PAR2 recovery promotion behind it — wait out
    /// a probe that had nothing left to say, on every job that lost an article
    /// early.
    pub(crate) fn job_has_pending_download_pipeline_work(&self, job_id: JobId) -> bool {
        self.job_has_pending_download_work_beyond_health_probe(job_id)
    }

    /// The same question, named for the one caller that has to be explicit
    /// about excluding the probe:
    /// [`Self::retire_health_probe_if_download_pipeline_drained`] asks whether
    /// every segment has reached a terminal state, which is the moment the
    /// probe's estimate is moot.
    pub(crate) fn job_has_pending_download_work_beyond_health_probe(&self, job_id: JobId) -> bool {
        let has_queued_work = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| !state.download_queue.is_empty());
        let has_inflight_downloads = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_inflight_decodes = self
            .active_decodes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_delayed_retries = self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_released_download_results = self
            .pending_released_download_results_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id.job_id == job_id);
        let has_buffered_segments = self
            .write_buffers
            .iter()
            .any(|(file_id, write_buf)| file_id.job_id == job_id && write_buf.buffered_len() > 0);
        let has_file_crc_recovery = self
            .file_crc_recoveries
            .keys()
            .any(|file_id| file_id.job_id == job_id);

        has_queued_work
            || has_inflight_downloads
            || has_inflight_decodes
            || has_delayed_retries
            || has_released_download_results
            || has_pending_decode
            || has_buffered_segments
            || has_file_crc_recovery
    }

    fn promoted_recovery_pipeline_state(&self, job_id: JobId) -> PromotedRecoveryPipelineState {
        let promoted_files: HashSet<u32> = self
            .par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .files
                    .iter()
                    .filter_map(|(&file_index, file)| file.promoted.then_some(file_index))
                    .collect()
            })
            .unwrap_or_default();
        let (
            download_queue_len,
            download_queue_has_recovery,
            download_queue_promoted_recovery,
            recovery_queue_len,
            parked_promoted_recovery,
        ) = self
            .jobs
            .get(&job_id)
            .map(|state| {
                (
                    state.download_queue.len(),
                    state.download_queue.has_recovery_work(),
                    state.download_queue.count_matching(|work| {
                        promoted_files.contains(&work.segment_id.file_id.file_index)
                    }),
                    state.recovery_queue.len(),
                    state.recovery_queue.count_matching(|work| {
                        promoted_files.contains(&work.segment_id.file_id.file_index)
                    }),
                )
            })
            .unwrap_or((0, false, 0, 0, 0));
        let incomplete_promoted_par2_files = self
            .jobs
            .get(&job_id)
            .map(|state| {
                promoted_files
                    .iter()
                    .filter(|file_index| {
                        state
                            .assembly
                            .file(NzbFileId {
                                job_id,
                                file_index: **file_index,
                            })
                            .is_none_or(|file| !file.is_complete())
                    })
                    .count()
            })
            .unwrap_or(0);
        let pending_promoted_decode = self
            .pending_decode
            .iter()
            .filter(|work| {
                work.segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&work.segment_id.file_id.file_index)
            })
            .count();
        let active_promoted_downloads = self
            .active_downloads_by_file
            .iter()
            .filter(|(file_id, _)| {
                file_id.job_id == job_id && promoted_files.contains(&file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let pending_promoted_retries = self
            .pending_retries_by_segment
            .iter()
            .filter(|(segment_id, _)| {
                segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&segment_id.file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let active_promoted_decodes = self
            .active_decodes_by_file
            .iter()
            .filter(|(file_id, _)| {
                file_id.job_id == job_id && promoted_files.contains(&file_id.file_index)
            })
            .map(|(_, count)| *count)
            .sum();
        let write_buffered_promoted_recovery = self
            .write_buffers
            .iter()
            .filter(|(file_id, buffer)| {
                file_id.job_id == job_id
                    && promoted_files.contains(&file_id.file_index)
                    && buffer.buffered_len() > 0
            })
            .count();
        let unavailable_promoted_recovery_segments = self
            .unavailable_promoted_recovery_segments
            .iter()
            .filter(|segment_id| {
                segment_id.file_id.job_id == job_id
                    && promoted_files.contains(&segment_id.file_id.file_index)
            })
            .count();

        PromotedRecoveryPipelineState {
            download_queue_len,
            download_queue_has_recovery,
            download_queue_promoted_recovery,
            recovery_queue_len,
            parked_promoted_recovery,
            promoted_par2_files: promoted_files.len(),
            incomplete_promoted_par2_files,
            active_promoted_downloads,
            pending_promoted_retries,
            pending_promoted_decode,
            active_promoted_decodes,
            write_buffered_promoted_recovery,
            unavailable_promoted_recovery_segments,
        }
    }

    /// Whether a wave of promoted PAR2 recovery is still moving through the
    /// pipeline for this job.
    ///
    /// `pub(crate)` because the direct-store seam asks the same question before
    /// it decides whether a damaged set should wait for recovery or demote, and
    /// "recovery is still coming" must mean exactly one thing across the two —
    /// a second, near-identical predicate is how one of them ends up waiting for
    /// work the other has already given up on.
    pub(crate) fn job_has_promoted_recovery_pipeline_work(
        &self,
        job_id: JobId,
        action: &'static str,
    ) -> bool {
        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
        if promoted_recovery.has_pending_work() {
            debug!(
                job_id = job_id.0,
                action,
                queued_downloads = promoted_recovery.download_queue_len,
                download_queue_has_recovery = promoted_recovery.download_queue_has_recovery,
                queued_promoted_recovery = promoted_recovery.download_queue_promoted_recovery,
                parked_recovery = promoted_recovery.recovery_queue_len,
                parked_promoted_recovery = promoted_recovery.parked_promoted_recovery,
                promoted_par2_files = promoted_recovery.promoted_par2_files,
                incomplete_promoted_par2_files = promoted_recovery.incomplete_promoted_par2_files,
                active_promoted_downloads = promoted_recovery.active_promoted_downloads,
                pending_promoted_retries = promoted_recovery.pending_promoted_retries,
                pending_promoted_decode = promoted_recovery.pending_promoted_decode,
                active_promoted_decodes = promoted_recovery.active_promoted_decodes,
                write_buffered_promoted_recovery =
                    promoted_recovery.write_buffered_promoted_recovery,
                unavailable_promoted_recovery_segments =
                    promoted_recovery.unavailable_promoted_recovery_segments,
                "deferring completion work — promoted PAR2 recovery work is pending"
            );
            return true;
        }

        false
    }
}

impl Pipeline {
    fn clean_par2_integrity_gate(&self, job_id: JobId) -> CleanPar2IntegrityGate {
        let Some(state) = self.jobs.get(&job_id) else {
            return CleanPar2IntegrityGate::None;
        };

        let mut gate = CleanPar2IntegrityGate::None;
        for topology in state.assembly.archive_topologies().values() {
            let topology_gate = match topology.archive_type {
                crate::jobs::assembly::ArchiveType::Split
                | crate::jobs::assembly::ArchiveType::Tar => CleanPar2IntegrityGate::WeakTransform,
                crate::jobs::assembly::ArchiveType::SevenZip => {
                    if topology.volume_map.len() <= 1 {
                        CleanPar2IntegrityGate::WeakTransform
                    } else {
                        CleanPar2IntegrityGate::StrongDecode
                    }
                }
                crate::jobs::assembly::ArchiveType::Rar
                | crate::jobs::assembly::ArchiveType::Zip
                | crate::jobs::assembly::ArchiveType::TarGz
                | crate::jobs::assembly::ArchiveType::TarBz2
                | crate::jobs::assembly::ArchiveType::TarXz
                | crate::jobs::assembly::ArchiveType::Gz
                | crate::jobs::assembly::ArchiveType::Deflate
                | crate::jobs::assembly::ArchiveType::Brotli
                | crate::jobs::assembly::ArchiveType::Zstd
                | crate::jobs::assembly::ArchiveType::Bzip2
                | crate::jobs::assembly::ArchiveType::Xz => CleanPar2IntegrityGate::StrongDecode,
            };
            gate = Self::fold_integrity_gate(gate, topology_gate);
        }

        Self::fold_integrity_gate(gate, self.direct_rar_integrity_gate(job_id))
    }

    /// The operator's PAR2 repair workspace ceiling, for the one repair seam
    /// that does not go through `Par2Repairer`.
    ///
    /// Direct-store's repair-while-direct drives `plan_repair`/`execute_repair`
    /// itself, because the repairer is filesystem-bound and a virtual volume has
    /// no file to give it. Sharing the knob is the point: the limit has to mean
    /// the same thing whichever seam spends it, and a method on `Pipeline` is
    /// how that crosses the module boundary without re-exporting the parser.
    pub(in crate::pipeline) fn par2_repair_memory_limit_bytes(&self) -> usize {
        configured_par2_repair_memory_limit_bytes()
    }

    /// Job-wide fold. The **strongest** contribution wins, which is why a
    /// contribution has to be earned per archive rather than assumed per job: a
    /// `StrongDecode` from one archive suppresses the authoritative pass for
    /// every other archive in the job as well.
    fn fold_integrity_gate(
        gate: CleanPar2IntegrityGate,
        contribution: CleanPar2IntegrityGate,
    ) -> CleanPar2IntegrityGate {
        match (gate, contribution) {
            (CleanPar2IntegrityGate::StrongDecode, _)
            | (_, CleanPar2IntegrityGate::StrongDecode) => CleanPar2IntegrityGate::StrongDecode,
            (CleanPar2IntegrityGate::WeakTransform, _)
            | (_, CleanPar2IntegrityGate::WeakTransform) => CleanPar2IntegrityGate::WeakTransform,
            _ => CleanPar2IntegrityGate::None,
        }
    }

    /// What a job's **direct** RAR sets contribute to the clean-PAR2 integrity
    /// gate.
    ///
    /// A direct set never enters the archive topology by construction, so the
    /// loop above cannot see it and a job made entirely of direct sets reads
    /// `None` — which forces the authoritative pass, whose repair branch
    /// materializes every still-routing set before handing the repairer files it
    /// can write into. While the job is downloading that costs little, because
    /// the dual-CRC grid claims blocks off the articles as they arrive. **After
    /// a restart no article arrives at all**: a byte-complete set feeds the grid
    /// nothing, so it can claim nothing, and a set that is byte-perfect on disk
    /// is materialized and redownloaded anyway.
    ///
    /// A **restored** RAR set has the same claim to `StrongDecode` a
    /// conventionally extracted one does, and for a stronger reason than
    /// routing alone: its checkpointed bytes were re-read off disk this run and
    /// re-composed through the member CRC32s by the gate re-arm, and its
    /// refetched bytes went through the router's gate on the way in. Three
    /// predicates make that claim honest, and each one is load-bearing:
    ///
    /// - **The job is only RAR archives.** The fold above is job-wide and the
    ///   strongest contribution wins, so a direct RAR set in a job that also
    ///   holds a conventional Split or Tar would suppress the authoritative pass
    ///   for the *split archive* — an archive whose integrity nothing in this job
    ///   has checked. A mixed job contributes nothing here.
    /// - **Every set was restored, and none is demoted.** A set this run
    ///   downloaded is deliberately left alone: its articles fed the dual-CRC
    ///   grid, which adjudicates blocks in the *volume* space that no member
    ///   checksum can see — a corrupted recovery record, say — and that
    ///   detection must not be traded away. Freshly downloaded sets keep
    ///   reaching the authoritative pass exactly as before; this is only about
    ///   the sets the grid could never have seen, because not one article of
    ///   them arrived.
    /// - **No set is carrying restart-seeded coverage.** Bytes restored from a
    ///   checkpoint are covered and *unverified* until the re-arm re-reads
    ///   them; a set still holding them has decoded nothing and may not claim
    ///   decode strength. The re-arm runs at the download/verify boundary, so
    ///   by the time this matters a healthy set has cleared it — and one that
    ///   could not has demoted.
    ///
    /// # What this does and does not vouch for
    ///
    /// The member CRC32s vouch for the **member payloads** — the bytes that
    /// become output — and not for the volume-space bytes PAR2 describes. A
    /// volume's envelope regions (headers, a recovery record) are covered by no
    /// member checksum. That is the same trade the conventional RAR path makes
    /// when it contributes `StrongDecode`: there too the guarantee is that the
    /// extractor's own per-file CRC32 passed, not that every byte of every volume
    /// file matched its PAR2 description.
    fn direct_rar_integrity_gate(&self, job_id: JobId) -> CleanPar2IntegrityGate {
        let sets = self.direct_store.sets_for(job_id);
        if sets.is_empty() {
            return CleanPar2IntegrityGate::None;
        }
        if !self.job_has_only_rar_archives(job_id) {
            return CleanPar2IntegrityGate::None;
        }
        if !sets.iter().all(|set| {
            set.was_restored() && !set.is_demoted() && !set.has_restart_seeded_coverage()
        }) {
            return CleanPar2IntegrityGate::None;
        }
        CleanPar2IntegrityGate::StrongDecode
    }

    /// [`Self::direct_rar_integrity_gate`] as a bool, so the predicate can be
    /// pinned on its own.
    ///
    /// It is worth a direct test rather than only an end-to-end one: the
    /// contribution is job-wide and the strongest wins, so the interesting case —
    /// a mixed job that must contribute *nothing* — is a **negative**, and a
    /// negative asserted through a whole job gate passes just as well when the
    /// gate never got that far.
    #[cfg(test)]
    pub(crate) fn direct_rar_contributes_strong_decode(&self, job_id: JobId) -> bool {
        matches!(
            self.direct_rar_integrity_gate(job_id),
            CleanPar2IntegrityGate::StrongDecode
        )
    }

    async fn load_existing_complete_file_hashes(
        &self,
        job_id: JobId,
    ) -> Result<HashMap<u32, [u8; 16]>, String> {
        self.db_blocking(move |db| db.load_complete_file_hashes(job_id))
            .await
            .map_err(|error| format!("failed to load completed-file hashes: {error}"))
    }

    fn par2_filesystem_placement_overrides(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        working_dir: &Path,
    ) -> HashMap<par2_rs::FileId, String> {
        let file_ids: Vec<NzbFileId> = self
            .jobs
            .get(&job_id)
            .map(|state| state.assembly.files().map(|file| file.file_id()).collect())
            .unwrap_or_default();
        file_ids
            .into_iter()
            .filter_map(|file_id| {
                let binding = self.resolve_par2_file_binding_in_set(file_id, set_id)?;
                let relative = binding.path.strip_prefix(working_dir).ok()?;
                Some((
                    binding.par2_file_id,
                    relative.to_string_lossy().into_owned(),
                ))
            })
            .collect()
    }

    async fn par2_session_evidence_candidates(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Result<Vec<Par2SessionEvidenceCandidate>, String> {
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let Some(runtime) = self.par2_runtime(job_id) else {
            return Ok(Vec::new());
        };
        let completed_checksums = runtime.completed_checksums.clone();
        let already_seeded = runtime
            .set_runtime(set_id)
            .map(|set_runtime| set_runtime.session_evidence_file_ids.clone())
            .unwrap_or_default();
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(Vec::new());
        };

        let mut candidates = Vec::new();
        for file in state.assembly.files() {
            let file_id = file.file_id();
            if !file.is_complete() || already_seeded.contains(&file_id) {
                continue;
            }

            let current_filename = self
                .effective_file_identity(job_id, file_id)
                .map(|identity| identity.current_filename)
                .unwrap_or_else(|| file.filename().to_string());
            // Decoded length: what the commits accumulated and what PAR2 and
            // the on-disk file measure. The NZB-declared `total_bytes()` is
            // yEnc-encoded and must never reach a description or metadata
            // comparison.
            let expected_length = file.received_bytes();
            let checksum = completed_checksums.get(&file_id).copied();
            // Full-MD5 evidence comes from the CURRENT file generation. A
            // runtime checksum entry, when one exists, always speaks for the
            // file: `Some(md5)` is the current digest, and `None` means the
            // current generation has none (a CRC-metadata completion, or the
            // md5-less sentinel a failed finalize records after a duplicate
            // rewrite) — the persisted row may then be a generation behind
            // and must NOT be revived to stand in for it. The database is
            // consulted only when the runtime has no entry at all: the
            // restart shape, where the provenance-filtered row
            // (`load_complete_file_hashes`) is the only generation there is.
            let full_md5 = match checksum {
                Some(current) => current.md5,
                None => completed_hashes.get(&file_id.file_index).copied(),
            };
            // A file any in-stream IFSC verdict already proved Damaged must
            // not seed the session with completion evidence of any kind: the
            // authoritative pass owns it. Unclaimed or NoReference blocks are
            // not damage — they simply leave this file to the settle/read
            // paths that always covered them.
            if self.block_crc_verdicts(file_id).is_some_and(|verdicts| {
                verdicts.values().any(|verdict| {
                    matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)
                })
            }) {
                crate::runtime::perf_probe::record(
                    "completion.par2_evidence.rejected.damaged_in_stream_verdict",
                    std::time::Duration::from_nanos(1),
                );
                continue;
            }
            let bound_candidates = par2_set
                .recovery_file_ids
                .iter()
                .chain(par2_set.non_recovery_file_ids.iter())
                .filter_map(|par2_file_id| par2_set.file_description(par2_file_id))
                .filter(|description| {
                    description.length == expected_length
                        && match full_md5 {
                            Some(md5) => description.hash_full == md5,
                            None => {
                                sanitize_download_filename(&description.filename)
                                    == current_filename
                            }
                        }
                })
                .map(|description| description.file_id)
                .collect::<Vec<_>>();
            let bound_file_id = (bound_candidates.len() == 1).then(|| bound_candidates[0]);
            candidates.push(Par2SessionEvidenceCandidate {
                file_id,
                path: state.working_dir.join(&current_filename),
                logical_name: current_filename,
                expected_length,
                full_md5,
                crc32: checksum.map_or(0, |checksum| checksum.crc32),
                contiguous_assembly_proven: checksum.is_some_and(|checksum| {
                    checksum.all_parts_crc_verified
                        && !file.has_duplicate_segments()
                        && file.contiguous_placements_proven()
                }),
                bound_file_id,
            });
        }
        Ok(candidates)
    }

    /// The digest to persist for a file an actual PAR2 verification pass just
    /// ruled intact: the trusted digest we already hold, if any. `None` — not
    /// a zero sentinel — when there is none: a row completed without a digest
    /// is honest, while a fabricated all-zero digest stamped with trusted
    /// provenance would be one more expectation dressed as an observation.
    fn expected_hash_for_verified_file(
        file_id: NzbFileId,
        existing_hashes: &HashMap<u32, [u8; 16]>,
    ) -> Option<[u8; 16]> {
        existing_hashes.get(&file_id.file_index).copied()
    }
}

mod completion;
mod finish;
mod repair;
mod verify;

#[cfg(test)]
mod tests;
