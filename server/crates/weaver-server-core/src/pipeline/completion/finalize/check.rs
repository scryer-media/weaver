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

type RetainedPar2SessionOutcome = (par2_rs::Par2RepairOutcome, Vec<NzbFileId>, bool);

#[derive(Debug)]
struct RetainedPar2SessionFailure {
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

type RetainedPar2SessionResult = Result<RetainedPar2SessionOutcome, RetainedPar2SessionFailure>;

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
    let combine = par2_rs::checksum::Crc32CombineOp::new(slice_size);
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
    par2_rs::checksum::Crc32CombineOp::new(padding).combine(
        measured_crc32,
        crate::pipeline::integrity::crc32_of_zeros(padding),
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
                    correct_name: file.filename.clone(),
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

    pub(crate) fn job_has_pending_download_pipeline_work(&self, job_id: JobId) -> bool {
        let has_queued_work = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.health_probing || !state.download_queue.is_empty());
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

    pub(in crate::pipeline) async fn try_deobfuscate_files_with_par2(
        &mut self,
        job_id: JobId,
    ) -> Par2DeobfuscationOutcome {
        let Some(state) = self.jobs.get(&job_id) else {
            return Par2DeobfuscationOutcome::default();
        };
        let rename_dir = state.working_dir.clone();

        if weaver_nzb::is_protected_media_structure(&rename_dir) {
            info!(
                job_id = job_id.0,
                "skipping PAR2 rename inside protected media structure"
            );
            return Par2DeobfuscationOutcome::default();
        }

        let mut suggestions = Vec::new();
        let mut seen_current_paths = HashSet::new();
        let mut seen_hashes = HashSet::<[u8; 16]>::new();
        let mut ambiguous_hashes = HashSet::<[u8; 16]>::new();
        let mut descriptions_by_name =
            HashMap::<String, Vec<(par2_rs::RecoverySetId, par2_rs::FileId, u64, [u8; 16])>>::new();
        let set_ids = self
            .par2_runtime(job_id)
            .map(crate::pipeline::Par2RuntimeState::ordered_set_ids)
            .unwrap_or_default();
        for set_id in set_ids {
            let Some(par2) = self.par2_set_for(job_id, set_id) else {
                continue;
            };
            for description in par2.files.values() {
                if !seen_hashes.insert(description.hash_16k) {
                    ambiguous_hashes.insert(description.hash_16k);
                }
                descriptions_by_name
                    .entry(sanitize_download_filename(&description.filename))
                    .or_default()
                    .push((
                        set_id,
                        description.file_id,
                        description.length,
                        description.hash_16k,
                    ));
            }

            let set_suggestions = match par2_rs::scan_for_renames(&rename_dir, par2) {
                Ok(suggestions) => suggestions,
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        recovery_set_id = %set_id,
                        error = %error,
                        "PAR2 rename scan failed"
                    );
                    continue;
                }
            };
            for suggestion in set_suggestions {
                if seen_current_paths.insert(suggestion.current_path.clone()) {
                    suggestions.push((set_id, suggestion));
                }
            }
        }

        let file_rows: Vec<(NzbFileId, crate::jobs::record::ActiveFileIdentity, bool)> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .map(|identity| (file.file_id(), identity, file.is_complete()))
            })
            .collect();
        let mut by_current = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_source = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_canonical = HashMap::<String, (NzbFileId, bool)>::new();
        for (file_id, identity, is_complete) in &file_rows {
            by_current.insert(identity.current_filename.clone(), (*file_id, *is_complete));
            by_source.insert(identity.source_filename.clone(), (*file_id, *is_complete));
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                by_canonical.insert(canonical.clone(), (*file_id, *is_complete));
            }
        }
        let mut occupied_filenames = HashSet::<String>::new();
        for (_, identity, _) in &file_rows {
            reserve_identity_filenames(identity, &mut occupied_filenames);
        }
        reserve_directory_filenames(&state.working_dir, &mut occupied_filenames);
        let _ = state;

        let mut outcome = Par2DeobfuscationOutcome::default();
        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        for (set_id, suggestion) in &suggestions {
            let old = &suggestion.current_path;
            let requested_correct_name = sanitize_download_filename(&suggestion.correct_name);
            let old_name = old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            let matched = by_current
                .get(&old_name)
                .copied()
                .or_else(|| by_source.get(&old_name).copied())
                .or_else(|| by_canonical.get(&old_name).copied());
            let Some(description) = self
                .par2_set_for(job_id, *set_id)
                .and_then(|set| set.file_description(&suggestion.file_id))
            else {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename with an unknown description"
                );
                continue;
            };
            if ambiguous_hashes.contains(&description.hash_16k) {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename with an ambiguous 16 KiB hash"
                );
                continue;
            }
            if descriptions_by_name
                .get(&requested_correct_name)
                .is_some_and(|descriptions| {
                    descriptions
                        .iter()
                        .any(|(described_set_id, _, length, hash_16k)| {
                            *described_set_id != *set_id
                                && (*length != description.length
                                    || *hash_16k != description.hash_16k)
                        })
                })
            {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %requested_correct_name,
                    "refusing PAR2 rename with conflicting recovery-set descriptions"
                );
                continue;
            }
            if crate::pipeline::is_split_fragment_of(&old_name, &suggestion.correct_name) {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %suggestion.correct_name,
                    "refusing PAR2 rename of a split fragment"
                );
                continue;
            }
            let disk_len = match std::fs::metadata(old) {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    debug!(
                        job_id = job_id.0,
                        from = %old.display(),
                        error = %error,
                        "refusing PAR2 rename whose source length could not be read"
                    );
                    continue;
                }
            };
            let length_contradicts = match matched {
                Some((_, true)) | None => disk_len != description.length,
                Some((_, false)) => disk_len > description.length,
            };
            if length_contradicts {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    observed_length = disk_len,
                    described_length = description.length,
                    "refusing PAR2 rename with a contradictory file length"
                );
                continue;
            }
            let mut target_occupied = occupied_filenames.clone();
            if let Some((file_id, _)) = matched
                && let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
            {
                forget_identity_filenames(identity, &mut target_occupied);
            }
            let correct_name =
                allocate_unique_download_filename(&requested_correct_name, &mut target_occupied);
            // A suggestion whose source is not an NZB entry is a stray on disk
            // — most often the pre-repair backup par2-rs leaves behind, whose
            // first 16 KiB still match the description because the damage the
            // repair fixed lay further in. When the canonical name is already
            // taken, renaming it here mints a `.duplicateN` sibling of the file
            // that just passed verification, and the final move — which
            // relocates the whole working directory — delivers both. That is
            // how a 962 MB damaged copy shipped beside its repaired original.
            //
            // Renaming an unmatched stray into a *free* canonical slot is still
            // allowed, so genuine deobfuscation is untouched; only the
            // duplicate-minting branch is closed.
            if matched.is_none() && correct_name != requested_correct_name {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    requested = %requested_correct_name,
                    "refusing to rename a file the NZB never declared into a duplicate name"
                );
                continue;
            }
            if old.strip_prefix(&rename_dir).is_err() {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename whose source escapes the job directory"
                );
                continue;
            }
            let new = old.parent().unwrap().join(&correct_name);
            if new.strip_prefix(&rename_dir).is_err() {
                warn!(
                    job_id = job_id.0,
                    to = %new.display(),
                    "refusing PAR2 rename whose target escapes the job directory"
                );
                continue;
            }
            if old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                == Some(correct_name.clone())
            {
                continue;
            }

            if new.exists() && !runtime_fs::paths_equivalent_for_placement(old, &new) {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %new.display(),
                    "PAR2 rename target already exists"
                );
                continue;
            }

            let renamed_successfully = match runtime_fs::rename_no_overwrite(old, &new) {
                Ok(()) => {
                    outcome.renamed += 1;
                    if correct_name == requested_correct_name
                        && let Some(descriptions) =
                            descriptions_by_name.get(&requested_correct_name)
                    {
                        for (set_id, file_id, _, _) in descriptions {
                            outcome
                                .canonical_description_file_ids
                                .entry(*set_id)
                                .or_default()
                                .insert(*file_id);
                        }
                    }
                    reserve_download_filename(&correct_name, &mut occupied_filenames);
                    info!(
                        job_id = job_id.0,
                        from = %old.file_name().unwrap().to_string_lossy(),
                        to = %correct_name,
                        "deobfuscated file via PAR2 metadata"
                    );
                    true
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        from = %old.display(),
                        to = %new.display(),
                        error = %error,
                        "PAR2 rename failed"
                    );
                    false
                }
            };
            if !renamed_successfully {
                continue;
            }

            if let Some((file_id, is_complete)) = matched {
                let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
                    .cloned()
                else {
                    continue;
                };
                let old_current_filename = identity.current_filename.clone();
                let old_rar_set_name =
                    identity.classification.as_ref().and_then(|classification| {
                        matches!(
                            classification.kind,
                            crate::jobs::assembly::DetectedArchiveKind::Rar
                        )
                        .then(|| classification.set_name.clone())
                    });
                let classification = Self::canonical_archive_identity_from_filename(&correct_name)
                    .or(identity.classification.clone());
                let new_rar_set_name = classification.as_ref().and_then(|classification| {
                    matches!(
                        classification.kind,
                        crate::jobs::assembly::DetectedArchiveKind::Rar
                    )
                    .then(|| classification.set_name.clone())
                });
                for set_name in [old_rar_set_name, new_rar_set_name].into_iter().flatten() {
                    touched_rar_files
                        .entry(set_name)
                        .or_default()
                        .insert(old_current_filename.clone());
                }
                let mut rebound_identity = identity;
                rebound_identity.current_filename = correct_name.clone();
                rebound_identity.canonical_filename = Some(correct_name.clone());
                rebound_identity.classification = classification;
                rebound_identity.classification_source =
                    crate::jobs::record::FileIdentitySource::Par2;
                if let Err(error) = self.set_file_identity(job_id, rebound_identity) {
                    warn!(
                        job_id = job_id.0,
                        file_index = file_id.file_index,
                        error = %error,
                        "failed to persist PAR2 deobfuscation identity"
                    );
                } else if is_complete {
                    touched_files.push(file_id);
                }
            }
        }

        for (set_name, touched_filenames) in &touched_rar_files {
            self.invalidate_archive_set_for_identity_rebind(job_id, set_name, touched_filenames);
        }
        for file_id in touched_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        if outcome.renamed > 0 {
            // Identity changed, not the bytes owned by this NzbFileId. The
            // binding resolver revalidates names live; raw grid evidence stays
            // available for every recovery set.
            info!(
                job_id = job_id.0,
                renamed = outcome.renamed,
                "PAR2 deobfuscation complete"
            );
        }

        outcome
    }

    async fn run_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        repair: bool,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
        let set_id = par2_set.recovery_set_id;
        if repair {
            // What the directory held before the repairer touched it, so the
            // artefacts it leaves behind can be named afterwards by difference
            // rather than by guessing at a backup-suffix convention that lives
            // in another crate.
            //
            // The FIRST such state is the baseline for the whole job, not the
            // most recent one. A second set's repair runs with the first set's
            // backups already on disk, and re-snapshotting here would enrol
            // them as though they had always been there — which is precisely
            // how a damaged original survives into the delivered output.
            self.par2_pre_repair_dir_entries
                .entry(job_id)
                .or_insert_with(|| directory_entry_names(&working_dir));
        }

        #[cfg(test)]
        {
            if repair {
                self.par2_repairer_execute_calls += 1;
            } else {
                self.par2_repairer_analyze_calls += 1;
            }
        }

        // Repair retires the live grid below, because its verdicts describe
        // the pre-repair file generation. Keep one local snapshot solely for
        // the descriptor-bounded retry: that retry validates every source
        // slice as it reads it and never returns the evidence to runtime state.
        let repair_slice_evidence =
            repair.then(|| self.in_stream_slice_evidence_paths_for_set(job_id, set_id));
        let repair_placement_overrides =
            repair.then(|| self.par2_filesystem_placement_overrides(job_id, set_id, &working_dir));

        if repair {
            // Retire only files the repairing set can write. Other parsed
            // sets may still have byte-exact evidence for their own files.
            let files: Vec<_> = self
                .jobs
                .get(&job_id)
                .map(|state| state.assembly.files().map(|file| file.file_id()).collect())
                .unwrap_or_default();
            for file_id in files {
                if self
                    .resolve_par2_file_binding(file_id)
                    .is_some_and(|binding| binding.recovery_set_id == set_id)
                {
                    self.block_crcs.forget_file(file_id);
                }
            }
        }

        let memory_limit = configured_par2_repair_memory_limit_bytes();
        let phase_counters = repair.then(|| self.phase_begin(job_id, JobPhase::Repairing, None));
        let session_progress = phase_counters.as_ref().map(|counters| {
            let counters = Arc::clone(counters);
            Arc::new(move |update: par2_rs::ProgressUpdate| {
                if !matches!(
                    update.stage,
                    par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
                ) {
                    return;
                }
                counters
                    .completed_bytes
                    .fetch_max(update.bytes_processed, Ordering::Relaxed);
                if let Some(total_bytes) = update.total_bytes {
                    counters
                        .total_bytes
                        .fetch_max(total_bytes, Ordering::Relaxed);
                }
            }) as par2_rs::ProgressCallback
        });

        let retained_session = match self
            .take_or_open_par2_repair_session(
                job_id,
                set_id,
                working_dir.clone(),
                memory_limit,
                session_progress.clone(),
                // By this point the repairer reads and writes real files: any
                // set still routing here materialized before it arrived.
                None,
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable; using one-shot repairer");
                None
            }
        };
        if let Some((session, newly_opened)) = retained_session {
            if newly_opened {
                self.ensure_par2_runtime(job_id)
                    .set_runtime_mut(set_id)
                    .expect("PAR2 session evidence belongs to the active recovery set")
                    .session_evidence_file_ids
                    .clear();
            }
            let candidates = match self
                .par2_session_evidence_candidates(job_id, set_id, &par2_set)
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    self.restore_par2_repair_session(job_id, set_id, session);
                    if repair {
                        self.phase_end(job_id, JobPhase::Repairing);
                    }
                    return Err(error);
                }
            };
            // Analysis reads the live grid here. Repair uses the local
            // pre-retirement snapshot captured above; it never puts those
            // verdicts back after the file generation changes.
            let slice_evidence = repair_slice_evidence
                .unwrap_or_else(|| self.in_stream_slice_evidence_paths_for_set(job_id, set_id));
            let bounded_repair = repair.then(|| {
                let evidence = slice_evidence
                    .iter()
                    .flat_map(|(_, evidence)| evidence.iter().copied())
                    .collect::<Vec<_>>();
                (repair_placement_overrides.unwrap_or_default(), evidence)
            });
            let mut repair_task = tokio::task::spawn_blocking(move || {
                if repair {
                    crate::e2e_failpoint::maybe_delay("repair.task_start");
                }
                run_retained_par2_session(session, candidates, slice_evidence, repair)
            });
            let repair_result = if repair {
                loop {
                    tokio::select! {
                        result = &mut repair_task => break result,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                            self.sample_phase_progress();
                        }
                    }
                }
            } else {
                repair_task.await
            };
            let retained_outcome = match repair_result {
                Ok((session, Ok((outcome, admitted_file_ids, retried_source_change)))) => {
                    self.restore_par2_repair_session(job_id, set_id, session);
                    let set_runtime = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(set_id)
                        .expect("PAR2 session evidence belongs to the active recovery set");
                    if repair || retried_source_change {
                        set_runtime.session_evidence_file_ids.clear();
                        if repair && let Some(session) = set_runtime.session.as_mut() {
                            session.invalidate_all_sources();
                        }
                    } else {
                        set_runtime
                            .session_evidence_file_ids
                            .extend(admitted_file_ids);
                    }
                    ensure_par2_repair_completed(&outcome, repair).map(|()| outcome)
                }
                Ok((session, Err(error))) if repair && error.file_descriptor_exhausted => {
                    let assessment = session
                        .assessment()
                        .ok()
                        .map(|outcome| outcome.verification.clone());
                    // The failed constructor drops every handle it opened.
                    // Do not retain this path-backed session: the retry reads
                    // through PlacementFileAccess and its assessment belongs
                    // to a different source kind.
                    drop(session);
                    let set_runtime = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(set_id)
                        .expect("PAR2 fallback belongs to the active recovery set");
                    set_runtime.session = None;
                    set_runtime.session_last_used = None;
                    set_runtime.session_evidence_file_ids.clear();

                    let (placement_overrides, evidence) = bounded_repair
                        .expect("a repairing retained session has bounded retry inputs");
                    if !assessment.as_ref().is_some_and(|verification| {
                        bounded_repair_evidence_covers_assessment(verification, &evidence)
                    }) {
                        warn!(
                            job_id = job_id.0,
                            error = %error.message,
                            evidence_slices = evidence.len(),
                            "filesystem PAR2 repair exhausted file descriptors; the bounded retry lacks a complete source map"
                        );
                        self.phase_end(job_id, JobPhase::Repairing);
                        return Err(error.message);
                    }
                    warn!(
                        job_id = job_id.0,
                        error = %error.message,
                        evidence_slices = evidence.len(),
                        "filesystem PAR2 repair exhausted file descriptors; retrying with bounded source access"
                    );
                    let cancellation = self.par2_cancellation_token(job_id);
                    let fallback_working_dir = working_dir.clone();
                    let fallback_set = (*par2_set).clone();
                    let fallback_progress = session_progress.clone();
                    let mut fallback_task = tokio::task::spawn_blocking(move || {
                        run_file_descriptor_bounded_par2_repair(
                            fallback_working_dir,
                            fallback_set,
                            placement_overrides,
                            evidence,
                            memory_limit,
                            cancellation,
                            fallback_progress,
                        )
                    });
                    let fallback_result = loop {
                        tokio::select! {
                            result = &mut fallback_task => break result,
                            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                                self.sample_phase_progress();
                            }
                        }
                    };
                    match fallback_result {
                        Ok(result) => result,
                        Err(error) => Err(format!(
                            "bounded filesystem PAR2 fallback task panicked: {error}"
                        )),
                    }
                }
                Ok((session, Err(error))) => {
                    self.restore_par2_repair_session(job_id, set_id, session);
                    Err(error.message)
                }
                Err(error) => Err(format!("retained PAR2 session task panicked: {error}")),
            };
            if repair {
                self.phase_end(job_id, JobPhase::Repairing);
            }
            return retained_outcome;
        }

        let cancellation = self.par2_cancellation_token(job_id);
        // The carry the previous pass over this set left behind — a repairer
        // analysis, a repair, or this module's own authoritative verification.
        // Seeding it is what keeps analysis → repair (and host verify →
        // analysis) from re-reading bytes the earlier pass already hashed;
        // par2-rs stat-gates the carry and re-checks bytes before mutating,
        // so a stale one degrades to the full scan this call always did.
        let carry_set_id = par2_set.recovery_set_id;
        let seeded_scan_carry = self
            .ensure_par2_runtime(job_id)
            .set_runtime_mut(carry_set_id)
            .and_then(|set_runtime| set_runtime.scan_carry.clone());
        #[cfg(test)]
        if seeded_scan_carry.is_some() {
            self.par2_scan_carry_seeded_calls += 1;
        }
        let mut repair_task = tokio::task::spawn_blocking(move || {
            if repair {
                crate::e2e_failpoint::maybe_delay("repair.task_start");
            }
            let mut options = par2_rs::Par2RepairerOptions::new(working_dir, Vec::new());
            options.file_set = Some((*par2_set).clone());
            options.repair = repair;
            options.memory_limit = Some(memory_limit);
            options.cancel = Some(cancellation);
            options.scan_carry = seeded_scan_carry;
            if let Some(counters) = phase_counters {
                options.progress = Some(Arc::new(move |update: par2_rs::ProgressUpdate| {
                    if !matches!(
                        update.stage,
                        par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
                    ) {
                        return;
                    }
                    counters
                        .completed_bytes
                        .fetch_max(update.bytes_processed, Ordering::Relaxed);
                    if let Some(total_bytes) = update.total_bytes {
                        counters
                            .total_bytes
                            .fetch_max(total_bytes, Ordering::Relaxed);
                    }
                }));
            }
            let repairer = par2_rs::Par2Repairer::new(options);
            let (outcome, scan_carry) = repairer
                .verify_or_repair_carrying()
                .map_err(|e| format!("PAR2 repairer failed: {e}"))?;
            ensure_par2_repair_completed(&outcome, repair)?;
            Ok((outcome, scan_carry))
        });
        let repair_result = if repair {
            loop {
                tokio::select! {
                    result = &mut repair_task => break result,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        self.sample_phase_progress();
                    }
                }
            }
        } else {
            repair_task.await
        };

        if repair {
            self.phase_end(job_id, JobPhase::Repairing);
        }

        match repair_result {
            Ok(Ok((outcome, scan_carry))) => {
                // Replaced unconditionally: a pass that produced no carry may
                // have moved bytes, which makes any older stash a lie about
                // the layout on disk.
                #[cfg(test)]
                if scan_carry.is_some() {
                    self.par2_scan_carry_stashed_calls += 1;
                }
                if let Some(set_runtime) = self
                    .ensure_par2_runtime(job_id)
                    .set_runtime_mut(carry_set_id)
                {
                    set_runtime.scan_carry = scan_carry;
                }
                Ok(outcome)
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("repair task panicked: {error}")),
        }
    }

    async fn analyze_par2_with_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
        if !preserve_repairing_status {
            self.transition_postprocessing_status(job_id, JobStatus::Verifying, Some("verifying"));
        } else {
            info!(
                job_id = job_id.0,
                "rerunning PAR2 analysis while preserving restored repair slot"
            );
        }
        self.emit_job_verification_started(job_id);
        let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
        });

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 damaged-path analysis started");

        let outcome_result = self
            .run_par2_repairer(job_id, par2_set, working_dir, false)
            .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        let mut outcome = outcome_result?;

        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, &mut outcome.verification);
        if skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks, "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks, "retained suspect eagerly-deleted volumes in damage count"
            );
        }
        // The same forgiveness the verify path applies. This
        // pass is reachable with a finalized direct set whenever `par2_verified`
        // was cleared underneath it — an extension asking for PAR re-entry does
        // exactly that — and a finalized set's source volumes are absent by
        // design.
        let forgiven_direct_blocks =
            self.forgive_finalized_direct_volumes(job_id, &mut outcome.verification);
        if forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks, "excluded finalized direct-store volumes from damage count"
            );
        }

        outcome.missing_blocks = outcome.verification.total_missing_blocks;
        self.recompute_volume_safety_from_verification(job_id, &outcome.verification);
        self.record_par2_set_verification_observation(job_id, &outcome.verification);
        let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
            job_id,
            passed: !par2_verification_needs_repair(&outcome.verification),
        });

        Ok(outcome)
    }

    async fn verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
        emit_events: bool,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
        if emit_events {
            if !preserve_repairing_status {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Verifying,
                    Some("verifying"),
                );
            } else {
                info!(
                    job_id = job_id.0,
                    "rerunning PAR2 verification while preserving restored repair slot"
                );
            }
            self.emit_job_verification_started(job_id);
            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
            });
        }

        let (mut verification, placement_plan) = self
            .run_par2_placement_pass(job_id, par2_set, working_dir, Par2PassScope::WholeSet)
            .await?;
        Self::log_placement_plan(job_id, &placement_plan);
        self.settle_par2_pass_result(job_id, &mut verification, emit_events);
        Ok((verification, placement_plan))
    }

    /// The post-repair authoritative pass, reading only the files the repair
    /// rewrote and standing in for the rest with the pre-repair pass's own
    /// entries.
    ///
    /// The pre-repair pass read every file in this set minutes ago, in this
    /// same flow, and the repair only ever writes the files that pass could not
    /// call complete ([`par2_repair_write_set`]). Re-reading and re-hashing the
    /// files it left alone answers a question that was already answered by
    /// reading the same bytes.
    pub(in crate::pipeline) async fn verify_repaired_par2_files_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        pre_repair: &par2_rs::VerificationResult,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
        let write_set = par2_repair_write_set(pre_repair);
        #[cfg(test)]
        {
            self.par2_post_repair_read_splits.push((
                pre_repair.files.len().saturating_sub(write_set.len()),
                write_set.len(),
            ));
        }
        info!(
            job_id = job_id.0,
            carried = pre_repair.files.len().saturating_sub(write_set.len()),
            rewritten = write_set.len(),
            "post-repair PAR2 verification reads only what the repair rewrote"
        );

        let (fresh, _) = self
            .run_par2_placement_pass(
                job_id,
                Arc::clone(&par2_set),
                working_dir,
                Par2PassScope::Selected(write_set),
            )
            .await?;

        // The carried entries are the pre-repair pass's, verbatim: same status,
        // same filename, same `valid_slices`. Everything downstream —
        // reconciliation, the authoritative digest refresh, placement — has to
        // see exactly what a full pass over unchanged bytes would have
        // reported, and the cheapest way to guarantee that is to hand it the
        // report that pass already made.
        //
        // The residual this accepts, stated rather than hidden: a file the
        // repair did not touch could be corrupted by something outside this job
        // in the minutes between the two passes, and this merge would not catch
        // it. That is the same window, and the same trust class, as the
        // in-stream claims the quick paths already stand a whole verification on
        // — evidence proven by reading the bytes, then relied on after the read.
        // It is unconditional for that reason: a knob would only offer the
        // choice of paying for a re-read that answers a different question than
        // the one this pass is asked.
        let mut verification =
            par2_rs::verify::merge_verification_results(&par2_set, pre_repair, fresh);
        self.settle_par2_pass_result(job_id, &mut verification, false);
        let placement_plan = placement_plan_from_verification(&verification);
        Self::log_placement_plan(job_id, &placement_plan);
        Ok((verification, placement_plan))
    }

    /// Everything an authoritative pass does with its raw result before a
    /// caller may read it: the direct-set damage adjustments, the volume-safety
    /// recomputation and, when the caller is emitting them, the verdict events.
    fn settle_par2_pass_result(
        &mut self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
        emit_events: bool,
    ) {
        let adjustments = self.apply_direct_damage_adjustments(job_id, verification);
        if adjustments.skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks = adjustments.skipped_blocks,
                "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if adjustments.retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks = adjustments.retained_suspect_blocks,
                "retained suspect eagerly-deleted volumes in damage count"
            );
        }
        if adjustments.forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks = adjustments.forgiven_direct_blocks,
                "excluded finalized direct-store volumes from damage count"
            );
        }

        self.recompute_volume_safety_from_verification(job_id, verification);
        self.record_par2_set_verification_observation(job_id, verification);

        if emit_events {
            let passed = !par2_verification_needs_repair(verification);
            let _ = self
                .event_tx
                .send(PipelineEvent::JobVerificationComplete { job_id, passed });
        }
    }

    /// The authoritative PAR2 read, in whichever of its shapes
    /// [`Par2PassScope`] asks for. Returns the raw result and the plan the pass
    /// read through; settling it is the caller's, so the selective shape can
    /// merge first and settle once over the combined set.
    async fn run_par2_placement_pass(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        scope: Par2PassScope,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
        #[cfg(test)]
        {
            // Counted by what the pass reads, not by how it resolves names: both
            // whole-set shapes read every described file, only the selective
            // ones read fewer.
            if matches!(
                scope,
                Par2PassScope::Selected(_) | Par2PassScope::SelectedProposed(_, _)
            ) {
                self.par2_selective_verify_calls += 1;
            } else {
                self.par2_authoritative_verify_calls += 1;
            }
        }

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 verification started");

        let pass_set_id = par2_set.recovery_set_id;
        let verify_dir = working_dir.clone();
        let pp_pool = self.pp_pool.clone();
        // A direct set's source volumes are not on disk, so the
        // pass reads them through the hybrid virtual-volume provider. Everything
        // else in the job — the PAR2 volumes, any conventional data file, a
        // demoted set's materialized volumes — keeps reading through
        // `PlacementFileAccess` exactly as before.
        let direct = self.direct_par2_overlay(job_id);
        let verify_result = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                // The whole-set shape PROPOSES placement from 16 KiB prefixes
                // and lets the verify below prove it (see
                // `build_prefix_placement_proposal` for why the library scan's
                // full-file MD5 confirmation was a second whole-set read this
                // pass never needed); the planned-selected shape reads through
                // a proposal its caller already built; the other two assert
                // placement. An empty plan *is* the identity placement for
                // reading, because `PlacementFileAccess::from_plan` takes its
                // overrides from `swaps` and `renames` alone and otherwise
                // resolves a file at the name its description gives it — which
                // is also why a post-repair pass never scans: this job applied
                // the pre-repair plan before repairing, and par2-rs installs
                // every file it rewrote at that file's canonical name.
                let mut plan = match &scope {
                    Par2PassScope::WholeSet => build_prefix_placement_proposal(
                        &verify_dir,
                        &par2_set,
                        None,
                        &HashSet::new(),
                    ),
                    Par2PassScope::SelectedProposed(file_ids, claimed_names) => {
                        let restrict: HashSet<par2_rs::FileId> = file_ids.iter().copied().collect();
                        build_prefix_placement_proposal(
                            &verify_dir,
                            &par2_set,
                            Some(&restrict),
                            claimed_names,
                        )
                    }
                    Par2PassScope::Selected(_) => par2_rs::PlacementPlan {
                        exact: Vec::new(),
                        swaps: Vec::new(),
                        renames: Vec::new(),
                        unresolved: Vec::new(),
                        conflicts: Vec::new(),
                    },
                };

                let Some(direct) = direct else {
                    let file_access = par2_rs::PlacementFileAccess::from_plan(
                        verify_dir.clone(),
                        &par2_set,
                        &plan,
                    );
                    let verification = verify_in_scope(&scope, &par2_set, &file_access);
                    // A damaged whole-set verdict is about to send this job to
                    // the repairer, whose analysis would re-read every byte
                    // this pass just hashed. Hand the pass across the boundary
                    // instead — only when every file sits at its described
                    // name, because that is the layout the carry attests.
                    let host_carry = if matches!(scope, Par2PassScope::WholeSet)
                        && plan.swaps.is_empty()
                        && plan.renames.is_empty()
                        && verification.needs_repair()
                    {
                        build_host_verification_carry(&verify_dir, &par2_set, &verification)
                    } else {
                        None
                    };
                    return Ok((verification, plan, host_carry));
                };

                // The scan walked a directory the direct volumes are absent
                // from, so it left every one of them `unresolved`. Reclassifying
                // them `exact` changes no behaviour today — the plan's only
                // consumers are `PlacementFileAccess::from_plan`,
                // `apply_placement_plan` and the plan log, and all three read
                // `swaps` and `renames` only, never `exact` or `unresolved`. It
                // is kept as defence: a direct volume *is* at its declared name
                // by construction (its identity is resolved by that name, in
                // `direct_par2_overlay`) and has no file to move, so the moment
                // anything does start reading these two lists it must see the
                // classification a correctly placed volume would have had, not
                // the one that invites a rename of a file that is not there.
                let direct_ids: HashSet<par2_rs::FileId> = direct
                    .volumes
                    .iter()
                    .map(|volume| volume.par2_file_id)
                    .collect();
                plan.unresolved
                    .retain(|file_id| !direct_ids.contains(file_id));
                for file_id in &direct.volumes {
                    if !plan.exact.contains(&file_id.par2_file_id) {
                        plan.exact.push(file_id.par2_file_id);
                    }
                }

                let inner = par2_rs::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                // Taken before the provider is moved into the
                // access: for an encrypted set the pass reads posted bytes the
                // overlay re-derives, and these are the only numbers that say
                // what that cost. Zero for every unencrypted set.
                let cipher = direct.provider.cipher_counters();
                let file_access =
                    crate::pipeline::direct_store::par2_access::DirectVolumeFileAccess::new(
                        inner,
                        direct.provider,
                        &direct.volumes,
                    );
                let counters = file_access.counters();
                let verification = verify_in_scope(&scope, &par2_set, &file_access);
                debug!(
                    virtual_volumes = direct.volumes.len(),
                    sequential_opens = counters.sequential_opens(),
                    // Volumes whose interior holes made the
                    // sequential sweep a lie, so the pass took the per-slice
                    // ranged path instead. Non-zero means the job paid for an
                    // accurate damage count, which is what a repair is sized
                    // from.
                    sequential_refusals = counters.sequential_refusals(),
                    ranged_reads = counters.ranged_reads(),
                    // The checkpoint bound, in production. `chained_bytes`
                    // is what checkpoint misses cost — bytes re-encrypted only
                    // to reach a ranged read's CBC seed and then discarded — and
                    // `seeded_from_start` counts the reads that had no reachable
                    // checkpoint at all. Both large against `reencrypted_bytes`
                    // means the checkpoint stride is too wide for this shape.
                    reencrypted_bytes = cipher.reencrypted_bytes(),
                    chained_bytes = cipher.chained_bytes(),
                    seeded_from_checkpoint = cipher.seeded_from_checkpoint(),
                    seeded_from_start = cipher.seeded_from_start(),
                    // A read the overlay would not answer: unreproducible posted
                    // bytes, which the pass sees as damage.
                    cipher_refusals = cipher.refusals(),
                    "authoritative PAR2 pass read a direct set's volumes virtually"
                );
                // No host carry from the virtual pass: its volumes are not on
                // disk, so there is nothing a stat fingerprint could attest.
                Ok((verification, plan, None))
            })
        })
        .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        match verify_result {
            Ok(Ok((verification, plan, host_carry))) => {
                if let Some(carry) = host_carry {
                    #[cfg(test)]
                    {
                        self.par2_host_carry_builds += 1;
                    }
                    if let Some(set_runtime) = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(pass_set_id)
                    {
                        set_runtime.scan_carry = Some(carry);
                    }
                }
                Ok((verification, plan))
            }
            Ok(Err(message)) => Err(message),
            Err(error) => Err(format!("verification task panicked: {error}")),
        }
    }

    /// What [`Pipeline::apply_direct_damage_adjustments`] moved, so each caller
    /// can log it in its own voice.
    ///
    /// Counts rather than a bool: "how many blocks were forgiven" is the number
    /// the operator needs to tell a job that was never damaged from one whose
    /// damage was excused.
    pub(crate) fn apply_direct_damage_adjustments(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> DamageAdjustments {
        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, verification);
        // A *finalized* direct set's source volumes were never written and never
        // will be: its partials are at their destinations and its envelopes are
        // gone, and the whole-member CRC32 gates plus this job's own earlier
        // PAR2 verdict are what let it commit in the first place. Every later
        // pass — and one conventional set failing extraction after the direct
        // set finalized is enough to cause one — would otherwise report those
        // volumes missing and either fail the job as unrepairable or have the
        // repairer write source volumes the job already finished without. Same
        // justification, same shape and the same position in the pass as
        // `apply_eager_delete_exclusions` above.
        let forgiven_direct_blocks = self.forgive_finalized_direct_volumes(job_id, verification);
        DamageAdjustments {
            skipped_blocks,
            retained_suspect_blocks,
            forgiven_direct_blocks,
        }
    }

    pub(in crate::pipeline) fn par2_servable_set_ids(
        &self,
        job_id: JobId,
    ) -> Vec<par2_rs::RecoverySetId> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .ordered_set_ids()
                    .into_iter()
                    .filter(|set_id| {
                        // A parsed set necessarily carries its descriptions;
                        // a set known only by sighting has no parsed set.
                        runtime
                            .set_runtime(*set_id)
                            .is_some_and(|set_runtime| set_runtime.set.is_some())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Discovery closes when no bounded collection bootstrap remains. Sibling
    /// recovery volumes stay cold after one carrier has supplied usable
    /// metadata, instead of being treated as completion-critical work.
    pub(in crate::pipeline) fn par2_metadata_discovery_closed(&self, job_id: JobId) -> bool {
        let candidates = self.par2_metadata_candidate_indices(job_id);
        if candidates.is_empty() {
            return true;
        }
        if candidates.iter().any(|(file_index, _, _)| {
            self.par2_discovery_state_for_candidate(job_id, *file_index)
                .work_is_queued()
        }) {
            return false;
        }
        self.next_par2_metadata_action(job_id).is_none()
    }

    /// Whether every servable set has reached a final answer and no later
    /// index can add one. A failed set is settled, but not verified.
    fn par2_gate_settlement_complete(&self, job_id: JobId) -> bool {
        let set_ids = self.par2_servable_set_ids(job_id);
        !set_ids.is_empty()
            && self.par2_metadata_discovery_closed(job_id)
            && self.par2_runtime(job_id).is_some_and(|runtime| {
                set_ids.iter().all(|set_id| {
                    runtime
                        .set_runtime(*set_id)
                        .is_some_and(|set_runtime| set_runtime.settled)
                })
            })
    }

    /// Recompute the job-level verification answer from immutable per-set
    /// answers. This is intentionally the sole writer of `par2_verified`: a
    /// newly parsed set can reopen the aggregate without invalidating a verdict
    /// another set has already reached.
    fn recompute_par2_verified(&mut self, job_id: JobId) -> bool {
        let set_ids = self.par2_servable_set_ids(job_id);
        let verified = !set_ids.is_empty()
            && self.par2_metadata_discovery_closed(job_id)
            && self.par2_runtime(job_id).is_some_and(|runtime| {
                set_ids.iter().all(|set_id| {
                    runtime.set_runtime(*set_id).is_some_and(|set_runtime| {
                        set_runtime.settled && set_runtime.failure.is_none()
                    })
                })
            });
        if verified {
            self.par2_verified.insert(job_id);
        } else {
            self.par2_verified.remove(&job_id);
        }
        verified
    }

    /// Mark one set settled, reset only that set's re-entry latch, then update
    /// the aggregate.  Direct outputs remain held until every servable set and
    /// metadata discovery have reached a final answer.
    pub(in crate::pipeline) async fn settle_par2_set(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        reason: Par2SetSettlementReason,
    ) -> SetGateOutcome {
        let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) else {
            return SetGateOutcome::Waiting;
        };
        set_runtime.settled = true;
        set_runtime.failure = None;
        set_runtime.post_verdict_reconcile_attempts = 0;
        self.mark_par2_verified(job_id).await;
        if let Par2SetSettlementReason::Clean {
            slice_size,
            verification_mode,
        } = reason
        {
            log_clean_par2_verification_source(job_id, set_id, slice_size, verification_mode);
        }
        SetGateOutcome::Settled
    }

    /// Records a set-local failure without aborting its siblings' passes.
    fn mark_par2_set_failed(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        message: String,
    ) -> SetGateOutcome {
        let index_filename = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(set_id))
            .map(|set_runtime| set_runtime.summary.index_filename.clone())
            .filter(|filename| !filename.is_empty())
            .unwrap_or_else(|| set_id.to_string());
        if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set_runtime.settled = true;
            set_runtime.failure = Some(message.clone());
            set_runtime.post_verdict_reconcile_attempts = 0;
        }
        self.recompute_par2_verified(job_id);
        self.note_aggregate_par2_verification_result(job_id);
        warn!(
            job_id = job_id.0,
            recovery_set_id = %set_id,
            index_filename = %index_filename,
            error = %message,
            "PAR2 recovery set failed after its own repair ladder"
        );
        SetGateOutcome::Failed(message)
    }

    async fn finish_par2_set_failure(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        message: String,
    ) {
        let _ = self.mark_par2_set_failed(job_id, set_id, message);
        self.finish_or_rearm_after_par2_set_failure(job_id);
    }

    /// A failed set must leave its siblings time to settle, but once the last
    /// one has answered the job failure belongs to this same gate entry.  In
    /// particular, a one-set job must retain the immediate failure behaviour
    /// it had before the aggregate existed.
    fn finish_or_rearm_after_par2_set_failure(&mut self, job_id: JobId) {
        if let Some(message) = self.aggregate_par2_failure_message(job_id) {
            self.fail_job(job_id, message);
        } else {
            self.schedule_job_completion_check(job_id);
        }
    }

    /// Make the earliest unsettled servable set the compatibility view used by
    /// existing repair helpers.  The selection changes only at a set boundary;
    /// a settled set is never selected again merely because another set arrives.
    fn activate_next_par2_gate_set(&mut self, job_id: JobId) -> Option<par2_rs::RecoverySetId> {
        let next_set_id = self
            .par2_servable_set_ids(job_id)
            .into_iter()
            .find(|set_id| {
                self.par2_runtime(job_id)
                    .and_then(|runtime| runtime.set_runtime(*set_id))
                    .is_some_and(|set_runtime| !set_runtime.settled)
            });
        if let Some(set_id) = next_set_id
            && let Some(runtime) = self.par2_runtime.get_mut(&job_id)
        {
            runtime.served = Some(set_id);
        }
        next_set_id
    }

    fn served_par2_set_needs_reconciliation(&self, job_id: JobId) -> bool {
        self.par2_served_set_id(job_id).is_some_and(|set_id| {
            self.par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| {
                    set_runtime.settled
                        && set_runtime.failure.is_none()
                        && self.settled_verdict_left_only_proven_protected_files(job_id, set_id)
                })
        })
    }

    /// A recovery set with no assembly binding and no bytes at any described
    /// path has nothing this job can verify or repair.  The binding condition
    /// is deliberately conservative: an empty but known assembly file still
    /// takes the ordinary pass, because it may be waiting for recoverable data.
    fn par2_set_is_absent_from_job(&self, job_id: JobId, set_id: par2_rs::RecoverySetId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(par2_set) = self.par2_set_for(job_id, set_id) else {
            return false;
        };
        let has_assembly_binding = state.assembly.files().any(|file| {
            self.resolve_par2_file_binding(file.file_id())
                .is_some_and(|binding| binding.recovery_set_id == set_id)
        });
        if has_assembly_binding {
            return false;
        }
        // A split topology can assemble the described output even when none
        // of its individual fragments binds to that description.  Treat that
        // relationship as evidence rather than skipping a recovery pass.
        let described_names = par2_set
            .files
            .values()
            .map(|description| sanitize_download_filename(&description.filename))
            .collect::<HashSet<_>>();
        if state
            .assembly
            .archive_topologies()
            .keys()
            .any(|name| described_names.contains(&sanitize_download_filename(name)))
        {
            return false;
        }
        par2_set.files.values().all(|description| {
            let path = state
                .working_dir
                .join(sanitize_download_filename(&description.filename));
            std::fs::metadata(path)
                .ok()
                .is_none_or(|metadata| metadata.len() == 0)
        })
    }

    fn record_par2_set_verification_observation(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return;
        };
        if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set_runtime.missing_blocks = verification.total_missing_blocks;
            set_runtime.needed_repair |= par2_verification_needs_repair(verification);
        }
    }

    fn note_aggregate_par2_verification_result(&mut self, job_id: JobId) {
        if !self.par2_gate_settlement_complete(job_id)
            || self.jobs_with_verification_outcome.contains(&job_id)
        {
            return;
        }
        let (passed, missing_blocks) = self
            .par2_runtime(job_id)
            .map(|runtime| {
                self.par2_servable_set_ids(job_id).into_iter().fold(
                    (true, 0u32),
                    |(passed, missing_blocks), set_id| {
                        let set_runtime = runtime
                            .set_runtime(set_id)
                            .expect("servable recovery set remains in its runtime");
                        (
                            passed && !set_runtime.needed_repair && set_runtime.failure.is_none(),
                            missing_blocks.saturating_add(set_runtime.missing_blocks),
                        )
                    },
                )
            })
            .unwrap_or((true, 0));
        self.note_job_verification_result(job_id, passed, missing_blocks);
    }

    /// Records the aggregate verdict and releases direct outputs exactly when
    /// every servable set has verified. A per-set repair must not commit
    /// neighbouring set B before B has had its own opportunity to verify or
    /// repair.
    pub(in crate::pipeline) async fn mark_par2_verified(&mut self, job_id: JobId) {
        let was_verified = self.par2_verified.contains(&job_id);
        if !self.recompute_par2_verified(job_id) {
            return;
        }
        self.note_aggregate_par2_verification_result(job_id);
        if !was_verified {
            self.finalize_ready_direct_sets(job_id).await;
            // The aggregate has just settled, so every set that was going to
            // rewrite this directory has done so and the leftovers can be named
            // by difference. This is the only place that is true: a job whose
            // last set settled *clean* never re-enters the repair tail, so
            // purging only from there leaves the earlier sets' backups on disk.
            // Ordered after direct finalization so a set that renames its
            // partials into place is already wearing its final names when the
            // keep-set is built from the assembly.
            self.purge_par2_repair_leftovers(job_id);
        }
    }

    pub(in crate::pipeline) fn aggregate_par2_failure_message(
        &self,
        job_id: JobId,
    ) -> Option<String> {
        if !self.par2_metadata_discovery_closed(job_id) {
            return None;
        }
        let runtime = self.par2_runtime(job_id)?;
        let set_ids = runtime.ordered_set_ids();
        if set_ids.is_empty() {
            return (!self.par2_metadata_candidate_indices(job_id).is_empty()).then(|| {
                "PAR2 metadata discovery exhausted without finding a recovery set".to_string()
            });
        }
        if set_ids.iter().any(|set_id| {
            runtime
                .set_runtime(*set_id)
                .is_some_and(|set_runtime| set_runtime.set.is_some() && !set_runtime.settled)
        }) {
            return None;
        }
        let failures = set_ids
            .into_iter()
            .filter_map(|set_id| {
                let set_runtime = runtime.set_runtime(set_id)?;
                let index = if set_runtime.summary.index_filename.is_empty() {
                    set_id.to_string()
                } else {
                    set_runtime.summary.index_filename.clone()
                };
                if set_runtime.set.is_none() {
                    return Some(format!(
                        "{index}: metadata discovery exhausted before the recovery set could be parsed"
                    ));
                }
                let failure = set_runtime.failure.as_ref()?;
                Some(format!(
                    "{index} ({}): {failure}",
                    set_runtime.summary.described_filenames.join(", ")
                ))
            })
            .collect::<Vec<_>>();
        (!failures.is_empty()).then(|| {
            format!(
                "PAR2 recovery failed for {} set(s): {}",
                failures.len(),
                failures.join("; ")
            )
        })
    }

    pub(super) fn emit_job_verification_started(&mut self, job_id: JobId) {
        // Low-frequency: a job enters PAR2 verification a handful of times, so
        // arming the stage timer here costs one clock read per pass and never
        // touches an article path.
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::Verify,
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::JobVerificationStarted { job_id });
    }

    /// Fold one job-level PAR2 verification verdict into the lifecycle metrics
    /// and close the verify stage timer.
    ///
    /// Low-frequency: one call per verification pass, never per segment. The
    /// four-way label is derived from what the pass actually produced — a pass
    /// that needs repair and found nothing at all on disk is `missing`, one
    /// that needs repair with blocks present is `damaged`.
    pub(super) fn note_job_verification_result(
        &mut self,
        job_id: JobId,
        passed: bool,
        missing_blocks: u32,
    ) {
        use crate::operations::instrumentation::{JobStageKind, VerificationOutcomeKind};
        let outcome = if passed {
            VerificationOutcomeKind::Intact
        } else if missing_blocks > 0 {
            VerificationOutcomeKind::Missing
        } else {
            VerificationOutcomeKind::Damaged
        };
        // Claim the job before recording, so a later `unverifiable` fallback
        // cannot add a second row for a job an actual pass already ruled on.
        // Re-verification of the same job (verify, repair, verify again) is a
        // real second outcome and still counts, which is why the claim gates
        // only the fallback and not this.
        self.jobs_with_verification_outcome.insert(job_id);
        self.metrics.job_lifecycle.note_verification(outcome);
        self.note_stage_finished(job_id, JobStageKind::Verify);
    }

    /// Record that this job ended with no PAR2 verdict to be had.
    ///
    /// A job with no recovery set can never produce `intact`, `damaged` or
    /// `missing`: there is nothing to verify the payload against. Without
    /// this, such jobs contribute nothing at all to
    /// `weaver_verifications_total`, and the ratio of verified to unverified
    /// downloads — the thing an operator actually wants from that series — is
    /// unanswerable.
    ///
    /// Low-frequency: at most one call per job, at the terminal transition.
    /// The guard set is the same per-job set a real verdict claims, so the two
    /// can never both fire for one job.
    pub(in crate::pipeline) fn note_job_verification_unavailable(&mut self, job_id: JobId) {
        use crate::operations::instrumentation::VerificationOutcomeKind;
        if self.jobs_with_verification_outcome.insert(job_id) {
            self.metrics
                .job_lifecycle
                .note_verification(VerificationOutcomeKind::Unverifiable);
        }
    }

    /// Called at the two terminal transitions — the final move and job failure
    /// — to attribute a job that never had a recovery set.
    pub(in crate::pipeline) fn note_job_unverifiable_if_no_par2_set(&mut self, job_id: JobId) {
        if self.par2_set(job_id).is_none() {
            self.note_job_verification_unavailable(job_id);
        }
    }

    async fn quick_verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        _working_dir: std::path::PathBuf,
    ) -> Result<QuickPar2Outcome, String> {
        // A live direct set's source volumes are not files. This pass answers in
        // *placement* terms — it hands back a plan whose swaps and renames move
        // real paths, and a clean verdict from it skips the direct-aware pass
        // entirely — so a volume that exists only as routed member partials and
        // envelopes has no business being decided here. The direct pass
        // (`verify_direct_sets_quietly`) is the one that knows how to stand in
        // for a grid-adjudicated virtual volume and how to read the rest, and it
        // reaches the same zero-I/O conclusion from the same evidence.
        //
        // Before the grid was fed from the direct seam this refusal happened by
        // accident: a direct volume carried neither a block verdict nor a
        // completed-file digest, so the loop below fell through its
        // `no_current_generation_digest` arm. It is stated rather than inherited
        // now that the first of those two is no longer true.
        if self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized())
        {
            crate::runtime::perf_probe::record(
                "completion.quick_verify.rejected.live_direct_set",
                std::time::Duration::from_nanos(1),
            );
            return Ok(QuickPar2Outcome::Inconclusive);
        }
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let runtime_checksums = self
            .par2_runtime(job_id)
            .map(|runtime| runtime.completed_checksums.clone())
            .unwrap_or_default();
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(QuickPar2Outcome::Inconclusive);
        };

        let mut current_hashes_by_name = HashMap::<String, [u8; 16]>::new();
        // Three arms decide a file, strongest first: the dual-CRC grid's
        // per-slice proof, the streamed whole-file CRC32 against the CRC the
        // description's own slice checksums fold to, and the persisted/runtime
        // whole-file MD5. None of them computes anything over the payload —
        // all three read evidence already in hand — and an in-stream `Damaged`
        // verdict vetoes all of them before any runs.
        let mut grid_matches_by_name = HashMap::<String, (par2_rs::FileId, String)>::new();
        let mut file_crc_matches_by_name = HashMap::<String, (par2_rs::FileId, String)>::new();
        // Built on first use: a set whose every file the grid already covered
        // never pays for the fold.
        let mut padded_file_crc_lookup: Option<PaddedFileCrcLookup> = None;
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let file_id = file.file_id();
            // The file's measured digest, by generation: a runtime checksum
            // entry always speaks for the CURRENT generation (`Some` = its
            // digest, `None` = it has none — a CRC-metadata completion or
            // the sentinel a failed finalize records after a duplicate
            // rewrite), and the persisted row stands in only when the
            // runtime holds no entry at all (the restart shape). An older
            // row must never outrank or revive over the current generation.
            let measured_md5 = match runtime_checksums.get(&file_id) {
                Some(current) => current.md5,
                None => completed_hashes.get(&file_id.file_index).copied(),
            };
            // In-stream IFSC verdicts veto every quick arm below. A Damaged
            // block means the dual-CRC grid saw bytes that contradict the
            // recovery set, so any hash that still matches — a stale trusted
            // row, say — is exactly what must not conclude verification here.
            // Conflicted files go to the authoritative pass, which reads the
            // real bytes.
            if self.block_crc_verdicts(file_id).is_some_and(|verdicts| {
                verdicts.values().any(|verdict| {
                    matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)
                })
            }) {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.rejected.damaged_in_stream_verdict",
                    std::time::Duration::from_nanos(1),
                );
                return Ok(QuickPar2Outcome::Inconclusive);
            }
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.as_str())
                .unwrap_or_else(|| file.filename());
            // Clean dual-CRC arm. Metadata-early downloads deliberately
            // stream no MD5 — the article/IFSC grids carry verification — so
            // a clean file has no digest anywhere and would otherwise fall to
            // the authoritative pass and re-read every byte it just wrote.
            // The grid match demands every described slice Intact with
            // independent coverage at exact length; the Damaged veto above
            // already refused conflicted files before any arm ran.
            if let Some(grid_match) = self.in_stream_verified_par2_match(file_id, &par2_set) {
                // A measured digest outranks the grid. When a trusted MD5
                // exists for this file — streamed, re-read after a duplicate,
                // or verified — and it disagrees with the description the
                // grid selected, the CRC evidence has been contradicted by a
                // stronger instrument and only the authoritative pass may
                // adjudicate. No digest is ever computed for this check; it
                // compares bytes already in hand.
                if let Some(measured) = measured_md5
                    && par2_set
                        .file_description(&grid_match.0)
                        .is_some_and(|description| description.hash_full != measured)
                {
                    crate::runtime::perf_probe::record(
                        "completion.quick_verify.rejected.grid_contradicts_measured_md5",
                        std::time::Duration::from_nanos(1),
                    );
                    return Ok(QuickPar2Outcome::Inconclusive);
                }
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.par2_match.in_stream_grid",
                    std::time::Duration::from_nanos(1),
                );
                grid_matches_by_name.insert(current_filename.to_string(), grid_match);
                continue;
            }
            // Whole-file-CRC arm. The grid proves a file slice by slice and
            // needs every slice; this proves the same file in one comparison
            // and needs none of them, so it picks up exactly what the grid
            // could not cover — a file whose articles all verified their yEnc
            // part CRC but whose bytes never composed onto the block grid.
            // Those files already stopped streaming an MD5, so without this
            // they fall to the authoritative pass and are re-read whole.
            //
            // The streamed CRC32 is a fold of part CRCs in arrival order, so it
            // means what it says only over a gapless, duplicate-free,
            // in-order assembly — the same three conditions the committed
            // evidence path requires before it will call an assembly
            // contiguous.
            if let Some(streamed) = runtime_checksums.get(&file_id).filter(|checksum| {
                checksum.all_parts_crc_verified
                    && !file.has_duplicate_segments()
                    && file.contiguous_placements_proven()
            }) {
                let measured_length = file.received_bytes();
                let slice_count = par2_set.slice_count_for_file(measured_length);
                let lookup = padded_file_crc_lookup
                    .get_or_insert_with(|| par2_padded_file_crc_lookup(&par2_set));
                let padded = pad_measured_file_crc32_to_slice_grid(
                    streamed.crc32,
                    measured_length,
                    u64::from(slice_count),
                    par2_set.slice_size,
                );
                match lookup
                    .get(&(measured_length, padded))
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                {
                    [] => {
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.skipped.file_crc_no_match",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                    [(par2_file_id, correct_name)] => {
                        // A measured digest outranks the CRC, exactly as it
                        // outranks the grid: a trusted MD5 for this generation
                        // that disagrees with the description this arm picked
                        // is a stronger instrument contradicting a weaker one,
                        // and only the authoritative pass may adjudicate that.
                        if let Some(measured) = measured_md5
                            && par2_set
                                .file_description(par2_file_id)
                                .is_some_and(|description| description.hash_full != measured)
                        {
                            crate::runtime::perf_probe::record(
                                "completion.quick_verify.rejected.file_crc_contradicts_measured_md5",
                                std::time::Duration::from_nanos(1),
                            );
                            return Ok(QuickPar2Outcome::Inconclusive);
                        }
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.par2_match.file_crc",
                            std::time::Duration::from_nanos(1),
                        );
                        file_crc_matches_by_name.insert(
                            current_filename.to_string(),
                            (*par2_file_id, correct_name.clone()),
                        );
                        continue;
                    }
                    _ => {
                        // Two descriptions of the same length folding to the
                        // same CRC32 is exactly where a 32-bit binding stops
                        // being a proof. Neither may claim the file.
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.skipped.file_crc_ambiguous",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                }
            } else {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.skipped.file_crc_unproven_assembly",
                    std::time::Duration::from_nanos(1),
                );
            }
            // `measured_md5` is generation-ordered (runtime first) and the
            // persisted side is provenance-filtered: a row without trusted
            // `md5_provenance` (legacy — possibly a PAR2 expectation
            // recorded by the removed substitution) never loads. An
            // evidence-less file may belong to another recovery set, so it is
            // not itself a reason to reject this set. Any protected file that
            // remains unproved is caught by the unresolved check below.
            let Some(file_hash) = measured_md5 else {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.skipped.no_current_generation_digest",
                    std::time::Duration::from_nanos(1),
                );
                continue;
            };
            current_hashes_by_name.insert(current_filename.to_string(), file_hash);
        }

        let mut all_file_ids: Vec<par2_rs::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .chain(par2_set.non_recovery_file_ids.iter())
            .copied()
            .collect();
        all_file_ids.sort_unstable_by_key(|file_id| *file_id.as_bytes());
        all_file_ids.dedup();

        let mut hash_lookup = HashMap::<[u8; 16], Vec<(par2_rs::FileId, String)>>::new();
        for file_id in &all_file_ids {
            let Some(desc) = par2_set.file_description(file_id) else {
                continue;
            };
            hash_lookup
                .entry(desc.hash_full)
                .or_default()
                .push((*file_id, sanitize_download_filename(&desc.filename)));
        }

        let mut matches = grid_matches_by_name;
        let had_grid_match = !matches.is_empty();
        let had_file_crc_match = !file_crc_matches_by_name.is_empty();
        matches.extend(file_crc_matches_by_name);
        let mut digest_matched_description = false;
        let mut match_counts = HashMap::<par2_rs::FileId, u32>::new();
        for (file_id, _) in matches.values() {
            *match_counts.entry(*file_id).or_default() += 1;
        }
        for (current_name, file_hash) in current_hashes_by_name {
            let Some(candidates) = hash_lookup.get(&file_hash) else {
                continue;
            };

            if let Some((file_id, correct_name)) = candidates.first() {
                matches.insert(current_name.clone(), (*file_id, correct_name.clone()));
                *match_counts.entry(*file_id).or_default() += 1;
                digest_matched_description = true;
            }
        }
        // Weakest arm that contributed. The `Digest` fallback also owns the
        // "nothing matched at all" shape — a set with no descriptions to match
        // — which claims no zero-read evidence and never did.
        let evidence = if digest_matched_description || !(had_grid_match || had_file_crc_match) {
            QuickPar2Evidence::Digest
        } else if had_file_crc_match {
            QuickPar2Evidence::FileCrc
        } else {
            QuickPar2Evidence::Grid
        };

        let conflict_ids: HashSet<par2_rs::FileId> = match_counts
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(file_id, _)| *file_id)
            .collect();
        matches.retain(|_, (file_id, _)| !conflict_ids.contains(file_id));

        let mut id_to_disk = HashMap::<par2_rs::FileId, String>::new();
        for (disk_name, (file_id, _)) in &matches {
            id_to_disk.insert(*file_id, disk_name.clone());
        }

        let mut files = Vec::new();
        let mut exact = Vec::new();
        let mut swaps = Vec::new();
        let mut renames = Vec::new();
        let mut unresolved = Vec::new();
        let mut seen_swap = HashSet::<par2_rs::FileId>::new();
        for file_id in all_file_ids.iter().copied() {
            let Some(desc) = par2_set.file_description(&file_id).cloned() else {
                continue;
            };
            let correct_filename = sanitize_download_filename(&desc.filename);

            if conflict_ids.contains(&file_id) {
                continue;
            }

            let Some(disk_name) = id_to_disk.get(&file_id).cloned() else {
                unresolved.push(file_id);
                continue;
            };

            if disk_name == correct_filename {
                exact.push(file_id);
            } else if !seen_swap.contains(&file_id) {
                let other_file_id = matches.get(correct_filename.as_str()).map(|(id, _)| *id);
                if let Some(other_id) = other_file_id
                    && other_id != file_id
                    && id_to_disk
                        .get(&other_id)
                        .is_some_and(|name| name == &correct_filename)
                {
                    let Some(other_desc) = par2_set.file_description(&other_id) else {
                        return Ok(QuickPar2Outcome::Inconclusive);
                    };
                    let other_correct_filename = sanitize_download_filename(&other_desc.filename);
                    swaps.push((
                        par2_rs::PlacementEntry {
                            file_id,
                            current_name: disk_name.clone(),
                            correct_name: correct_filename.clone(),
                        },
                        par2_rs::PlacementEntry {
                            file_id: other_id,
                            current_name: correct_filename.clone(),
                            correct_name: other_correct_filename,
                        },
                    ));
                    seen_swap.insert(file_id);
                    seen_swap.insert(other_id);
                } else {
                    renames.push(par2_rs::PlacementEntry {
                        file_id,
                        current_name: disk_name.clone(),
                        correct_name: correct_filename.clone(),
                    });
                }
            }

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(par2_rs::verify::FileVerification {
                file_id,
                filename: correct_filename,
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        }

        if !conflict_ids.is_empty() {
            // Two disk files matched one description: the evidence is
            // internally contradictory, and standing ANY of it in would build
            // a verdict on an identification this pass could not make. The
            // authoritative pass reads everything, which is the correct price
            // for ambiguity.
            return Ok(QuickPar2Outcome::Inconclusive);
        }

        if !unresolved.is_empty() {
            // Unproven is not distrusted. Every entry in `files` carries a
            // zero-read proof that stands on its own; only the remainder needs
            // a read, and throwing the proven entries away here is what used
            // to turn one evidence-less file into a whole-set re-read.
            let recovery_ids: HashSet<par2_rs::FileId> =
                par2_set.recovery_file_ids.iter().copied().collect();
            let unproven_recovery: Vec<par2_rs::FileId> = unresolved
                .iter()
                .copied()
                .filter(|file_id| recovery_ids.contains(file_id))
                .collect();
            if files.is_empty() {
                return Ok(QuickPar2Outcome::Inconclusive);
            }
            #[cfg(test)]
            {
                self.par2_quick_partial_verify_calls += 1;
            }
            let claimed_disk_names: HashSet<String> = matches.keys().cloned().collect();
            return Ok(QuickPar2Outcome::Partial(QuickPar2PartialEvidence {
                proven: files,
                proven_plan: par2_rs::PlacementPlan {
                    exact,
                    swaps,
                    renames,
                    unresolved: Vec::new(),
                    conflicts: Vec::new(),
                },
                unproven_recovery,
                claimed_disk_names,
            }));
        }

        #[cfg(test)]
        {
            self.par2_quick_verify_calls += 1;
        }

        Ok(QuickPar2Outcome::Full(
            par2_rs::VerificationResult {
                files,
                recovery_blocks_available: par2_set.recovery_block_count(),
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            },
            par2_rs::PlacementPlan {
                exact,
                swaps,
                renames,
                unresolved,
                conflicts: conflict_ids.into_iter().collect(),
            },
            evidence,
        ))
    }

    /// Test-only entry onto [`Self::quick_verify_par2_with_placement`].
    ///
    /// The quick pass is private to this module, but its `Some`/`None` verdict
    /// and the placement plan it hands back are exactly what a diagnostic for a
    /// misplaced-payload shape needs to read first-hand, rather than inferring
    /// them from the completion gate's downstream effects. Compiled only under
    /// test, so it adds nothing to the shipped path.
    #[cfg(test)]
    pub(in crate::pipeline) async fn quick_verify_par2_with_placement_for_test(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
    ) -> Result<
        Option<(
            par2_rs::VerificationResult,
            par2_rs::PlacementPlan,
            QuickPar2Evidence,
        )>,
        String,
    > {
        Ok(
            match self
                .quick_verify_par2_with_placement(job_id, par2_set, working_dir)
                .await?
            {
                QuickPar2Outcome::Full(verification, plan, evidence) => {
                    Some((verification, plan, evidence))
                }
                QuickPar2Outcome::Partial(_) | QuickPar2Outcome::Inconclusive => None,
            },
        )
    }

    /// Shared completion handling for a clean PAR2 verdict.
    ///
    /// Every fast path that proves a job clean without the authoritative pass
    /// funnels through here, so their downstream effects — placement, identity,
    /// reconciliation, `par2_verified`, status transitions — are the same code,
    /// not parallel copies that can drift.
    async fn finish_clean_par2_verification(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        working_dir: std::path::PathBuf,
        outcome: CleanPar2Verification,
        has_crc_failures: bool,
        archive_extraction_applicable: bool,
    ) {
        let CleanPar2Verification {
            verification,
            placement_plan,
            slice_size,
            verification_mode,
            reconcile_context,
            retry_message,
        } = outcome;
        Self::log_placement_plan(job_id, &placement_plan);

        self.try_deobfuscate_files_with_par2(job_id).await;
        if let Err(error) = self
            .apply_placement_plan_for_retry_or_repair(job_id, working_dir, &placement_plan)
            .await
        {
            self.finish_par2_set_failure(job_id, set_id, error).await;
            return;
        }
        self.retry_par2_authoritative_identity(job_id).await;
        // Before refreshing topologies, adopt any RAR volume PAR2 rebuilt that
        // the NZB never carried. 0.7.9 calls this at each of its repair exits;
        // 0.8 funnels them through here, so one call covers them all. Without
        // it a repaired interior volume sits on disk under a name the assembly
        // has never heard of, extraction goes on waiting for it, and the repair
        // that just succeeded changes nothing.
        let registration = match self
            .register_verified_par2_rar_outputs(job_id, &verification)
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                self.finish_par2_set_failure(job_id, set_id, error).await;
                return;
            }
        };
        // No repair ran on this arm, so nothing was rewritten; a rebuilt volume
        // the NZB never carried still invalidates its set's plan.
        self.refresh_verified_complete_archive_topologies(job_id, &verification, &HashSet::new())
            .await;
        self.invalidate_rar_plans_for_repaired_sets(job_id, registration.set_names);
        if let Err(error) = self
            .reconcile_and_classify_par2_verification(
                job_id,
                &verification,
                has_crc_failures,
                reconcile_context,
            )
            .await
        {
            self.finish_par2_set_failure(job_id, set_id, error).await;
            return;
        }

        let settled = self
            .settle_par2_set(
                job_id,
                set_id,
                Par2SetSettlementReason::Clean {
                    slice_size,
                    verification_mode,
                },
            )
            .await;
        if settled == SetGateOutcome::Settled
            && verification_mode == CleanPar2VerificationMode::Grid
        {
            info!(
                job_id = job_id.0,
                recovery_set_id = %set_id,
                slice_size,
                verdict = "clean",
                verification_read_bytes = 0u64,
                "PAR2 set settled clean from in-stream grid evidence"
            );
        }
        self.continue_after_aggregate_clean_par2_settlement(
            job_id,
            has_crc_failures,
            archive_extraction_applicable,
            retry_message,
        )
        .await;
    }

    /// Run the pre-existing job-level continuation exactly once, after the
    /// final clean set has settled. Earlier sets re-arm the gate instead, so a
    /// one-set job still follows this path in the same completion check.
    async fn continue_after_aggregate_clean_par2_settlement(
        &mut self,
        job_id: JobId,
        has_crc_failures: bool,
        archive_extraction_applicable: bool,
        retry_message: &str,
    ) {
        if !self.par2_verified.contains(&job_id) {
            self.schedule_job_completion_check(job_id);
            return;
        }

        if has_crc_failures {
            if self.normalization_retried.contains(&job_id) {
                let msg = "clean PAR2 verification but extraction still failing after retry";
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg.to_string());
                return;
            }

            self.set_normalization_retried_state(job_id, true);
            let failed_members = self
                .failed_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default();
            self.replace_failed_extraction_members(job_id, HashSet::new());
            let cleared = failed_members.len();
            self.recompute_rar_retry_frontier(job_id).await;
            if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                if !failed_members.is_empty() {
                    self.replace_failed_extraction_members(job_id, failed_members);
                }
                let msg =
                    format!("invalid RAR retry frontier after placement correction: {reason}");
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            info!(
                job_id = job_id.0,
                cleared, retry_message, "cleared failed extractions after clean PAR2 verification"
            );

            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        if archive_extraction_applicable {
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        self.reconcile_job_progress(job_id).await;
        self.schedule_job_completion_check(job_id);
    }

    /// Bind a PAR2 verification back onto the assembly and promote every file
    /// it vouches for.
    ///
    /// # Identity, not string equality
    ///
    /// Binding runs through [`Self::resolve_par2_file_binding`] — the same
    /// resolver the dual-CRC grid measures its in-stream block verdicts
    /// against — so it inherits the sanitized comparison, the full alias set
    /// (posted, current, source, canonical), the 16 KiB content fallback that
    /// binds an obfuscated post by its bytes, and outright refusal of
    /// ambiguity.
    ///
    /// What this replaces compared *raw* assembly names against descriptions
    /// that had already been sanitized on the way in, so every name that needed
    /// sanitizing silently bound to nothing; it resolved a duplicate alias
    /// first-writer-wins; and it answered with a bare count, which cannot tell
    /// "nothing needed doing" apart from "a repaired, re-verified file bound to
    /// nothing and is still sitting incomplete". The caller needs that
    /// distinction to classify its veto, so the report carries it.
    ///
    /// A name-keyed fallback is kept for the files the resolver declines, but
    /// it is sanitized on both sides and refuses duplicates rather than taking
    /// the first.
    pub(in crate::pipeline) async fn reconcile_verified_par2_files(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> Result<Par2Reconciliation, String> {
        let existing_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let mut report = Par2Reconciliation::default();
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return Ok(report);
        };

        let files_to_complete: Vec<(NzbFileId, String)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(report);
            };
            let working_dir = state.working_dir.clone();
            let assembly_file_ids: Vec<NzbFileId> =
                state.assembly.files().map(|file| file.file_id()).collect();

            // Identity map, inverted. A description that two assembly files
            // both answer to is contested and binds to neither; `None` records
            // the contest rather than dropping the entry, so it stays visible
            // instead of degrading into a name match that would guess.
            let mut by_identity = HashMap::<par2_rs::FileId, Option<NzbFileId>>::new();
            for file_id in &assembly_file_ids {
                let Some(binding) = self.resolve_par2_file_binding(*file_id) else {
                    continue;
                };
                if binding.recovery_set_id != par2_set.recovery_set_id {
                    continue;
                }
                by_identity
                    .entry(binding.par2_file_id)
                    .and_modify(|slot| {
                        if *slot != Some(*file_id) {
                            *slot = None;
                        }
                    })
                    .or_insert(Some(*file_id));
            }

            // Sanitized alias map, for the files identity could not bind.
            let mut by_name = HashMap::<String, Option<NzbFileId>>::new();
            for file in state.assembly.files() {
                let file_id = file.file_id();
                let mut aliases = vec![file.filename().to_string()];
                if let Some(identity) = self.effective_file_identity(job_id, file_id) {
                    aliases.push(identity.current_filename.clone());
                    aliases.push(identity.source_filename.clone());
                    if let Some(canonical) = identity.canonical_filename.clone() {
                        aliases.push(canonical);
                    }
                }
                for alias in aliases {
                    let key = sanitize_download_filename(&alias);
                    if key.is_empty() {
                        continue;
                    }
                    match by_name.entry(key) {
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            if *slot.get() != Some(file_id) {
                                slot.insert(None);
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(Some(file_id));
                        }
                    }
                }
            }

            // The PAR2 files that legitimately have no file on disk: volumes of
            // a direct set that is still routing, which are verified through
            // the set's own access layer and are never written under their own
            // name. A finalized or demoted set has put its bytes at a real
            // path, and every conventional file always had one, so for those
            // absence contradicts the verdict rather than explaining it.
            //
            // Built once per pass: the overlay is derived, not cached.
            let live_virtual_par2_files: HashSet<par2_rs::FileId> = self
                .direct_par2_overlay(job_id)
                .map(|overlay| {
                    par2_set
                        .files
                        .keys()
                        .copied()
                        .filter(|par2_file_id| {
                            overlay.owner_of(par2_file_id).is_some_and(|index| {
                                self.direct_store
                                    .set(job_id, index)
                                    .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut matched = HashMap::<NzbFileId, String>::new();
            // Assembly files that more than one description laid claim to. Kept
            // apart from `matched` so a third claimant is refused too, rather
            // than filling the slot the second one vacated.
            let mut contested_file_ids = HashSet::<NzbFileId>::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }

                let bound = match by_identity.get(&file_verification.file_id) {
                    Some(Some(file_id)) => Some(*file_id),
                    // Contested identity. Content cannot break a tie that names
                    // and bytes both answer to, so it is refused, not guessed.
                    Some(None) => {
                        report.contested.push(file_verification.filename.clone());
                        continue;
                    }
                    None => {
                        let mut candidate_names = vec![file_verification.filename.clone()];
                        if let par2_rs::verify::FileStatus::Renamed(path) =
                            &file_verification.status
                            && let Some(filename) = path.file_name()
                        {
                            candidate_names.push(filename.to_string_lossy().to_string());
                        }
                        let mut found = None;
                        for candidate_name in &candidate_names {
                            match by_name.get(&sanitize_download_filename(candidate_name)) {
                                Some(Some(file_id)) => {
                                    found = Some(*file_id);
                                    break;
                                }
                                Some(None) => {
                                    report.contested.push(file_verification.filename.clone());
                                    break;
                                }
                                None => {}
                            }
                        }
                        found
                    }
                };

                let Some(file_id) = bound else {
                    // A verdict vouching for bytes on disk that names no
                    // assembly entry. Harmless when nothing is waiting on it,
                    // so the caller decides: only it knows whether the job
                    // still has incomplete files to answer for.
                    report.unbound.push(file_verification.filename.clone());
                    continue;
                };

                if state
                    .assembly
                    .file(file_id)
                    .is_some_and(|file| file.is_complete())
                {
                    continue;
                }

                // The described length is the only length worth checking here.
                // An NZB's declared total is yEnc-*encoded* — about 1.03x the
                // decoded bytes, and about 1.38x for uuencode — so it can never
                // equal `desc.length` for a real post.
                let Some(described_length) = par2_set
                    .file_description(&file_verification.file_id)
                    .map(|desc| desc.length)
                else {
                    report.unbound.push(file_verification.filename.clone());
                    continue;
                };

                let current_filename = self
                    .current_filename_for_file_id(job_id, file_id)
                    .unwrap_or_else(|| file_verification.filename.clone());
                let canonical_filename = sanitize_download_filename(&file_verification.filename);

                // Confirm the bytes the verdict vouched for are the bytes at
                // the name we are about to call complete — when there is a file
                // there to look at. Placement has already run by the time this
                // is reached, so the canonical name is where a repaired file
                // lives and the current name is the fallback for one the plan
                // left alone.
                //
                // Absence excuses only a live virtual volume. Requiring a file
                // for *every* binding refused every routing direct-store volume
                // PAR2 had just proven; excusing every binding went too far the
                // other way and would call a file complete on the strength of a
                // verdict about bytes that are no longer anywhere. The
                // exemption is therefore exactly as wide as the thing that
                // earns it.
                let installed = [canonical_filename.as_str(), current_filename.as_str()]
                    .into_iter()
                    .filter(|name| !name.is_empty())
                    .find_map(|name| {
                        let path = working_dir.join(name);
                        std::fs::metadata(&path)
                            .ok()
                            .filter(|meta| meta.is_file())
                            .map(|meta| (name.to_string(), path, meta.len()))
                    });
                let verified_filename = match installed {
                    Some((_, path, length)) if length != described_length => {
                        report.length_mismatch.push(format!(
                            "{} (on disk {length} bytes, PAR2 describes {described_length})",
                            path.display()
                        ));
                        continue;
                    }
                    Some((filename, _, _)) => filename,
                    None if live_virtual_par2_files.contains(&file_verification.file_id) => {
                        current_filename.clone()
                    }
                    None => {
                        report.length_mismatch.push(format!(
                            "{canonical_filename} (verdict vouched for bytes that are at neither \
                             the canonical nor the current name, and no live direct set owns them)"
                        ));
                        continue;
                    }
                };

                if contested_file_ids.contains(&file_id) {
                    report.contested.push(file_verification.filename.clone());
                    continue;
                }
                match matched.entry(file_id) {
                    std::collections::hash_map::Entry::Occupied(slot) => {
                        // Two descriptions claiming one assembly file — the
                        // mirror of the `by_identity` contest above, and refused
                        // the same way. Silently keeping the first would call
                        // the file complete under one of two names with no
                        // reason to prefer either.
                        slot.remove();
                        contested_file_ids.insert(file_id);
                        report.contested.push(file_verification.filename.clone());
                    }
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(verified_filename);
                    }
                }
            }

            matched.into_iter().collect()
        };

        if files_to_complete.is_empty() {
            return Ok(report);
        }

        for (file_id, verified_filename) in &files_to_complete {
            let Some(mut identity) = self.effective_file_identity(job_id, *file_id) else {
                continue;
            };
            if identity.current_filename == *verified_filename {
                continue;
            }
            identity.current_filename = verified_filename.clone();
            identity.canonical_filename = Some(verified_filename.clone());
            if let Some(classification) =
                Self::canonical_archive_identity_from_filename(verified_filename)
            {
                identity.classification = Some(classification);
            }
            identity.classification_source = crate::jobs::record::FileIdentitySource::Par2;
            self.set_file_identity(job_id, identity)?;
        }

        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(report);
            };
            for (file_id, _) in &files_to_complete {
                let Some(file) = state.assembly.file_mut(*file_id) else {
                    continue;
                };
                file.mark_complete();
            }
        }

        let complete_entries: Vec<(u32, String, Option<[u8; 16]>)> = files_to_complete
            .iter()
            .map(|(file_id, filename)| {
                crate::runtime::perf_probe::record(
                    "download.file_progress.complete_file_row_covers_restart",
                    std::time::Duration::ZERO,
                );
                (
                    file_id.file_index,
                    filename.clone(),
                    Self::expected_hash_for_verified_file(*file_id, &existing_hashes),
                )
            })
            .collect();
        self.db_blocking(move |db| {
            db.complete_files(
                job_id,
                &complete_entries,
                crate::jobs::persistence::CompletedHashProvenance::Verified,
            )
        })
        .await
        .map_err(|error| format!("failed to persist PAR2-reconciled files: {error}"))?;

        for (file_id, _filename) in &files_to_complete {
            self.pending_file_progress.remove(file_id);
            self.persisted_file_progress.remove(file_id);
            self.file_hash_states.remove(file_id);
            self.expected_file_crcs.remove(file_id);
            self.file_hash_reread_required.remove(file_id);
            self.refresh_archive_state_for_completed_file(job_id, *file_id, true)
                .await;
        }

        report.completed = files_to_complete.len();
        Ok(report)
    }

    /// Classify a job that still has incomplete data files after a PAR2 pass
    /// reconciled — into the failure it actually is, or into no failure at all.
    ///
    /// # Why a bare count was the wrong question
    ///
    /// The veto this replaces compared `complete_data_file_count()` against
    /// `data_file_count()` and failed the job on the difference, so every cause
    /// reported identically: a genuinely undownloadable unprotected file, an
    /// obfuscated name the reconciler could not bind, and a contested alias
    /// were one message. Job 11737 was the middle case wearing the first one's
    /// clothes — a standalone MKV that PAR2 had repaired and re-verified, failed
    /// for an article bitmap that the repair had already made irrelevant.
    ///
    /// # The invariant
    ///
    /// Once PAR2 has repaired and re-verified a protected output, that
    /// verification is authoritative. Missing article state remains diagnostic
    /// history; it cannot independently fail the repaired file. Both reference
    /// implementations draw the line in exactly this place — NZBGet conjoins its
    /// health test with `psSkipped`, so a successful par status removes article
    /// state from the decision outright, and SABnzbd derives its whole par
    /// verdict from the repair's own re-verification and never re-consults the
    /// articles afterwards.
    ///
    /// # Nothing here fails the job
    ///
    /// The invariant is about the *pass*, not about one file: once a PAR2
    /// verification has succeeded, no article-completeness state may fail the
    /// job — protected or unprotected. Both oracles are absolute about this.
    /// NZBGet's `FAILURE/HEALTH` requires `(psNone || psSkipped)`, so a
    /// successful par status takes health out of the verdict entirely
    /// (`DownloadInfo.cpp` `MakeTextStatus`); SABnzbd never sets `fail_msg`
    /// from missing articles at all — every one of its failure messages comes
    /// from unpack, repair, encryption or an unwanted extension.
    ///
    /// The concrete case that forced this: a 1.09 GB job whose payload PAR2
    /// repaired and re-verified, failed because a 738 KB `.nfo` — which no
    /// recovery set ever covered — was short a few articles. Health 999. Both
    /// oracles deliver that job. So does the final move, which relocates the
    /// working directory wholesale rather than a completeness-filtered
    /// selection, so the bytes reach the user either way and refusing them buys
    /// nothing.
    ///
    /// What survives is the *distinction*. An unprotected file short of
    /// articles is ordinary Usenet damage: warn, deliver, never fail. A
    /// protected file left incomplete after an authoritative pass is our own
    /// reconciliation failing — the recovery set had a verdict for it either
    /// way — and what to do about that turns on one question: are the verified
    /// bytes still reachable?
    ///
    /// If they are (a real file of the described length, or a volume of a
    /// direct set still routing), the defect is bookkeeping. Warn loudly, keep
    /// the download. If they are not, the verdict is vouching for bytes that
    /// are nowhere, and delivering the job would ship a hole as if it were
    /// verified — so that, and only that, still fails.
    pub(in crate::pipeline) fn classify_incomplete_after_par2(
        &self,
        job_id: JobId,
        reconciliation: &Par2Reconciliation,
        context: &str,
    ) -> Option<Par2IncompleteReport> {
        let state = self.jobs.get(&job_id)?;
        let incomplete: Vec<NzbFileId> = state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    )
                    // A part of a split set the verdict already joined is a
                    // spent input, not an outstanding file: its bytes are
                    // inside the output that was vouched for.
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .map(|file| file.file_id())
            .collect();
        if incomplete.is_empty() {
            return None;
        }

        // Every parsed, described set receives its own gate pass.  A binding to
        // any such set is protected, regardless of which set happens to be the
        // compatibility view during this particular re-entry.
        let servable_set_ids = self.par2_servable_set_ids(job_id);
        let (protected, unprotected): (Vec<_>, Vec<_>) =
            incomplete.into_iter().partition(|file_id| {
                self.resolve_par2_file_binding(*file_id)
                    .is_some_and(|binding| servable_set_ids.contains(&binding.recovery_set_id))
            });

        // A set without a posted index cannot receive a pass at all.  Keep that
        // distinct from an ordinary unprotected file in the diagnostic.
        let (unservable_set, unprotected): (Vec<_>, Vec<_>) =
            unprotected.into_iter().partition(|file_id| {
                self.file_is_described_only_by_an_unservable_recovery_set(*file_id)
            });

        // Furniture the recovery set happens to cover. It is delivered as it
        // stands — the verdict arm above already declined to spend a full-set
        // read repairing it — so it never reaches the proven/unproven question
        // that decides whether a protected file can fail a job.
        let ignore_extensions = self.par2_ignore_extensions();
        let (ignorable, protected): (Vec<_>, Vec<_>) = protected.into_iter().partition(|file_id| {
            self.par2_bound_file_is_ignorable(job_id, *file_id, &ignore_extensions)
        });

        let names = |ids: &[NzbFileId]| -> String {
            ids.iter()
                .filter_map(|file_id| self.current_filename_for_file_id(job_id, *file_id))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let (proven, unproven): (Vec<_>, Vec<_>) = protected
            .into_iter()
            .partition(|file_id| self.par2_output_presence_proven(job_id, *file_id));

        let mut parts = Vec::new();
        if !unprotected.is_empty() {
            // Ordinary Usenet damage on a file no recovery set covered. It is
            // delivered as-is, short articles and all, exactly as both oracles
            // deliver it.
            parts.push(format!(
                "delivering {} unprotected file(s) short of articles, unrepairable by design: {}",
                unprotected.len(),
                names(&unprotected)
            ));
        }
        if !unservable_set.is_empty() {
            parts.push(format!(
                "delivering {} file(s) covered only by a recovery set with no posted index: {}",
                unservable_set.len(),
                names(&unservable_set)
            ));
        }
        if !ignorable.is_empty() {
            parts.push(format!(
                "delivering {} damaged ignorable file(s) the recovery set covered: {}",
                ignorable.len(),
                names(&ignorable)
            ));
        }
        let detail = || {
            if reconciliation.has_failures() {
                reconciliation.failure_detail()
            } else {
                "no PAR2 verdict claimed them".to_string()
            }
        };
        if !proven.is_empty() {
            parts.push(format!(
                "BUG: {} PAR2-protected file(s) stayed incomplete after an authoritative \
                 verification vouched for them ({}): {}. The verified bytes are on disk and \
                 are delivered; this is a reconciliation defect, not a download failure",
                proven.len(),
                names(&proven),
                detail()
            ));
        }
        if !unproven.is_empty() {
            parts.push(format!(
                "{} PAR2-protected file(s) stayed incomplete and their verified bytes are \
                 nowhere on disk ({}): {}",
                unproven.len(),
                names(&unproven),
                detail()
            ));
        }
        Some(Par2IncompleteReport {
            message: format!("{context}: {}", parts.join("; ")),
            unproven_protected: unproven.len(),
        })
    }

    /// Incomplete data files the recovery set actually describes.
    ///
    /// The completion gate's question after a PAR2 verdict is not "is every
    /// file whole" but "is anything left that PAR2 could still act on".
    /// Ignorable furniture is not: it is delivered as it stands, so counting it
    /// here would re-arm the gate on a file nothing is going to change.
    pub(in crate::pipeline) fn incomplete_par2_protected_data_file_count(
        &self,
        job_id: JobId,
    ) -> usize {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let ignore_extensions = self.par2_ignore_extensions();
        let servable_set_ids = self.par2_servable_set_ids(job_id);
        state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    )
                    && self
                        .resolve_par2_file_binding(file.file_id())
                        .is_some_and(|binding| servable_set_ids.contains(&binding.recovery_set_id))
                    && !self.par2_bound_file_is_ignorable(
                        job_id,
                        file.file_id(),
                        &ignore_extensions,
                    )
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .count()
    }

    /// Whether a settled verdict has left protected files outstanding whose
    /// verified bytes are demonstrably still on disk.
    ///
    /// The current set's portion of
    /// [`Self::incomplete_par2_protected_data_file_count`], narrowed to an
    /// incomplete file whose verified bytes can be shown to be present at its
    /// described length.
    ///
    /// That narrowing carries the whole distinction. Bytes that are present
    /// under a verdict which already vouched for them mean the download is
    /// sound and our own binding is not, and re-reading the recovery set cannot
    /// change either fact. Bytes that are absent or short mean something really
    /// is missing, which is a question the authoritative pass alone can answer.
    fn settled_verdict_left_only_proven_protected_files(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let ignore_extensions = self.par2_ignore_extensions();
        let outstanding: Vec<NzbFileId> = state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    )
                    && self
                        .resolve_par2_file_binding(file.file_id())
                        .is_some_and(|binding| binding.recovery_set_id == set_id)
                    && !self.par2_bound_file_is_ignorable(
                        job_id,
                        file.file_id(),
                        &ignore_extensions,
                    )
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .map(|file| file.file_id())
            .collect();
        !outstanding.is_empty()
            && outstanding
                .into_iter()
                .all(|file_id| self.par2_output_presence_proven(job_id, file_id))
    }

    /// The ignore-extension list in force for this job.
    ///
    /// Process-global configuration, read where it is used rather than cached,
    /// exactly as the repair memory limit is. The test hook exists so the
    /// "override disables it" case can be exercised without mutating a
    /// process-global environment other tests are reading concurrently.
    pub(in crate::pipeline) fn par2_ignore_extensions(&self) -> Vec<String> {
        #[cfg(test)]
        if let Some(extensions) = self.par2_ignore_extensions_override.as_ref() {
            return extensions.clone();
        }
        configured_par2_ignore_extensions()
    }

    /// Whether the file this assembly entry binds to is ignorable furniture,
    /// judged by the name the recovery set describes it under as well as the
    /// name it currently carries.
    fn par2_bound_file_is_ignorable(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        ignore_extensions: &[String],
    ) -> bool {
        if ignore_extensions.is_empty() {
            return false;
        }
        if self
            .current_filename_for_file_id(job_id, file_id)
            .is_some_and(|filename| par2_damage_ignorable(&filename, ignore_extensions))
        {
            return true;
        }
        self.resolve_par2_file_binding(file_id)
            .and_then(|binding| {
                self.par2_set_for(job_id, binding.recovery_set_id)
                    .and_then(|set| set.file_description(&binding.par2_file_id))
                    .map(|description| description.filename.clone())
            })
            .is_some_and(|filename| par2_damage_ignorable(&filename, ignore_extensions))
    }

    /// The damaged and missing descriptions in a verdict, when every one of
    /// them is ignorable furniture.
    ///
    /// `None` means the ordinary repair/fail ladder applies: either the verdict
    /// carries no damage at all, or something that is not furniture is damaged
    /// too. That second case still fails on short recovery, and deliberately —
    /// a payload file's missing slices are unknowns in every equation the solve
    /// has, so the furniture's blocks cannot be excused out of it. Both
    /// reference downloaders draw the line in the same place.
    ///
    /// A `Renamed` verdict is not damage but it is not this function's business
    /// either: placement decides what to do with it, so its presence sends the
    /// verdict down the ordinary path untouched.
    fn par2_damage_is_only_ignorable(
        &self,
        verification: &par2_rs::VerificationResult,
    ) -> Option<Vec<String>> {
        let ignore_extensions = self.par2_ignore_extensions();
        if ignore_extensions.is_empty() {
            return None;
        }
        let mut ignorable = Vec::new();
        for file in &verification.files {
            if matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                continue;
            }
            if !matches!(
                file.status,
                par2_rs::verify::FileStatus::Damaged(_) | par2_rs::verify::FileStatus::Missing
            ) {
                return None;
            }
            if !par2_damage_ignorable(&file.filename, &ignore_extensions) {
                return None;
            }
            ignorable.push(file.filename.clone());
        }
        (!ignorable.is_empty()).then_some(ignorable)
    }

    /// Whether the bytes a PAR2 verdict vouched for are still reachable.
    ///
    /// Two ways they can be: a real file of the described length at the
    /// canonical or the current name, or a volume of a direct set that is still
    /// routing — those are verified through the set's own access layer and are
    /// never written under their own name, so having no file is what correct
    /// looks like for them.
    fn par2_output_presence_proven(&self, job_id: JobId, file_id: NzbFileId) -> bool {
        let Some(binding) = self.resolve_par2_file_binding(file_id) else {
            return false;
        };
        if self
            .direct_par2_overlay(job_id)
            .and_then(|overlay| overlay.owner_of(&binding.par2_file_id))
            .and_then(|index| self.direct_store.set(job_id, index))
            .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
        {
            return true;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let canonical = self
            .par2_set_for(job_id, binding.recovery_set_id)
            .and_then(|set| set.file_description(&binding.par2_file_id))
            .map(|desc| {
                state
                    .working_dir
                    .join(sanitize_download_filename(&desc.filename))
            });
        canonical
            .into_iter()
            .chain(std::iter::once(binding.path))
            .any(|path| {
                std::fs::metadata(&path)
                    .ok()
                    .filter(|meta| meta.is_file())
                    .is_some_and(|meta| meta.len() == binding.described_length)
            })
    }

    /// Reconcile a PAR2 verification onto the assembly, then decide whether what
    /// is left standing is a failure.
    ///
    /// Every PAR2 exit — the clean fast paths and both repair paths — funnels
    /// through here, so the binding rules and the classification are stated once
    /// instead of once per exit. They had already drifted apart across five
    /// copies; each copy is a place for the next one to drift again.
    pub(in crate::pipeline) async fn reconcile_and_classify_par2_verification(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        has_crc_failures: bool,
        context: &str,
    ) -> Result<(), String> {
        let reconciliation = self
            .reconcile_verified_par2_files(job_id, verification)
            .await?;
        if reconciliation.completed > 0 || reconciliation.has_failures() {
            info!(
                job_id = job_id.0,
                completed = reconciliation.completed,
                unbound = reconciliation.unbound.len(),
                contested = reconciliation.contested.len(),
                length_mismatch = reconciliation.length_mismatch.len(),
                context,
                "PAR2 reconciliation"
            );
        }
        // An extraction retry is still owed a pass, so an incomplete count here
        // is not a verdict yet — the retry is what decides. Reconciliation still
        // had to run first: the retry reads the files this pass just promoted.
        if has_crc_failures {
            return Ok(());
        }
        if let Some(report) = self.classify_incomplete_after_par2(job_id, &reconciliation, context)
        {
            if report.unproven_protected > 0 {
                return Err(report.message);
            }
            warn!(job_id = job_id.0, "{}", report.message);
        }
        Ok(())
    }

    /// Everything that happens after the repairer returns, for every path that
    /// runs it.
    ///
    /// # Why it is shared
    ///
    /// Both repair call sites used to carry their own copy of this tail, and the
    /// copies had drifted into different answers to the same question. One
    /// re-read the installed output and judged *that*; the other trusted the
    /// repairer's staged `outcome.verification` and never looked at what
    /// actually landed on disk. A repair that does not verify what it installed
    /// is not verified, so both now run the authoritative pass — which is also
    /// where the direct-set damage adjustments and the volume-safety
    /// recomputation live, so the path that skipped it was missing those too.
    ///
    /// # Ordering
    ///
    /// `RepairComplete` is emitted last: after canonical placement, post-repair
    /// verification, identity reconciliation and durable persistence have all
    /// succeeded. Any failure among them emits `RepairFailed` instead. Emitting
    /// completion first produced the contradictory sequence this replaces —
    /// `RepairComplete`, then the job failing a moment later, with nothing on
    /// the event stream to say the repair had not held.
    ///
    /// This mirrors what both reference implementations announce: SABnzbd runs
    /// an explicit "verifying repaired files" phase before it accepts a repair,
    /// and NZBGet reaches `psRepaired` only from a `Process(true)` that came
    /// back successful, having passed through `ptVerifyingRepaired` first.
    pub(in crate::pipeline) async fn finish_par2_repair(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        pre_repair: &par2_rs::VerificationResult,
        outcome: par2_rs::Par2RepairOutcome,
        has_crc_failures: bool,
    ) {
        // Covers the whole tail including every failure exit below, which the
        // per-stage stamps cannot: a repair that is rejected still spent the
        // time it spent.
        let _finish_scope = crate::runtime::perf_probe::scope("par2_repair.finish");
        let mut stage_start = std::time::Instant::now();
        let slices_repaired = par2_repair_slices_repaired(pre_repair);
        info!(
            job_id = job_id.0,
            status = ?outcome.status,
            slices_repaired,
            bytes_copied = outcome.bytes_copied,
            bytes_reconstructed = outcome.bytes_reconstructed,
            files_complete = outcome.files_complete,
            files_renamed = outcome.files_renamed,
            files_damaged = outcome.files_damaged,
            files_missing = outcome.files_missing,
            "PAR2 repair wrote its outputs — verifying what was installed"
        );

        self.emit_job_verification_started(job_id);
        let (mut post_repair_verification, post_repair_placement_plan) = match self
            .verify_repaired_par2_files_with_placement(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone(),
                pre_repair,
            )
            .await
        {
            Ok(result) => result,
            Err(message) => return self.fail_par2_repair(job_id, message),
        };
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.verify_repaired", stage_start);

        if let Some(msg) = par2_post_repair_damage_failure(&post_repair_verification) {
            return self.fail_par2_repair(job_id, msg);
        }

        // Rename obfuscated files using PAR2 metadata (16KB hash matching).
        // Must happen after repair and before extraction retry/finalize.
        let deobfuscation = self.try_deobfuscate_files_with_par2(job_id).await;
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.deobfuscate", stage_start);
        let deobfuscation_canonical_file_ids = deobfuscation
            .canonical_description_file_ids
            .get(&par2_set.recovery_set_id)
            .map(|file_ids| file_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let deobfuscation_moved_current_canonical = !deobfuscation_canonical_file_ids.is_empty();
        let placement_moves_paths = !post_repair_placement_plan.swaps.is_empty()
            || !post_repair_placement_plan.renames.is_empty();
        if let Err(error) = self
            .apply_placement_plan_for_retry_or_repair(
                job_id,
                working_dir.clone(),
                &post_repair_placement_plan,
            )
            .await
        {
            return self.fail_par2_repair(job_id, error);
        }
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.apply_placement", stage_start);

        // A content rename invalidates this set's verdict only when it populated
        // one of this set's canonical description paths. Moving a damaged source
        // aside as `.duplicateN` leaves the selectively verified canonical bytes
        // untouched, while a rename into a free canonical path was identified
        // from its 16 KiB prefix and still needs strict whole-file proof.
        //
        // When no canonical description path moved, the earlier answer still
        // describes the disk exactly, and re-reading would throw away the whole
        // point of the selective post-repair pass: it reads only what the repair
        // rewrote.
        // `misplaced_before_placement` closes the gap the other two conditions
        // leave: a file can verify as `Renamed` and still produce no plan entry
        // (a rename whose path has no file name lands in `unresolved`), and
        // accepting a repair whose set was never re-read where it now sits is
        // exactly what this pass exists to prevent.
        let misplaced_before_placement = post_repair_verification
            .files
            .iter()
            .any(|file| !matches!(file.status, par2_rs::verify::FileStatus::Complete));
        if deobfuscation_moved_current_canonical
            || placement_moves_paths
            || misplaced_before_placement
        {
            // Only the moved files owe a re-read. A rename moves no bytes:
            // every Complete entry whose canonical path did not change was
            // proven where it sat minutes ago, in this same flow, and reading
            // it again answers a question the disk already answered. What
            // genuinely needs strict proof at its canonical name is exactly
            // the moved set — deobfuscation targets identified from a 16 KiB
            // prefix, placement-plan swaps and renames, and anything the
            // selective pass could not call Complete where it stood.
            let mut must_read: HashSet<par2_rs::FileId> =
                deobfuscation_canonical_file_ids.iter().copied().collect();
            for entry in &post_repair_placement_plan.renames {
                must_read.insert(entry.file_id);
            }
            for (left, right) in &post_repair_placement_plan.swaps {
                must_read.insert(left.file_id);
                must_read.insert(right.file_id);
            }
            for file in &post_repair_verification.files {
                if !matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                    must_read.insert(file.file_id);
                }
            }
            let mut must_read: Vec<par2_rs::FileId> = must_read.into_iter().collect();
            must_read.sort_unstable_by_key(|file_id| *file_id.as_bytes());
            info!(
                job_id = job_id.0,
                deobfuscated = deobfuscation.renamed,
                deobfuscation_moved_current_canonical,
                placement_moves_paths,
                misplaced_before_placement,
                moved_files_reread = must_read.len(),
                carried = post_repair_verification
                    .files
                    .len()
                    .saturating_sub(must_read.len()),
                "paths moved after repair — re-verifying the moved files where they now sit"
            );
            let settled =
                match self
                    .run_par2_placement_pass(
                        job_id,
                        Arc::clone(&par2_set),
                        working_dir.clone(),
                        Par2PassScope::Selected(must_read),
                    )
                    .await
                {
                    Ok((fresh, _)) => {
                        // The merge below iterates the BASE's files, so a fresh
                        // entry for a description outside it — a deobfuscation
                        // extra the recovery data does not protect — would drop
                        // out of the merged result and out of the damage check
                        // with it. Every file this pass just read must be Complete
                        // where it now sits, extras included, so they are judged
                        // here on the fresh result directly.
                        if let Some(failed) = fresh.files.iter().find(|file| {
                            !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                        }) {
                            let msg = format!(
                                "PAR2 repair completed but {} was not intact at its canonical \
                             path after deobfuscation and placement",
                                failed.filename
                            );
                            return self.fail_par2_repair(job_id, msg);
                        }
                        // Merge THEN settle, the order the selective post-repair
                        // pass documents as load-bearing: the merge recomputes
                        // totals over carried and fresh entries alike, and settle
                        // re-forgives what the recompute resurrected.
                        let mut merged = par2_rs::verify::merge_verification_results(
                            &par2_set,
                            &post_repair_verification,
                            fresh,
                        );
                        self.settle_par2_pass_result(job_id, &mut merged, false);
                        merged
                    }
                    Err(message) => return self.fail_par2_repair(job_id, message),
                };
            if par2_verification_needs_repair(&settled) {
                let msg = format!(
                    "PAR2 repair completed but verification after deobfuscation and placement \
                     found {} damaged slices or file placements remaining",
                    settled.total_missing_blocks
                );
                return self.fail_par2_repair(job_id, msg);
            }
            post_repair_verification = settled;
            stage_start = note_par2_repair_stage(
                job_id,
                "par2_repair.finish.verify_after_placement",
                stage_start,
            );
        }
        self.retry_par2_authoritative_identity(job_id).await;
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.identity", stage_start);
        let registration = match self
            .register_verified_par2_rar_outputs(job_id, &post_repair_verification)
            .await
        {
            Ok(registration) => registration,
            Err(error) => return self.fail_par2_repair(job_id, error),
        };
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.register_outputs", stage_start);
        // The descriptions the repair wrote, taken from the pre-repair verdict
        // the repairer itself acted on rather than re-derived from names here.
        let rewritten: HashSet<par2_rs::FileId> =
            par2_repair_write_set(pre_repair).into_iter().collect();
        self.refresh_verified_complete_archive_topologies(
            job_id,
            &post_repair_verification,
            &rewritten,
        )
        .await;
        // Outputs the NZB never carried have no file id to travel through the
        // refresh set, so their sets are invalidated from the registration.
        self.invalidate_rar_plans_for_repaired_sets(job_id, registration.set_names);
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.refresh_topologies", stage_start);
        if let Err(error) = self
            .reconcile_and_classify_par2_verification(
                job_id,
                &post_repair_verification,
                has_crc_failures,
                "PAR2 repair",
            )
            .await
        {
            return self.fail_par2_repair(job_id, error);
        }
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.reconcile", stage_start);
        // Repair rewrote bytes; digests streamed before it describe content
        // that is gone. Every `Complete` entry in the merged result is vouched
        // by a pass that read the disk — the files the repair rewrote by the
        // selective pass just above, the ones it left alone by the pre-repair
        // pass that decided they were complete — so the described digests are
        // proven observations, not expectations.
        if let Err(error) = self
            .refresh_authoritative_verified_hashes(job_id, &par2_set, &post_repair_verification)
            .await
        {
            return self.fail_par2_repair(job_id, error);
        }
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.refresh_hashes", stage_start);

        // Only now is the repair a fact worth announcing.
        //
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Complete,
            u64::from(slices_repaired),
        );
        let _ = self.event_tx.send(PipelineEvent::RepairComplete {
            job_id,
            slices_repaired,
        });

        let set_id = par2_set.recovery_set_id;
        let _ = self
            .settle_par2_set(job_id, set_id, Par2SetSettlementReason::Repaired)
            .await;
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.mark_verified", stage_start);
        if !self.par2_verified.contains(&job_id) {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            self.schedule_job_completion_check(job_id);
            return;
        }

        // Leftovers are purged when the aggregate settles — see
        // `mark_par2_verified`, which is reached from here through
        // `settle_par2_set` and also from a clean final set that never
        // runs this tail at all.
        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));

        if has_crc_failures {
            let cleared = self.failed_extractions.get(&job_id).map_or(0, HashSet::len);
            self.replace_failed_extraction_members(job_id, HashSet::new());
            if cleared > 0 {
                info!(
                    job_id = job_id.0,
                    cleared, "cleared failed extractions for post-repair retry"
                );
            }
        }

        // A repaired interior RAR volume is only visible to the incremental
        // scheduler after its synchronous refresh. Do that before scheduling
        // another completion check, or a stale WaitingForVolumes plan can
        // re-enter PAR2 forever.
        if has_crc_failures || self.job_has_live_rar_waiting_for_missing_volumes(job_id) {
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            note_par2_repair_stage(job_id, "par2_repair.finish.retry_extraction", stage_start);
            return;
        }

        self.reconcile_job_progress(job_id).await;
        note_par2_repair_stage(job_id, "par2_repair.finish.reconcile_progress", stage_start);
        self.schedule_job_completion_check(job_id);
    }

    /// Fail a job that was mid-repair, announcing it as a repair failure.
    ///
    /// Every non-success exit from [`Self::finish_par2_repair`] comes through
    /// here, so a job can no longer fail silently after `RepairComplete` has
    /// already told the UI that the repair held.
    /// Remove what the repair left behind, now that the repair has been
    /// accepted.
    ///
    /// Mirrors NZBGet's `DeleteLeftovers()`, which it reaches only from the
    /// branch where `Process(true)` came back successful, and SABnzbd's
    /// `deletables` after a finished repair. The gating is the whole point: on
    /// any failure these files are the evidence, and this is not reached.
    ///
    /// Only entries that *appeared during* the repair are candidates, and only
    /// when they are neither an NZB entry under any of its names nor a file the
    /// recovery set describes. A file the repair reconstructed from `Missing`
    /// lands at a described name and is therefore never a candidate — the test
    /// is membership, not a suffix convention borrowed from another crate.
    pub(in crate::pipeline) fn purge_par2_repair_leftovers(&mut self, job_id: JobId) {
        let Some(before) = self.par2_pre_repair_dir_entries.remove(&job_id) else {
            return;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let working_dir = state.working_dir.clone();

        let mut keep = HashSet::<String>::new();
        for file in state.assembly.files() {
            keep.insert(sanitize_download_filename(file.filename()));
            if let Some(identity) = self.effective_file_identity(job_id, file.file_id()) {
                keep.insert(sanitize_download_filename(&identity.current_filename));
                keep.insert(sanitize_download_filename(&identity.source_filename));
                if let Some(canonical) = identity.canonical_filename.as_ref() {
                    keep.insert(sanitize_download_filename(canonical));
                }
            }
        }
        for set_id in self.par2_servable_set_ids(job_id) {
            if let Some(set) = self.par2_set_for(job_id, set_id) {
                for desc in set.files.values() {
                    keep.insert(sanitize_download_filename(&desc.filename));
                }
            }
        }

        for name in directory_entry_names(&working_dir) {
            if before.contains(&name) || keep.contains(&sanitize_download_filename(&name)) {
                continue;
            }
            let path = working_dir.join(&name);
            if !path.is_file() {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => info!(
                    job_id = job_id.0,
                    path = %path.display(),
                    "removed repair leftover after acceptance"
                ),
                Err(error) => warn!(
                    job_id = job_id.0,
                    path = %path.display(),
                    error = %error,
                    "could not remove repair leftover"
                ),
            }
        }
    }

    pub(in crate::pipeline) fn fail_par2_repair(&mut self, job_id: JobId, error: String) {
        // Dropped unread: the artefacts stay on disk for diagnosis, and the
        // snapshot must not survive into the next attempt.
        self.par2_pre_repair_dir_entries.remove(&job_id);
        warn!(job_id = job_id.0, error = %error, "PAR2 repair failed");
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Failed,
            0,
        );
        let _ = self.event_tx.send(PipelineEvent::RepairFailed {
            job_id,
            error: error.clone(),
        });
        if let Some(set_id) = self.par2_served_set_id(job_id) {
            let _ = self.mark_par2_set_failed(job_id, set_id, error);
            self.finish_or_rearm_after_par2_set_failure(job_id);
        } else {
            self.fail_job(job_id, error);
        }
    }

    /// After an *authoritative* post-repair verification, re-persist every
    /// confirmed file's digest from the recovery set with `Verified`
    /// provenance.
    ///
    /// Repair rewrites bytes in place, so a digest streamed before the
    /// rewrite describes content that is gone; left standing, a restart
    /// would load it as trusted and compare stale bytes' MD5 against the
    /// recovery set forever. Persisting the description's digest is sound
    /// here — and only here — because every `Complete` entry the caller hands
    /// over is vouched by a pass that read the bytes off disk. The quick paths'
    /// synthetic all-valid results, which read nothing, must never reach this
    /// function.
    ///
    /// # The vouching is two passes, not one
    ///
    /// The post-repair result is a merge, and each half is proven by its own
    /// read:
    ///
    /// - the files the repair **rewrote** are proven by the post-repair pass,
    ///   which read them back after the repair installed them;
    /// - the files it **did not touch** are proven by the pre-repair pass,
    ///   which read them in this same flow — that pass is what decided they
    ///   were complete, and being complete is exactly why the repair left them
    ///   alone.
    ///
    /// Both halves are measured bytes; neither is a description standing in for
    /// a read. What the merge accepts is a *window* rather than a gap in
    /// evidence: an untouched file that some other writer corrupts between the
    /// two passes still carries its earlier verdict. That is the same trust
    /// class as an in-stream claim relied on across the same interval, and it
    /// is stated at the merge site in
    /// [`Pipeline::verify_repaired_par2_files_with_placement`].
    ///
    /// A `Verified` digest is attached only through an UNAMBIGUOUS identity:
    /// every alias a name resolves to is kept (never first-wins), a
    /// `Renamed` result prefers the actual verified path over the expected
    /// description name, each verification entry must resolve to exactly one
    /// assembly file, no two entries may claim the same file, and the file
    /// on disk must measure exactly the described length. Anything short of
    /// that keeps whatever digest state already exists.
    pub(crate) async fn refresh_authoritative_verified_hashes(
        &mut self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
        verification: &par2_rs::VerificationResult,
    ) -> Result<(), String> {
        struct ResolvedRefresh {
            file_index: u32,
            filename: String,
            path: std::path::PathBuf,
            described_length: u64,
            hash: [u8; 16],
        }

        let resolved: Vec<ResolvedRefresh> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };
            let working_dir = state.working_dir.clone();

            // Every alias each name could mean — never collapsed first-wins.
            // Current filenames are also kept separately: a `Renamed` result
            // names a physical path, and physical paths may only resolve
            // against where files live NOW. The path the pre-plan
            // verification saw a file at can, by refresh time, be nothing
            // but some other file's immutable source alias — resolving a
            // renamed result through source/canonical aliases is how a
            // digest lands on the wrong file.
            let mut by_name = HashMap::<String, Vec<NzbFileId>>::new();
            let mut by_current = HashMap::<String, Vec<NzbFileId>>::new();
            for file in state.assembly.files() {
                if !file.is_complete() {
                    continue;
                }
                let file_id = file.file_id();
                let mut aliases: Vec<String> = Vec::new();
                let current_name;
                if let Some(identity) = self.effective_file_identity(job_id, file_id) {
                    current_name = identity.current_filename.clone();
                    aliases.push(identity.current_filename);
                    aliases.push(identity.source_filename);
                    if let Some(canonical) = identity.canonical_filename {
                        aliases.push(canonical);
                    }
                } else {
                    current_name = file.filename().to_string();
                    aliases.push(current_name.clone());
                }
                aliases.sort();
                aliases.dedup();
                for alias in aliases {
                    let ids = by_name.entry(alias).or_default();
                    if !ids.contains(&file_id) {
                        ids.push(file_id);
                    }
                }
                let ids = by_current.entry(current_name).or_default();
                if !ids.contains(&file_id) {
                    ids.push(file_id);
                }
            }

            let mut matched = HashMap::<NzbFileId, (String, u64, [u8; 16])>::new();
            let mut contested: HashSet<NzbFileId> = HashSet::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }
                let Some(description) = par2_set.file_description(&file_verification.file_id)
                else {
                    continue;
                };

                // The name the verified bytes actually live under outranks
                // the name the description expected: for `Renamed`, that is
                // the renamed path. Renamed results are physical-path
                // claims, so they resolve against CURRENT names only —
                // never through source or canonical aliases.
                let mut candidate_names: Vec<String> = Vec::new();
                let name_map = match &file_verification.status {
                    par2_rs::verify::FileStatus::Renamed(path) => {
                        if let Some(filename) = path.file_name() {
                            candidate_names.push(filename.to_string_lossy().to_string());
                        }
                        &by_current
                    }
                    _ => &by_name,
                };
                candidate_names.push(file_verification.filename.clone());

                // The first name that resolves at all decides — and it must
                // resolve to exactly one file, or this entry is ambiguous
                // and attaches nothing.
                let resolved_id = candidate_names.iter().find_map(|name| {
                    let ids = name_map.get(name)?;
                    Some(if ids.len() == 1 { Some(ids[0]) } else { None })
                });
                let Some(Some(file_id)) = resolved_id else {
                    if resolved_id.is_some() {
                        crate::runtime::perf_probe::record(
                            "completion.post_repair.refresh_skipped.ambiguous_alias",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                    continue;
                };
                // Two verification entries claiming one assembly file prove
                // the mapping is not one-to-one; neither may attach.
                if matched.remove(&file_id).is_some() || contested.contains(&file_id) {
                    contested.insert(file_id);
                    crate::runtime::perf_probe::record(
                        "completion.post_repair.refresh_skipped.contested_file",
                        std::time::Duration::from_nanos(1),
                    );
                    continue;
                }
                let current_filename = self
                    .current_filename_for_file_id(job_id, file_id)
                    .unwrap_or_else(|| file_verification.filename.clone());
                matched.insert(
                    file_id,
                    (current_filename, description.length, description.hash_full),
                );
            }

            matched
                .into_iter()
                .map(
                    |(file_id, (filename, described_length, hash))| ResolvedRefresh {
                        file_index: file_id.file_index,
                        path: working_dir.join(&filename),
                        filename,
                        described_length,
                        hash,
                    },
                )
                .collect()
        };

        // The digest may only attach to a file whose bytes measure exactly
        // the described length. `received_bytes` is unusable here — repair
        // reconciliation marks files complete with the encoded NZB total —
        // so ask the filesystem; the authoritative pass just read these
        // files, and this is one bounded stat per confirmed file on the
        // exceptional post-repair path.
        let mut entries: Vec<(u32, String, Option<[u8; 16]>)> = Vec::new();
        for refresh in resolved {
            match tokio::fs::metadata(&refresh.path).await {
                Ok(metadata) if metadata.len() == refresh.described_length => {
                    entries.push((refresh.file_index, refresh.filename, Some(refresh.hash)));
                }
                Ok(_) | Err(_) => {
                    crate::runtime::perf_probe::record(
                        "completion.post_repair.refresh_skipped.length_mismatch",
                        std::time::Duration::from_nanos(1),
                    );
                }
            }
        }

        if entries.is_empty() {
            return Ok(());
        }
        crate::runtime::perf_probe::record(
            "completion.post_repair.verified_hashes_refreshed",
            std::time::Duration::from_nanos(1),
        );
        self.db_blocking(move |db| {
            db.complete_files(
                job_id,
                &entries,
                crate::jobs::persistence::CompletedHashProvenance::Verified,
            )
        })
        .await
        .map_err(|error| format!("failed to refresh post-repair verified hashes: {error}"))
    }

    /// `rewritten` is the repair's write set, empty on a pass that repaired
    /// nothing. See
    /// [`Self::verified_complete_archive_file_ids_needing_refresh`].
    pub(in crate::pipeline) async fn refresh_verified_complete_archive_topologies(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> usize {
        let targets =
            self.verified_complete_archive_refresh_targets(job_id, verification, rewritten);
        let file_ids: Vec<NzbFileId> = targets.iter().map(|(file_id, _)| *file_id).collect();
        if !file_ids.is_empty() {
            info!(
                job_id = job_id.0,
                files = file_ids.len(),
                "refreshing archive topology from verified PAR2 outputs"
            );
        }
        // Only the files the repair actually rewrote invalidate a plan. A
        // renamed file, or one whose set is being given its first topology, is
        // ordinary progress and the refresh walk below is the whole of it.
        let rewritten_file_ids: Vec<NzbFileId> = targets
            .iter()
            .filter_map(|(file_id, was_rewritten)| was_rewritten.then_some(*file_id))
            .collect();
        let rewritten_set_names = self.rar_set_names_for_files(job_id, &rewritten_file_ids);
        for file_id in &file_ids {
            self.refresh_archive_state_for_completed_file(job_id, *file_id, false)
                .await;
        }
        self.invalidate_rar_plans_for_repaired_sets(job_id, rewritten_set_names);
        // Every arm that accepts a verdict passes through here on its way to
        // reconciliation, so a set the verdict has already joined is retired in
        // one place rather than at each of them.
        self.retire_par2_joined_split_topologies(job_id, verification);
        file_ids.len()
    }

    /// The RAR set names owning a list of the job's files, deduplicated.
    pub(in crate::pipeline) fn rar_set_names_for_files(
        &self,
        job_id: JobId,
        file_ids: &[NzbFileId],
    ) -> BTreeSet<String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return BTreeSet::new();
        };
        file_ids
            .iter()
            .filter_map(|file_id| {
                let file = state.assembly.file(*file_id)?;
                if !matches!(
                    self.classified_role_for_file(job_id, file),
                    weaver_model::files::FileRole::RarVolume { .. }
                ) {
                    return None;
                }
                self.classified_archive_set_name_for_file(job_id, file)
            })
            .collect()
    }

    /// Force a header-level plan rebuild for every set a repair touched, and
    /// hold extraction until it lands.
    ///
    /// Registering the repaired volume's facts is not enough on its own. The
    /// derived plan — the member chain, and with it the volume range extraction
    /// opens — was computed while those volumes were missing, and nothing about
    /// installing new facts retires it. Re-deriving from the facts alone would
    /// not do either: the member chain comes from the volumes' *headers*, which
    /// is exactly what the repair rewrote.
    ///
    /// [`RefreshReason::IdentityRebind`] is the existing reason for "the bytes
    /// behind this set are not what the plan was built from". It marks the
    /// refresh state `structure_dirty`, which is what makes
    /// `rar_member_refresh_request` demand a rebuild before a member may start,
    /// and it leaves the request `in_flight`, which is what
    /// `job_has_pending_rar_refresh_for_current_sets` reports and the
    /// `pending_rar_refresh` arm of the completion gate already defers on. No
    /// new gate: the repaired set becomes pending in the one the extraction path
    /// has always honoured.
    fn invalidate_rar_plans_for_repaired_sets(
        &mut self,
        job_id: JobId,
        set_names: BTreeSet<String>,
    ) {
        for set_name in set_names {
            let target = self.latest_completed_rar_volume(job_id, &set_name);
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                target_completed_volume = target,
                "rebuilding a repaired RAR set's plan from its repaired headers"
            );
            self.enqueue_rar_set_refresh(
                job_id,
                &set_name,
                target,
                crate::pipeline::RefreshReason::IdentityRebind,
            );
        }
    }

    /// Retire the split topologies a verdict has already produced the output of.
    ///
    /// A plain split posting ships `<name>.001/.002/.003` while its recovery
    /// data is computed over `<name>` — a file the posting never carries. The
    /// recovery pass reads the parts as one file and installs `<name>` itself,
    /// so by the time a verdict vouches for it the join has happened. Running
    /// the joiner afterwards writes the parts' bytes back over the output that
    /// was just verified, and waiting for every part to be whole fails a job
    /// whose payload is already on disk and proven.
    ///
    /// The match is a plain key lookup: a split topology is named by
    /// `archive_base_name` of its parts, which is exactly the joined-output
    /// name, so a verdict naming the *parts* — the ordinary shape, where the
    /// recovery set protects what the posting actually carries — finds no
    /// topology and retires nothing.
    ///
    /// Paths are checked before use, as they are for rebuilt RAR volumes: a
    /// PAR2 description names its own file, so an absolute path or one
    /// containing `..` is refused rather than resolved.
    fn retire_par2_joined_split_topologies(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> usize {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return 0;
        };

        let mut retired = 0usize;
        for file in &verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                continue;
            }
            let path = Path::new(&file.filename);
            if file.filename.is_empty()
                || !path.is_relative()
                || !path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                continue;
            }
            let is_split_set = self.jobs.get(&job_id).is_some_and(|state| {
                state
                    .assembly
                    .archive_topology_for(&file.filename)
                    .is_some_and(|topology| {
                        topology.archive_type == crate::jobs::assembly::ArchiveType::Split
                    })
            });
            if !is_split_set {
                continue;
            }

            // The verdict says the bytes hashed; this says they are still where
            // the job can deliver them from, at the length the set describes.
            let described_length = par2_set
                .files
                .values()
                .find(|description| description.filename == file.filename)
                .map(|description| description.length);
            let output_present = self
                .resolve_job_input_path(job_id, &file.filename)
                .and_then(|path| std::fs::metadata(path).ok())
                .is_some_and(|metadata| {
                    metadata.is_file() && Some(metadata.len()) == described_length
                });
            if !output_present {
                continue;
            }

            let Some(state) = self.jobs.get_mut(&job_id) else {
                return retired;
            };
            let Some(topology) = state.assembly.remove_archive_topology(&file.filename) else {
                continue;
            };
            let parts: HashSet<String> = topology.volume_map.keys().cloned().collect();
            info!(
                job_id = job_id.0,
                set_name = %file.filename,
                parts = parts.len(),
                "recovery set produced the joined output — retiring the split topology"
            );
            self.par2_joined_split_sets
                .entry(job_id)
                .or_default()
                .insert(file.filename.clone(), parts);
            retired += 1;
        }

        retired
    }

    /// Whether a file is a posted part of a split set a verdict has already
    /// joined.
    ///
    /// Such a part is a consumed input, not payload the job is short of: its
    /// bytes are inside the output the recovery set vouched for, and there is
    /// nothing left to download, repair or wait for. It therefore belongs in
    /// none of the post-verdict incomplete buckets — which matters most for the
    /// *first* part, whose 16 KiB prefix is the joined file's own, so PAR2
    /// content identity answers the joined description with it.
    pub(in crate::pipeline) fn par2_join_consumed_split_part(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> bool {
        let Some(sets) = self.par2_joined_split_sets.get(&job_id) else {
            return false;
        };
        if sets.is_empty() {
            return false;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return false;
        };
        let current = self.current_filename_for_file(job_id, file);
        sets.values()
            .any(|parts| parts.contains(&current) || parts.contains(file.filename()))
    }

    /// The parts of every split set a verdict has joined for this job.
    pub(in crate::pipeline) fn par2_joined_split_part_names(&self, job_id: JobId) -> Vec<String> {
        self.par2_joined_split_sets
            .get(&job_id)
            .map(|sets| sets.values().flatten().cloned().collect())
            .unwrap_or_default()
    }

    /// Delete the parts a verified join consumed, before finalization ships
    /// them alongside the file they joined into.
    ///
    /// This is the same removal the post-extraction cleanups perform for the
    /// sources of an archive that was extracted, on the same unconditional
    /// terms: the join happened, so the parts are spent inputs.
    pub(in crate::pipeline) async fn cleanup_par2_joined_split_parts(&mut self, job_id: JobId) {
        let parts = self.par2_joined_split_part_names(job_id);
        if parts.is_empty() {
            return;
        }
        let mut removed = 0u32;
        for filename in &parts {
            let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                continue;
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    file = %path.display(),
                    error = %error,
                    "failed to clean up a joined split part"
                ),
            }
        }
        info!(
            job_id = job_id.0,
            removed,
            total = parts.len(),
            "removed the split parts a verified join consumed"
        );
    }

    /// Register RAR volumes that PAR2 *rebuilt* and that the NZB never carried.
    ///
    /// Ported from release-0.7.9. A missing interior volume repaired from
    /// recovery blocks lands on disk under a name the job's assembly has never
    /// heard of, so extraction goes on believing the volume is absent and the
    /// repair achieves nothing. This walks a clean verification result, keeps
    /// the entries that are RAR volumes the job does not already know, and
    /// persists their parsed facts against the set.
    ///
    /// Paths are checked before use: a PAR2 description names its own file, so
    /// an absolute path or one containing `..` is refused rather than resolved.
    pub(crate) async fn register_verified_par2_rar_outputs(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> Result<Par2RarOutputRegistration, String> {
        let registered_filenames = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .map(|file| file.filename().to_string())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let mut registration = Par2RarOutputRegistration::default();
        for file in &verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                || registered_filenames.contains(&file.filename)
            {
                continue;
            }

            let path = Path::new(&file.filename);
            if file.filename.is_empty()
                || !path.is_relative()
                || !path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                return Err(format!(
                    "refusing unsafe PAR2-verified RAR output path {:?}",
                    file.filename
                ));
            }

            let role = weaver_model::files::FileRole::from_filename(&file.filename);
            let weaver_model::files::FileRole::RarVolume { volume_number } = role else {
                continue;
            };
            let Some(set_name) = weaver_model::files::archive_base_name(&file.filename, &role)
            else {
                continue;
            };
            let path = self
                .resolve_job_input_path(job_id, &file.filename)
                .ok_or_else(|| {
                    format!(
                        "PAR2 verified RAR output {} has no active job directory",
                        file.filename
                    )
                })?;
            if !path.is_file() {
                return Err(format!(
                    "PAR2 verified RAR output {} is missing from staging",
                    path.display()
                ));
            }

            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            let facts = Self::parse_rar_volume_facts_from_path(path, password_candidates)
                .await
                .map_err(|error| {
                    format!(
                        "failed to parse PAR2-verified RAR output {}: {error}",
                        file.filename
                    )
                })?;
            if self.persist_rar_volume_facts(
                job_id,
                &set_name,
                &file.filename,
                Some(volume_number),
                facts,
            )? {
                registration.registered += 1;
                // A registered output is a volume the plan was built without.
                // Its set's member chain is therefore derived from a strictly
                // smaller set of headers than the one now on disk, whether or
                // not any NZB file of that set was itself rewritten.
                registration.set_names.insert(set_name);
            }
        }

        if registration.registered > 0 {
            info!(
                job_id = job_id.0,
                registered = registration.registered,
                "registered PAR2-verified RAR outputs absent from the NZB"
            );
        }
        Ok(registration)
    }

    /// The archive files whose topology must be rebuilt from what a verdict
    /// proved about the disk.
    ///
    /// `rewritten` names the descriptions a repair just wrote — the write set of
    /// [`par2_repair_write_set`], empty on every pass that repaired nothing.
    /// Those files are included **unconditionally**, and that is the whole
    /// reason the parameter exists.
    ///
    /// A repaired volume re-verifies as `Complete`, not `Renamed`, and its set
    /// already carries a plan — one derived from cached headers back while the
    /// volume was still missing. So neither of the two conditions that admit a
    /// file here holds for precisely the files whose bytes just changed, and the
    /// refresh walks past them: the plan a repair exists to correct is the one
    /// left standing. A member chain that ended at the repaired volume keeps
    /// ending there, extraction opens the truncated volume range, and the packed
    /// data fails its CRC against bytes that are in fact perfect.
    ///
    /// `needs_refresh` asks whether a set has a plan *at all*, which is a
    /// question about existence where this needs one about staleness. Repair is
    /// the one place staleness is known rather than inferred — the repairer says
    /// which descriptions it wrote — so the answer is threaded in rather than
    /// re-derived from names here.
    /// The refresh set without the rewritten flags — the shape the tests assert
    /// against. Production reads
    /// [`Self::verified_complete_archive_refresh_targets`], which keeps them.
    #[cfg(test)]
    pub(crate) fn verified_complete_archive_file_ids_needing_refresh(
        &self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> Vec<NzbFileId> {
        self.verified_complete_archive_refresh_targets(job_id, verification, rewritten)
            .into_iter()
            .map(|(file_id, _)| file_id)
            .collect()
    }

    /// The refresh set, each entry flagged with whether the repair rewrote it.
    ///
    /// The flag is what separates "rebuild this file's topology" from "this
    /// file's set was built from bytes that no longer exist". Only the latter
    /// may invalidate a set's plan: a rename or a first-time topology build is
    /// ordinary progress, and forcing a header-level rebuild for those would
    /// re-derive a plan from the same headers it already holds.
    fn verified_complete_archive_refresh_targets(
        &self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> Vec<(NzbFileId, bool)> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };

        let mut by_name = HashMap::<String, (NzbFileId, bool)>::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let role = self.classified_role_for_file(job_id, file);
            if !Self::role_refreshes_archive_topology(&role) {
                continue;
            }

            let needs_refresh = self.archive_topology_needs_refresh(job_id, file, &role);
            let file_id = file.file_id();
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.clone())
                .unwrap_or_else(|| file.filename().to_string());
            Self::insert_par2_name_candidates(
                &mut by_name,
                &current_filename,
                file_id,
                needs_refresh,
            );
            if let Some(identity) = identity {
                Self::insert_par2_name_candidates(
                    &mut by_name,
                    &identity.source_filename,
                    file_id,
                    needs_refresh,
                );
                if let Some(canonical) = &identity.canonical_filename {
                    Self::insert_par2_name_candidates(
                        &mut by_name,
                        canonical,
                        file_id,
                        needs_refresh,
                    );
                }
            }
        }

        let mut matched = HashMap::<NzbFileId, bool>::new();
        for file_verification in &verification.files {
            let renamed = matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Renamed(_)
            );
            if !matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
            ) {
                continue;
            }

            // Additive: the two original conditions are untouched, and a file
            // the repair rewrote joins them regardless of what either says.
            let was_rewritten = rewritten.contains(&file_verification.file_id);
            for candidate_name in Self::par2_verification_candidate_names(file_verification) {
                let Some((file_id, needs_refresh)) = by_name.get(&candidate_name).copied() else {
                    continue;
                };
                if renamed || needs_refresh || was_rewritten {
                    // A file can match more than one description name; it is
                    // rewritten if any of them says so.
                    *matched.entry(file_id).or_insert(false) |= was_rewritten;
                }
                break;
            }
        }

        let mut targets = matched.into_iter().collect::<Vec<_>>();
        targets.sort_by_key(|(file_id, _)| file_id.file_index);
        targets
    }

    fn role_refreshes_archive_topology(role: &weaver_model::files::FileRole) -> bool {
        matches!(
            role,
            weaver_model::files::FileRole::RarVolume { .. }
                | weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. }
                | weaver_model::files::FileRole::SplitFile { .. }
                | weaver_model::files::FileRole::ZipArchive
                | weaver_model::files::FileRole::TarArchive
                | weaver_model::files::FileRole::TarGzArchive
                | weaver_model::files::FileRole::TarBz2Archive
                | weaver_model::files::FileRole::TarXzArchive
                | weaver_model::files::FileRole::GzArchive
                | weaver_model::files::FileRole::DeflateArchive
                | weaver_model::files::FileRole::BrotliArchive
                | weaver_model::files::FileRole::ZstdArchive
                | weaver_model::files::FileRole::Bzip2Archive
                | weaver_model::files::FileRole::XzArchive
        )
    }

    fn archive_topology_needs_refresh(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
        role: &weaver_model::files::FileRole,
    ) -> bool {
        let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file) else {
            return false;
        };

        if matches!(role, weaver_model::files::FileRole::RarVolume { .. }) {
            return self
                .rar_sets
                .get(&(job_id, set_name))
                .is_none_or(|set_state| set_state.plan.is_none());
        }

        self.jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topology_for(&set_name).is_none())
    }

    fn insert_par2_name_candidates(
        by_name: &mut HashMap<String, (NzbFileId, bool)>,
        name: &str,
        file_id: NzbFileId,
        needs_refresh: bool,
    ) {
        for candidate in Self::name_and_basename_candidates(name) {
            by_name.entry(candidate).or_insert((file_id, needs_refresh));
        }
    }

    fn par2_verification_candidate_names(
        file_verification: &par2_rs::verify::FileVerification,
    ) -> Vec<String> {
        let mut candidates = Self::name_and_basename_candidates(&file_verification.filename);
        if let par2_rs::verify::FileStatus::Renamed(path) = &file_verification.status {
            Self::push_name_candidate(&mut candidates, &path.to_string_lossy());
            if let Some(filename) = path.file_name() {
                Self::push_name_candidate(&mut candidates, &filename.to_string_lossy());
            }
        }
        candidates
    }

    fn name_and_basename_candidates(name: &str) -> Vec<String> {
        let mut candidates = Vec::new();
        Self::push_name_candidate(&mut candidates, name);
        if let Some(basename) = name.rsplit(['/', '\\']).next()
            && basename != name
        {
            Self::push_name_candidate(&mut candidates, basename);
        }
        candidates
    }

    fn push_name_candidate(candidates: &mut Vec<String>, name: &str) {
        if name.is_empty() || candidates.iter().any(|candidate| candidate == name) {
            return;
        }
        candidates.push(name.to_string());
    }

    async fn check_rar_job_completion(&mut self, job_id: JobId) {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return;
        }

        if self.has_active_rar_workers(job_id) {
            if self
                .jobs
                .get(&job_id)
                .is_some_and(|state| !matches!(state.status, JobStatus::Extracting))
            {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Extracting,
                    Some("extracting"),
                );
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
            }
            return;
        }

        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut forced_recompute = false;
        let (fallback_sets, has_incomplete_sets, has_ready_incremental_work) = loop {
            let mut fallback_sets = Vec::new();
            let mut has_incomplete_sets = false;
            let mut has_ready_incremental_work = false;
            let mut impossible_sets = Vec::new();

            for set_name in &set_names {
                let set_state = self.rar_sets.get(&(job_id, set_name.clone()));
                let set_complete = extracted_archives.contains(set_name)
                    || set_state
                        .and_then(|state| state.plan.as_ref())
                        .is_some_and(|plan| {
                            !plan.member_names.is_empty()
                                && plan
                                    .member_names
                                    .iter()
                                    .all(|member| extracted.contains(member))
                        });
                if set_complete {
                    self.extracted_archives
                        .entry(job_id)
                        .or_default()
                        .insert(set_name.clone());
                    continue;
                }

                has_incomplete_sets = true;
                if let Some(state) = set_state
                    && let Some(plan) = state.plan.as_ref()
                {
                    if matches!(
                        plan.phase,
                        crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
                    ) {
                        fallback_sets.push(set_name.clone());
                    } else if plan.ready_members.iter().any(|ready_member| {
                        self.rar_member_can_start_extraction(job_id, set_name, &ready_member.name)
                    }) {
                        has_ready_incremental_work = true;
                    } else if plan.waiting_on_volumes.is_empty() {
                        impossible_sets.push(set_name.clone());
                    }
                } else {
                    fallback_sets.push(set_name.clone());
                }
            }

            if impossible_sets.is_empty() {
                break (
                    fallback_sets,
                    has_incomplete_sets,
                    has_ready_incremental_work,
                );
            }

            if forced_recompute {
                let set_list = impossible_sets.join(", ");
                let msg = format!(
                    "invalid RAR state after recompute: sets [{set_list}] are incomplete with no ready members, no fallback, and no waiting volumes"
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            forced_recompute = true;
            for set_name in impossible_sets {
                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed forced RAR recompute for impossible state"
                    );
                }
            }
        };

        if has_incomplete_sets {
            if (has_ready_incremental_work || !fallback_sets.is_empty())
                && !self.maybe_start_extraction(job_id).await
            {
                return;
            }

            if has_ready_incremental_work {
                self.try_rar_extraction(job_id).await;
                return;
            }

            for set_name in &fallback_sets {
                if let Err(error) = self.extract_rar_set(job_id, set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to start RAR full-set extraction"
                    );
                    self.fail_job(job_id, error);
                    return;
                }
            }
            if !fallback_sets.is_empty() {
                return;
            }

            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    pub(in crate::pipeline) fn only_archive_residuals_or_loaded_par2_index_are_incomplete(
        &self,
        job_id: JobId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if self.job_has_active_extraction_tasks(job_id) {
            return false;
        }

        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_members = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let par2_loaded = self.par2_set(job_id).is_some();
        // An index that can still arrive is not a residual: it may carry a
        // recovery set of its own, and this job would finalize without ever
        // verifying or repairing what that set covers. Only an index nothing
        // can deliver any more is furniture.
        let metadata_discovery_closed = self.par2_metadata_discovery_closed(job_id);
        let mut saw_incomplete = false;

        for file in state.assembly.files() {
            if file.is_complete() {
                continue;
            }
            match self.classified_role_for_file(job_id, file) {
                weaver_model::files::FileRole::Par2 {
                    is_index: false, ..
                } => {}
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
                    if par2_loaded && metadata_discovery_closed =>
                {
                    saw_incomplete = true;
                }
                _ => {
                    let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file)
                    else {
                        return false;
                    };
                    let set_complete = extracted_archives.contains(&set_name)
                        || self
                            .rar_sets
                            .get(&(job_id, set_name.clone()))
                            .and_then(|state| state.plan.as_ref())
                            .is_some_and(|plan| {
                                !plan.member_names.is_empty()
                                    && plan
                                        .member_names
                                        .iter()
                                        .all(|member| extracted_members.contains(member))
                            });
                    if !set_complete {
                        return false;
                    }
                    saw_incomplete = true;
                }
            }
        }

        saw_incomplete
    }

    /// Check if all data files in a job are complete, and trigger post-processing.
    ///
    /// PAR2 is treated as a repair tool only — damage is detected via yEnc CRC
    /// (per-segment) and RAR CRC (per-member extraction). If
    /// CRC failures occur, recovery files are promoted for download and repair
    /// runs from disk using `verify_all` + `plan_repair` + `execute_repair`.
    pub(crate) async fn check_job_completion(&mut self, job_id: JobId) {
        let current_status = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state.status.clone()
        };
        let (total_data_files, complete_data_files, failed_bytes, queued_downloads) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            (
                state.assembly.data_file_count(),
                state.assembly.complete_data_file_count(),
                state.failed_bytes,
                !state.download_queue.is_empty(),
            )
        };
        // Once PAR2 has ruled on this job, a data file the recovery set never
        // described stops being something this gate can act on: there is no
        // repair for it and no download left to try, so counting it here only
        // keeps the job out of finalization for good. That is the livelock
        // removing the veto exposed — an unprotected `.nfo` three articles
        // short kept `needs_completion_repair_evaluation` true, and the gate
        // re-ran a full authoritative pass over a gigabyte every two seconds,
        // forever.
        //
        // Both oracles stop counting at the same place. NZBGet's health
        // failure requires par to have been *skipped*; SABnzbd's verdict is the
        // PAR result alone. A file PAR2 *does* describe still counts, because
        // for that one a verdict and a repair are genuinely still possible.
        let has_incomplete_data_files = if self.par2_verified.contains(&job_id) {
            self.incomplete_par2_protected_data_file_count(job_id) > 0
        } else {
            complete_data_files < total_data_files
        };

        // Step 1: Are all data files (non-recovery) complete?
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            if matches!(
                state.status,
                JobStatus::Paused
                    | JobStatus::Checking
                    | JobStatus::Moving
                    | JobStatus::Complete
                    | JobStatus::Failed { .. }
            ) {
                return;
            }
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total_data_files == 0 && queued_downloads {
                return;
            }
        }

        if matches!(current_status, JobStatus::QueuedRepair) {
            if self.active_repair_jobs() == 0 {
                self.promote_queued_repairs();
            }
            return;
        }

        self.reapply_promoted_recovery_queue(job_id);
        // Restored jobs retain completed bytes but not the bounded decode
        // prefix cache. Inspect a single header here, never during startup,
        // so an obfuscated PAR2 carrier can rejoin normal discovery.
        self.probe_restored_par2_headers(job_id).await;

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        if !par2_bypassed
            && self.job_spec_has_par2_file(job_id)
            && !self.par2_metadata_discovery_closed(job_id)
            && self.promote_par2_metadata(job_id)
        {
            info!(
                job_id = job_id.0,
                "waiting for bounded PAR2 metadata discovery before finalization"
            );
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            return;
        }
        if !par2_bypassed && !self.served_par2_set_needs_reconciliation(job_id) {
            self.activate_next_par2_gate_set(job_id);
            let has_settled_set = self.par2_runtime(job_id).is_some_and(|runtime| {
                runtime
                    .ordered_set_ids()
                    .into_iter()
                    .any(|set_id| runtime.set_runtime(set_id).is_some_and(|set| set.settled))
            });
            if has_settled_set {
                // Extraction topology maintenance may clear the compatibility
                // bit without discarding the per-set answers. Recompute from
                // those answers here rather than asking a clean set to run
                // again.
                self.mark_par2_verified(job_id).await;
            }
        }

        // A direct set whose job carries no PAR2 set to verify
        // against — bypassed, or no recovery article ever landed — would
        // otherwise wait forever for a verdict that is not coming. Asked here,
        // once per completion check, because this is where the job's PAR2 state
        // is settled; `mark_par2_verified` covers the verdict case.
        self.finalize_ready_direct_sets(job_id).await;

        if let Some(message) = self.aggregate_par2_failure_message(job_id) {
            self.fail_job(job_id, message);
            return;
        }
        let par2_loaded = !self.par2_servable_set_ids(job_id).is_empty();
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        if download_pipeline_exhausted {
            self.emit_download_pipeline_drained_if_pending(job_id);
            if has_incomplete_data_files {
                // The download pass is over and files are still short of
                // segments: this is where "cannot be assembled from articles"
                // becomes a fact rather than a race with work still in flight.
                self.note_incomplete_files_after_download_drain(job_id);
            }
        }
        let only_rar_archives = self.job_has_only_rar_archives(job_id);

        // A RAR set that PAR2 owes a verdict on, in the two shapes that reach
        // this while the downloads have not drained. Ordinarily an incomplete
        // data file defers validation until they do, which is right — PAR2
        // cannot tell a file still arriving from a damaged one — but in both
        // of these the wait is for something that is not coming, and the
        // authoritative pass is what says so.
        // Hoisted here by the 0.7.9 port: the repair-readiness predicates below
        // need it, and it used to be defined further down at "Step 2".
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|failed| !failed.is_empty());
        // A clean PAR2 verdict says the described files hashed correctly. It does
        // not say the archives open. When extraction fails afterwards *and* the
        // set is left waiting on a volume, that verdict is stale evidence and
        // PAR2 has to be allowed to rule again.
        //
        // Declining 0.7.9's relaxation outright (2efa19d9) livelocked every PAR2
        // repair scenario in the corpus. `has_crc_failures` does re-open repair
        // *evaluation* — which is why the unit suite was satisfied — but every
        // PAR2 route inside that block is gated on `par2_validation_needed`, so
        // the check re-entered forever with nothing able to act: 1865 identical
        // completion checkpoints on one job, no repair, no verdict.
        //
        // Narrower than 0.7.9's blanket `!verified`, deliberately, and on the
        // *absent*-volumes predicate rather than the waiting phase: a job that
        // merely finalized its direct sets has a conventional failed member and
        // no set waiting on anything, so its verdict stands and the
        // repair-first branch stays skipped
        // (`a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass`),
        // and a swap-corrected set whose volumes are all present is left to the
        // retry frontier rather than dragged back through PAR2.
        let par2_verdict_stale_after_failed_extraction = self.par2_verified.contains(&job_id)
            && has_crc_failures
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id);
        if par2_verdict_stale_after_failed_extraction
            && let Some(runtime) = self.par2_runtime.get_mut(&job_id)
            && let Some(set_runtime) = runtime.served_mut()
        {
            // A reopened verdict is owed a fresh pass, so the post-verdict
            // re-entry budget starts over with it.
            set_runtime.post_verdict_reconcile_attempts = 0;
        }
        let served_set_settled = self.par2_served_set_id(job_id).is_some_and(|set_id| {
            self.par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| {
                    (set_runtime.settled && set_runtime.failure.is_none())
                        // A retained job-level verdict predating per-set
                        // runtime answers is already authoritative for its
                        // one served set. Runtime reconstruction supplies the
                        // set-local answer on the next metadata replay.
                        || self.par2_verified.contains(&job_id)
                })
        });
        let par2_verdict_open = !served_set_settled || par2_verdict_stale_after_failed_extraction;
        let par2_may_still_rule = par2_loaded && !par2_bypassed && par2_verdict_open;
        let extraction_settled = !self.job_has_active_extraction_tasks(job_id);
        // One: extraction was attempted and failed. The archives cannot be
        // opened now, and the reason may well be the volume that never came.
        // This is the same question the scheduler asks before it latches the
        // failed member out, and it is asked through the same predicate so the
        // two cannot answer differently — a latch with no verdict coming is a
        // stalled job.
        let failed_rar_par2_repair_ready = self.par2_recovery_evaluation_pending(job_id);
        // Two: extraction was never attempted, because a volume is missing.
        // Nothing lands in `failed_extractions` for this shape — there was no
        // failure, only an absence — so it needs naming separately or a job
        // whose interior volume never posted waits forever with the recovery
        // blocks that would rebuild it sitting right there.
        let missing_rar_volume_par2_repair_ready = par2_may_still_rule
            && extraction_settled
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id)
            && (self.par2_served_set_id(job_id).is_some_and(|set_id| {
                self.recovery_blocks_available_or_targeted(job_id, set_id) > 0
            }) || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.recovery_queue.has_recovery_work()));
        // 0.8 only: a direct set still taking articles has holes where its
        // outstanding ranges will go, and PAR2 cannot tell a hole from
        // corruption. Declaring a verdict owed while one is filling walks the
        // job into the authoritative branch, which then defers on exactly this
        // condition and returns — so the pass never runs and the escape has
        // achieved nothing but a later re-check.
        let rar_par2_repair_ready = (failed_rar_par2_repair_ready
            || missing_rar_volume_par2_repair_ready)
            && self.direct_sets_ready_for_authoritative_par2(job_id);

        let par2_primary_payload_ready =
            !has_incomplete_data_files || download_pipeline_exhausted || rar_par2_repair_ready;
        let par2_validation_needed = par2_loaded
            && !par2_bypassed
            // The other half of the coupled pair above. 0.7.9 relaxes this to
            // `(rar_par2_repair_ready || !verified)`; this is the same
            // relaxation narrowed to the state that actually needs it — a
            // verified job whose extraction failed and whose set is still
            // waiting on a volume. Moving only one of the two halves does
            // nothing: `rar_par2_repair_ready` cannot become true while
            // `par2_may_still_rule` is false, and validation cannot run while
            // this is false.
            && par2_verdict_open
            && par2_primary_payload_ready
            // The residuals check reads a job still missing archive pieces as
            // nothing to validate yet. That is the very state a repair-ready
            // RAR set is in, so it cannot be what turns validation away.
            && (rar_par2_repair_ready
                || !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id));
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_live_rar_waiting_for_missing_volumes(job_id);
        let pending_rar_refresh = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_pending_rar_refresh_for_current_sets(job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let clean_par2_integrity_gate = self.clean_par2_integrity_gate(job_id);
        let archive_extraction_applicable = self.extraction_readiness_for_job(job_id)
            != ExtractionReadiness::NotApplicable
            || only_rar_archives;
        let authoritative_par2_verification_needed = par2_validation_needed
            && (rar_par2_repair_ready
                || has_crc_failures
                || (has_incomplete_data_files && download_pipeline_exhausted)
                || rar_waiting_for_missing_volumes
                || matches!(current_status, JobStatus::Repairing)
                || matches!(
                    clean_par2_integrity_gate,
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None
                ));
        // Shared by every fast path that skips the authoritative pass, so the
        // live short-circuit can never fire where the quick path would be
        // refused.
        let clean_par2_integrity_gate_allows_fast_path = match clean_par2_integrity_gate {
            CleanPar2IntegrityGate::StrongDecode => {
                only_rar_archives && (has_crc_failures || rar_waiting_for_missing_volumes)
            }
            CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => true,
        };
        // What the quick pass can be blocked by: a file the recovery set
        // *describes*, not any short file in the posting.
        //
        // Post-verdict this is already the question `has_incomplete_data_files`
        // asks; asking it the same way before a verdict exists is what stops a
        // clean payload beside a short unprotected file from paying for a
        // whole-set read every time. Nothing is loosened by it: the quick pass
        // skips incomplete files outright, so a described file that is short
        // lands in its `unresolved` bucket and the pass declines, exactly as it
        // does today. Only a file the set never describes — which the pass
        // could not have spoken for either way — stops standing in the way.
        let described_data_files_incomplete = if par2_loaded {
            self.incomplete_par2_protected_data_file_count(job_id) > 0
        } else {
            has_incomplete_data_files
        };
        let quick_par2_verification_allowed = par2_validation_needed
            && !matches!(current_status, JobStatus::Repairing)
            // A set waiting on a volume that never posted needs the
            // *authoritative* analyzer: only that names the exact missing
            // blocks a recovery promotion has to target, and the quick pass has
            // nothing to answer from — those bytes are absent, not wrong.
            //
            // Narrower than 0.7.9's, deliberately. A *failed* extraction whose
            // files are all present is a case 0.8's quick pass settles on its
            // own — swap correction, and the eager-delete retry frontier — and
            // forcing the authoritative pass there loses both.
            && !missing_rar_volume_par2_repair_ready
            && (!described_data_files_incomplete || !download_pipeline_exhausted)
            && clean_par2_integrity_gate_allows_fast_path;
        let needs_completion_repair_evaluation = has_crc_failures
            || (has_incomplete_data_files && download_pipeline_exhausted)
            || rar_waiting_for_missing_volumes
            || par2_validation_needed;
        let exhausted_rar_activity = if download_pipeline_exhausted && only_rar_archives {
            let inflight_extractions = self
                .inflight_extractions
                .get(&job_id)
                .map_or(0, HashSet::len);
            let has_active_rar_workers = self.has_active_rar_workers(job_id);
            Some((
                inflight_extractions,
                has_active_rar_workers,
                has_active_rar_workers || inflight_extractions > 0,
            ))
        } else {
            None
        };
        let has_exhausted_rar_active_extraction_tasks =
            exhausted_rar_activity.is_some_and(|(_, _, active)| active);

        if download_pipeline_exhausted && only_rar_archives {
            let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
            let (inflight_extractions, has_active_rar_workers, has_active_extraction_tasks) =
                exhausted_rar_activity.unwrap_or((0, false, false));
            let only_archive_residuals =
                self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
            let mut rar_set_state = self
                .rar_sets
                .iter()
                .filter(|((rar_job_id, _), _)| *rar_job_id == job_id)
                .map(|((_, set_name), set_state)| summarize_rar_set_phase(set_name, set_state))
                .collect::<Vec<_>>();
            rar_set_state.sort();
            let mut failed_extractions = self
                .failed_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            failed_extractions.sort();

            info!(
                job_id = job_id.0,
                status = ?current_status,
                complete_data_files,
                total_data_files,
                failed_bytes,
                par2_loaded,
                has_crc_failures,
                rar_waiting_for_missing_volumes,
                pending_rar_refresh,
                has_active_rar_workers,
                inflight_extractions,
                has_active_extraction_tasks,
                only_archive_residuals,
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
                promoted_recovery_pending = promoted_recovery.has_pending_work(),
                failed_extractions = ?failed_extractions,
                rar_set_state = ?rar_set_state,
                "RAR completion checkpoint"
            );
        }

        if has_incomplete_data_files
            && !download_pipeline_exhausted
            && !self.job_has_active_extraction_tasks(job_id)
            // ...unless PAR2 owes this job's RAR sets a verdict. Waiting for
            // downloads that are not coming is how such a job stalls, and this
            // return is what keeps PAR2 from ever being asked.
            && !rar_par2_repair_ready
        {
            return;
        }

        if pending_rar_refresh {
            debug!(
                job_id = job_id.0,
                "deferring completion — RAR topology refresh pending"
            );
            return;
        }

        // Standalone `.rev` files can rebuild a volume that never posted just as
        // well as one that failed CRC. Gating this on `has_crc_failures` alone
        // meant a set whose hole was known *before* any extraction was tried
        // — the common case, since the topology usually knows the missing
        // index from the neighbouring volumes' headers — went straight to "no
        // retryable work remains" without the recovery volumes ever being read.
        if download_pipeline_exhausted
            && only_rar_archives
            && (has_crc_failures || rar_waiting_for_missing_volumes)
            && !self.job_has_active_extraction_tasks(job_id)
            && self.job_has_rar_recovery_volume_files(job_id)
        {
            match self.try_restore_rar_recovery_volumes(job_id).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => warn!(
                    job_id = job_id.0,
                    error = %error,
                    "RAR recovery-volume restore failed; continuing with normal repair evaluation"
                ),
            }
        }

        // Don't finalize while concatenation is still pending.
        if self
            .pending_concat
            .get(&job_id)
            .is_some_and(|s| !s.is_empty())
        {
            debug!(
                job_id = job_id.0,
                "deferring completion — pending concatenation"
            );
            return;
        }

        if let Some(error) = self.ownerless_live_rar_plan_error_for_job(job_id) {
            self.fail_job(job_id, error);
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && !has_crc_failures
            && !par2_validation_needed
            && !has_exhausted_rar_active_extraction_tasks
            && self.job_has_idle_startable_rar_work(job_id)
            && matches!(
                current_status,
                JobStatus::Downloading | JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "restarting idle RAR extraction work"
            );
            self.try_rar_extraction(job_id).await;
            return;
        }

        if !has_crc_failures
            && self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id)
        {
            self.finalize_completed_archive_job(job_id).await;
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && has_crc_failures
            && !has_exhausted_rar_active_extraction_tasks
            && matches!(
                current_status,
                JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "normalizing idle RAR extraction status before repair evaluation"
            );
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }

        if rar_waiting_for_missing_volumes && self.job_has_incoherent_rar_waiting_state(job_id) {
            info!(
                job_id = job_id.0,
                "healing incoherent RAR waiting state before PAR2 verification"
            );
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        if needs_completion_repair_evaluation && !par2_bypassed {
            // Restored from 0.7.9 after an e2e run showed the cost of removing
            // it: one job re-ran `par2 damaged-path analysis` 21 times while
            // waiting for its promoted recovery to arrive, at ~64 slow par2
            // file scans, starving 75 other jobs into harness timeouts. The
            // analysis is what is expensive, and it has to be skipped *before*
            // it runs — 0.8's `job_has_promoted_recovery_pipeline_work` guard
            // below is too late to prevent the rescan.
            // Each pass of this analysis is a full, slow PAR2 scan, so the only
            // question that matters is whether re-running it can learn anything
            // new. Four e2e runs mapped the edges:
            //
            //  - Defer while the *parked* pool (`recovery_queue`) is non-empty
            //    and the job deadlocks: it is this analysis that promotes parked
            //    blocks, so the pool never drains (~1800 identical completion
            //    checkpoints per job, every PAR2-repair scenario in the corpus).
            //  - Defer on any narrower signal — recovery on the wire, parked
            //    work after a promotion — and heavy damage storms: the verdict
            //    keeps changing while the payload is still arriving, so the
            //    check re-scans on every tick that slips through (30+ slow
            //    scans, suite starved to 21 of 92).
            //
            // The scan can learn something exactly twice: once before any wave
            // has been promoted (it is the pass that decides what to promote),
            // and once after everything that could change its answer has
            // landed. In between — the wave still arriving, or the payload
            // still filling the very holes being counted — the verdict cannot
            // move, and the pass is pure cost. `job_has_pending_download_
            // pipeline_work` deliberately excludes the parked pool, so this
            // cannot re-create the deadlock above: a job whose only remaining
            // work is parked recovery is not "pending" here, analyses run, and
            // promotion drains the pool.
            let promoted_recovery_state = self.promoted_recovery_pipeline_state(job_id);
            if rar_par2_repair_ready
                && promoted_recovery_state.promoted_par2_files > 0
                && (promoted_recovery_state.incomplete_promoted_par2_files > 0
                    || self.job_has_pending_download_pipeline_work(job_id))
            {
                debug!(
                    job_id = job_id.0,
                    promoted_par2_files = promoted_recovery_state.promoted_par2_files,
                    incomplete_promoted_par2_files =
                        promoted_recovery_state.incomplete_promoted_par2_files,
                    "deferring repair evaluation — promoted recovery or payload is still arriving"
                );
                return;
            }
            if self.job_has_promoted_recovery_pipeline_work(job_id, "verify") {
                return;
            }

            let has_active_extraction_tasks = if download_pipeline_exhausted && only_rar_archives {
                has_exhausted_rar_active_extraction_tasks
            } else {
                self.job_has_active_extraction_tasks(job_id)
            };
            if has_active_extraction_tasks {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active extraction workers"
                );
                return;
            }

            // Every promoted recovery wave has settled by here, so a volume
            // still short of articles is short for good. Its surviving packets
            // are read back before the set is cloned for the passes below:
            // whichever of them runs, its view of how much recovery this job
            // has is the merged set, and so is the fail-fast arithmetic that
            // decides whether to wait, repair, or give up.
            self.salvage_partial_promoted_recovery_volumes(job_id).await;

            // Latched, so an indexless recovery set is named once rather than
            // on every entry to this gate.
            self.warn_unservable_recovery_sets_once(job_id);

            let par2_set = self.par2_set(job_id).cloned();
            let par2_set_id = self.par2_served_set_id(job_id);

            if let Some(set_id) = par2_set_id
                && !self.demoted_materializations_ready_for_par2(job_id, set_id)
            {
                debug!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    "deferring PAR2 settlement — a demoted direct set is still materializing"
                );
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                return;
            }

            if let Some(set_id) = par2_set_id
                && self.par2_set_is_absent_from_job(job_id, set_id)
            {
                let index_filename = self
                    .par2_runtime(job_id)
                    .and_then(|runtime| runtime.set_runtime(set_id))
                    .map(|set_runtime| set_runtime.summary.index_filename.clone())
                    .filter(|filename| !filename.is_empty())
                    .unwrap_or_else(|| set_id.to_string());
                info!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    index_filename = %index_filename,
                    "skipping absent PAR2 recovery set with no bound payload bytes"
                );
                let _ = self
                    .settle_par2_set(
                        job_id,
                        set_id,
                        Par2SetSettlementReason::AbsentUnboundPayload,
                    )
                    .await;
                self.continue_after_aggregate_clean_par2_settlement(
                    job_id,
                    has_crc_failures,
                    archive_extraction_applicable,
                    "skipped absent PAR2 recovery set",
                )
                .await;
                return;
            }

            if par2_set.is_some() {
                // What the dual-CRC grid managed to claim off the download for
                // this job, recorded once at the verification gate — the point
                // where its work is finished and every arm below is about to
                // read it. `blocks_claimed` is the read the download path
                // already paid for; `articles_without_usable_segments` is the
                // shortfall, articles whose yEnc segmentation could not be
                // rebased onto the block grid and which therefore claim
                // nothing.
                debug!(
                    job_id = job_id.0,
                    blocks_claimed_in_stream = self.block_crcs.blocks_derived(),
                    articles_without_usable_segments = self.block_crcs.rebased_articles(),
                    "in-stream block verification diagnostics"
                );
            }

            // Partial quick evidence survives past its own arm: when the flow
            // below decides an authoritative pass is owed, this is what lets
            // that pass read only the unproven remainder instead of the set.
            let mut quick_partial: Option<QuickPar2PartialEvidence> = None;
            if quick_par2_verification_allowed && let Some(par2_set) = par2_set.as_ref() {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                Self::trip_par2_verification_started_failpoint();
                match self
                    .quick_verify_par2_with_placement(
                        job_id,
                        Arc::clone(par2_set),
                        working_dir.clone(),
                    )
                    .await
                {
                    Ok(QuickPar2Outcome::Full(verification, placement_plan, evidence)) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification passed for clean exhausted job"
                        );
                        self.finish_clean_par2_verification(
                            job_id,
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID"),
                            working_dir.clone(),
                            CleanPar2Verification {
                                verification,
                                placement_plan,
                                slice_size: par2_set.slice_size,
                                verification_mode: evidence.verification_mode(),
                                reconcile_context: "clean PAR2 quick verification",
                                retry_message:
                                    "cleared failed extractions after quick verify — retrying",
                            },
                            has_crc_failures,
                            archive_extraction_applicable,
                        )
                        .await;
                        return;
                    }
                    Ok(QuickPar2Outcome::Partial(partial)) => {
                        info!(
                            job_id = job_id.0,
                            proven = partial.proven.len(),
                            unproven = partial.unproven_recovery.len(),
                            "quick PAR2 verification proved part of the set — any \
                             authoritative pass below reads only the remainder"
                        );
                        quick_partial = Some(partial);
                    }
                    Ok(QuickPar2Outcome::Inconclusive) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification was inconclusive — falling back to authoritative verify"
                        );
                    }
                    Err(message) => {
                        let set_id =
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID");
                        self.finish_par2_set_failure(job_id, set_id, message).await;
                        return;
                    }
                }
            }

            if par2_validation_needed && !authoritative_par2_verification_needed {
                match clean_par2_integrity_gate {
                    CleanPar2IntegrityGate::StrongDecode => {
                        info!(
                            job_id = job_id.0,
                            "skipping authoritative PAR2 verify for clean exhausted strong-decode job"
                        );

                        self.try_deobfuscate_files_with_par2(job_id).await;
                        self.retry_par2_authoritative_identity(job_id).await;
                        let set_id =
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID");
                        let slice_size = par2_set
                            .as_ref()
                            .expect("PAR2 validation has a parsed recovery set")
                            .slice_size;
                        let _ = self
                            .settle_par2_set(
                                job_id,
                                set_id,
                                Par2SetSettlementReason::Clean {
                                    slice_size,
                                    verification_mode: CleanPar2VerificationMode::StrongDecode,
                                },
                            )
                            .await;

                        if !self.par2_verified.contains(&job_id) {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }

                        if archive_extraction_applicable {
                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        self.reconcile_job_progress(job_id).await;
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => {}
                }
            }

            if let Some(par2_set) = par2_set {
                let set_id = par2_set.recovery_set_id;
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                // Two direct-store preconditions for *any*
                // authoritative pass below, whichever branch it takes. Both are
                // no-ops for a job with no live direct set, so a conventional
                // job reaches the same code it always did.
                //
                // A set that is still receiving articles has holes where its
                // outstanding ranges will go, and PAR2 cannot tell a hole from
                // corruption — so the pass waits for the same thing the branch
                // above waits for in `par2_primary_payload_ready`: the payload,
                // or the download pipeline draining. `needs_completion_repair_
                // evaluation` can be true well before either (another set's
                // extraction failing is enough), which is how a healthy
                // mid-download set would otherwise be demoted for damage that
                // is just bytes in flight.
                if !self.direct_sets_ready_for_authoritative_par2(job_id) {
                    debug!(
                        job_id = job_id.0,
                        "deferring PAR2 verification — a direct set is still downloading"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A volume with no unambiguous PAR2 identity cannot be put
                // behind the overlay *or* attributed back to its set
                // afterwards, so it leaves direct mode before the pass rather
                // than being discovered as unattributable damage inside it.
                if self.demote_unbindable_direct_sets(job_id).await {
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A live direct set reaches this branch as a matter of course,
                // not as defence in depth: it contributes nothing to
                // `clean_par2_integrity_gate` — a direct set never enters the
                // archive topology — so a `None` gate sends it straight here,
                // damaged or not. The repairer cannot read a virtual volume, so
                // before it is allowed to force a whole-set materialization the
                // sets get one direct-aware verdict of their own: damage repairs
                // in place, and a clean verdict skips the repairer so the
                // ordinary verify path below can record it.
                let mut run_par2_repairer = authoritative_par2_verification_needed;
                // Stashed rather than discarded when the direct gate reaches
                // `Clean`: the verdict below is the same one the ordinary
                // whole-set pass would have reached over the same virtual
                // volumes, and asking that pass to read them again would be
                // the second whole-set read this gate exists to avoid. See
                // where `direct_verdict` is consumed, further down, for how it
                // stands in for `verify_par2_with_placement`.
                let mut direct_verdict: Option<par2_rs::VerificationResult> = None;
                if authoritative_par2_verification_needed {
                    match self
                        .resolve_direct_sets_before_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                        )
                        .await
                    {
                        _ if self.shared_state.is_job_cancellation_requested(job_id) => {
                            return;
                        }
                        DirectPar2Resolution::Repaired => {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        DirectPar2Resolution::Clean(verification) => {
                            run_par2_repairer = false;
                            direct_verdict = Some(*verification);
                        }
                        DirectPar2Resolution::Pending => return,
                        DirectPar2Resolution::Deferred => {
                            // The same wait the analysis below performs when it
                            // promotes recovery, reported the same way: the job
                            // is downloading again, because that is literally
                            // what it is doing.
                            //
                            // Deliberately *not* re-armed. Nothing this gate can
                            // do moves the answer — the sets are waiting on
                            // articles — and each lap costs a full PAR2 scan.
                            // The re-arm comes from the recovery itself: a
                            // completing PAR2 file merges its slices and checks
                            // the job, which is the one event that changes the
                            // verdict.
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );
                            return;
                        }
                        DirectPar2Resolution::Unresolved => {}
                    }
                }
                if run_par2_repairer {
                    // The filesystem analysis below is a whole-directory
                    // authoritative read, and it holds the pipeline actor for
                    // its duration. Running it while this job still has wire
                    // work in flight buys nothing — a damaged verdict cannot
                    // repair better than the same verdict after the remaining
                    // articles land, and an insufficient-recovery verdict
                    // parks on exactly the drain this gate waits for. Without
                    // the gate every completing recovery volume re-runs the
                    // full pass, which starves dispatch for the whole queue.
                    // The direct-store arm above waits for the same drain in
                    // `direct_sets_ready_for_authoritative_par2`; this is the
                    // conventional path's mirror of it. The re-arm is the
                    // drain itself: the last completing file schedules a
                    // completion check, and the quiescent flush sweeps a
                    // parked tail.
                    if self.job_has_pending_download_pipeline_work(job_id) {
                        info!(
                            job_id = job_id.0,
                            "deferring PAR2 damaged-path analysis until the job's downloads drain"
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }
                    // The repairer reads and *writes* volume files through
                    // `DiskFileAccess`, which a virtual volume has none of. So
                    // any set still routing here — one whose repair refused, or
                    // one whose damage is not the reason the job is in this
                    // branch — materializes first, and the repairer sees real
                    // files.
                    if self.demote_live_direct_sets_for_par2_repair(job_id).await {
                        // Re-armed rather than left to the 30 s reconcile
                        // sweep: the job is one pass away from its verdict and
                        // the materialized volumes are already on disk.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    let repair_analysis = match self
                        .analyze_par2_with_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            matches!(current_status, JobStatus::Repairing),
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(_) if self.shared_state.is_job_cancellation_requested(job_id) => {
                            return;
                        }
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    };
                    let verification = &repair_analysis.verification;
                    let damaged = verification.total_missing_blocks;
                    let recovery_now = repair_analysis.recovery_blocks_available;
                    let total_recovery_capacity =
                        self.total_recovery_block_capacity(job_id, par2_set.recovery_set_id);
                    let blocks_needed = match &verification.repairable {
                        par2_rs::verify::Repairability::NotNeeded => 0,
                        par2_rs::verify::Repairability::Repairable { blocks_needed, .. }
                        | par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } => {
                            *blocks_needed
                        }
                        par2_rs::verify::Repairability::ResourceLimited { .. } => 0,
                    };

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                        &verification.repairable
                    {
                        let msg = par2_resource_limit_message(reason);
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    // Damage confined to furniture is delivered rather than
                    // repaired: rebuilding one slice of an `.nfo` would force a
                    // read of the whole set to assemble the decode matrix, and
                    // the file is shipped as-is either way if the blocks are
                    // short. Anything else damaged alongside it sends the
                    // verdict down the ordinary ladder, which repairs the
                    // furniture in the same pass at no extra cost.
                    let ignorable_damage = self.par2_damage_is_only_ignorable(verification);
                    // A verdict with nothing damaged, nothing missing and no
                    // slice to reconstruct has no repair in it: every described
                    // file's content is on disk and whole, and the only work
                    // left is moving some of it onto the names the descriptions
                    // give it. Running the repairer over that shape is how a set
                    // that was never damaged ends up with a directory full of
                    // `<name>.N` backups — it installs each file at its
                    // canonical name and moves the occupant aside — and the
                    // placement that follows is then asked to rename files onto
                    // names those installs already filled.
                    //
                    // So it takes the road the verify arm takes for a clean
                    // verdict instead. The plan has to come from a directory
                    // scan: a pairwise swap is only visible to something that
                    // looks at what is actually on each name, and the plan
                    // derived from statuses can express it only as two renames
                    // into occupied targets. The scan's own read is what stands
                    // as this set's verdict from here on.
                    //
                    // Ordered after the furniture rule above, which owns any
                    // verdict whose damage is all ignorable; a `Renamed` entry
                    // sends that rule to `None`, so the two never contend.
                    let placement_only = par2_verification_needs_repair(verification)
                        && ignorable_damage.is_none()
                        && par2_verification_is_placement_only(verification);
                    let mut placement_pass: Option<par2_rs::VerificationResult> = None;
                    if placement_only {
                        info!(
                            job_id = job_id.0,
                            files_renamed = repair_analysis.files_renamed,
                            "PAR2 analysis — placement only, nothing to repair"
                        );
                        let (scanned, placement_plan) = match self
                            .verify_par2_with_placement(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                matches!(current_status, JobStatus::Repairing),
                                true,
                            )
                            .await
                        {
                            Ok(result) => result,
                            Err(message) => {
                                self.finish_par2_set_failure(job_id, set_id, message).await;
                                return;
                            }
                        };
                        // The scan is a second read of a directory the analysis
                        // described a moment ago, and it is the one this arm
                        // stands its verdict on. If the two disagree —
                        // `verify_all` cannot report misplacement, so a
                        // disagreement is damage or an absence that appeared in
                        // between — then "placement only" was concluded from a
                        // state that no longer holds, and marking the set
                        // verified on it would accept a verdict nothing re-read.
                        // Nothing is moved and the gate is re-armed instead: the
                        // next lap analyses the disk as it is now and takes
                        // whichever ladder that answer deserves.
                        if par2_verification_needs_repair(&scanned) {
                            warn!(
                                job_id = job_id.0,
                                damaged = scanned.total_missing_blocks,
                                "PAR2 placement pass disagreed with the analysis it stood in \
                                 for — re-checking before placing anything"
                            );
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        self.try_deobfuscate_files_with_par2(job_id).await;
                        if let Err(error) = self
                            .apply_placement_plan_for_retry_or_repair(
                                job_id,
                                working_dir.clone(),
                                &placement_plan,
                            )
                            .await
                        {
                            self.finish_par2_set_failure(job_id, set_id, error).await;
                            return;
                        }
                        placement_pass = Some(scanned);
                    }
                    // From here the placement-only arm carries the scanned
                    // pass's answer, which read every file through the plan it
                    // just applied. Every other arm is unchanged: nothing was
                    // scanned, so this resolves to the analysis result itself.
                    let verification = placement_pass.as_ref().unwrap_or(verification);
                    if !par2_verification_needs_repair(verification)
                        || ignorable_damage.is_some()
                        || placement_only
                    {
                        if let Some(ignorable) = ignorable_damage.as_ref() {
                            warn!(
                                job_id = job_id.0,
                                "delivering {} damaged ignorable file(s) without repair: {}",
                                ignorable.len(),
                                ignorable.join(", ")
                            );
                        } else if !placement_only {
                            info!(job_id = job_id.0, "PAR2 analysis passed — no repair needed");
                        }

                        self.retry_par2_authoritative_identity(job_id).await;
                        // A clean verdict repaired nothing.
                        self.refresh_verified_complete_archive_topologies(
                            job_id,
                            verification,
                            &HashSet::new(),
                        )
                        .await;
                        if let Err(error) = self
                            .reconcile_and_classify_par2_verification(
                                job_id,
                                verification,
                                has_crc_failures,
                                "clean PAR2 verification",
                            )
                            .await
                        {
                            self.finish_par2_set_failure(job_id, set_id, error).await;
                            return;
                        }
                        let _ = self
                            .settle_par2_set(
                                job_id,
                                set_id,
                                Par2SetSettlementReason::Clean {
                                    slice_size: par2_set.slice_size,
                                    verification_mode: CleanPar2VerificationMode::Authoritative,
                                },
                            )
                            .await;

                        if !self.par2_verified.contains(&job_id) {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }

                        if has_crc_failures {
                            if self.normalization_retried.contains(&job_id) {
                                let msg =
                                    "clean PAR2 verification but extraction still failing after retry"
                                        .to_string();
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            self.set_normalization_retried_state(job_id, true);
                            let failed_members = self
                                .failed_extractions
                                .get(&job_id)
                                .cloned()
                                .unwrap_or_default();
                            self.replace_failed_extraction_members(job_id, HashSet::new());
                            let cleared = failed_members.len();
                            self.recompute_rar_retry_frontier(job_id).await;
                            if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                                if !failed_members.is_empty() {
                                    self.replace_failed_extraction_members(job_id, failed_members);
                                }
                                let msg = format!(
                                    "invalid RAR retry frontier after placement correction: {reason}"
                                );
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            info!(
                                job_id = job_id.0,
                                cleared,
                                "cleared failed extractions after PAR2 analysis — retrying"
                            );

                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        if archive_extraction_applicable {
                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        self.reconcile_job_progress(job_id).await;
                        self.schedule_job_completion_check(job_id);
                        return;
                    }

                    // What the authoritative analysis of a damaged job actually
                    // read. Evidence seeding is supposed to make this the size
                    // of the damaged files rather than the size of the job, and
                    // this is the only number that says whether it does.
                    //
                    // It is also the number that decides whether intra-file
                    // slice skipping is ever worth building: while a damaged
                    // file is read whole, the floor is its length, and only a
                    // figure that stays far above the damaged bytes would
                    // justify going finer than per-file.
                    let authoritative_bytes_read = repair_analysis.scan.bytes_scanned;
                    crate::runtime::perf_probe::record_value(
                        "par2.authoritative.bytes_read",
                        authoritative_bytes_read,
                    );
                    #[cfg(test)]
                    self.par2_authoritative_bytes_read
                        .push(authoritative_bytes_read);
                    info!(
                        job_id = job_id.0,
                        damaged,
                        blocks_needed,
                        recovery_now,
                        total_recovery_capacity,
                        files_renamed = repair_analysis.files_renamed,
                        files_damaged = repair_analysis.files_damaged,
                        files_missing = repair_analysis.files_missing,
                        authoritative_bytes_read,
                        "PAR2 analysis — repair required"
                    );

                    if total_recovery_capacity < blocks_needed {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            blocks_needed,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();
                        if recovery_still_settling {
                            info!(
                                job_id = job_id.0,
                                blocks_needed,
                                total_recovery_capacity,
                                promoted_blocks = promoted,
                                "waiting for targeted recovery downloads before repair"
                            );
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );
                            return;
                        }
                        self.finish_par2_set_failure(
                            job_id,
                            set_id,
                            format!(
                                "not repairable: {blocks_needed} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        )
                        .await;
                        return;
                    }

                    if recovery_now < blocks_needed {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            blocks_needed,
                        );
                        let targeted_total = self.recovery_blocks_available_or_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();

                        if targeted_total < blocks_needed && !recovery_still_settling {
                            let msg = format!(
                                "not repairable: {blocks_needed} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        if let Some(msg) = par2_unreachable_recovery_failure(
                            job_id,
                            blocks_needed,
                            recovery_now,
                            targeted_total,
                            promoted,
                            recovery_still_settling,
                            promoted_recovery.parked_promoted_recovery,
                        ) {
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            blocks_needed,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }

                    if matches!(
                        &verification.repairable,
                        par2_rs::verify::Repairability::Insufficient { .. }
                            | par2_rs::verify::Repairability::ResourceLimited { .. }
                    ) {
                        let msg = format!(
                            "not repairable: PAR2 analysis found incomplete critical repair metadata or unusable recovery despite {recovery_now} available recovery blocks"
                        );
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    match self
                        .run_par2_repairer(job_id, Arc::clone(&par2_set), working_dir.clone(), true)
                        .await
                    {
                        Ok(outcome) => {
                            self.finish_par2_repair(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                verification,
                                outcome,
                                has_crc_failures,
                            )
                            .await;
                            return;
                        }
                        Err(error_msg) => {
                            self.fail_par2_repair(job_id, error_msg);
                            return;
                        }
                    }
                }

                // A settled verdict re-entered because a *protected* file is
                // still incomplete while its verified bytes sit on disk.
                //
                // Nothing about the recovery set has changed, so reading it
                // again — seconds of it, over the whole payload — can only
                // return the verdict it already returned, and the reconciler
                // that failed to bind the file will fail to bind it again. The
                // recovery set had an answer for that file either way, so the
                // only thing that can put a job here is our own bookkeeping. It
                // gets exactly one more lap to settle and is then reported as
                // the bug it is: a named, reproducible failure beats an
                // invisible loop burning a core.
                //
                // Two neighbouring states deliberately keep their read:
                //
                //  - A protected file whose bytes are not on disk, or are short
                //    of the described length. Something really is missing
                //    there, and the authoritative pass is the only thing that
                //    names which blocks a recovery promotion has to fetch — a
                //    set waiting on a volume that never posted arrives at this
                //    gate in exactly that shape.
                //  - A failed extraction. The pass it runs is also what applies
                //    the placement correction that can make the retry succeed,
                //    and there is no way to obtain that without a verification.
                //    That path carries a single-retry latch of its own.
                if !par2_verdict_open
                    && !has_crc_failures
                    && self.settled_verdict_left_only_proven_protected_files(job_id, set_id)
                {
                    let attempts = {
                        let set_runtime = self.ensure_par2_runtime(job_id).served_mut().expect(
                            "post-verdict reconciliation belongs to the served recovery set",
                        );
                        set_runtime.post_verdict_reconcile_attempts = set_runtime
                            .post_verdict_reconcile_attempts
                            .saturating_add(1);
                        set_runtime.post_verdict_reconcile_attempts
                    };
                    if attempts > 1 {
                        let message = self
                            .classify_incomplete_after_par2(
                                job_id,
                                &Par2Reconciliation::default(),
                                "PAR2 verdict settled but reconciliation left files outstanding",
                            )
                            .map(|report| report.message)
                            .unwrap_or_else(|| {
                                "BUG: PAR2-protected file(s) stayed incomplete after a settled \
                                 verification"
                                    .to_string()
                            });
                        warn!(job_id = job_id.0, error = %message);
                        self.fail_job(job_id, message);
                        return;
                    }
                    info!(
                        job_id = job_id.0,
                        "PAR2 verdict is settled — retrying reconciliation once instead of \
                         re-reading the recovery set"
                    );
                    self.reconcile_job_progress(job_id).await;
                    return;
                }

                let emit_verification_events = !has_crc_failures
                    || !self.par2_verified.contains(&job_id)
                    || authoritative_par2_verification_needed
                    || matches!(current_status, JobStatus::Repairing);
                let (verification, placement_plan) = match direct_verdict {
                    Some(mut verification) => {
                        // The direct gate already read this set — virtually,
                        // through the overlay — and reached exactly this
                        // verdict. Asking `verify_par2_with_placement` to read
                        // it again would be the second whole-set pass this
                        // gate exists to avoid, so its observable effects are
                        // replicated here instead of its read: the same status
                        // transition and verification-started announcement
                        // `emit_events` would have produced, then the one
                        // settlement this verdict gets — `verify_direct_sets_quietly`
                        // adjusts direct damage before returning but never
                        // settles, so this is the first and only settle call
                        // this verification instance sees.
                        if emit_verification_events {
                            if !matches!(current_status, JobStatus::Repairing) {
                                self.transition_postprocessing_status(
                                    job_id,
                                    JobStatus::Verifying,
                                    Some("verifying"),
                                );
                            } else {
                                info!(
                                    job_id = job_id.0,
                                    "rerunning PAR2 verification while preserving restored \
                                     repair slot"
                                );
                            }
                            self.emit_job_verification_started(job_id);
                            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                                file_id: NzbFileId {
                                    job_id,
                                    file_index: 0,
                                },
                            });
                        }
                        self.settle_par2_pass_result(
                            job_id,
                            &mut verification,
                            emit_verification_events,
                        );
                        let plan = placement_plan_from_verification(&verification);
                        Self::log_placement_plan(job_id, &plan);
                        (verification, plan)
                    }
                    None if quick_partial.is_some() => {
                        // The quick pass proved part of this set from zero-read
                        // evidence and left only a remainder unproven. Reading
                        // the whole set here would throw that proof away, so
                        // the pass reads exactly the remainder — through a
                        // 16 KiB-prefix placement proposal, since an unproven
                        // file may sit under an obfuscated name — and the
                        // proven entries are carried into the merged verdict,
                        // the same merge-then-settle discipline as every other
                        // selective pass in this gate.
                        let partial = quick_partial.take().expect("checked by the match guard");
                        if emit_verification_events {
                            if !matches!(current_status, JobStatus::Repairing) {
                                self.transition_postprocessing_status(
                                    job_id,
                                    JobStatus::Verifying,
                                    Some("verifying"),
                                );
                            } else {
                                info!(
                                    job_id = job_id.0,
                                    "rerunning PAR2 verification while preserving restored \
                                     repair slot"
                                );
                            }
                            self.emit_job_verification_started(job_id);
                            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                                file_id: NzbFileId {
                                    job_id,
                                    file_index: 0,
                                },
                            });
                        }
                        let (fresh, fresh_plan) = if partial.unproven_recovery.is_empty() {
                            // Every recovery member was proven; nothing to
                            // read. (Only non-recovery descriptions were
                            // unresolved, and the authoritative pass never
                            // read those either.)
                            (
                                par2_rs::VerificationResult {
                                    files: Vec::new(),
                                    recovery_blocks_available: par2_set.recovery_block_count(),
                                    total_missing_blocks: 0,
                                    repairable: par2_rs::verify::Repairability::NotNeeded,
                                },
                                par2_rs::PlacementPlan {
                                    exact: Vec::new(),
                                    swaps: Vec::new(),
                                    renames: Vec::new(),
                                    unresolved: Vec::new(),
                                    conflicts: Vec::new(),
                                },
                            )
                        } else {
                            match self
                                .run_par2_placement_pass(
                                    job_id,
                                    Arc::clone(&par2_set),
                                    working_dir.clone(),
                                    Par2PassScope::SelectedProposed(
                                        partial.unproven_recovery.clone(),
                                        partial.claimed_disk_names.clone(),
                                    ),
                                )
                                .await
                            {
                                Ok(result) => result,
                                Err(message) => {
                                    self.finish_par2_set_failure(job_id, set_id, message).await;
                                    return;
                                }
                            }
                        };
                        let base = quick_partial_base_verification(&par2_set, &partial);
                        let mut verification =
                            par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);
                        self.settle_par2_pass_result(
                            job_id,
                            &mut verification,
                            emit_verification_events,
                        );
                        let plan = merge_partial_placement_plan(partial.proven_plan, fresh_plan);
                        Self::log_placement_plan(job_id, &plan);
                        (verification, plan)
                    }
                    None => match self
                        .verify_par2_with_placement(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            matches!(current_status, JobStatus::Repairing),
                            emit_verification_events,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    },
                };
                // Damage on a virtual volume has nothing to repair *into* — the
                // bytes live in a member's partial and an envelope, and a
                // recovered slice belongs to neither — so the set materializes
                // **only its damaged volumes** into scratch, repairs those
                // while every clean volume is still read virtually, routes the
                // repaired spans back through the router and throws the scratch
                // away. The set stays direct and loses no output. Every refusal
                // along the way falls back to the whole-set demotion, which
                // materializes everything and hands the job to the conventional
                // repair path. Either way the job's next move is this gate
                // again, over bytes that changed.
                match self
                    .resolve_direct_sets_with_par2_damage(job_id, &verification)
                    .await
                {
                    DirectDamageResolution::Resolved => {
                        // Re-armed rather than left to the 30 s reconcile sweep:
                        // the repaired bytes are already in the partials, or the
                        // demotion has already materialized (or queued the
                        // refetch of) the volumes, so the next pass can run
                        // immediately.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    DirectDamageResolution::Deferred => {
                        // Damage the merged recovery cannot cover but the
                        // recovery *set* can. The sets keep their outputs and
                        // their virtual volumes while the targeted recovery
                        // downloads; the job is reported as what it is doing.
                        // No re-arm — the completing recovery file merges its
                        // slices and checks the job itself, and a lap of this
                        // gate in the meantime is a slow scan that cannot reach
                        // a different answer.
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }
                    DirectDamageResolution::Unresolved => {}
                }
                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity =
                    self.total_recovery_block_capacity(job_id, par2_set.recovery_set_id);

                if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                    &verification.repairable
                {
                    let msg = par2_resource_limit_message(reason);
                    self.finish_par2_set_failure(job_id, set_id, msg).await;
                    return;
                }

                // The same furniture rule the analysis arm applies, stated once
                // per arm because each owns its own ladder.
                let ignorable_damage = self.par2_damage_is_only_ignorable(&verification);
                if !par2_verification_needs_repair(&verification) || ignorable_damage.is_some() {
                    if let Some(ignorable) = ignorable_damage.as_ref() {
                        warn!(
                            job_id = job_id.0,
                            "delivering {} damaged ignorable file(s) without repair: {}",
                            ignorable.len(),
                            ignorable.join(", ")
                        );
                    } else {
                        info!(
                            job_id = job_id.0,
                            "PAR2 verification passed — no damaged slices"
                        );
                    }

                    // Rename obfuscated files using PAR2 metadata even when
                    // verification is clean (files may be intact but obfuscated).
                    self.try_deobfuscate_files_with_par2(job_id).await;
                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }
                    self.retry_par2_authoritative_identity(job_id).await;
                    // A clean verdict repaired nothing.
                    self.refresh_verified_complete_archive_topologies(
                        job_id,
                        &verification,
                        &HashSet::new(),
                    )
                    .await;
                    if let Err(error) = self
                        .reconcile_and_classify_par2_verification(
                            job_id,
                            &verification,
                            has_crc_failures,
                            "clean PAR2 verification",
                        )
                        .await
                    {
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }
                    let _ = self
                        .settle_par2_set(
                            job_id,
                            set_id,
                            Par2SetSettlementReason::Clean {
                                slice_size: par2_set.slice_size,
                                verification_mode: CleanPar2VerificationMode::Authoritative,
                            },
                        )
                        .await;

                    if !self.par2_verified.contains(&job_id) {
                        self.schedule_job_completion_check(job_id);
                        return;
                    }

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        self.set_normalization_retried_state(job_id, true);
                        let failed_members = self
                            .failed_extractions
                            .get(&job_id)
                            .cloned()
                            .unwrap_or_default();
                        self.replace_failed_extraction_members(job_id, HashSet::new());
                        let cleared = failed_members.len();
                        self.recompute_rar_retry_frontier(job_id).await;
                        if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                            if !failed_members.is_empty() {
                                self.replace_failed_extraction_members(job_id, failed_members);
                            }
                            let msg = format!(
                                "invalid RAR retry frontier after placement correction: {reason}"
                            );
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            cleared,
                            "cleared failed extractions after authoritative verify — retrying"
                        );

                        self.retry_archive_extraction_after_verify_or_repair(job_id)
                            .await;
                        return;
                    }

                    if archive_extraction_applicable {
                        self.retry_archive_extraction_after_verify_or_repair(job_id)
                            .await;
                        return;
                    }

                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                } else {
                    info!(
                        job_id = job_id.0,
                        damaged,
                        recovery_now,
                        total_recovery_capacity,
                        "PAR2 verification — damage detected"
                    );

                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }

                    let repair_preview = match self
                        .run_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            false,
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    };
                    let repairer_damaged = repair_preview.verification.total_missing_blocks;
                    let repairer_recovery_now = repair_preview.recovery_blocks_available;
                    if repairer_damaged != damaged || repairer_recovery_now != recovery_now {
                        info!(
                            job_id = job_id.0,
                            placement_damaged = damaged,
                            repairer_damaged,
                            placement_recovery = recovery_now,
                            repairer_recovery = repairer_recovery_now,
                            files_renamed = repair_preview.files_renamed,
                            available_blocks = repair_preview.available_blocks,
                            "PAR2 repairer scan adjusted repair requirements"
                        );
                    }
                    let damaged = repairer_damaged;
                    let recovery_now = repairer_recovery_now;

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                        &repair_preview.verification.repairable
                    {
                        let msg = par2_resource_limit_message(reason);
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    if total_recovery_capacity < damaged {
                        self.finish_par2_set_failure(
                            job_id,
                            set_id,
                            format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        )
                        .await;
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            damaged,
                        );
                        let targeted_total = self.recovery_blocks_available_or_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();

                        // If all available/targeted recovery is still insufficient,
                        // fail immediately instead of waiting for downloads that
                        // won't help.
                        if targeted_total < damaged && !recovery_still_settling {
                            let msg = format!(
                                "not repairable: {damaged} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        if let Some(msg) = par2_unreachable_recovery_failure(
                            job_id,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted,
                            recovery_still_settling,
                            promoted_recovery.parked_promoted_recovery,
                        ) {
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    match self
                        .run_par2_repairer(job_id, Arc::clone(&par2_set), working_dir.clone(), true)
                        .await
                    {
                        Ok(outcome) => {
                            self.finish_par2_repair(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                &verification,
                                outcome,
                                has_crc_failures,
                            )
                            .await;
                            return;
                        }
                        Err(error_msg) => {
                            self.fail_par2_repair(job_id, error_msg);
                            return;
                        }
                    }
                }
            } else {
                if !par2_bypassed && self.promote_par2_metadata(job_id) {
                    info!(
                        job_id = job_id.0,
                        "waiting for PAR2 metadata download before repair evaluation"
                    );
                    self.transition_postprocessing_status(
                        job_id,
                        JobStatus::Downloading,
                        Some("downloading"),
                    );
                    return;
                }
                if has_incomplete_data_files {
                    let msg = format!(
                        "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete and no PAR2 metadata is available for repair"
                    );
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
                    }
                }
                if rar_waiting_for_missing_volumes {
                    let reason = self.invalid_rar_retry_frontier_reason(job_id).unwrap_or_else(|| {
                        "RAR extraction stalled waiting for missing volumes after downloads finished"
                            .to_string()
                    });
                    let msg = format!("{reason}; no PAR2 metadata is available for repair");
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if !has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
                    }
                }

                let failed_members: Vec<String> = self
                    .failed_extractions
                    .get(&job_id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                let msg = format!(
                    "extraction CRC failures with no PAR2 data: {:?}",
                    failed_members
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }
        } else if has_incomplete_data_files {
            if !download_pipeline_exhausted || self.job_has_active_extraction_tasks(job_id) {
                return;
            }
            if !par2_bypassed && self.promote_par2_metadata(job_id) {
                info!(
                    job_id = job_id.0,
                    "waiting for PAR2 metadata download before incomplete-download failure"
                );
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                return;
            }
            let repair_context = if par2_bypassed {
                "PAR2 recovery is bypassed"
            } else if self.par2_set(job_id).is_some() {
                "PAR2 recovery did not become eligible"
            } else {
                "no PAR2 metadata is available for repair"
            };
            let byte_detail = if failed_bytes > 0 {
                format!(", {failed_bytes} bytes unavailable")
            } else {
                String::new()
            };
            let msg = format!(
                "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete{byte_detail}, {repair_context}"
            );
            warn!(job_id = job_id.0, error = %msg);
            self.fail_job(job_id, msg);
            return;
        }

        // Every branch above this line either returns or leaves the job with
        // all of its data files complete and its PAR2 question settled, and
        // every branch below dispatches the job onward — to extraction, or
        // straight to the final move. So this is the one point a job with no
        // recovery set passes through on its way to completion, and it is
        // where a `.sfv` listing is both readable and still meaningful:
        //
        //  - after the PAR2 block, so a job with a set is adjudicated by it and
        //    the fallback's own scope guard sees a settled answer rather than
        //    racing one;
        //  - after deobfuscation, which only ever runs off PAR2 metadata, so
        //    the names a listing is matched against are the job's final ones;
        //  - before extraction, which is what consumes the posted files — the
        //    RAR volumes and split parts a listing actually names — and whose
        //    cleanup deletes them;
        //  - before the move to complete, so the working directory paths still
        //    resolve;
        //  - before the terminal transition records history, so a verdict
        //    reaches history and the UI through the same family PAR2 verdicts
        //    use rather than arriving after the job is already filed.
        if let Some(error) = self.verify_par2_less_job_with_sfv(job_id).await {
            self.fail_job(job_id, error);
            return;
        }

        if only_rar_archives {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        if self.job_has_promoted_recovery_pipeline_work(job_id, "extraction") {
            return;
        }

        // Check extraction readiness.
        let readiness = self.extraction_readiness_for_job(job_id);
        match readiness {
            ExtractionReadiness::NotApplicable => {
                // A complete non-archive payload can still have explicitly
                // promoted PAR2 recovery segments in flight. Do not let stale
                // completion checks finalize damaged direct/gzip/etc. payloads
                // before those recovery files are decoded and repaired.
                if self.job_has_promoted_recovery_pipeline_work(job_id, "completion") {
                    return;
                }
                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                if !par2_bypassed {
                    self.cleanup_par2_files(job_id).await;
                }
                // A split set the recovery data joined for us lands here rather
                // than in the extraction arm, so its spent parts are removed
                // here too — the final move relocates the whole directory, and
                // the parts are not part of the release.
                self.cleanup_par2_joined_split_parts(job_id).await;
                // No archives — move to complete and finish.
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
            }
            ExtractionReadiness::Ready => {
                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
                let already_extracted = self
                    .extracted_archives
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let already_spawned = self
                    .inflight_extractions
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let sets_to_extract: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    state
                        .assembly
                        .archive_topologies()
                        .iter()
                        .filter(|(name, _)| {
                            !already_extracted.contains(*name) && !already_spawned.contains(*name)
                        })
                        .map(|(name, topo)| (name.clone(), topo.archive_type))
                        .collect()
                };

                // If extractions are still in-flight, wait for them to complete.
                if !already_spawned.is_empty() && sets_to_extract.is_empty() {
                    return;
                }

                if !sets_to_extract.is_empty() {
                    // Spawn extraction tasks in the background.
                    // handle_extraction_done will re-enter check_job_completion
                    // when each set finishes, and we'll reach the empty branch below.
                    if !self.maybe_start_extraction(job_id).await {
                        return;
                    }

                    self.spawn_extractions(job_id, &sets_to_extract).await;
                    // Return — extraction runs in background.
                    // handle_extraction_done will call check_job_completion again.
                    return;
                }

                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                let cleanup_files: HashSet<String> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    let mut cleanup_files: HashSet<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                self.classified_role_for_file(job_id, f),
                                weaver_model::files::FileRole::Par2 { .. }
                                    | weaver_model::files::FileRole::RarVolume { .. }
                                    | weaver_model::files::FileRole::SevenZipArchive
                                    | weaver_model::files::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| self.current_filename_for_file(job_id, f))
                        .collect();
                    for topology in state.assembly.archive_topologies().values() {
                        cleanup_files.extend(topology.volume_map.keys().cloned());
                    }
                    cleanup_files.extend(self.par2_joined_split_part_names(job_id));
                    cleanup_files
                };
                let nested_decision = match self.maybe_start_nested_extraction(job_id).await {
                    Ok(decision) => decision,
                    Err(error) => {
                        self.fail_job(job_id, error);
                        return;
                    }
                };
                match nested_decision {
                    NestedExtractionDecision::Started
                    | NestedExtractionDecision::NoNestedArchives => {
                        let mut removed = 0u32;
                        for filename in &cleanup_files {
                            let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                                continue;
                            };
                            match tokio::fs::remove_file(&path).await {
                                Ok(()) => removed += 1,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => {
                                    warn!(
                                        file = %path.display(),
                                        error = %e,
                                        "failed to clean up source file"
                                    );
                                }
                            }
                        }
                        info!(
                            job_id = job_id.0,
                            removed,
                            total = cleanup_files.len(),
                            "post-extraction cleanup complete"
                        );
                        if matches!(nested_decision, NestedExtractionDecision::Started) {
                            return;
                        }
                    }
                    NestedExtractionDecision::PreserveOutputsAtDepthLimit => {}
                }

                info!(job_id = job_id.0, "extraction complete");
                // Low-frequency: one observation per job-level extraction, never on a
                // per-segment path. Records the metric next to the event that already
                // announces the same fact.
                self.metrics.job_lifecycle.note_extraction(
                    crate::operations::instrumentation::StageOutcomeKind::Complete,
                );
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Move extracted files to complete directory.
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
            }
            ExtractionReadiness::Blocked { reason } => {
                if reason.starts_with("archive topology not yet available") {
                    info!(
                        job_id = job_id.0,
                        reason = %reason,
                        "deferring completion until archive topology is available"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                self.fail_job(job_id, reason);
            }
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                // Some archives are ready (e.g. all 7z split files arrived)
                // while others are still downloading. Spawn what we can.
                let already_done = self
                    .extracted_archives
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let already_inflight = self
                    .inflight_extractions
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let to_spawn: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    extractable
                        .iter()
                        .filter(|name| {
                            !already_done.contains(*name) && !already_inflight.contains(*name)
                        })
                        .filter_map(|name| {
                            state
                                .assembly
                                .archive_topology_for(name)
                                .map(|topo| (name.clone(), topo.archive_type))
                        })
                        .collect()
                };

                if to_spawn.is_empty() {
                    return;
                }

                if !self.maybe_start_extraction(job_id).await {
                    return;
                }

                let spawned = self.spawn_extractions(job_id, &to_spawn).await;
                info!(
                    job_id = job_id.0,
                    spawned,
                    waiting = ?waiting_on,
                    "started extraction for ready archives, waiting on remaining"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_par2_verification_mode_labels_are_stable() {
        assert_eq!(CleanPar2VerificationMode::Grid.as_str(), "grid");
        assert_eq!(CleanPar2VerificationMode::FileCrc.as_str(), "file_crc");
        assert_eq!(
            CleanPar2VerificationMode::QuickDigest.as_str(),
            "quick_digest"
        );
        assert_eq!(
            CleanPar2VerificationMode::StrongDecode.as_str(),
            "strong_decode"
        );
        assert_eq!(
            CleanPar2VerificationMode::Authoritative.as_str(),
            "authoritative"
        );
    }

    fn file_id_from(seed: u8) -> par2_rs::FileId {
        par2_rs::FileId::from_bytes([seed; 16])
    }

    fn file_verification(
        seed: u8,
        filename: &str,
        status: par2_rs::verify::FileStatus,
        valid_slices: Vec<bool>,
    ) -> par2_rs::verify::FileVerification {
        let missing_slice_count = valid_slices.iter().filter(|valid| !**valid).count() as u32;
        par2_rs::verify::FileVerification {
            file_id: file_id_from(seed),
            filename: filename.to_string(),
            status,
            valid_slices,
            missing_slice_count,
        }
    }

    /// A four-file pre-repair verdict with one of each status.
    fn mixed_pre_repair_verification() -> par2_rs::VerificationResult {
        let files = vec![
            file_verification(
                1,
                "intact.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
            file_verification(
                2,
                "damaged.bin",
                par2_rs::verify::FileStatus::Damaged(1),
                vec![true, false],
            ),
            file_verification(
                3,
                "missing.bin",
                par2_rs::verify::FileStatus::Missing,
                vec![false, false],
            ),
            file_verification(
                4,
                "moved.bin",
                par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from(
                    "/work/elsewhere.bin",
                )),
                vec![true, true],
            ),
        ];
        let total_missing_blocks = files
            .iter()
            .map(|file| file.missing_slice_count)
            .sum::<u32>();
        par2_rs::VerificationResult {
            files,
            recovery_blocks_available: 8,
            total_missing_blocks,
            repairable: par2_rs::verify::Repairability::Repairable {
                blocks_needed: total_missing_blocks,
                blocks_available: 8,
            },
        }
    }

    /// Six intact parts at the wrong names is not a failed repair.
    ///
    /// This is the shape the placement-normalization fixture produces: every
    /// article arrived, nothing is damaged, and the parts simply need to be
    /// moved to the names the recovery set describes. The post-repair guard
    /// used to reject it — `needs_repair()` is true for a `Renamed` file — and
    /// report it as "0 damaged slices", the zero being the tell that there was
    /// nothing to repair at all. The rename entries the plan carries are
    /// derived from those very statuses, so the guard was refusing the repair
    /// for the one thing the next step fixes.
    fn multi_rename_post_repair_verification() -> par2_rs::VerificationResult {
        let files = (1..=6u8)
            .map(|part| {
                file_verification(
                    part,
                    &format!("fixture_rar5_lz_plain.part{part}.rar"),
                    par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from(format!(
                        "misplaced-{part}.rar"
                    ))),
                    vec![true, true],
                )
            })
            .collect::<Vec<_>>();
        par2_rs::verify::VerificationResult {
            files,
            recovery_blocks_available: 0,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        }
    }

    #[test]
    fn a_placement_only_post_repair_result_is_not_damage() {
        let verification = multi_rename_post_repair_verification();

        assert!(
            par2_verification_needs_repair(&verification),
            "precondition: misplacement alone still makes the crate's own \
             predicate report that something needs doing"
        );
        assert_eq!(
            verification.total_missing_blocks, 0,
            "precondition: and yet nothing is damaged, which is why the old \
             message could only ever say zero"
        );

        assert_eq!(
            par2_post_repair_damage_failure(&verification),
            None,
            "so the repair tail must not fail here: there is no damaged or \
             missing file to fail over"
        );

        let plan = placement_plan_from_verification(&verification);
        assert_eq!(
            plan.renames.len(),
            6,
            "and every one of them is a rename the plan already knows how to \
             apply; plan = {plan:?}"
        );
        assert!(
            plan.unresolved.is_empty(),
            "none of it is unresolvable; plan = {plan:?}"
        );
        assert!(
            plan.swaps.is_empty(),
            "the derived plan never emits swaps, so a swap-shaped fixture would \
             not reproduce this at all; plan = {plan:?}"
        );
    }

    #[test]
    fn post_repair_damage_still_fails_and_counts_what_remains() {
        let mut verification = multi_rename_post_repair_verification();
        verification.files.push(file_verification(
            9,
            "fixture_rar5_lz_plain.part7.rar",
            par2_rs::verify::FileStatus::Damaged(2),
            vec![false, false],
        ));
        verification.total_missing_blocks = 2;

        let failure = par2_post_repair_damage_failure(&verification)
            .expect("a damaged file after repair is still a failed repair");
        assert!(
            failure.contains("2 damaged slice(s) across 1 file(s)"),
            "and the message names what actually remains rather than a bare \
             zero; failure = {failure}"
        );
        assert!(
            failure.contains("6 file(s) still to be placed"),
            "including the misplacement it is not failing over; failure = {failure}"
        );
    }

    /// The write set is every file the repair could have acted on, which is
    /// every file that was not already complete at its canonical name.
    ///
    /// `Renamed` belongs in it. The rule used to read "Damaged and Missing",
    /// on the reasoning that misplaced content already exists intact somewhere
    /// else and is moved by placement rather than rewritten. A repair over a set
    /// whose only fault was misplacement disproved it: the repairer copied every
    /// one of those files onto its canonical name — the run reconstructed no
    /// slice and still reported bytes copied — and left the displaced originals
    /// as `<name>.N`. Carrying the pre-repair `Renamed` entries through that
    /// reported six placed files as still misplaced, and the placement step then
    /// tried to rename them onto names the repair had just filled.
    #[test]
    fn par2_repair_write_set_is_everything_not_already_complete() {
        let write_set = par2_repair_write_set(&mixed_pre_repair_verification());

        assert_eq!(
            write_set,
            vec![file_id_from(2), file_id_from(3), file_id_from(4)],
            "Damaged, Missing and Renamed are all files the repair installs at a \
             canonical name, so all three have to be re-read there afterwards. \
             Only Complete is carried: it is the one verdict a repair cannot \
             have invalidated."
        );
    }

    #[test]
    fn selective_pass_opts_into_slice_proof_verification() {
        let options = selective_pass_verify_options();

        assert!(
            options.fast_verify,
            "the selective post-repair arm proves a rewritten file from its \
             per-slice IFSC checksums (multi-buffer engine, ~12ms/128MiB) \
             instead of the inherently serial whole-file MD5 (~149ms/128MiB). \
             The identity the strict digest exists to establish is already \
             fixed here: the file was read at the canonical name the repairer \
             just installed it to, IFSC-verified before install."
        );
        assert!(
            options.cancel.is_none() && options.progress.is_none(),
            "fast-verify is the only option the selective arm sets"
        );
    }

    #[test]
    fn placement_plan_from_verification_places_by_verdict() {
        let plan = placement_plan_from_verification(&mixed_pre_repair_verification());

        assert_eq!(plan.exact, vec![file_id_from(1)]);
        assert_eq!(plan.unresolved, vec![file_id_from(2), file_id_from(3)]);
        assert_eq!(plan.renames.len(), 1);
        assert_eq!(plan.renames[0].file_id, file_id_from(4));
        assert_eq!(plan.renames[0].current_name, "elsewhere.bin");
        assert_eq!(plan.renames[0].correct_name, "moved.bin");
        assert!(
            plan.swaps.is_empty() && plan.conflicts.is_empty(),
            "a verdict-derived plan can express neither: the result holds one \
             entry per file ID, so nothing can be contested, and a pairwise \
             displacement was already normalized before the repair ran"
        );
    }

    /// A recovery set carrying nothing but the two numbers the merge reads:
    /// the recovery-block count and the file order.
    fn merge_test_par2_set(recovery_blocks: u32) -> par2_rs::Par2FileSet {
        let recovery_slices = (0..recovery_blocks)
            .map(|exponent| {
                (
                    exponent,
                    par2_rs::RecoverySlice {
                        exponent,
                        data: bytes::Bytes::from_static(&[0u8; 4]).into(),
                    },
                )
            })
            .collect();
        par2_rs::Par2FileSet {
            recovery_set_id: par2_rs::RecoverySetId::from_bytes([9u8; 16]),
            slice_size: 4,
            recovery_file_ids: (1..=4).map(file_id_from).collect(),
            non_recovery_file_ids: Vec::new(),
            files: HashMap::new(),
            slice_checksums: HashMap::new(),
            recovery_slices,
            creator: None,
        }
    }

    /// The shape the merge actually sees. `verify_all` and
    /// `verify_selected_file_ids` only ever report `Complete`, `Damaged` or
    /// `Missing` — `Renamed` comes from the repairer's own scanner, never from
    /// the pass that produces the pre-repair result this merges onto.
    fn production_pre_repair_verification() -> par2_rs::VerificationResult {
        let mut verification = mixed_pre_repair_verification();
        verification
            .files
            .retain(|file| !matches!(file.status, par2_rs::verify::FileStatus::Renamed(_)));
        verification
    }

    #[test]
    fn post_repair_merge_carries_untouched_entries_verbatim() {
        let par2_set = merge_test_par2_set(8);
        let base = production_pre_repair_verification();
        // What a selective pass over the write set comes back with once the
        // repair has installed both files. Deliberately out of recovery-set
        // order, so the reorder is doing work.
        let fresh = par2_rs::VerificationResult {
            files: vec![
                file_verification(
                    3,
                    "missing.bin",
                    par2_rs::verify::FileStatus::Complete,
                    vec![true, true],
                ),
                file_verification(
                    2,
                    "damaged.bin",
                    par2_rs::verify::FileStatus::Complete,
                    vec![true, true],
                ),
            ],
            recovery_blocks_available: 8,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        };

        let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

        // Recovery-set order, whatever order the selective read came back in.
        let order: Vec<par2_rs::FileId> = merged.files.iter().map(|file| file.file_id).collect();
        assert_eq!(
            order,
            vec![file_id_from(1), file_id_from(2), file_id_from(3)]
        );

        // The carried entry is the pre-repair pass's, untouched.
        let before = &base.files[0];
        let after = merged
            .files
            .iter()
            .find(|file| file.file_id == file_id_from(1))
            .expect("carried file must survive the merge");
        assert_eq!(after.filename, before.filename);
        assert_eq!(after.valid_slices, before.valid_slices);
        assert_eq!(after.missing_slice_count, before.missing_slice_count);
        assert_eq!(
            format!("{:?}", after.status),
            format!("{:?}", before.status),
            "downstream has to see exactly what a full pass over unchanged bytes \
             would have reported"
        );

        // The rewritten files carry the fresh verdict, and the set-level
        // numbers were recomputed rather than inherited.
        assert!(!par2_verification_needs_repair(&merged));
        assert_eq!(merged.total_missing_blocks, 0);
        assert!(matches!(
            merged.repairable,
            par2_rs::verify::Repairability::NotNeeded
        ));
    }

    #[test]
    fn a_carried_renamed_entry_would_keep_the_post_repair_gate_red() {
        // Not a production shape — the pre-repair pass cannot report `Renamed`
        // — but the carry rule is stated over all four statuses, so what it
        // would mean is pinned rather than assumed. `needs_repair` is any
        // status that is not `Complete`, so carrying a `Renamed` entry into the
        // post-repair gate would fail the job on "file placements remain". The
        // classification is still right: `Renamed` content is not rewritten by
        // a repair, so re-reading it would not change the verdict either.
        let par2_set = merge_test_par2_set(8);
        let base = mixed_pre_repair_verification();
        let fresh = par2_rs::VerificationResult {
            files: vec![
                file_verification(
                    2,
                    "damaged.bin",
                    par2_rs::verify::FileStatus::Complete,
                    vec![true, true],
                ),
                file_verification(
                    3,
                    "missing.bin",
                    par2_rs::verify::FileStatus::Complete,
                    vec![true, true],
                ),
            ],
            recovery_blocks_available: 8,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        };

        let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

        let carried = merged
            .files
            .iter()
            .find(|file| file.file_id == file_id_from(4))
            .expect("carried file must survive the merge");
        assert!(matches!(
            carried.status,
            par2_rs::verify::FileStatus::Renamed(_)
        ));
        assert_eq!(merged.total_missing_blocks, 0);
        assert!(par2_verification_needs_repair(&merged));
    }

    #[test]
    fn post_repair_merge_still_fails_a_file_the_repair_left_damaged() {
        let par2_set = merge_test_par2_set(8);
        let base = production_pre_repair_verification();
        let fresh = par2_rs::VerificationResult {
            files: vec![
                file_verification(
                    2,
                    "damaged.bin",
                    par2_rs::verify::FileStatus::Damaged(1),
                    vec![true, false],
                ),
                file_verification(
                    3,
                    "missing.bin",
                    par2_rs::verify::FileStatus::Complete,
                    vec![true, true],
                ),
            ],
            recovery_blocks_available: 8,
            total_missing_blocks: 1,
            repairable: par2_rs::verify::Repairability::Repairable {
                blocks_needed: 1,
                blocks_available: 8,
            },
        };

        let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

        assert!(
            par2_verification_needs_repair(&merged),
            "the gate that fails the job after a repair reads the merged result, \
             so a rewritten file that is still damaged has to survive the merge \
             as damage"
        );
        assert_eq!(merged.total_missing_blocks, 1);
    }

    #[test]
    fn placement_plan_from_a_clean_post_repair_result_is_a_no_op() {
        let mut verification = mixed_pre_repair_verification();
        for file in &mut verification.files {
            file.status = par2_rs::verify::FileStatus::Complete;
            file.valid_slices.fill(true);
            file.missing_slice_count = 0;
        }
        verification.total_missing_blocks = 0;

        let plan = placement_plan_from_verification(&verification);

        assert_eq!(plan.exact.len(), 4);
        assert!(
            plan.swaps.is_empty() && plan.renames.is_empty(),
            "which is what makes `apply_placement_plan_for_retry_or_repair` a \
             no-op for the post-repair pass: every file is already at the name \
             its description gives it"
        );
    }

    #[test]
    fn par2_repair_memory_limit_defaults_when_unset() {
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(None),
            DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
        );
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("  ")),
            DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
        );
    }

    #[test]
    fn par2_repair_memory_limit_accepts_positive_bytes() {
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("134217728")),
            134_217_728
        );
    }

    #[test]
    fn par2_repair_memory_limit_rejects_invalid_or_zero_values() {
        let default_bytes = default_par2_repair_memory_limit_bytes();
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("not-bytes")),
            default_bytes
        );
        assert_eq!(
            parse_par2_repair_memory_limit_bytes(Some("0")),
            default_bytes
        );
    }

    #[test]
    fn par2_ignore_extensions_default_to_the_baked_metadata_set() {
        assert_eq!(
            parse_par2_ignore_extensions(None),
            DEFAULT_PAR2_IGNORE_EXTENSIONS
                .iter()
                .map(|extension| (*extension).to_string())
                .collect::<Vec<_>>()
        );
        assert!(parse_par2_ignore_extensions(None).contains(&"nfo".to_string()));
        assert!(parse_par2_ignore_extensions(None).contains(&"sfv".to_string()));
    }

    #[test]
    fn an_empty_par2_ignore_extension_override_disables_the_rule() {
        assert!(parse_par2_ignore_extensions(Some("")).is_empty());
        assert!(parse_par2_ignore_extensions(Some("   ")).is_empty());
        assert!(!par2_damage_ignorable(
            "silver.horizon.nfo",
            &parse_par2_ignore_extensions(Some(""))
        ));
    }

    #[test]
    fn par2_ignore_extensions_accept_both_separators_and_normalize_entries() {
        assert_eq!(
            parse_par2_ignore_extensions(Some(" .NFO, sfv ;nfo;.Srr ")),
            vec!["nfo".to_string(), "sfv".to_string(), "srr".to_string()]
        );
    }

    #[test]
    fn par2_damage_is_ignorable_by_extension_only() {
        let extensions = parse_par2_ignore_extensions(None);
        assert!(par2_damage_ignorable("silver.horizon.nfo", &extensions));
        assert!(par2_damage_ignorable("SILVER.HORIZON.SFV", &extensions));
        // The extension is the file's own, not a substring of its name: a
        // payload that merely mentions one of these words is still payload.
        assert!(!par2_damage_ignorable(
            "silver.horizon.nfo.mkv",
            &extensions
        ));
        assert!(!par2_damage_ignorable("silver.horizon.mkv", &extensions));
        assert!(!par2_damage_ignorable("nfo", &extensions));
    }

    #[test]
    fn quick_proof_uses_crc_verified_contiguous_assembly() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("payload.bin");
        let bytes = b"crc-verified-contiguous-payload";
        std::fs::write(&path, bytes).unwrap();
        let candidate = Par2SessionEvidenceCandidate {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            path,
            logical_name: "payload.bin".to_string(),
            expected_length: bytes.len() as u64,
            full_md5: None,
            crc32: par2_rs::checksum::crc32(bytes),
            contiguous_assembly_proven: true,
            bound_file_id: None,
        };

        let evidence = committed_evidence_from_candidate(&candidate)
            .unwrap()
            .expect("contiguous CRC-verified assembly should be quick-proved");
        assert!(evidence.assembly_proof().is_some());
        assert_eq!(evidence.assembly_crc32(), Some(candidate.crc32));
    }

    #[test]
    fn quick_proof_refuses_unproven_assembly() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("payload.bin");
        std::fs::write(&path, b"payload").unwrap();
        let candidate = Par2SessionEvidenceCandidate {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            path,
            logical_name: "payload.bin".to_string(),
            expected_length: 7,
            full_md5: None,
            crc32: 0,
            contiguous_assembly_proven: false,
            bound_file_id: None,
        };

        assert!(
            committed_evidence_from_candidate(&candidate)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn source_changed_retry_is_limited_to_one_fresh_analysis() {
        let changed: Result<(), par2_rs::Par2SessionError> =
            Err(par2_rs::Par2SessionError::SourceChanged {
                path: std::path::PathBuf::from("payload.bin"),
            });
        assert!(should_retry_par2_source_change(&changed, false));
        assert!(!should_retry_par2_source_change(&changed, true));
    }

    #[test]
    fn incomplete_promoted_recovery_without_concrete_work_is_not_pending() {
        let state = PromotedRecoveryPipelineState {
            promoted_par2_files: 1,
            incomplete_promoted_par2_files: 1,
            ..Default::default()
        };

        assert!(!state.has_pending_work());
    }

    #[test]
    fn concrete_promoted_recovery_work_is_pending() {
        for state in [
            PromotedRecoveryPipelineState {
                download_queue_promoted_recovery: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                active_promoted_downloads: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                pending_promoted_retries: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                pending_promoted_decode: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                active_promoted_decodes: 1,
                ..Default::default()
            },
            PromotedRecoveryPipelineState {
                write_buffered_promoted_recovery: 1,
                ..Default::default()
            },
        ] {
            assert!(state.has_pending_work(), "{state:?}");
        }
    }

    #[test]
    fn parked_promoted_recovery_is_not_pending_until_reapplied() {
        let state = PromotedRecoveryPipelineState {
            parked_promoted_recovery: 1,
            ..Default::default()
        };

        assert!(!state.has_pending_work());
    }
}
