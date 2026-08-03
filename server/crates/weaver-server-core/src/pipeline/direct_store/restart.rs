//! Restart-side validation of direct-store coverage (plan 135, D6).
//!
//! Synchronous restart is bounded and **reads zero destination bytes**. The
//! barrier ordered data sync → checkpoint commit → floor publish, so a committed
//! floor is already durable. Restart therefore does exactly three things before
//! the job resumes:
//!
//! 1. validate the checkpoint framing, schema, generation and plan digest;
//! 2. confirm every claimed destination exists;
//! 3. confirm each is at least as long as its claimed extents.
//!
//! No byte verification and no destination reads beyond fs metadata. Revision 4's
//! "verify at most 64 MiB of destination tails" was dropped because the blob
//! stores no tail digests, so the check had no reference value; the integrity
//! gates re-arm in the background, from the verifier that must touch those bytes
//! anyway — never synchronously at startup, and never twice.
//!
//! File length never implies coverage. A **longer** destination than the claim
//! is expected and is not truncated; a **shorter** one means the claim outran
//! what is on disk, which is not a partial-trust situation — the whole set drops
//! to no coverage and redownloads.
//!
//! # What the probe is, and is not
//!
//! The probe is `std::fs::metadata`, which **follows symlinks**: a claimed path
//! that is a symlink to a long enough regular file is accepted, and the length
//! read is the target's. That is deliberate and it is not this module's stance
//! to make — restart only reads metadata, and reading a symlinked file's length
//! discloses nothing and writes nothing. The stance that matters belongs to the
//! phase 4 writer, which opens these paths for writing: it owns whether a
//! destination may be a symlink at all (`O_NOFOLLOW`, or an `is_symlink` refusal
//! before the open), and once it refuses to write through one, no checkpoint can
//! come to claim one either. Snapshot decoding independently refuses paths that
//! escape the working directory lexically, so the probe is only ever handed
//! job-relative paths.
//!
//! A probe that cannot be completed is **not** a pass. "Could not check" is a
//! refusal ([`CoverageRejection::ProbeFailed`]): accepting a checkpoint whose
//! destinations were never validated is the one outcome this module exists to
//! prevent.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::DirectStoreGate;
use super::barrier::CoveragePersist;
use super::snapshot::{CoverageSnapshot, SnapshotError, decode};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobSpec;
use crate::jobs::service::segments_covered_by_floor;

/// Why a checkpoint row was refused. Every variant means the same thing for the
/// set: **no coverage**, redownload from zero, and delete the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoverageRejection {
    Decode(SnapshotError),
    /// A committed checkpoint always carries a generation of at least 1.
    InvalidGeneration,
    /// Hard stop: safe redownload or demotion, never partial trust.
    PlanDigestMismatch,
    /// The row names a set the current plan does not have.
    UnknownSet,
    MissingDestination {
        path: String,
    },
    ShortDestination {
        path: String,
        claimed: u64,
        actual: u64,
    },
    /// The blob's volume-to-file mapping disagrees with the layout plan's.
    /// `expected` is `None` when the plan has no such volume at all.
    ///
    /// The blob carries `file_index` so it is self-contained, but the plan is
    /// authoritative: a flipped index would derive the refetch floor for the
    /// wrong NZB file and skip segments of a file nothing ever wrote.
    FileIndexMismatch {
        volume_index: u32,
        expected: Option<u32>,
        found: u32,
    },
    /// The destination probe did not complete — the blocking task panicked, or
    /// the runtime was torn down under it. Never an acceptance: a checkpoint
    /// whose destinations went unchecked is not a checked checkpoint.
    ProbeFailed {
        error: String,
    },
}

impl std::fmt::Display for CoverageRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "{error}"),
            Self::InvalidGeneration => {
                write!(formatter, "direct-store checkpoint has generation 0")
            }
            Self::PlanDigestMismatch => write!(
                formatter,
                "direct-store checkpoint was written against a different layout plan"
            ),
            Self::UnknownSet => write!(
                formatter,
                "direct-store checkpoint names an archive set this job no longer plans"
            ),
            Self::MissingDestination { path } => write!(
                formatter,
                "direct-store destination {path} claimed by the checkpoint does not exist"
            ),
            Self::ShortDestination {
                path,
                claimed,
                actual,
            } => write!(
                formatter,
                "direct-store destination {path} is {actual} bytes but the checkpoint claims {claimed}"
            ),
            Self::FileIndexMismatch {
                volume_index,
                expected,
                found,
            } => match expected {
                Some(expected) => write!(
                    formatter,
                    "direct-store checkpoint maps volume {volume_index} to NZB file {found}, but the plan maps it to {expected}"
                ),
                None => write!(
                    formatter,
                    "direct-store checkpoint maps volume {volume_index} to NZB file {found}, but the plan has no such volume"
                ),
            },
            Self::ProbeFailed { error } => write!(
                formatter,
                "direct-store destination probe did not complete: {error}"
            ),
        }
    }
}

/// The plan facts a checkpoint is validated against.
///
/// Both are read from the layout plan the job is resuming with, and both are
/// authoritative over whatever the blob says about itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExpectedSet {
    /// Digest of the exact layout plan. A mismatch is a hard stop.
    pub(crate) plan_digest: [u8; 32],
    /// Volume index to NZB file index, from the plan. Checked against every
    /// [`VolumeFloor`](super::snapshot::VolumeFloor) in the blob: the blob keeps
    /// its own copy so a row is self-describing, but only the plan decides
    /// which file a volume's floor is a floor *of*.
    pub(crate) volume_files: HashMap<u32, u32>,
}

#[derive(Debug, Default)]
pub(crate) struct RestoreOutcome {
    /// Set name to accepted checkpoint.
    pub(crate) accepted: HashMap<String, CoverageSnapshot>,
    /// Set name and why it was refused. Each of these rows has been deleted.
    pub(crate) rejected: Vec<(String, CoverageRejection)>,
    /// Rows left untouched because the gate is off.
    pub(crate) ignored: usize,
}

/// One destination the checkpoint claims, ready to be probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DestinationProbe {
    /// Absolute path: the working directory joined with the claim's path.
    pub(super) path: PathBuf,
    /// The claim's path, for messages.
    pub(super) relative_path: String,
    /// The length the file must have for the claim to be admissible.
    pub(super) claimed: u64,
}

/// A probe's answer. `actual` is `None` when nothing usable is at the path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProbedDestination {
    pub(super) relative_path: String,
    pub(super) claimed: u64,
    pub(super) actual: Option<u64>,
}

/// The real probe: fs metadata, nothing else. Regular files only — a directory
/// or a socket where a destination should be is a missing destination.
fn probe_destination_lengths(probes: Vec<DestinationProbe>) -> Vec<ProbedDestination> {
    probes
        .into_iter()
        .map(|probe| ProbedDestination {
            relative_path: probe.relative_path,
            claimed: probe.claimed,
            actual: std::fs::metadata(&probe.path)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len()),
        })
        .collect()
}

/// Validates one checkpoint against the current plan and the destinations on
/// disk.
///
/// The fs probes run on the blocking pool; the decision itself is pure.
pub(crate) async fn restore_set(
    working_dir: &Path,
    blob: &[u8],
    expected: &ExpectedSet,
) -> Result<CoverageSnapshot, CoverageRejection> {
    restore_set_with_probe(working_dir, blob, expected, probe_destination_lengths).await
}

/// [`restore_set`] with the destination probe injected, so the failure modes of
/// probing — a panic, a torn-down runtime, a short answer — are testable.
pub(super) async fn restore_set_with_probe<F>(
    working_dir: &Path,
    blob: &[u8],
    expected: &ExpectedSet,
    probe: F,
) -> Result<CoverageSnapshot, CoverageRejection>
where
    F: FnOnce(Vec<DestinationProbe>) -> Vec<ProbedDestination> + Send + 'static,
{
    let snapshot = decode(blob).map_err(CoverageRejection::Decode)?;
    if snapshot.generation == 0 {
        return Err(CoverageRejection::InvalidGeneration);
    }
    if snapshot.plan_digest != expected.plan_digest {
        return Err(CoverageRejection::PlanDigestMismatch);
    }
    // The plan owns the volume-to-file mapping. A blob that disagrees would
    // derive its refetch floors against the wrong NZB files, skipping segments
    // of a file nothing ever wrote — so a single disagreement retires the whole
    // row, exactly like a digest mismatch.
    for entry in &snapshot.floors {
        let planned = expected.volume_files.get(&entry.volume_index).copied();
        if planned != Some(entry.file_index) {
            return Err(CoverageRejection::FileIndexMismatch {
                volume_index: entry.volume_index,
                expected: planned,
                found: entry.file_index,
            });
        }
    }

    let probes: Vec<DestinationProbe> = snapshot
        .destinations
        .iter()
        .map(|claim| DestinationProbe {
            path: working_dir.join(&claim.relative_path),
            relative_path: claim.relative_path.clone(),
            claimed: claim.claimed_len(),
        })
        .collect();
    let expected_probes = probes.len();

    // A `JoinError` — the probe panicked, or the runtime was torn down during
    // startup — is a refusal, never an empty answer. Defaulting to no probe
    // results would walk an empty loop and accept the checkpoint having
    // validated nothing at all.
    let probed = tokio::task::spawn_blocking(move || probe(probes))
        .await
        .map_err(|error| CoverageRejection::ProbeFailed {
            error: error.to_string(),
        })?;
    if probed.len() != expected_probes {
        return Err(CoverageRejection::ProbeFailed {
            error: format!(
                "probed {} of {expected_probes} claimed destinations",
                probed.len()
            ),
        });
    }

    for destination in probed {
        let Some(actual) = destination.actual else {
            return Err(CoverageRejection::MissingDestination {
                path: destination.relative_path,
            });
        };
        if actual < destination.claimed {
            return Err(CoverageRejection::ShortDestination {
                path: destination.relative_path,
                claimed: destination.claimed,
                actual,
            });
        }
    }

    Ok(snapshot)
}

/// Validates every checkpoint row a job carries, deleting the ones it refuses.
///
/// With the gate off the rows are **ignored, not deleted**: a downgraded or
/// temporarily disabled binary must not destroy coverage a re-enabled one could
/// still validate. It simply sees no floors and redownloads, which is safe.
///
/// # `expected` must be complete
///
/// `expected` is the job's **whole** set of planned archive sets. A row naming a
/// set that is absent from it is refused as [`CoverageRejection::UnknownSet`]
/// and its row is **deleted** — that is the point of the variant, since a set
/// the plan no longer has is a set whose coverage can never be validated again.
/// So a caller that passes a partial map does not merely fail to restore those
/// sets; it destroys their checkpoints. Build the map from the same layout plan
/// the job is resuming with, for every set in it, before calling.
pub(crate) async fn restore_job<P: CoveragePersist + ?Sized>(
    gate: DirectStoreGate,
    job_id: JobId,
    working_dir: &Path,
    rows: HashMap<String, Vec<u8>>,
    expected: &HashMap<String, ExpectedSet>,
    persist: &mut P,
) -> RestoreOutcome {
    if !gate.is_enabled() {
        return RestoreOutcome {
            ignored: rows.len(),
            ..RestoreOutcome::default()
        };
    }

    let mut outcome = RestoreOutcome::default();
    let mut set_names = rows.keys().cloned().collect::<Vec<_>>();
    set_names.sort();

    for set_name in set_names {
        let blob = rows.get(&set_name).expect("set name came from rows");
        let result = match expected.get(&set_name) {
            Some(expected_set) => restore_set(working_dir, blob, expected_set).await,
            None => Err(CoverageRejection::UnknownSet),
        };
        match result {
            Ok(snapshot) => {
                outcome.accepted.insert(set_name, snapshot);
            }
            Err(rejection) => {
                if let Err(error) = persist.delete(job_id, &set_name) {
                    tracing::warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        %error,
                        "failed to delete a refused direct-store coverage row"
                    );
                }
                outcome.rejected.push((set_name, rejection));
            }
        }
    }

    outcome
}

/// Per-NZB-file refetch floors derived from a checkpoint.
///
/// Everything above a floor is redownloaded. Actual refetch can exceed the
/// barrier interval, because floors are contiguous and coverage sitting above a
/// stalled floor — a source-volume hole waiting on a slow article — goes with
/// it. That is the accepted cost of a contiguous-floor model.
pub(crate) fn refetch_floors(snapshot: &CoverageSnapshot) -> HashMap<u32, u64> {
    let mut floors: HashMap<u32, u64> = HashMap::with_capacity(snapshot.floors.len());
    for entry in &snapshot.floors {
        let slot = floors.entry(entry.file_index).or_insert(entry.floor);
        // One NZB file is one source volume, but a malformed blob could repeat
        // a file index; take the safest floor rather than the last one seen.
        *slot = (*slot).min(entry.floor);
    }
    floors
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectSkipPlan {
    pub(crate) skip: HashSet<SegmentId>,
    /// NZB file index to the contiguous byte floor whole segments account for.
    pub(crate) file_progress: HashMap<u32, u64>,
}

/// Derives the segments a direct set may skip on restart.
///
/// Shares `segments_covered_by_floor` with the legacy restore path, but
/// deliberately **without** its `metadata.len()` clamp: for a direct set the
/// source volume never exists as a file, so clamping to its length would zero
/// every floor and redownload the whole job. File length never implies
/// coverage.
pub(crate) fn coverage_skip_plan(
    job_id: JobId,
    spec: &JobSpec,
    floors: &HashMap<u32, u64>,
) -> DirectSkipPlan {
    let mut plan = DirectSkipPlan::default();
    for (file_index, file_spec) in spec.files.iter().enumerate() {
        let file_index = file_index as u32;
        let Some(floor) = floors.get(&file_index).copied() else {
            continue;
        };
        let file_id = NzbFileId { job_id, file_index };
        if floor == 0 {
            plan.file_progress.insert(file_index, 0);
            continue;
        }
        let covered = segments_covered_by_floor(file_id, &file_spec.segments, floor);
        plan.skip.extend(covered.segments);
        plan.file_progress.insert(file_index, covered.floor);
    }
    plan
}
