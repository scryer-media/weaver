//! Restart-side validation of direct-store coverage.
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
//! No byte verification and no destination reads beyond fs metadata. An earlier
//! revision's "verify at most 64 MiB of destination tails" was dropped because
//! the blob stores no tail digests, so the check had no reference value; the
//! integrity gates re-arm in the background, from the verifier that must touch
//! those bytes anyway — never synchronously at startup, and never twice.
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
//! writer, which opens these paths for writing: it owns whether a destination
//! may be a symlink at all (`O_NOFOLLOW`, or an `is_symlink` refusal before the
//! open), and once it refuses to write through one, no checkpoint can come to
//! claim one either. Snapshot decoding independently refuses paths that escape
//! their root lexically, so the probe is only ever handed job-relative paths.
//!
//! A probe that cannot be completed is **not** a pass. "Could not check" is a
//! refusal ([`CoverageRejection::ProbeFailed`]): accepting a checkpoint whose
//! destinations were never validated is the one outcome this module exists to
//! prevent.
//!
//! # Where job restore enters
//!
//! [`Pipeline::restore_direct_store_coverage`] is the seam. It runs before the
//! job's assembly is built, because its output *is* part of the restore skip
//! set, and it does five things in order:
//!
//! 1. rediscovers the job's candidate sets from the restored spec, gate-aware;
//! 2. rebuilds each one's layout from `active_rar_volume_facts` — the header
//!    bytes sit below the published floors and are never refetched, so the facts
//!    are the only way back to the members, their destinations and the plan
//!    digest;
//! 3. validates every checkpoint row against those rebuilt plans ([`restore_job`]);
//! 4. turns each accepted row's floors into skipped segments
//!    ([`coverage_skip_plan`]), exactly the way legacy floors feed the same
//!    machinery;
//! 5. sweeps **both** of the job's roots — the working directory and the
//!    staging root — of direct-store files nothing claims.
//!
//! A set with no accepted row is installed **fresh**: it redownloads and routes
//! from zero, which is what an unwired restart already did, and its stale
//! partials and envelopes are swept first so no byte of them survives.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::DirectStoreGate;
use super::barrier::{CoveragePersist, DatabaseCoveragePersist};
use super::plan::DirectSetPlan;
use super::set::DirectSet;
use super::snapshot::{CoverageSnapshot, SnapshotError, decode};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobSpec;
use crate::jobs::service::segments_covered_by_floor;
use crate::pipeline::Pipeline;

/// The durable per-volume facts a restart rebuilds a set's layout from.
///
/// One row per (job, set, volume) of `active_rar_volume_facts`, whose column is
/// an opaque blob — which is what lets a second container family join it with
/// no migration and no invalidation of anything already written.
///
/// # The untagged fallback is the compatibility guarantee
///
/// Rows written before this envelope existed are a bare `RarVolumeFacts` map,
/// and they belong to sets that are mid-download in a shipped release. Decoding
/// tries the tagged form first and falls back to the bare one, so those rows
/// restore exactly as they did — a new tag on the wire would otherwise turn
/// every in-flight direct set into a redownload on the upgrade.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum DirectVolumeFacts {
    Rar(Box<unrar_rs::RarVolumeFacts>),
    SevenZip(Box<SevenZipVolumeFacts>),
}

/// What one volume of a 7z set contributes to the restored layout.
///
/// A 7z container states its map once, at the tail, so exactly one volume's row
/// carries `container` and every row carries the one thing that is genuinely
/// per volume: the length the wire declared for it. Both halves are needed —
/// the map gives container offsets, and only the lengths turn those into the
/// (volume, offset) pairs everything downstream is expressed in.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SevenZipVolumeFacts {
    pub(crate) declared_len: u64,
    pub(crate) container: Option<super::router::sevenz::SevenZipContainerFacts>,
}

impl DirectVolumeFacts {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec_named(self)
    }

    pub(crate) fn decode(blob: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        match rmp_serde::from_slice::<Self>(blob) {
            Ok(facts) => Ok(facts),
            Err(tagged) => rmp_serde::from_slice::<unrar_rs::RarVolumeFacts>(blob)
                .map(|facts| Self::Rar(Box::new(facts)))
                .map_err(|_| tagged),
        }
    }
}

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
    /// The row claims coverage in a volume the rebuilt layout has no cached
    /// facts for, so nothing it restores for that volume could ever be
    /// classified.
    UnclassifiableVolume {
        volume_index: u32,
    },
    /// A volume the row still claims already has bytes in a conventional
    /// file: a demotion's per-volume handback was interrupted before the row
    /// retired. Two images of one volume are never reconciled.
    ConventionalBytes {
        file_index: u32,
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
            Self::ConventionalBytes { file_index } => write!(
                formatter,
                "direct-store checkpoint claims NZB file {file_index}, which already has conventional bytes on disk"
            ),
            Self::UnclassifiableVolume { volume_index } => write!(
                formatter,
                "direct-store checkpoint claims coverage in volume {volume_index}, which the \
                 rebuilt layout has no cached facts for"
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
    /// The volumes the rebuilt layout actually has cached facts for.
    ///
    /// Not the same question as `volume_files`, which is what the *plan* has. A
    /// set's facts are cached per volume and any subset of them can be missing
    /// — a volume that never finished its confirming parse, a row that failed
    /// to decode — and the layout rebuild happily proceeds without them,
    /// because a volume that contributed no member changes nothing the plan
    /// digest covers. The digest therefore still matches, and the row is
    /// accepted for a set with a volume the router cannot classify a byte of:
    /// its restored coverage can never be mapped, so its bytes are held for the
    /// life of the set and it wedges exactly the way the other
    /// unclassifiable-volume cases do. A row claiming coverage in a volume that
    /// is not in here is refused instead.
    pub(crate) fact_volumes: HashSet<u32>,
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

/// The two roots a direct set's destinations resolve against.
///
/// Envelopes are working data and live in the job's working directory; member
/// payload lives in the job's staging root on the complete volume, so its commit
/// rename and completion's publish are both same-filesystem. A checkpoint's
/// claims mix the two, keyed by destination index, so restart has to carry both
/// roots to probe them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DestinationRoots {
    pub(crate) working_dir: PathBuf,
    pub(crate) destination_dir: PathBuf,
}

impl DestinationRoots {
    /// The roots a plan resolves its own derived paths against.
    ///
    /// Production code reaches the two roots through the plan itself; this is
    /// for the tests that hand a plan's roots straight to [`restore_set`].
    #[cfg(test)]
    pub(crate) fn for_plan(plan: &DirectSetPlan) -> Self {
        Self {
            working_dir: plan.working_dir.clone(),
            destination_dir: plan.destination_dir.clone(),
        }
    }

    /// The root one claim's relative path hangs off, decided by the destination
    /// key band rather than by the path text — see
    /// [`super::set::envelope_volume_for_key`]. `volumes` is the plan's volume
    /// set, which is what makes the classification exact.
    fn resolve(
        &self,
        destination_key: u32,
        relative_path: &str,
        volumes: &HashSet<u32>,
    ) -> PathBuf {
        if volumes.contains(&super::set::envelope_volume_for_key(destination_key)) {
            self.working_dir.join(relative_path)
        } else {
            self.destination_dir.join(relative_path)
        }
    }
}

/// One destination the checkpoint claims, ready to be probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DestinationProbe {
    /// Absolute path: the claim's own root joined with the claim's path.
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
    roots: &DestinationRoots,
    blob: &[u8],
    expected: &ExpectedSet,
) -> Result<CoverageSnapshot, CoverageRejection> {
    restore_set_with_probe(roots, blob, expected, probe_destination_lengths).await
}

/// [`restore_set`] with the destination probe injected, so the failure modes of
/// probing — a panic, a torn-down runtime, a short answer — are testable.
pub(super) async fn restore_set_with_probe<F>(
    roots: &DestinationRoots,
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
        // The layout rebuild is per volume and tolerant: a volume whose cached
        // facts did not come back simply contributes no member, which changes
        // neither the plan digest nor the member destinations the digest binds
        // — so the row sails through every check above and is accepted for a
        // set that cannot classify a byte of that volume. Its restored coverage
        // then maps to nothing, its refetched articles are held rather than
        // routed, and the set neither finalizes nor demotes.
        //
        // Only a volume the row actually claims matters: a floor of zero with no
        // completion claims nothing, and refusing on it would retire perfectly
        // good rows for volumes that had simply not started.
        if (entry.floor > 0 || entry.complete)
            && !expected.fact_volumes.contains(&entry.volume_index)
        {
            return Err(CoverageRejection::UnclassifiableVolume {
                volume_index: entry.volume_index,
            });
        }
    }

    // A volume's envelope key is derived from the volume index, which is a
    // layout coordinate and therefore the same on every run; a member key is an
    // in-run counter but always in the low band. So the plan's volume set is
    // enough to send each claim to the root it was written under, without
    // trusting the blob's own idea of which is which.
    let volumes: HashSet<u32> = expected.volume_files.keys().copied().collect();
    let probes: Vec<DestinationProbe> = snapshot
        .destinations
        .iter()
        .map(|claim| DestinationProbe {
            path: roots.resolve(claim.member_index, &claim.relative_path, &volumes),
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
    roots: &DestinationRoots,
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
            Some(expected_set) => restore_set(roots, blob, expected_set).await,
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

/// Whether a finalized set's installation marker still describes this spec and
/// this staging root.
///
/// Metadata only, like every other restart probe: the members were verified
/// before they were committed, and what a restart can lose is the file, not its
/// bytes. A probe that cannot complete is a refusal, for the same reason
/// [`CoverageRejection::ProbeFailed`] is.
async fn installed_set_still_present(
    plan: &DirectSetPlan,
    blob: &[u8],
) -> Result<super::snapshot::InstalledSet, String> {
    let marker = super::snapshot::decode_installed(blob).map_err(|error| error.to_string())?;
    let volumes: Vec<(u32, u32)> = plan
        .volumes
        .iter()
        .map(|(volume, file)| (*volume, *file))
        .collect();
    if marker.volumes != volumes {
        return Err("the set's volumes no longer match the marker".to_string());
    }
    if marker.members.is_empty() {
        return Err("the marker names no committed member".to_string());
    }
    for member in &marker.members {
        let path = plan.destination_path(&member.relative_path);
        match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() && metadata.len() == member.len => {}
            Ok(metadata) => {
                return Err(format!(
                    "committed member {} is {} bytes, expected {}",
                    member.relative_path,
                    metadata.len(),
                    member.len
                ));
            }
            Err(error) => {
                return Err(format!(
                    "committed member {} could not be probed: {error}",
                    member.relative_path
                ));
            }
        }
    }
    for directory in &marker.directories {
        let path = plan.destination_path(directory);
        match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(format!(
                    "installed directory {directory} is not a directory"
                ));
            }
            Err(error) => {
                return Err(format!(
                    "installed directory {directory} could not be probed: {error}"
                ));
            }
        }
    }
    Ok(marker)
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

/// NZB file indices whose source volume the checkpoint says finished
/// downloading.
///
/// Kept apart from [`refetch_floors`] because it answers a different question in
/// different units: a floor is decoded source bytes and the spec's segment sizes
/// are yEnc-encoded, so no floor can ever prove a file complete. A repeated file
/// index in a malformed blob is resolved the same conservative way — every entry
/// naming the file must agree it is complete.
pub(crate) fn complete_files(snapshot: &CoverageSnapshot) -> HashSet<u32> {
    let mut complete: HashSet<u32> = HashSet::new();
    let mut refuted: HashSet<u32> = HashSet::new();
    for entry in &snapshot.floors {
        if entry.complete {
            complete.insert(entry.file_index);
        } else {
            refuted.insert(entry.file_index);
        }
    }
    complete.retain(|file_index| !refuted.contains(file_index));
    complete
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
    complete: &HashSet<u32>,
) -> DirectSkipPlan {
    let mut plan = DirectSkipPlan::default();
    for (file_index, file_spec) in spec.files.iter().enumerate() {
        let file_index = file_index as u32;
        let Some(floor) = floors.get(&file_index).copied() else {
            continue;
        };
        let file_id = NzbFileId { job_id, file_index };
        // A volume the checkpoint calls complete skips **every** segment. The
        // floor cannot reach the last one — see
        // [`super::snapshot::VolumeFloor::complete`] — and refetching it for a
        // set that is entirely on disk is the exact cost the flag exists to
        // remove.
        if complete.contains(&file_index) {
            let total: u64 = file_spec
                .segments
                .iter()
                .map(|segment| segment.bytes as u64)
                .sum();
            plan.skip
                .extend(file_spec.segments.iter().map(|segment| SegmentId {
                    file_id,
                    segment_number: segment.ordinal,
                }));
            plan.file_progress.insert(file_index, total);
            continue;
        }
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

/// Everything a job restore learned about its direct sets.
#[derive(Debug, Default)]
pub(crate) struct DirectRestore {
    /// Rebuilt sets, ready to install once the job state exists.
    pub(crate) sets: Vec<DirectSet>,
    /// Segments the accepted checkpoints cover, to union into the restore skip
    /// set exactly like legacy floors.
    pub(crate) skip: HashSet<SegmentId>,
    pub(crate) file_progress: HashMap<u32, u64>,
    /// Sets whose installation marker was accepted: each is already in
    /// [`Self::sets`], finalized, and carries the names its finalization
    /// recorded as extracted, which only the live job state can hold.
    pub(crate) installed: Vec<(String, Vec<String>)>,
    pub(crate) accepted: usize,
    pub(crate) rejected: usize,
    pub(crate) ignored: usize,
    pub(crate) swept: usize,
}

/// Filename markers of the files direct-store owns inside a working directory.
///
/// Only the partial suffix is still matched as a *pattern*; envelopes are swept
/// by name, from the plans that produce them, because `.envelope` is an extension
/// an extracted member can legitimately carry. See
/// [`sweep_orphan_direct_files`].
const DIRECT_PARTIAL_SUFFIX: &str = ".direct.partial";
/// Prefix of the holds scratch files. Matched as a prefix rather than a suffix
/// because the set name is the tail of the component.
pub(crate) const HOLDS_SCRATCH_PREFIX: &str = ".weaver-holds.";

/// How deep the sweep walks below the working directory. Member partials live
/// wherever their member's stored path puts them, which is archive-controlled;
/// a bound keeps a hostile or pathological tree from turning the sweep into an
/// unbounded startup cost.
const SWEEP_MAX_DEPTH: usize = 8;

impl Pipeline {
    /// Restores a job's direct-store sets and the segments their coverage lets
    /// the job skip.
    ///
    /// Called from `restore_job` before the assembly is built. It never inserts
    /// anything into the pipeline itself — the job state does not exist yet — so
    /// the caller installs [`DirectRestore::sets`] once it does.
    pub(crate) async fn restore_direct_store_coverage(
        &mut self,
        job_id: JobId,
        spec: &JobSpec,
        working_dir: &Path,
        conventional_floors: &HashMap<u32, u64>,
    ) -> DirectRestore {
        let gate = self.direct_store.gate();
        // Deterministic, and it has to be: this runs before the job state
        // exists, so `state.staging_dir` cannot be consulted — and a resumed set
        // must derive exactly the destinations the previous run wrote, or every
        // claim fails the probe. `Pipeline::extraction_staging_dir` can only
        // ever hand out this same path for a live job, so the live seam
        // (`ensure_direct_sets`) and this one agree by construction.
        let destination_dir = self.deterministic_extraction_staging_dir(job_id);
        let roots = DestinationRoots {
            working_dir: working_dir.to_path_buf(),
            destination_dir: destination_dir.clone(),
        };
        // Discovery is pure planning over the spec, so it runs whatever the gate
        // says: the sweep below needs to know which files direct-store *could*
        // own in this working directory even when nothing will route into them,
        // and those are the files a previously enabled binary wrote over the same
        // spec.
        let (planned, refused) = DirectSetPlan::discover(spec, working_dir, &destination_dir);
        if gate.is_enabled() {
            // The same counters the live admission seam emits. Restoring a job is
            // an admission decision too, and a set refused here is one whose
            // coverage is about to be swept — leaving it uncounted made a
            // restart-only refusal invisible to the metric that exists to explain
            // exactly that. Scoped to an enabled gate because a refusal is only a
            // decision when something would otherwise have been admitted.
            for (set_name, refusal) in &refused {
                crate::runtime::perf_probe::record_owned(
                    format!("direct_store.refused.{}", refusal.metric()),
                    std::time::Duration::from_nanos(1),
                );
                tracing::debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    reason = refusal.metric(),
                    "direct-store set refused at restore"
                );
            }
        }
        let admitted: Vec<DirectSetPlan> = if gate.is_enabled() {
            planned.clone()
        } else {
            Vec::new()
        };

        let rows = match self
            .db_blocking(move |db| db.load_direct_coverage(job_id))
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(
                    job_id = job_id.0,
                    %error,
                    "failed to load direct-store coverage rows; the job redownloads"
                );
                HashMap::new()
            }
        };
        // A finalized set leaves an installation marker where its coverage row
        // was. It is not coverage and `restore_job` would refuse it as a bad
        // magic, so it is judged on its own terms here: a marker whose set is
        // still admitted under the same volumes, and whose every committed
        // member is still in the staging root at its exact length, brings the
        // set back finalized instead of fresh. Anything else is deleted, and the
        // set redownloads as it would have with no marker at all.
        let (marker_rows, rows): (HashMap<String, Vec<u8>>, HashMap<String, Vec<u8>>) = rows
            .into_iter()
            .partition(|(_, blob)| super::snapshot::is_installed_marker(blob));
        let mut installed: HashMap<String, super::snapshot::InstalledSet> = HashMap::new();
        let mut rejected_markers = 0usize;
        let ignored_markers = if gate.is_enabled() {
            0
        } else {
            marker_rows.len()
        };
        if gate.is_enabled() {
            let mut marker_rows: Vec<(String, Vec<u8>)> = marker_rows.into_iter().collect();
            marker_rows.sort();
            for (set_name, blob) in marker_rows {
                let verdict = match admitted.iter().find(|plan| plan.set_name == set_name) {
                    Some(plan) => installed_set_still_present(plan, &blob).await,
                    None => Err("the set is not admitted from this spec".to_string()),
                };
                match verdict {
                    Ok(marker) => {
                        installed.insert(set_name, marker);
                    }
                    Err(reason) => {
                        rejected_markers += 1;
                        crate::runtime::perf_probe::record_owned(
                            "direct_store.restart.rejected".to_string(),
                            std::time::Duration::from_nanos(1),
                        );
                        tracing::info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            %reason,
                            "direct-store installation marker refused at restore; the set \
                             redownloads"
                        );
                        if let Err(error) = self
                            .db_blocking({
                                let set_name = set_name.clone();
                                move |db| db.delete_direct_coverage(job_id, &set_name)
                            })
                            .await
                        {
                            tracing::warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                %error,
                                "failed to delete a refused direct-store installation marker"
                            );
                        }
                    }
                }
            }
        }
        // Nothing to validate and nothing admitted still leaves the sweep to
        // run: a job that used to route and no longer does (the gate went off,
        // the spec changed) is exactly the job whose working directory holds
        // partials nothing will ever claim again.
        let facts = if admitted.is_empty() {
            HashMap::new()
        } else {
            self.load_direct_volume_facts(job_id).await
        };

        // Step 2: rebuild every admitted set's layout from its cached facts. A
        // set whose facts no longer form a routable archive is dropped from
        // `expected` on purpose — `restore_job` then refuses its row as
        // `UnknownSet` and deletes it, which is the same redownload a digest
        // mismatch produces.
        let mut restored: HashMap<String, DirectSet> = HashMap::new();
        let mut expected: HashMap<String, ExpectedSet> = HashMap::new();
        for plan in &admitted {
            if installed.contains_key(&plan.set_name) {
                continue;
            }
            let set_name = plan.set_name.clone();
            let mut set = DirectSet::new(job_id, plan.clone());
            self.direct_store.apply_ceilings(&mut set);
            // Bound **before** the layout is rebuilt, because
            // rebuilding it runs the same encrypted-store admission the live
            // parse does: a restore with no password reaches it, refuses, and
            // the set redownloads conventionally under a named reason. The
            // password itself was never persisted — this is the live job spec,
            // which restore has already re-derived from the stored NZB and the
            // job's password override.
            set.router.set_password(spec.password.as_deref());
            let par2_available = super::plan::spec_carries_par2(spec);
            set.router.note_par2_available(par2_available);
            set.router
                .note_par3_available(super::plan::spec_defers_to_par3(spec));
            let volume_facts = facts.get(&set_name).cloned().unwrap_or_default();
            if volume_facts.is_empty() {
                continue;
            }
            if let Err(reason) = set.restore_layout(&volume_facts) {
                tracing::info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    reason = reason.metric(),
                    "the direct set's cached facts no longer rebuild a routable layout; it \
                     redownloads"
                );
                continue;
            }
            expected.insert(set_name.clone(), set.expected_set());
            restored.insert(set_name, set);
        }

        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        let mut outcome = restore_job(gate, job_id, &roots, rows, &expected, &mut persist).await;
        // A row is only as trustworthy as the invariant it was written under:
        // a volume binds while none of its bytes live in a conventional file.
        // A demotion's handback breaks that on purpose, one volume at a time —
        // each rebuilt volume gets a conventional floor or a completed-file
        // row the moment the sweep finishes it, while the set's one coverage
        // row stands until the sweep finishes them all. A crash inside that
        // window leaves a row that still claims volumes the conventional path
        // now owns. Refused whole, never reconciled: the volumes with
        // conventional bytes keep them, and the rest redownload, which is what
        // the demotion was about to cost anyway.
        for plan in &admitted {
            if !outcome.accepted.contains_key(&plan.set_name) {
                continue;
            }
            let Some(file_index) = plan.files.keys().copied().find(|file_index| {
                conventional_floors
                    .get(file_index)
                    .copied()
                    .unwrap_or_default()
                    > 0
            }) else {
                continue;
            };
            outcome.accepted.remove(&plan.set_name);
            restored.remove(&plan.set_name);
            if let Err(error) = persist.delete(job_id, &plan.set_name) {
                tracing::warn!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    error = %error,
                    "failed to delete a direct-store coverage row refused for conventional bytes"
                );
            }
            outcome.rejected.push((
                plan.set_name.clone(),
                CoverageRejection::ConventionalBytes { file_index },
            ));
        }
        for (set_name, rejection) in &outcome.rejected {
            crate::runtime::perf_probe::record_owned(
                "direct_store.restart.rejected".to_string(),
                std::time::Duration::from_nanos(1),
            );
            tracing::info!(
                job_id = job_id.0,
                set_name = %set_name,
                reason = %rejection,
                "direct-store coverage refused at restore; the set redownloads"
            );
        }

        let mut result = DirectRestore {
            accepted: outcome.accepted.len() + installed.len(),
            rejected: outcome.rejected.len() + rejected_markers,
            ignored: outcome.ignored + ignored_markers,
            ..DirectRestore::default()
        };

        // Step 4: floors become skipped segments, and a file every one of whose
        // segments is skipped is a source volume whose download is finished.
        let mut applied: HashMap<String, BTreeMap<u32, u64>> = HashMap::new();
        for (set_name, snapshot) in &outcome.accepted {
            let Some(set) = restored.get(set_name) else {
                continue;
            };
            let floors = refetch_floors(snapshot);
            let complete = complete_files(snapshot);
            let plan = coverage_skip_plan(job_id, spec, &floors, &complete);
            // Volume index to the volume's decoded length. A published `complete`
            // means the floor covers every decoded byte of the volume — the
            // barrier re-derives the bit from exactly that comparison — so the
            // row's own floor *is* the length, and a "complete" volume with no
            // floor entry at all (which the encoder cannot produce) is dropped
            // rather than assumed.
            let volume_floors: HashMap<u32, u64> = snapshot
                .floors
                .iter()
                .map(|entry| (entry.volume_index, entry.floor))
                .collect();
            let complete_volumes: BTreeMap<u32, u64> = set
                .plan()
                .volumes
                .iter()
                .filter(|(_, file_index)| complete.contains(file_index))
                .filter_map(|(volume_index, _)| {
                    volume_floors
                        .get(volume_index)
                        .map(|floor| (*volume_index, *floor))
                })
                .collect();
            for (file_index, floor) in plan.file_progress {
                let slot = result.file_progress.entry(file_index).or_insert(floor);
                *slot = (*slot).max(floor);
            }
            result.skip.extend(plan.skip);
            applied.insert(set_name.clone(), complete_volumes);
        }

        // Step 5's claim set, and step 3's installation. A set only keeps its
        // rebuilt state when its row was accepted; everything else starts fresh,
        // which is what makes the sweep below safe to run against it.
        let mut claimed: HashSet<PathBuf> = HashSet::new();
        for plan in &admitted {
            // An installed set is done: every segment of its volumes is
            // skipped and it comes back finalized, with no layout, because
            // nothing will ever route into it again. Its committed members are
            // not direct-store working files, so the sweep never considers
            // them; its envelopes are, and are swept — a retained image is
            // runtime-only and is not rebuilt, so a neighbour's repair meets
            // this set the way it meets any finalized set that kept none.
            if let Some(marker) = installed.remove(&plan.set_name) {
                let complete: HashSet<u32> = plan.files.keys().copied().collect();
                let floors: HashMap<u32, u64> =
                    complete.iter().map(|file_index| (*file_index, 0)).collect();
                let skip = coverage_skip_plan(job_id, spec, &floors, &complete);
                for (file_index, floor) in skip.file_progress {
                    let slot = result.file_progress.entry(file_index).or_insert(floor);
                    *slot = (*slot).max(floor);
                }
                result.skip.extend(skip.skip);
                let mut set = DirectSet::new(job_id, plan.clone());
                set.mark_finalized();
                tracing::info!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    volumes = plan.volumes.len(),
                    "direct-store set restored as installed from its finalization marker"
                );
                result
                    .installed
                    .push((plan.set_name.clone(), marker.extracted));
                result.sets.push(set);
                continue;
            }
            let accepted = applied.get(&plan.set_name).cloned();
            let mut set = match (accepted.as_ref(), restored.remove(&plan.set_name)) {
                (Some(_), Some(set)) => set,
                _ => {
                    // The invariant every admission path shares: a volume
                    // binds only while none of its bytes live in a
                    // conventional file. A set starting FRESH here has no
                    // validated coverage, and if the legacy restore rebuilt a
                    // conventional floor for any of its volumes, that file
                    // already owns the prefix — installing a set would route
                    // every remaining article into an envelope the file never
                    // meets, and extraction would walk real headers into a
                    // hole. The set stays conventional; the sweep below
                    // reclaims whatever direct artifacts the previous run
                    // left.
                    let prior_bytes = plan.files.keys().any(|file_index| {
                        conventional_floors
                            .get(file_index)
                            .copied()
                            .unwrap_or_default()
                            > 0
                    });
                    if prior_bytes {
                        crate::runtime::perf_probe::record(
                            "direct_store.refused.prior_conventional_bytes",
                            std::time::Duration::from_nanos(1),
                        );
                        tracing::info!(
                            job_id = job_id.0,
                            set_name = %plan.set_name,
                            "direct-store set not readmitted at restore: a volume                              already has conventional bytes, so the set stays on                              the path that owns them"
                        );
                        continue;
                    }
                    let mut set = DirectSet::new(job_id, plan.clone());
                    self.direct_store.apply_ceilings(&mut set);
                    set.router.set_password(spec.password.as_deref());
                    let par2_available = super::plan::spec_carries_par2(spec);
                    set.router.note_par2_available(par2_available);
                    set.router
                        .note_par3_available(super::plan::spec_defers_to_par3(spec));
                    set
                }
            };
            if let (Some(complete_volumes), Some(snapshot)) =
                (accepted, outcome.accepted.get(&plan.set_name))
            {
                set.apply_restored_snapshot(snapshot, &complete_volumes);
                for volume_index in plan.volumes.keys() {
                    claimed.insert(plan.envelope_path(*volume_index));
                }
                for (_, _, partial) in set.router.member_partials() {
                    claimed.insert(plan.destination_path(partial));
                }
                // Holds scratch is deliberately **not** claimed, not even by a
                // set whose checkpoint was accepted. Its regions are named by an
                // in-memory index that did not survive, so the file is bytes
                // with no meaning: the set re-pages what it needs from a fresh
                // one. Sweeping it here is what keeps a killed run's scratch
                // from accumulating across restarts.
                tracing::info!(
                    job_id = job_id.0,
                    set_name = %plan.set_name,
                    volumes = plan.volumes.len(),
                    "direct-store set resumed from its coverage checkpoint"
                );
            }
            result.sets.push(set);
        }

        // Every file direct-store could own in this working directory, named
        // rather than pattern-matched (nit). Envelopes and holds scratch are
        // derivable from the plan alone, so they are enumerable for a set whose
        // row was refused as well as one whose row was kept; member partials come
        // from the rebuilt layout, which is why `restored` is consulted before it
        // is drained above rather than after.
        let mut owned: HashSet<PathBuf> = HashSet::new();
        for plan in &planned {
            owned.insert(plan.holds_scratch_path());
            for volume_index in plan.volumes.keys() {
                owned.insert(plan.envelope_path(*volume_index));
                // Repair scratch, owned but never *claimed*: a repair that was
                // interrupted mid-flight left a materialized volume whose spans
                // were never routed back, and the coverage row it would have
                // been read against was deleted before the repair started. The
                // bytes are meaningless without it, so the file is swept and
                // the set repairs again from its own routed bytes if the damage
                // is still there.
                owned.insert(plan.repair_path(*volume_index));
            }
        }
        for set in result.sets.iter().chain(restored.values()) {
            for (_, _, partial) in set.router.member_partials() {
                owned.insert(set.plan().destination_path(partial));
            }
        }

        // The sweep is a directory walk, and it runs at restore for every job.
        // A job whose spec declares no RAR volume cannot have produced a
        // `.direct.partial`, an envelope or a holds scratch — routing only ever
        // touches a set discovered from those roles — so it is skipped outright
        // rather than paying a walk per restored job at startup. Gate-independent
        // on purpose: the files a *disabled* gate has to sweep were written by an
        // enabled one, over the same spec.
        let could_have_routed = spec
            .files
            .iter()
            .any(|file| matches!(file.role, weaver_model::files::FileRole::RarVolume { .. }));
        if could_have_routed {
            result.swept = sweep_orphan_direct_files(&roots, &claimed, &owned).await;
        }

        // The restart ledger, in the four numbers that separate "resumed" from
        // "rolled back". `rejected` already has its own counter above; these
        // are the three that were only ever visible as a log line, plus the
        // segments the accepted floors actually saved — without which "restart
        // discarded a barrier interval" is unfalsifiable in production.
        crate::runtime::perf_probe::record_value(
            "direct_store.restart.accepted_sets",
            result.accepted as u64,
        );
        crate::runtime::perf_probe::record_value(
            "direct_store.restart.ignored_rows",
            result.ignored as u64,
        );
        crate::runtime::perf_probe::record_value("direct_store.restart.swept", result.swept as u64);
        crate::runtime::perf_probe::record_value(
            "direct_store.restart.skipped_segments",
            result.skip.len() as u64,
        );
        result
    }

    /// Puts back the runtime record of every set restored as installed: its
    /// name in `extracted_archives` and its members in the extracted-member
    /// sets, exactly as its finalization left them.
    ///
    /// Called once the job's persisted extracted members are in place, because
    /// that load replaces the job's entry wholesale.
    pub(crate) fn reinstate_installed_direct_sets(
        &mut self,
        job_id: JobId,
        installed: Vec<(String, Vec<String>)>,
    ) {
        for (set_name, extracted) in installed {
            for name in extracted {
                self.record_direct_extracted(job_id, name);
            }
            self.extracted_archives
                .entry(job_id)
                .or_default()
                .insert(set_name);
        }
    }

    /// The cached facts for every set of a job, decoded and keyed by set name.
    async fn load_direct_volume_facts(
        &self,
        job_id: JobId,
    ) -> HashMap<String, BTreeMap<u32, DirectVolumeFacts>> {
        let rows = match self
            .db_blocking(move |db| db.load_all_rar_volume_facts(job_id))
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(
                    job_id = job_id.0,
                    %error,
                    "failed to load cached RAR volume facts; direct sets redownload"
                );
                return HashMap::new();
            }
        };
        let mut decoded: HashMap<String, BTreeMap<u32, DirectVolumeFacts>> = HashMap::new();
        for (set_name, volumes) in rows {
            for (volume_index, blob) in volumes {
                match DirectVolumeFacts::decode(&blob) {
                    Ok(facts) => {
                        decoded
                            .entry(set_name.clone())
                            .or_default()
                            .insert(volume_index, facts);
                    }
                    Err(error) => {
                        tracing::warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume = volume_index,
                            %error,
                            "failed to decode cached RAR volume facts"
                        );
                    }
                }
            }
        }
        decoded
    }
}

/// Deletes every direct-store file in the working directory that no restored
/// set claims.
///
/// Three populations end up here and all three are dead weight:
///
/// - a set whose checkpoint was refused, whose partials and envelopes hold bytes
///   nothing may read and which is about to redownload over them;
/// - a set that was killed before its first barrier, so no row exists at all;
/// - holds scratch from a killed run, which is append-only and meaningless
///   without the in-memory index that named its regions.
///
/// With the gate **off** nothing is claimed, so everything direct-store owns is
/// swept. That is deliberate even though the rows themselves are kept: an
/// operator who turns the switch off wants the working directory to be what the
/// conventional path expects, and a re-enabled binary refuses the surviving rows
/// on the destination probe rather than trusting them — the safe direction either
/// way.
///
/// # What "direct-store's" means here
///
/// `owned` is the set of paths this job's *plans* name — every envelope, every
/// holds scratch, every member partial the rebuilt layouts produce — and it is
/// the primary rule. Matching by extension alone was collateral waiting to
/// happen: the walk descends eight levels into an archive-controlled tree, and an
/// **extracted member** called `chapter.envelope` is a file a user's archive can
/// perfectly well contain. Deleting it is silent data loss in a job that
/// otherwise succeeded.
///
/// Two pattern rules survive, each for a file that exists precisely because it is
/// *not* in any current plan:
///
/// - holds scratch at the **top level only**, by prefix. A killed run's scratch
///   for a set this spec no longer produces has no plan to name it, and it lives
///   at the top level by construction ([`DirectSetPlan::holds_scratch_path`]).
/// - `.direct.partial` at any depth. A member partial's path comes from the RAR
///   header, so a set whose cached facts no longer rebuild has no way to name
///   its own partials — and the suffix is a two-part one this codebase invented,
///   not an extension an archive plausibly carries.
///
/// # Both roots
///
/// Member payload lives in the staging root and everything else in the working
/// directory, so both are walked. The working directory is still walked for
/// `.direct.partial` as well, and deliberately: a job checkpointed by a build
/// that wrote payload there has partials sitting in it that no plan will ever
/// name again, and this is what clears them.
///
/// The holds-scratch prefix rule applies to the working directory **only**. It
/// is a name match rather than a plan match, and the staging root is shared with
/// the incremental extractor's output — a member an archive happened to call
/// `.weaver-holds.something` is a file a user's archive can contain, and the
/// working directory is the one place nothing but this subsystem writes such a
/// name.
async fn sweep_orphan_direct_files(
    roots: &DestinationRoots,
    claimed: &HashSet<PathBuf>,
    owned: &HashSet<PathBuf>,
) -> usize {
    let roots = roots.clone();
    let claimed = claimed.clone();
    let owned = owned.clone();
    tokio::task::spawn_blocking(move || {
        let mut swept =
            sweep_orphan_direct_files_blocking(&roots.working_dir, true, &claimed, &owned);
        if roots.destination_dir != roots.working_dir {
            swept +=
                sweep_orphan_direct_files_blocking(&roots.destination_dir, false, &claimed, &owned);
        }
        swept
    })
    .await
    .unwrap_or(0)
}

fn sweep_orphan_direct_files_blocking(
    root: &Path,
    sweep_holds_scratch: bool,
    claimed: &HashSet<PathBuf>,
    owned: &HashSet<PathBuf>,
) -> usize {
    let mut swept = 0usize;
    let mut queue = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = queue.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            // `file_type` does not follow symlinks, which is what keeps the walk
            // inside the working directory: a symlinked directory is never
            // descended and a symlink to a file is never matched.
            if file_type.is_dir() {
                if depth < SWEEP_MAX_DEPTH {
                    queue.push((path, depth + 1));
                }
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let is_direct = owned.contains(&path)
                || name.ends_with(DIRECT_PARTIAL_SUFFIX)
                || (sweep_holds_scratch && depth == 0 && name.starts_with(HOLDS_SCRATCH_PREFIX));
            if !is_direct || claimed.contains(&path) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    swept += 1;
                    tracing::info!(
                        path = %path.display(),
                        "swept an unclaimed direct-store file at restore"
                    );
                }
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    %error,
                    "failed to sweep an unclaimed direct-store file"
                ),
            }
        }
    }
    swept
}

#[cfg(test)]
mod facts_envelope_tests {
    use super::{DirectVolumeFacts, SevenZipVolumeFacts};

    /// Rows written before the envelope existed are bare `RarVolumeFacts`
    /// blobs, and there are live ones in shipped databases. Decoding has to
    /// keep reading them as RAR, because the alternative is every in-flight
    /// direct set on an upgraded install restarting from zero.
    #[test]
    fn a_bare_rar_blob_decodes_as_the_rar_arm() {
        let facts = unrar_rs::RarVolumeFacts {
            format: 5,
            volume_number: Some(0),
            more_volumes: true,
            is_solid: false,
            is_encrypted: false,
            is_volume: true,
            has_recovery_record: false,
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: false,
            quick_open_offset: None,
            headers_from_quick_open: false,
            recovery_record_offset: None,
            original_name: None,
            original_name_raw: None,
            original_creation_time_ns: None,
            members: Vec::new(),
            services: Vec::new(),
        };
        // Exactly what the old encoder wrote: the struct, untagged.
        let blob = rmp_serde::to_vec_named(&facts).expect("the old encoding still encodes");
        assert_eq!(
            DirectVolumeFacts::decode(&blob).expect("a shipped row still decodes"),
            DirectVolumeFacts::Rar(Box::new(facts))
        );
    }

    /// And the envelope round-trips both arms, so a row written now is read
    /// back as what it is rather than falling through to the RAR fallback.
    #[test]
    fn the_envelope_round_trips_both_arms() {
        for facts in [
            DirectVolumeFacts::SevenZip(Box::new(SevenZipVolumeFacts {
                declared_len: 4096,
                container: None,
            })),
            DirectVolumeFacts::Rar(Box::new(unrar_rs::RarVolumeFacts {
                format: 5,
                volume_number: Some(1),
                more_volumes: false,
                is_solid: false,
                is_encrypted: false,
                is_volume: true,
                has_recovery_record: false,
                is_locked: false,
                has_authenticity_verification: false,
                has_locator: false,
                quick_open_offset: None,
                headers_from_quick_open: false,
                recovery_record_offset: None,
                original_name: None,
                original_name_raw: None,
                original_creation_time_ns: None,
                members: Vec::new(),
                services: Vec::new(),
            })),
        ] {
            let blob = facts.encode().expect("the envelope encodes");
            assert_eq!(
                DirectVolumeFacts::decode(&blob).expect("the envelope decodes"),
                facts
            );
        }
    }
}
