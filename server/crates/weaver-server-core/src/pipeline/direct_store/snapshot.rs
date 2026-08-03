//! The direct-store coverage snapshot blob (plan 135, D6).
//!
//! One compact encoded blob per archive set, holding everything the checkpoint
//! knows: schema version, generation counter, the exact layout-plan digest,
//! destination identities with their claimed extents, and every per-volume
//! contiguous floor. Encoded and decoded in **one** operation — no per-volume
//! statements and no per-volume round trips, which matters most on Postgres
//! where per-statement RTT dominates.
//!
//! Framing is `magic | schema version | MessagePack body`. MessagePack matches
//! how the rest of this crate persists binary state (cached RAR headers and
//! `RarVolumeFacts` both go through `rmp_serde`), but this codec uses the
//! **compact** positional form rather than `to_vec_named`: at 2 000 volumes the
//! field names would be most of the blob, and the explicit schema version in
//! the frame already does the job field names would otherwise do. Adding,
//! removing or reordering a field is a schema-version bump.

use serde::{Deserialize, Serialize};

use super::ByteRanges;
use crate::pipeline::extraction::validate_sanitized_rar_member_path;

/// `W`eaver `D`irect `S`tore `C`overage.
pub(crate) const SNAPSHOT_MAGIC: [u8; 4] = *b"WDSC";

/// Bump on any change to the body layout below. Decoding is forward-refusing:
/// a newer writer's blob is rejected outright rather than partially trusted.
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u16 = 1;

const FRAME_HEADER_LEN: usize = 6;

/// One contiguous claimed span of a destination file, half-open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct DestinationExtent {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl DestinationExtent {
    pub(crate) fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

/// A destination file the set claims coverage in.
///
/// Keyed by **member identity**, not by the final sanitized path: sanitized
/// destinations are only committed in archive order at finalization (D3), so the
/// path here is the working-directory-relative partial and the member index is
/// the stable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DestinationClaim {
    pub(crate) member_index: u32,
    pub(crate) relative_path: String,
    /// Sorted, disjoint, coalesced.
    pub(crate) extents: Vec<DestinationExtent>,
}

impl DestinationClaim {
    /// The length the destination file must have for this claim to be
    /// admissible at restart. A **longer** file is expected and fine: file
    /// length never implies coverage, in either direction.
    pub(crate) fn claimed_len(&self) -> u64 {
        self.extents.last().map(|extent| extent.end).unwrap_or(0)
    }
}

/// One source volume's durable contiguous coverage floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VolumeFloor {
    /// Volume index within the archive set — the layout coordinate.
    pub(crate) volume_index: u32,
    /// NZB file index for that volume — the coordinate segments live in, and
    /// therefore what the refetch derivation needs.
    pub(crate) file_index: u32,
    /// Contiguous bytes of the source volume durably written to destinations.
    /// Everything above this is redownloaded.
    pub(crate) floor: u64,
}

/// The decoded checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CoverageSnapshot {
    /// Monotonic per set. A committed checkpoint is always at least 1.
    pub(crate) generation: u64,
    /// Digest of the exact layout plan the coverage was produced against.
    /// A mismatch is a hard stop: safe redownload or demotion, never partial
    /// trust.
    pub(crate) plan_digest: [u8; 32],
    /// Sorted by `member_index`.
    pub(crate) destinations: Vec<DestinationClaim>,
    /// Sorted by `volume_index`.
    pub(crate) floors: Vec<VolumeFloor>,
}

impl CoverageSnapshot {
    /// Restart-side lookup; unwired with the rest of the reader (see `restart`).
    #[allow(dead_code)]
    pub(crate) fn floor_for_volume(&self, volume_index: u32) -> Option<u64> {
        self.floors
            .binary_search_by_key(&volume_index, |entry| entry.volume_index)
            .ok()
            .map(|index| self.floors[index].floor)
    }

    /// Canonical ordering, so equal content always encodes to equal bytes.
    fn normalized(&self) -> Self {
        let mut normalized = self.clone();
        normalized
            .destinations
            .sort_by_key(|claim| claim.member_index);
        for claim in &mut normalized.destinations {
            let mut ranges = ByteRanges::new();
            for extent in &claim.extents {
                ranges.insert(extent.start, extent.len());
            }
            claim.extents = ranges
                .ranges()
                .iter()
                .map(|&(start, end)| DestinationExtent { start, end })
                .collect();
        }
        normalized.floors.sort_by_key(|entry| entry.volume_index);
        normalized
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotError {
    /// Shorter than the fixed frame header.
    Truncated {
        len: usize,
    },
    BadMagic,
    /// Written by a schema this binary does not know. Forward-refusing.
    UnsupportedVersion {
        found: u16,
        supported: u16,
    },
    /// Well-framed but structurally invalid, or not decodable as the body.
    Malformed(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated { len } => {
                write!(formatter, "direct-store snapshot truncated ({len} bytes)")
            }
            Self::BadMagic => write!(formatter, "direct-store snapshot has a bad magic"),
            Self::UnsupportedVersion { found, supported } => write!(
                formatter,
                "direct-store snapshot schema version {found} is not supported (this binary reads {supported})"
            ),
            Self::Malformed(detail) => {
                write!(formatter, "direct-store snapshot is malformed: {detail}")
            }
        }
    }
}

/// Encodes one checkpoint. Deterministic: equal content always yields equal
/// bytes, because the body is canonically ordered first.
pub(crate) fn encode(snapshot: &CoverageSnapshot) -> Result<Vec<u8>, SnapshotError> {
    let normalized = snapshot.normalized();
    let body = rmp_serde::to_vec(&normalized)
        .map_err(|error| SnapshotError::Malformed(error.to_string()))?;
    let mut blob = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    blob.extend_from_slice(&SNAPSHOT_MAGIC);
    blob.extend_from_slice(&SNAPSHOT_SCHEMA_VERSION.to_le_bytes());
    blob.extend_from_slice(&body);
    Ok(blob)
}

/// Decodes one checkpoint, validating framing, schema version and structure.
///
/// Every failure mode is total: there is no "decoded the floors but not the
/// destinations" outcome, because a partially trusted checkpoint would claim
/// coverage nothing verified.
pub(crate) fn decode(blob: &[u8]) -> Result<CoverageSnapshot, SnapshotError> {
    if blob.len() < FRAME_HEADER_LEN {
        return Err(SnapshotError::Truncated { len: blob.len() });
    }
    if blob[..4] != SNAPSHOT_MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let version = u16::from_le_bytes([blob[4], blob[5]]);
    if version != SNAPSHOT_SCHEMA_VERSION {
        return Err(SnapshotError::UnsupportedVersion {
            found: version,
            supported: SNAPSHOT_SCHEMA_VERSION,
        });
    }

    // Read-based rather than `rmp_serde::from_slice`, purely because the reader
    // reports how much it consumed. A body with anything appended to it decodes
    // happily otherwise, and a row that is not *exactly* one snapshot is not a
    // snapshot: the surplus is either a different writer's framing or a torn
    // row, and neither is something to partially trust.
    let body = &blob[FRAME_HEADER_LEN..];
    let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(body));
    let snapshot = CoverageSnapshot::deserialize(&mut deserializer)
        .map_err(|error| SnapshotError::Malformed(error.to_string()))?;
    let consumed = deserializer.position();
    if consumed != body.len() as u64 {
        return Err(SnapshotError::Malformed(format!(
            "{} trailing bytes after the snapshot body",
            body.len() as u64 - consumed
        )));
    }
    validate(&snapshot)?;
    Ok(snapshot)
}

fn validate(snapshot: &CoverageSnapshot) -> Result<(), SnapshotError> {
    let mut previous_member: Option<u32> = None;
    for claim in &snapshot.destinations {
        if previous_member.is_some_and(|previous| previous >= claim.member_index) {
            return Err(SnapshotError::Malformed(
                "destination claims are not sorted by member index".into(),
            ));
        }
        previous_member = Some(claim.member_index);
        // A claimed path is joined onto the working directory at restart, so a
        // row that escaped the working directory — or that a hostile blob wrote
        // deliberately — would have restart probing, and phase 4 writing,
        // outside the job. The house validator is the same one RAR extraction
        // gates member paths with: no absolute paths, no `..`, no root or
        // prefix components, no embedded NUL, never empty.
        if let Err(error) = validate_sanitized_rar_member_path(&claim.relative_path) {
            return Err(SnapshotError::Malformed(format!(
                "destination claim has an unsafe path ({error})"
            )));
        }
        let mut previous_end: Option<u64> = None;
        for extent in &claim.extents {
            if extent.end <= extent.start {
                return Err(SnapshotError::Malformed(
                    "destination extent is empty or inverted".into(),
                ));
            }
            if previous_end.is_some_and(|previous| previous >= extent.start) {
                return Err(SnapshotError::Malformed(
                    "destination extents are not sorted and disjoint".into(),
                ));
            }
            previous_end = Some(extent.end);
        }
    }

    let mut previous_volume: Option<u32> = None;
    for entry in &snapshot.floors {
        if previous_volume.is_some_and(|previous| previous >= entry.volume_index) {
            return Err(SnapshotError::Malformed(
                "volume floors are not sorted by volume index".into(),
            ));
        }
        previous_volume = Some(entry.volume_index);
    }

    Ok(())
}
