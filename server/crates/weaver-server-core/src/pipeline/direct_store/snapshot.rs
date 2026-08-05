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

/// Bump on any change to the body layout below. Decoding accepts **exactly**
/// this version — a newer writer's blob is rejected rather than partially
/// trusted, and so is an older one — so a bump is also the way to retire rows
/// whose *meaning* changed under a field that kept its type.
///
/// - 2: `VolumeFloor::complete` added.
/// - 3: `VolumeFloor::complete` narrowed from "every article arrived" to "every
///   article arrived **and** the floor covers all of them". A v2 writer could
///   publish `{floor: 0, complete: true}` for a volume whose bytes were still
///   held, and a v3 reader trusting that bit would skip every segment of a
///   volume no byte of which exists. Refusing the row costs one redownload;
///   trusting it wedges the set permanently.
/// - 4: `DestinationClaim::crypt` added (plan 136, E-D4). An encrypted member's
///   destination holds **plaintext**, so the claim alone no longer describes
///   what a resumed run needs: the crypt facts to rebuild a key without
///   re-parsing, the ≤15 tail-padding bytes that exist nowhere on disk, and the
///   cipher checkpoints that let a resumed span decrypt at the coverage frontier
///   without re-encrypting the member from its IV. A v3 reader would see a claim
///   over plaintext and treat it as posted bytes, which is why this is a version
///   bump and not an optional field.
/// - 5: `MemberCryptSnapshot`'s flat RAR5 crypt fields became the
///   `MemberCryptKeying` discriminant (plan 136, E3). RAR4 file encryption is
///   keyed by an 8-byte per-file salt and no KDF count, and its IV is a KDF
///   output rather than a header field — so it does not fit v4's `salt[16] +
///   kdf_count_lg2 + iv[16] + psw_check_present` shape, and squeezing it in
///   (zero-padding the salt, inventing a sentinel count) would let a RAR5 row
///   and a RAR4 row compare *equal* at restore, which is precisely the
///   "different archive" case the comparison exists to catch. The body is the
///   compact **positional** MessagePack form, so a changed field set is a
///   changed array shape whatever the field names would have said: this is a
///   bump by the codec's own rule, not by choice.
///
///   Operationally it costs nothing. v4 has never shipped — it landed on
///   `release-0.8.0` after the 0.7 line — so the only rows it can refuse are
///   ones written by an unreleased build of the same branch, and the refusal
///   costs exactly one redownload of a set that was mid-flight across a
///   developer's rebuild. The v3 note below is the one that reaches users.
///
/// # The v3 refusal is a release note (plan 136, E1 review F8)
///
/// Refusing rather than upgrading means **a direct-store set checkpointed by a
/// pre-0.8.0 build re-downloads its volumes once on the first start after the
/// upgrade**. Nothing is lost and no job fails — the cost is exactly one
/// redownload per set that was mid-download across the upgrade — but it is
/// user-visible traffic and belongs in the notes rather than in a support
/// thread.
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u16 = 5;

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
    /// Present exactly for an encrypted member direct-store decrypted at write
    /// time (plan 136, E-D4). `None` for every plaintext member and for every
    /// envelope destination.
    ///
    /// It carries no password and never will: what is here is what the headers
    /// already state in the clear, plus the two things this process computed
    /// that no restart could re-derive — the retained tail padding, and the
    /// cipher checkpoints. A restore rebuilds the key from the job's live
    /// password and these facts, and **refuses** when they disagree with the
    /// headers the layout was rebuilt from.
    pub(crate) crypt: Option<super::router::crypt::MemberCryptSnapshot>,
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
    /// Every article of the source volume arrived **and** every one of its bytes
    /// is below `floor`, so restart may skip the volume's segments outright.
    ///
    /// Carried explicitly because the floor **cannot** say it. A floor counts
    /// *decoded* source bytes, while an NZB's `<segment bytes>` is the
    /// yEnc-**encoded** size — about 3% larger — so walking the spec's segments
    /// against a decoded floor always stops one article short of the truth. That
    /// is safe (it refetches), but for a byte-complete volume it means refetching
    /// the last article of every volume of a set that is entirely on disk, which
    /// is precisely the restart the PAR2 finalization wait makes common. This
    /// flag is the one bit that closes the gap, and it is a bit **per volume**,
    /// not per segment, so it costs nothing the D6 shape objects to.
    ///
    /// # It is a conjunction, not a latch
    ///
    /// "Every article arrived" on its own is **not** what restart reads. Restart
    /// reads this as *all bytes durable* and skips every segment of the file on
    /// the strength of it. A volume can finish downloading while its bytes are
    /// still held — payload staged before the header that classifies it, an
    /// out-of-order volume the layout cannot place yet — and a bit latched at the
    /// article-complete seam would checkpoint `{floor: 0, complete: true}`. That
    /// row skips every segment of a volume no byte of which exists: the set can
    /// then neither finalize (its member gate has nothing to compose) nor demote
    /// (its reconstruction has nothing to read), which is a permanent zombie.
    ///
    /// So the writer re-derives it at **every** barrier as `download finished &&
    /// floor >= decoded length` ([`super::barrier::CoverageBarrier`]), and a
    /// volume whose held bytes have not reached the floor publishes `false` until
    /// they do. `complete == true` therefore always implies `floor` is the
    /// volume's whole decoded length, which is what the restore seam relies on
    /// when it derives a restored volume's confirmation.
    ///
    /// Trusting it is bounded: the bytes it lets restart skip are the same bytes
    /// the destination probe length-checks and the member gate re-reads and
    /// re-composes before the set may finalize, so a wrong flag fails a checksum
    /// rather than committing a hole.
    ///
    /// No `#[serde(default)]`: the body is the **compact positional** MessagePack
    /// form, where a missing trailing field is a short array rather than an absent
    /// name, and decoding refuses any schema version but its own — so a default
    /// here could never fire.
    pub(crate) complete: bool,
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
    /// Restart-side lookup by volume. The restore seam derives its refetch
    /// floors per **NZB file** ([`super::restart::refetch_floors`]), which is the
    /// coordinate segments live in; this is the layout-side view, kept for the
    /// tests that assert the two agree.
    #[cfg(test)]
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
