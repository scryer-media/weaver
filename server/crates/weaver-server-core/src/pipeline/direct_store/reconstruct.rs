//! Byte-exact volume reconstruction for archive-group demotion.
//!
//! The first shape's demotion was the conservative form: throw the routed bytes
//! away and refetch every article of every volume. That is correct and it is
//! expensive — a set that demotes at 90% has already paid for 90% of its own
//! download and then pays again.
//!
//! Reconstruction says what should happen instead: *"reconstruct byte-exact
//! source volumes for every volume the group needs from envelope plus member
//! extents, verify the reconstructed covered ranges, sync, atomically persist
//! legacy floors and completed-file rows for the now-physical volumes, retire
//! the direct coverage row, delete the group's partial direct outputs … and
//! hand the whole group to the existing repair/extraction scheduler."*
//!
//! This module owns the first two steps — the sweep and its verification. It
//! runs entirely on the blocking pool: every read goes through the hybrid
//! virtual-volume provider, and every write is a plain positioned write to the
//! volume file the conventional path is about to take over.
//!
//! # What is reconstructed, and what is not
//!
//! Only the volume's **covered** ranges: the bytes the coverage barrier saw a
//! successful destination write for. A hole is left as a hole — the file stays
//! sparse there and the segments above the contiguous floor are refetched. Bytes
//! covered *above* a hole are still written, because they cost nothing to write
//! while the sweep is already open on the file and they save the refetch from
//! having to be byte-correct about a range it will simply overwrite.
//!
//! # Why a failure here is not fatal
//!
//! Reconstruction is an optimisation over refetching. Every failure mode — a
//! deleted envelope, a truncated partial, a covered range whose composed CRC32
//! disagrees with what came back off disk — falls back to the conservative
//! refetch, which is always correct. What it must never do is *half* succeed
//! and then let the caller persist a floor over bytes it did not actually
//! rebuild, so the floor a volume reports is the contiguous prefix it
//! verifiably wrote and nothing beyond it.
//!
//! # Verification fails closed
//!
//! An earlier shape wrote a covered run **unverified** whenever the yEnc part
//! composition had no reference value for it, on the grounds that
//! reconstruction asks for verification "where available". That is the wrong
//! default here: this sweep reads through an overlay of two sparse files, and
//! the failure it exists to survive — a source that silently answers with zeros
//! — produces bytes that look like data and pass nothing. So a covered run the
//! composition cannot vouch for is [`ReconstructionFailure::UnverifiableRun`],
//! which falls back to refetching. [`CrcRuns::compose`] is what keeps that
//! rare: it composes a reference for any sub-range that starts and ends on an
//! article boundary, so a held tail or a volume that stops mid-download still
//! verifies the prefix it does have.
//!
//! The one exception is a run that stops **inside** an article, and it is only
//! an exception for the repair scratch. The composition can vouch for such a
//! run up to its last article boundary and no further; the remainder is a
//! proper prefix of one article whose bytes passed the yEnc part CRC32 on the
//! way in. The demotion sweep refuses it all the same, because it publishes a
//! floor. The in-place repair sweep carries it through, because nothing
//! publishes a floor over the scratch, PAR2 has already judged every slice of
//! it and rewrites the ones that failed, and the bytes past the boundary are
//! exactly the valid slices the repair needs as input — see
//! [`PartialArticle`].

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::ByteRanges;
use super::provider::{HybridVolumeProvider, is_hole};
use super::router::CrcRuns;
use super::sparse::SparseMarking;

/// Read/write chunk for the sweep. Large enough that a 50 MiB volume is a few
/// hundred iterations, small enough to stay off the large-allocation path.
const SWEEP_CHUNK_BYTES: usize = 256 * 1024;

/// One volume to rebuild.
#[derive(Debug, Clone)]
pub(crate) struct VolumeReconstruction {
    pub(crate) volume_index: u32,
    /// Where the conventional path expects the volume file.
    pub(crate) path: PathBuf,
    /// Decoded length of the source volume — its true length when the file
    /// assembly has every article, and only a lower bound otherwise.
    pub(crate) len: u64,
    /// Every article of the source volume reached the assembly, so `len` really
    /// is the volume's length.
    ///
    /// Without this a volume whose only article was its first would look
    /// "completely covered" — coverage reaching the end of what arrived — and
    /// would be recorded as a completed file at a third of its size.
    pub(crate) assembly_complete: bool,
    /// Physical ranges the barrier confirmed were written.
    pub(crate) covered: ByteRanges,
    /// The volume's whole yEnc part-CRC32 composition, in source space.
    ///
    /// Carried whole rather than pre-resolved to `(start, len, crc)` triples so
    /// the sweep can compose a reference for the range it actually reads. The
    /// pre-resolved form had to guess the ranges up front, and it guessed with
    /// the *unclamped* coverage: a run the sweep then clipped to the volume's
    /// length looked up a key that was never inserted and silently lost its
    /// reference value (nit).
    pub(crate) crcs: CrcRuns,
    /// What to do with a covered run that stops inside an article.
    pub(crate) partial_article: PartialArticle,
}

/// What the sweep does with a covered run that stops **inside** an article.
///
/// The composition is article-shaped, so it vouches for a run only up to the
/// last article boundary the run reaches. What lies past that boundary is a
/// proper prefix of one article: bytes that passed the yEnc part CRC32 on the
/// way in and were placed, whose article was not placed whole. An encrypted
/// member leaves exactly this shape before every hole — its final cipher block
/// cannot be decrypted until the block after it arrives, so the placed frontier
/// stops at the block floor inside the last article — and a routing that
/// demotes mid-article leaves it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialArticle {
    /// Refuse the run as [`ReconstructionFailure::UnverifiableRun`]. The
    /// demotion sweep's policy: a floor is published over what it writes, and
    /// a floor must not cover bytes the composition did not check.
    Refuse,
    /// Verify the run up to its last article boundary, then write the
    /// remainder with no reference. The repair scratch's policy: nothing
    /// publishes a floor over the scratch, PAR2 has already judged every slice
    /// of it against its own checksums and rewrites the ones that failed, and
    /// refusing would demote a set whose repair needs exactly those bytes as
    /// input. The remainder is bounded by one article and must be a prefix of
    /// an article the composition knows: a run that starts off a boundary, or
    /// that reaches into bytes no article ever covered, is refused as before.
    CarryThrough,
}

/// What one volume's sweep produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReconstructedVolume {
    pub(crate) volume_index: u32,
    /// Contiguous physical bytes rebuilt from zero. The legacy floor is derived
    /// from this and nothing else.
    pub(crate) contiguous: u64,
    /// Every byte of `[0, len)` is on disk, so the volume is indistinguishable
    /// from one the conventional path downloaded.
    pub(crate) complete: bool,
    /// MD5 of the whole volume, for the completed-file row. Only computed when
    /// the volume came out complete, because that is the only case where it
    /// describes the whole file.
    pub(crate) md5: Option<[u8; 16]>,
}

/// Why a set could not be demoted by reconstruction. Each is a metric bucket,
/// and each falls back to refetching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReconstructionFailure {
    /// The set has no layout, so no volume can be mapped back at all.
    NoLayout,
    /// A covered byte was not in the envelope or any partial — the file was
    /// deleted, truncated, or never received the write the coverage claims.
    MissingBytes { volume_index: u32, offset: u64 },
    /// A covered run came back off disk with a different CRC32 than the yEnc
    /// part composition recorded for it.
    ChecksumMismatch { volume_index: u32, offset: u64 },
    /// A covered run has no reference CRC32 at all, so rebuilding it would put
    /// bytes nothing checked under a published floor. Refused rather than
    /// written; the refetch is always correct.
    UnverifiableRun { volume_index: u32, offset: u64 },
    /// The volume file could not be written.
    WriteFailed { volume_index: u32, error: String },
    /// The volume file could not be marked sparse. Refused **before** `set_len`
    /// opens a hole, so on Windows nothing has been allocated yet and the
    /// refetch pays only what the conventional path always pays.
    SparseMarkFailed { volume_index: u32, error: String },
    /// The set routed an **encrypted** member whose posted bytes the provider
    /// overlay cannot reproduce.
    ///
    /// Reconstruction reads posted bytes back out of destinations that hold
    /// plaintext, and the overlay re-encrypts them on the way — so an encrypted
    /// set is reconstructed like any other. This is what is left when the
    /// overlay itself refuses: a routed member with no declared cipher size, or
    /// one whose tail padding never arrived whole, so its final block has no
    /// byte-exact source. Refused up front rather than discovered mid-sweep,
    /// because the alternative is a volume that is right everywhere except one
    /// block and a floor published over it.
    EncryptedPostedBytes,
}

impl ReconstructionFailure {
    pub(crate) fn metric(&self) -> &'static str {
        match self {
            Self::NoLayout => "no_layout",
            Self::MissingBytes { .. } => "missing_bytes",
            Self::ChecksumMismatch { .. } => "checksum_mismatch",
            Self::UnverifiableRun { .. } => "unverifiable_run",
            Self::WriteFailed { .. } => "write_failed",
            Self::SparseMarkFailed { .. } => "sparse_mark_failed",
            Self::EncryptedPostedBytes => "encrypted_posted_bytes",
        }
    }
}

impl std::fmt::Display for ReconstructionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoLayout => formatter.write_str("the set never learned a stored layout"),
            Self::MissingBytes {
                volume_index,
                offset,
            } => write!(
                formatter,
                "volume {volume_index} is missing a byte the coverage map claims at {offset}"
            ),
            Self::ChecksumMismatch {
                volume_index,
                offset,
            } => write!(
                formatter,
                "volume {volume_index} rebuilt a run at {offset} that fails its composed CRC32"
            ),
            Self::UnverifiableRun {
                volume_index,
                offset,
            } => write!(
                formatter,
                "volume {volume_index} has no composed CRC32 for the covered run at {offset}"
            ),
            Self::WriteFailed {
                volume_index,
                error,
            } => write!(
                formatter,
                "volume {volume_index} could not be written: {error}"
            ),
            Self::SparseMarkFailed {
                volume_index,
                error,
            } => write!(
                formatter,
                "volume {volume_index} could not be marked sparse: {error}"
            ),
            Self::EncryptedPostedBytes => write!(
                formatter,
                "an encrypted member the set routed cannot reproduce its posted bytes"
            ),
        }
    }
}

/// Rebuilds every volume of a set. Blocking: call it on the blocking pool.
pub(crate) fn reconstruct_volumes(
    provider: &HybridVolumeProvider,
    volumes: &[VolumeReconstruction],
    sparse: SparseMarking,
) -> Result<Vec<ReconstructedVolume>, ReconstructionFailure> {
    let mut rebuilt = Vec::with_capacity(volumes.len());
    for volume in volumes {
        match reconstruct_volume(provider, volume, sparse) {
            Ok(outcome) => rebuilt.push(outcome),
            Err(failure) => {
                // All or nothing. The fallback refetches every article of the
                // set, and it must not find a half-written volume file sitting
                // where it is about to write: the refetch publishes no floor for
                // these files, so a stray prefix would be bytes nothing claims,
                // nothing verifies and nothing overwrites below the first
                // segment it does fetch.
                for volume in volumes {
                    let _ = std::fs::remove_file(&volume.path);
                }
                return Err(failure);
            }
        }
    }
    Ok(rebuilt)
}

fn reconstruct_volume(
    provider: &HybridVolumeProvider,
    volume: &VolumeReconstruction,
    sparse: SparseMarking,
) -> Result<ReconstructedVolume, ReconstructionFailure> {
    if volume.covered.is_empty() || volume.len == 0 {
        return Ok(ReconstructedVolume {
            volume_index: volume.volume_index,
            contiguous: 0,
            complete: false,
            md5: None,
        });
    }

    let mut reader = provider
        .open(volume.volume_index)
        .ok_or(ReconstructionFailure::NoLayout)?;
    // Marked sparse at creation and **before** the `set_len` below: the sweep
    // seeks past every hole, so on Windows an unmarked file would have NTFS
    // allocate and zero-fill the whole volume the moment the length is set. A
    // marking failure is refused here, with no hole yet in existence.
    let mut file =
        super::sparse::create_sparse(&volume.path, &sparse).map_err(|error| match error {
            // An ordinary create failure is the write failure it has always
            // been; only a refused *marking* is its own bucket.
            super::sparse::SparseCreateError::Open(error) => ReconstructionFailure::WriteFailed {
                volume_index: volume.volume_index,
                error: error.to_string(),
            },
            super::sparse::SparseCreateError::Mark(error) => {
                ReconstructionFailure::SparseMarkFailed {
                    volume_index: volume.volume_index,
                    error: error.to_string(),
                }
            }
        })?;
    // Opened without truncating, so the sweep can leave holes for the refetch to
    // fill rather than rewriting them as zeros — but a file already sitting at
    // this path (an interrupted earlier attempt, a refetch that started and
    // stopped) can be *longer* than the volume, and its stale tail would sit
    // above everything this writes and be read as the volume's own bytes.
    // Setting the length is not a truncate of the covered region: every byte
    // below `len` that the sweep writes is written after this call (nit).
    file.set_len(volume.len)
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index: volume.volume_index,
            error: error.to_string(),
        })?;

    // Sequential from zero over the covered runs. Holes are skipped by seeking
    // past them, which is what leaves the volume file sparse in exactly the
    // places the refetch is about to fill.
    let mut md5 = par2_rs::checksum::FileHashState::new();
    let mut contiguous = 0u64;
    let mut written_total = 0u64;
    let mut buffer = vec![0u8; SWEEP_CHUNK_BYTES];

    for &(start, end) in volume.covered.ranges() {
        let end = end.min(volume.len);
        if end <= start {
            continue;
        }
        // Composed against the range the sweep is about to read — clipped end
        // included — so a clamp can never lose the reference value the way a
        // pre-resolved lookup key did. A range with no reference is refused
        // before a byte of it is written — unless the run merely stops inside
        // an article and the caller carries that shape through, in which case
        // the reference covers the run up to its last article boundary.
        let (verified_end, reference) = match volume.crcs.compose(start, end - start) {
            Some(reference) => (end, reference),
            None => match volume.partial_article {
                PartialArticle::Refuse => {
                    return Err(ReconstructionFailure::UnverifiableRun {
                        volume_index: volume.volume_index,
                        offset: start,
                    });
                }
                PartialArticle::CarryThrough => {
                    let Some((prefix_len, reference)) =
                        volume.crcs.compose_prefix(start, end - start)
                    else {
                        return Err(ReconstructionFailure::UnverifiableRun {
                            volume_index: volume.volume_index,
                            offset: start,
                        });
                    };
                    let verified_end = start.saturating_add(prefix_len);
                    // The remainder must be a proper prefix of one article the
                    // composition knows. Anything else — a gap in the
                    // composition, a remainder spanning two articles — is bytes
                    // no article record accounts for.
                    let inside_one_article = volume.crcs.run_starting_at(verified_end).is_some_and(
                        |(article_start, article_len)| {
                            article_start.saturating_add(article_len) > end
                        },
                    );
                    if !inside_one_article {
                        return Err(ReconstructionFailure::UnverifiableRun {
                            volume_index: volume.volume_index,
                            offset: verified_end,
                        });
                    }
                    (verified_end, reference)
                }
            },
        };

        // Verify every reconstructed covered range. The composition is over
        // yEnc part CRC32s in source space, so it checks the bytes that came
        // back off the partials and envelope against what the wire delivered —
        // end to end, through both hops.
        if verified_end > start {
            let run_crc = copy_run(
                &mut reader,
                &mut file,
                volume.volume_index,
                start,
                verified_end,
                &mut buffer,
                &mut md5,
                &mut written_total,
            )?;
            if run_crc != reference {
                return Err(ReconstructionFailure::ChecksumMismatch {
                    volume_index: volume.volume_index,
                    offset: start,
                });
            }
            if start == contiguous {
                contiguous = verified_end;
            }
        }

        // The carried remainder: the placed prefix of the article the run stops
        // inside. Written, hashed into the MD5 for the accounting's sake, and
        // never part of the contiguous floor.
        if verified_end < end {
            tracing::debug!(
                volume_index = volume.volume_index,
                offset = verified_end,
                len = end - verified_end,
                "reconstruction carried a run that stops inside an article through with no \
                 composed reference; PAR2 judges those bytes slice by slice"
            );
            copy_run(
                &mut reader,
                &mut file,
                volume.volume_index,
                verified_end,
                end,
                &mut buffer,
                &mut md5,
                &mut written_total,
            )?;
        }
    }

    file.flush()
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index: volume.volume_index,
            error: error.to_string(),
        })?;
    file.sync_all()
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index: volume.volume_index,
            error: error.to_string(),
        })?;

    let complete =
        volume.assembly_complete && contiguous >= volume.len && written_total >= volume.len;
    Ok(ReconstructedVolume {
        volume_index: volume.volume_index,
        contiguous,
        complete,
        md5: complete.then(|| md5.finalize()),
    })
}

/// Copies `[start, end)` of the volume from the provider into the file and
/// returns the CRC32 of what it copied. Every byte must be there: a short read
/// or a hole is [`ReconstructionFailure::MissingBytes`] at the byte it stopped
/// on.
#[allow(clippy::too_many_arguments)]
fn copy_run<R: Read + Seek, W: Write + Seek>(
    reader: &mut R,
    file: &mut W,
    volume_index: u32,
    start: u64,
    end: u64,
    buffer: &mut [u8],
    md5: &mut par2_rs::checksum::FileHashState,
    written_total: &mut u64,
) -> Result<u32, ReconstructionFailure> {
    reader
        .seek(SeekFrom::Start(start))
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index,
            error: error.to_string(),
        })?;
    file.seek(SeekFrom::Start(start))
        .map_err(|error| ReconstructionFailure::WriteFailed {
            volume_index,
            error: error.to_string(),
        })?;

    let mut run_crc = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    let mut cursor = start;
    while cursor < end {
        let want = ((end - cursor) as usize).min(buffer.len());
        let read = match reader.read(&mut buffer[..want]) {
            Ok(0) => {
                return Err(ReconstructionFailure::MissingBytes {
                    volume_index,
                    offset: cursor,
                });
            }
            Ok(read) => read,
            Err(error) if is_hole(&error) => {
                return Err(ReconstructionFailure::MissingBytes {
                    volume_index,
                    offset: cursor,
                });
            }
            Err(error) => {
                return Err(ReconstructionFailure::WriteFailed {
                    volume_index,
                    error: error.to_string(),
                });
            }
        };
        let chunk = &buffer[..read];
        file.write_all(chunk)
            .map_err(|error| ReconstructionFailure::WriteFailed {
                volume_index,
                error: error.to_string(),
            })?;
        run_crc.update(chunk);
        // The MD5 is only meaningful for a volume that comes out whole, and a
        // whole volume is exactly one run from zero — so feeding every run and
        // only *using* the digest on the complete path costs nothing and cannot
        // silently hash a gapped file.
        md5.update(chunk);
        cursor = cursor.saturating_add(read as u64);
        *written_total = written_total.saturating_add(read as u64);
    }
    Ok(run_crc.finalize() as u32)
}

/// The articles of one volume a reconstruction materializes, and the
/// segment-aligned contiguous floor they add up to.
///
/// Both are in the **decoded** space the extents were observed in. The legacy
/// floor family — `segments_covered_by_floor` and direct-store's own
/// `coverage_skip_plan` — reads a floor back by walking the NZB's
/// `<segment bytes>`, which is the yEnc-*encoded* size and about 3% larger, so
/// deriving the floor that way here would decide a fully rebuilt two-article
/// volume was missing its last article. Writing the decoded floor is what the
/// conventional download path does (it publishes its write cursor), and reading
/// it back through the encoded walk errs in the safe direction: it can only ever
/// claim fewer whole segments than are really below the floor. Coverage above a
/// hole is still retained in the live assembly and write buffer, but is not
/// persisted as a restart floor.
pub(crate) fn segments_on_disk(
    extents: &BTreeMap<u32, (u64, u64)>,
    coverage: &ByteRanges,
    contiguous: u64,
) -> (Vec<u32>, u64) {
    let mut segments = Vec::new();
    let mut floor = 0u64;
    for (segment_number, (offset, len)) in extents {
        let Some(end) = offset.checked_add(*len) else {
            continue;
        };
        if !coverage.missing(*offset, *len).is_empty() {
            continue;
        }
        segments.push(*segment_number);
        if end <= contiguous {
            floor = floor.max(end);
        }
    }
    (segments, floor)
}
