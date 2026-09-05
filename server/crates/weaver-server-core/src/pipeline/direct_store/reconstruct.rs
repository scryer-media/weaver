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
//! disagrees with what came back off disk — falls back to refetching, which is
//! always correct. What it must never do is *half* succeed and then let the
//! caller persist a floor over bytes it did not actually rebuild, so the floor
//! a volume reports is the contiguous prefix it verifiably wrote and nothing
//! beyond it.
//!
//! The fallback is scoped to the **run**, not to the volume and not to the set.
//! A refused run costs that run's articles and whatever the refusal leaves
//! unreachable; every other run of the same volume keeps its rebuilt bytes and
//! is reported in [`ReconstructedVolume::verified`], which is the only thing
//! the caller may derive a keep set or a floor from. That matters most for the
//! volumes a demotion catches mid-download: their coverage stops at the article
//! frontier, and refusing the whole volume for a frontier the composition
//! cannot close would throw away every byte below it and fetch the volume twice.
//! The one failure that still abandons a volume is a failing *destination* — a
//! create, a write, or a sparse marking — because then nothing about the file
//! on disk can be relied on.
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
    /// Exactly the physical ranges this sweep **wrote and checked**: every one
    /// of them came back out of the overlay and matched the yEnc part-CRC32 the
    /// wire delivered for it.
    ///
    /// This is what the caller must decide the keep set from, never the
    /// coverage the plan asked for. The two agree only for a volume that swept
    /// end to end; a volume that refused a run in the middle wrote the runs on
    /// either side and checked neither the refused one nor anything the refusal
    /// made unreachable, and claiming an article the sweep did not make durable
    /// would leave a floor or a committed segment over bytes nothing vouched
    /// for. A carried run that stops inside an article
    /// ([`PartialArticle::CarryThrough`]) is written but deliberately absent
    /// here: nothing composed a reference for it.
    pub(crate) verified: ByteRanges,
    /// Every byte of `[0, len)` is on disk, so the volume is indistinguishable
    /// from one the conventional path downloaded.
    pub(crate) complete: bool,
    /// MD5 of the whole volume, for the completed-file row. Only computed when
    /// the volume came out complete, because that is the only case where it
    /// describes the whole file.
    pub(crate) md5: Option<[u8; 16]>,
    /// Why this volume could not be rebuilt **in full**, when it could not be.
    ///
    /// The failure no longer implies the volume is a write-off. A run the sweep
    /// cannot vouch for costs that run and whatever the refusal makes
    /// unreachable — the rest of the volume keeps its rebuilt bytes and shows
    /// up in `verified` — so the caller refetches the articles `verified` does
    /// not back and nothing else. Only a failing *destination* (a create, a
    /// write, a sparse marking) abandons the whole volume, and that one comes
    /// back with an empty `verified` and its file removed.
    ///
    /// The first refusal is the one reported: it is the one that names the
    /// offset where the image stopped being reproducible.
    pub(crate) failure: Option<ReconstructionFailure>,
}

/// Why a **volume** could not be rebuilt. Each is a metric bucket, and each
/// costs a refetch of that volume and nothing else.
///
/// [`ReconstructionFailure::NoLayout`] and
/// [`ReconstructionFailure::EncryptedPostedBytes`] are the two the *caller*
/// raises before the sweep starts, and they are properties of the whole set
/// rather than of one volume.
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
///
/// # Per run, not per volume, and never per set
///
/// The first shape deleted **every** volume file of the set and refetched the
/// whole group the moment one volume hit an `UnverifiableRun`, a `MissingBytes`
/// or a `ChecksumMismatch`. The reasoning it gave for that is real — a range
/// nothing verified must not be left where the caller would claim it, because
/// the refetch publishes no floor and stray bytes would be bytes nothing
/// claims, nothing verifies and nothing overwrites — but the reasoning is about
/// the *range*, not about its volume and not about the volume's siblings.
///
/// So a refused run is simply left out of [`ReconstructedVolume::verified`],
/// and the caller derives its keep set and its floor from that one field: every
/// article the sweep did not vouch for is refetched, and every article it did
/// stays. A volume that refuses its frontier keeps everything below it; a
/// volume that refuses a run in its middle keeps the runs on both sides and
/// publishes a floor only up to the hole. On a 125-volume set where two volumes
/// were mid-download this is the difference between two whole volumes off the
/// wire and the handful of articles that had not arrived.
///
/// The exception is a failing **destination** — the file could not be created,
/// written, or marked sparse. Nothing about that file can be relied on, so its
/// volume is abandoned whole: the file is removed, `verified` comes back empty,
/// and the caller gives it the full-refetch treatment.
pub(crate) fn reconstruct_volumes(
    provider: &HybridVolumeProvider,
    volumes: &[VolumeReconstruction],
    sparse: SparseMarking,
) -> Vec<ReconstructedVolume> {
    volumes
        .iter()
        .map(
            |volume| match reconstruct_volume(provider, volume, sparse) {
                Ok(outcome) => outcome,
                Err(failure) => {
                    let _ = std::fs::remove_file(&volume.path);
                    ReconstructedVolume {
                        volume_index: volume.volume_index,
                        contiguous: 0,
                        verified: ByteRanges::new(),
                        complete: false,
                        md5: None,
                        failure: Some(failure),
                    }
                }
            },
        )
        .collect()
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
            verified: ByteRanges::new(),
            complete: false,
            md5: None,
            failure: None,
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
    let mut verified = ByteRanges::new();
    let mut written_total = 0u64;
    let mut buffer = vec![0u8; SWEEP_CHUNK_BYTES];
    // The first chunk the sweep could not vouch for. Kept rather than returned
    // so the chunks after it still get their chance: it names where the image
    // stopped being reproducible, and everything outside it is still exactly
    // the bytes the wire delivered.
    let mut refused: Option<ReconstructionFailure> = None;

    for &(range_start, range_end) in volume.covered.ranges() {
        let range_end = range_end.min(volume.len);
        if range_end <= range_start {
            continue;
        }
        // A covered range is a *merged* run: the coverage map abuts adjacent
        // articles into one entry, so a range can be a whole 6 GB volume. It is
        // walked one article at a time all the same, because the article is the
        // unit the composition can vouch for independently and therefore the
        // unit a refusal may cost. Verifying the merged range as a whole is the
        // same check — a composed CRC32 matches exactly when every part does —
        // but it charges one bad article to every article beside it, which on a
        // contiguous volume is the whole volume.
        let mut start = range_start;
        while start < range_end {
            // The article that starts here, when one does and it fits inside
            // the covered range. Otherwise the remainder of the range: the
            // shape the composition can only refuse or, for the repair scratch,
            // carry through.
            let end = match volume.crcs.run_starting_at(start) {
                Some((_, len)) if start.saturating_add(len) <= range_end => start + len,
                _ => range_end,
            };
            // Composed against the range the sweep is about to read — clipped end
            // included — so a clamp can never lose the reference value the way a
            // pre-resolved lookup key did. A range with no reference is refused
            // before a byte of it is written — unless the run merely stops inside
            // an article and the caller carries that shape through, in which case
            // the reference covers the run up to its last article boundary.
            // `Err` carries the offset the refusal is about, which is not always
            // the start of the run: a carried remainder that is not the prefix of
            // one known article is refused at the boundary the composition ran out
            // on.
            let composed: Result<(u64, u32), u64> = match volume.crcs.compose(start, end - start) {
                Some(reference) => Ok((end, reference)),
                None => match volume.partial_article {
                    PartialArticle::Refuse => Err(start),
                    PartialArticle::CarryThrough => {
                        match volume.crcs.compose_prefix(start, end - start) {
                            None => Err(start),
                            Some((prefix_len, reference)) => {
                                let verified_end = start.saturating_add(prefix_len);
                                // The remainder must be a proper prefix of one
                                // article the composition knows. Anything else — a
                                // gap in the composition, a remainder spanning two
                                // articles — is bytes no article record accounts
                                // for, and the run is refused whole rather than
                                // split at a boundary the record does not describe.
                                let inside_one_article = volume
                                    .crcs
                                    .run_starting_at(verified_end)
                                    .is_some_and(|(article_start, article_len)| {
                                        article_start.saturating_add(article_len) > end
                                    });
                                match inside_one_article {
                                    true => Ok((verified_end, reference)),
                                    false => Err(verified_end),
                                }
                            }
                        }
                    }
                },
            };
            let (verified_end, reference) = match composed {
                Ok(composed) => composed,
                Err(offset) => {
                    // No reference for this chunk, so nothing may claim it. It
                    // is skipped rather than written: the refetch owns its
                    // articles now, and leaving the file sparse there is what
                    // keeps a byte nothing vouched for from sitting under an
                    // article that never arrives.
                    if refused.is_none() {
                        refused = Some(ReconstructionFailure::UnverifiableRun {
                            volume_index: volume.volume_index,
                            offset,
                        });
                    }
                    start = end;
                    continue;
                }
            };

            // Verify every reconstructed covered range. The composition is over
            // yEnc part CRC32s in source space, so it checks the bytes that came
            // back off the partials and envelope against what the wire delivered —
            // end to end, through both hops.
            if verified_end > start {
                let run_crc = match copy_run(
                    &mut reader,
                    &mut file,
                    volume.volume_index,
                    start,
                    verified_end,
                    &mut buffer,
                    &mut md5,
                    &mut written_total,
                ) {
                    Ok(run_crc) => run_crc,
                    // A byte the coverage claims and the overlay does not have
                    // is this chunk's problem: the partial holding it was
                    // deleted or truncated, which says nothing about the
                    // articles on either side of it.
                    Err(failure @ ReconstructionFailure::MissingBytes { .. }) => {
                        if refused.is_none() {
                            refused = Some(failure);
                        }
                        start = end;
                        continue;
                    }
                    // Anything else here is the destination failing, and no
                    // chunk of this volume can be trusted after it.
                    Err(failure) => return Err(failure),
                };
                if run_crc != reference {
                    if refused.is_none() {
                        refused = Some(ReconstructionFailure::ChecksumMismatch {
                            volume_index: volume.volume_index,
                            offset: start,
                        });
                    }
                    start = end;
                    continue;
                }
                verified.insert(start, verified_end - start);
            }

            // The carried remainder: the placed prefix of the article the run stops
            // inside. Written, hashed into the MD5 for the accounting's sake, and
            // never part of `verified` — nothing composed a reference for it, so
            // nothing may publish a floor or keep an article over it.
            if verified_end < end {
                tracing::debug!(
                    volume_index = volume.volume_index,
                    offset = verified_end,
                    len = end - verified_end,
                    "reconstruction carried a run that stops inside an article through with no \
                     composed reference; PAR2 judges those bytes slice by slice"
                );
                match copy_run(
                    &mut reader,
                    &mut file,
                    volume.volume_index,
                    verified_end,
                    end,
                    &mut buffer,
                    &mut md5,
                    &mut written_total,
                ) {
                    Ok(_) => {}
                    Err(failure @ ReconstructionFailure::MissingBytes { .. }) => {
                        if refused.is_none() {
                            refused = Some(failure);
                        }
                    }
                    Err(failure) => return Err(failure),
                }
            }
            start = end;
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

    // A sweep that vouched for nothing leaves nothing — under the policy that
    // publishes floors. The volume refetches whole from here, and an empty
    // full-length file at the volume's path is worse than no file: a PAR2 pass
    // or an extraction reaching it before the refetch lands reads a hole as
    // damage where an absent file reads as missing, which is what it is.
    // `CarryThrough` is exempt because its caller wants the scratch precisely
    // for the bytes nothing composed a reference for.
    if volume.partial_article == PartialArticle::Refuse && verified.is_empty() {
        drop(file);
        let _ = std::fs::remove_file(&volume.path);
        return Ok(ReconstructedVolume {
            volume_index: volume.volume_index,
            contiguous: 0,
            verified,
            complete: false,
            md5: None,
            failure: refused,
        });
    }

    let contiguous = verified.contiguous_from_zero();
    // A refusal anywhere disqualifies the whole-file claims outright, the MD5
    // included: the digest was fed every run the sweep read in the order it read
    // them, and a skipped run makes that sequence something other than the
    // file's bytes.
    let complete = refused.is_none()
        && volume.assembly_complete
        && contiguous >= volume.len
        && written_total >= volume.len;
    Ok(ReconstructedVolume {
        volume_index: volume.volume_index,
        contiguous,
        verified,
        complete,
        md5: complete.then(|| md5.finalize()),
        failure: refused,
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
/// `coverage` must be [`ReconstructedVolume::verified`] — the ranges the sweep
/// wrote *and* checked — never the coverage its plan asked for. The two differ
/// exactly when the sweep refused a run, and that is the case where the
/// difference matters: an article the sweep skipped is one whose bytes are not
/// on disk, and naming it here would keep it committed in the assembly and
/// leave the volume with a hole nothing ever fetches.
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
