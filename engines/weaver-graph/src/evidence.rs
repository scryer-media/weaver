// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a level hands the level below with its bytes: the digests of their
// pieces that something already computed, so no level hashes again what a
// level above proved. The decoder hashes every article as it decodes it; the
// root hands those digests down with the bytes, each object cuts them where
// it cuts the bytes, and whoever needs a digest of the kind it has in hand
// folds it instead of reading the bytes again.
//
// A digest names its algorithm, and the algorithm alone knows how digests
// join and part: one level never assumes another's kind. Cutting a piece of
// evidence hashes the smaller side of the cut and derives the other from the
// whole, for a kind that can; a kind that cannot loses the cut piece, and
// whoever needs it hashes those bytes itself. Bytes with no evidence of the
// kind a level needs are hashed by that level once: deciphered bytes, bytes
// read back from disk, a repair's output.
//
// Evidence is a fact of the bytes, never of where they sit: its ranges are
// offsets into the bytes it travels with, so it holds wherever a level puts
// them, and two carriers of the same bytes are the same bytes.

use std::fmt;
use std::ops::{Bound, Deref, RangeBounds};

use bytes::Bytes;
use crc_turbo::{CrcAlgorithm, CrcCombine};

use crate::volume::ByteRange;

// A hash function the evidence can carry the digests of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Algorithm {
    // CRC-32/ISO-HDLC: the checksum yEnc, RAR, 7z, zip, SFV and PAR2 state.
    Crc32,
}

// One algorithm's digest of some bytes. The value is the algorithm's to
// read; it is wide enough for any kind of up to 64 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Digest {
    pub algorithm: Algorithm,
    pub value: u64,
}

impl Algorithm {
    fn crc(self) -> CrcAlgorithm {
        match self {
            Algorithm::Crc32 => CrcAlgorithm::Crc32IsoHdlc,
        }
    }

    // The digest of `bytes`, hashed here.
    #[inline]
    pub fn hash(self, bytes: &[u8]) -> Digest {
        #[cfg(any(test, feature = "test-support"))]
        counting::count(bytes.len());
        Digest {
            algorithm: self,
            value: crc_turbo::checksum(self.crc(), bytes),
        }
    }

    // The digest of no bytes.
    pub fn empty(self) -> Digest {
        Digest {
            algorithm: self,
            value: crc_turbo::checksum(self.crc(), &[]),
        }
    }

    // Whether this kind's digests of adjacent runs join into the digest of
    // both, and part again: what lets a cut derive one side from the other.
    // Every kind there is now does; a kind that does not loses a piece cut
    // through it, and its digests are only ever taken whole.
    pub fn composes(self) -> bool {
        match self {
            Algorithm::Crc32 => true,
        }
    }

    // The digest of `first` followed by `len` bytes whose digest is
    // `second`. `None` for a kind that does not compose.
    pub fn join(self, first: u64, second: u64, len: u64) -> Option<u64> {
        self.composes()
            .then(|| CrcCombine::new(self.crc(), len).combine(first, second))
    }

    // The digest of the `len` bytes that follow `head` in bytes whose
    // digest is `whole`.
    fn tail_of(self, whole: u64, head: u64, len: u64) -> Option<u64> {
        // The join is linear: the head's part of the whole is the head
        // joined with nothing.
        Some(whole ^ self.join(head, 0, len)?)
    }

    // The digest of bytes whose digest is `whole` once the bytes whose
    // digest is `old`, with `behind` bytes after them, are replaced by as
    // many bytes whose digest is `new`. A composing kind is linear in its
    // bytes: the old part gives way to the new without reading the rest.
    pub fn replace(self, whole: u64, old: u64, new: u64, behind: u64) -> Option<u64> {
        Some(whole ^ self.join(old, 0, behind)? ^ self.join(new, 0, behind)?)
    }

    // The digest of what comes before `len` bytes whose digest is `tail`,
    // in bytes whose digest is `whole`.
    pub fn head_of(self, whole: u64, tail: u64, len: u64) -> Option<u64> {
        self.composes()
            .then(|| CrcCombine::new(self.crc(), len).uncombine(whole, tail))
            .flatten()
    }
}

impl Digest {
    pub fn crc32(value: u32) -> Self {
        Self {
            algorithm: Algorithm::Crc32,
            value: u64::from(value),
        }
    }
}

// One piece of evidence: the digest of the bytes at `range` of the bytes it
// travels with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Evidence {
    pub range: ByteRange,
    pub digest: Digest,
}

// Bytes as a level hands them down, with the evidence in hand of them.
// Slicing is a view, as `Bytes` slicing is: no byte is copied.
#[derive(Clone, Default)]
pub struct Evidenced {
    bytes: Bytes,
    // In order, none overlapping, every one inside the bytes.
    evidence: Vec<Evidence>,
}

impl Evidenced {
    // Bytes no one has hashed.
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            evidence: Vec::new(),
        }
    }

    // Bytes with the digests of their pieces. A piece outside the bytes,
    // or one overlapping a piece of its kind before it, is not evidence of
    // them and is left out.
    pub fn with(bytes: Bytes, evidence: impl IntoIterator<Item = Evidence>) -> Self {
        let len = bytes.len() as u64;
        let mut kept: Vec<Evidence> = evidence
            .into_iter()
            .filter(|piece| piece.range.end <= len && !piece.range.is_empty())
            .collect();
        kept.sort_by_key(|piece| piece.range.start);
        let mut ends: Vec<(Algorithm, u64)> = Vec::new();
        kept.retain(|piece| {
            let kind = piece.digest.algorithm;
            let end = match ends.iter_mut().find(|(algorithm, _)| *algorithm == kind) {
                Some((_, end)) => end,
                None => {
                    ends.push((kind, 0));
                    &mut ends.last_mut().expect("just pushed").1
                }
            };
            let apart = piece.range.start >= *end;
            if apart {
                *end = piece.range.end;
            }
            apart
        });
        Self {
            bytes,
            evidence: kept,
        }
    }

    // Hash, once, every run of the bytes with no evidence of `algorithm`,
    // and keep what was hashed as evidence: a level that needs the digest
    // more than once hashes nothing the second time, and the level below is
    // handed it too.
    pub fn fill(&mut self, algorithm: Algorithm) {
        let len = self.bytes.len() as u64;
        let mut bare = Vec::new();
        let mut at = 0;
        for piece in &self.evidence {
            if piece.digest.algorithm != algorithm {
                continue;
            }
            if piece.range.start > at {
                bare.push(ByteRange::new(at, piece.range.start));
            }
            at = piece.range.end;
        }
        if at < len {
            bare.push(ByteRange::new(at, len));
        }
        if bare.is_empty() {
            return;
        }
        for range in bare {
            let digest = algorithm.hash(&self.bytes[range.start as usize..range.end as usize]);
            self.evidence.push(Evidence { range, digest });
        }
        self.evidence.sort_by_key(|piece| piece.range.start);
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn evidence(&self) -> &[Evidence] {
        &self.evidence
    }

    // The bytes at `range`, with the evidence cut where they are cut. A
    // piece of evidence the cut goes through keeps the part inside: the
    // smaller side is hashed and the other derived, for a kind that
    // composes; a kind that does not loses that piece.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let (start, end) = bounds(&range, self.bytes.len());
        if (start, end) == (0, self.bytes.len()) {
            return self.clone();
        }
        let (from, to) = (start as u64, end as u64);
        let evidence = self
            .evidence
            .iter()
            .filter_map(|piece| {
                let inside = piece.range.intersect(ByteRange::new(from, to))?;
                let digest = self.part(piece, inside)?;
                Some(Evidence {
                    range: inside.shift(-(from as i64)),
                    digest,
                })
            })
            .collect();
        Self {
            bytes: self.bytes.slice(start..end),
            evidence,
        }
    }

    // The bytes before `at` and the bytes from it, with the evidence cut
    // between them. A piece the cut goes through is hashed once, on its
    // smaller side, and the other side derived from the whole.
    pub fn split_at(&self, at: usize) -> (Self, Self) {
        assert!(at <= self.bytes.len(), "split {at} out of {}", self.len());
        let cut = at as u64;
        let (mut before, mut after) = (Vec::new(), Vec::new());
        for piece in &self.evidence {
            let range = piece.range;
            if range.end <= cut {
                before.push(*piece);
                continue;
            }
            if range.start >= cut {
                after.push(Evidence {
                    range: range.shift(-(cut as i64)),
                    digest: piece.digest,
                });
                continue;
            }
            let algorithm = piece.digest.algorithm;
            let (head, tail) = (
                ByteRange::new(range.start, cut),
                ByteRange::new(cut, range.end),
            );
            let of = |range: ByteRange| &self.bytes[range.start as usize..range.end as usize];
            let whole = piece.digest.value;
            let derived = if !algorithm.composes() {
                None
            } else if head.len() <= tail.len() {
                let hashed = algorithm.hash(of(head)).value;
                algorithm
                    .tail_of(whole, hashed, tail.len())
                    .map(|derived| (hashed, derived))
            } else {
                let hashed = algorithm.hash(of(tail)).value;
                algorithm
                    .head_of(whole, hashed, tail.len())
                    .map(|derived| (derived, hashed))
            };
            let Some((first, second)) = derived else {
                continue;
            };
            before.push(Evidence {
                range: head,
                digest: Digest {
                    algorithm,
                    value: first,
                },
            });
            after.push(Evidence {
                range: tail.shift(-(cut as i64)),
                digest: Digest {
                    algorithm,
                    value: second,
                },
            });
        }
        (
            Self {
                bytes: self.bytes.slice(..at),
                evidence: before,
            },
            Self {
                bytes: self.bytes.slice(at..),
                evidence: after,
            },
        )
    }

    // The digest of `inside`, which lies in `piece`.
    fn part(&self, piece: &Evidence, inside: ByteRange) -> Option<Digest> {
        if inside == piece.range {
            return Some(piece.digest);
        }
        let algorithm = piece.digest.algorithm;
        if !algorithm.composes() {
            return None;
        }
        let of = |range: ByteRange| &self.bytes[range.start as usize..range.end as usize];
        let head = ByteRange::new(piece.range.start, inside.start);
        let tail = ByteRange::new(inside.end, piece.range.end);
        if inside.len() <= head.len() + tail.len() {
            return Some(algorithm.hash(of(inside)));
        }
        let mut whole = piece.digest.value;
        if !tail.is_empty() {
            let hashed = algorithm.hash(of(tail)).value;
            whole = algorithm.head_of(whole, hashed, tail.len())?;
        }
        if !head.is_empty() {
            let hashed = algorithm.hash(of(head)).value;
            whole = algorithm.tail_of(whole, hashed, inside.len())?;
        }
        Some(Digest {
            algorithm,
            value: whole,
        })
    }

    // The `algorithm` digest of all the bytes: the evidence of that kind
    // joined, and what it leaves bare hashed here. A kind that does not
    // compose is taken whole from one piece that covers every byte, or
    // hashed.
    pub fn digest(&self, algorithm: Algorithm) -> Digest {
        let len = self.bytes.len() as u64;
        if !algorithm.composes() {
            return self
                .evidence
                .iter()
                .find(|piece| piece.digest.algorithm == algorithm && piece.range.len() == len)
                .map_or_else(|| algorithm.hash(&self.bytes), |piece| piece.digest);
        }
        let mut digest = algorithm.empty().value;
        let mut at = 0;
        let join = |digest: u64, range: ByteRange, value: Option<u64>| {
            let value = value.unwrap_or_else(|| {
                algorithm
                    .hash(&self.bytes[range.start as usize..range.end as usize])
                    .value
            });
            algorithm
                .join(digest, value, range.len())
                .expect("a composing kind joins")
        };
        for piece in &self.evidence {
            if piece.digest.algorithm != algorithm {
                continue;
            }
            if piece.range.start > at {
                digest = join(digest, ByteRange::new(at, piece.range.start), None);
            }
            digest = join(digest, piece.range, Some(piece.digest.value));
            at = piece.range.end;
        }
        if at < len {
            digest = join(digest, ByteRange::new(at, len), None);
        }
        Digest {
            algorithm,
            value: digest,
        }
    }

    // Whether every byte has evidence of `algorithm`: a level that needs
    // that kind hashes nothing for these bytes.
    pub fn proven(&self, algorithm: Algorithm) -> bool {
        let mut at = 0;
        for piece in &self.evidence {
            if piece.digest.algorithm == algorithm && piece.range.start == at {
                at = piece.range.end;
            }
        }
        at == self.bytes.len() as u64
    }
}

fn bounds(range: &impl RangeBounds<usize>, len: usize) -> (usize, usize) {
    let start = match range.start_bound() {
        Bound::Included(&at) => at,
        Bound::Excluded(&at) => at + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&at) => at + 1,
        Bound::Excluded(&at) => at,
        Bound::Unbounded => len,
    };
    assert!(
        start <= end && end <= len,
        "slice {start}..{end} out of {len} bytes"
    );
    (start, end)
}

impl Deref for Evidenced {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsRef<[u8]> for Evidenced {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

// The same bytes are the same, whatever was proved of them: evidence is a
// fact of the bytes.
impl PartialEq for Evidenced {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Evidenced {}

impl PartialEq<Bytes> for Evidenced {
    fn eq(&self, other: &Bytes) -> bool {
        self.bytes == *other
    }
}

impl PartialEq<[u8]> for Evidenced {
    fn eq(&self, other: &[u8]) -> bool {
        self.bytes == other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for Evidenced {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.bytes == other[..]
    }
}

impl FromIterator<u8> for Evidenced {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl fmt::Debug for Evidenced {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.bytes, formatter)
    }
}

impl From<Bytes> for Evidenced {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes)
    }
}

impl From<Vec<u8>> for Evidenced {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes.into())
    }
}

impl From<&'static [u8]> for Evidenced {
    fn from(bytes: &'static [u8]) -> Self {
        Self::new(Bytes::from_static(bytes))
    }
}

impl<const N: usize> From<&'static [u8; N]> for Evidenced {
    fn from(bytes: &'static [u8; N]) -> Self {
        Self::new(Bytes::from_static(bytes))
    }
}

// How many bytes this thread hashed: what a test reads to say a level
// derived its digest from what it was handed instead of hashing again.
#[cfg(any(test, feature = "test-support"))]
pub mod counting {
    use std::cell::Cell;

    thread_local! {
        static HASHED: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn count(len: usize) {
        HASHED.with(|hashed| hashed.set(hashed.get() + len as u64));
    }

    pub fn hashed() -> u64 {
        HASHED.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize) -> Bytes {
        (0..len)
            .map(|at| (at as u8).wrapping_mul(29) ^ 0x5a)
            .collect::<Vec<u8>>()
            .into()
    }

    fn crc(bytes: &[u8]) -> Digest {
        Digest::crc32(crc_turbo::checksum(CrcAlgorithm::Crc32IsoHdlc, bytes) as u32)
    }

    // Every kind of evidence there is: a new kind joins this list, and each
    // test runs for it.
    fn kinds() -> impl Iterator<Item = Algorithm> {
        [Algorithm::Crc32]
            .into_iter()
            .inspect(|algorithm| match algorithm {
                Algorithm::Crc32 => {}
            })
    }

    // The evidence a decoder hands with an article: its digests cut at 1000.
    fn decoded_as(algorithm: Algorithm, len: usize) -> Evidenced {
        let bytes = bytes(len);
        let pieces: Vec<Evidence> = (0..len)
            .step_by(1000)
            .map(|start| {
                let range = ByteRange::new(start as u64, (start + 1000).min(len) as u64);
                Evidence {
                    range,
                    digest: algorithm.hash(&bytes[start..range.end as usize]),
                }
            })
            .collect();
        Evidenced::with(bytes, pieces)
    }

    fn decoded(len: usize) -> Evidenced {
        decoded_as(Algorithm::Crc32, len)
    }

    #[test]
    fn a_cut_yields_two_sides_that_join_back_to_the_whole_hashing_one_fragment() {
        for algorithm in kinds() {
            let whole = decoded_as(algorithm, 3000);
            let before = counting::hashed();
            // 1100 cuts the second piece 100 in: its head is the smaller side.
            let (left, right) = whole.split_at(1100);
            // One fragment hashed: the 100 bytes of the smaller side.
            assert_eq!(counting::hashed() - before, 100, "{algorithm:?}");
            assert_eq!(left.evidence().len(), 2);
            assert_eq!(right.evidence().len(), 2);
            for side in [&left, &right] {
                assert!(side.proven(algorithm));
                for piece in side.evidence() {
                    let range = piece.range.start as usize..piece.range.end as usize;
                    assert_eq!(piece.digest, algorithm.hash(&side[range]));
                }
            }
            // The two sides join back to the digest of the whole.
            let before = counting::hashed();
            let joined = algorithm
                .join(
                    left.digest(algorithm).value,
                    right.digest(algorithm).value,
                    right.len() as u64,
                )
                .unwrap();
            assert_eq!(counting::hashed() - before, 0);
            assert_eq!(joined, algorithm.hash(&whole).value);
            assert_eq!(whole.digest(algorithm), algorithm.hash(&whole));
            // A cut past the middle hashes the tail side instead.
            let before = counting::hashed();
            let (head, tail) = whole.split_at(1900);
            assert_eq!(counting::hashed() - before, 100, "{algorithm:?}");
            assert_eq!(head.evidence()[1].digest, algorithm.hash(&head[1000..]));
            assert_eq!(tail.evidence()[0].digest, algorithm.hash(&tail[..100]));
        }
    }

    #[test]
    fn a_slice_inside_a_piece_keeps_evidence_on_both_sides() {
        let whole = decoded(3000);
        let before = counting::hashed();
        let (left, right) = (whole.slice(..1100), whole.slice(1100..));
        // Each slice hashed only the smaller side of its cut.
        assert_eq!(counting::hashed() - before, 200);
        for side in [&left, &right] {
            assert!(side.proven(Algorithm::Crc32));
            for piece in side.evidence() {
                let range = piece.range.start as usize..piece.range.end as usize;
                assert_eq!(piece.digest, crc(&side[range]));
            }
        }
    }

    #[test]
    fn a_cut_through_both_ends_of_a_piece_hashes_only_the_ends() {
        let whole = decoded(1000);
        let before = counting::hashed();
        let middle = whole.slice(10..990);
        assert_eq!(counting::hashed() - before, 20);
        assert_eq!(middle.evidence()[0].digest, crc(&middle));
        // Where the middle is the smaller side, the middle is hashed.
        let before = counting::hashed();
        let small = whole.slice(400..500);
        assert_eq!(counting::hashed() - before, 100);
        assert_eq!(small.evidence()[0].digest, crc(&small));
    }

    #[test]
    fn bytes_without_evidence_are_hashed_once_where_a_digest_is_needed() {
        let bare = Evidenced::new(bytes(2500));
        let before = counting::hashed();
        assert!(!bare.proven(Algorithm::Crc32));
        assert_eq!(bare.slice(500..).evidence(), &[]);
        assert_eq!(counting::hashed() - before, 0);
        assert_eq!(bare.digest(Algorithm::Crc32), crc(&bare));
        assert_eq!(counting::hashed() - before, 2500);
        // Evidence with a gap: only the gap is hashed.
        let whole = decoded(3000);
        let gapped = Evidenced::with(
            whole.bytes().clone(),
            [whole.evidence()[0], whole.evidence()[2]],
        );
        let before = counting::hashed();
        assert_eq!(gapped.digest(Algorithm::Crc32), crc(&whole));
        assert_eq!(counting::hashed() - before, 1000);
        // Filled, the gap is hashed once and kept: a second digest hashes
        // nothing.
        let mut filled = gapped;
        let before = counting::hashed();
        filled.fill(Algorithm::Crc32);
        assert_eq!(counting::hashed() - before, 1000);
        assert!(filled.proven(Algorithm::Crc32));
        assert_eq!(filled.digest(Algorithm::Crc32), crc(&whole));
        assert_eq!(counting::hashed() - before, 1000);
    }
}
