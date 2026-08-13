//! Segment-CRC checkpointing: one CRC pass over decoded article bytes that
//! also yields PAR2 block CRC32s.
//!
//! The decode path already computes a CRC32 over every decoded byte to verify
//! the yEnc `pcrc32`. That pass covers exactly the bytes PAR2 blocks are made
//! of, just cut at article boundaries instead of block boundaries. Checkpointing
//! the pass at block boundaries makes one pass serve both integrity families:
//!
//! - the article `pcrc32` is the in-order combine-fold of the article's own
//!   segments, and
//! - a block CRC32 is the combine-fold of the segments tiling that block, which
//!   may span several articles.
//!
//! Segment assembly across articles happens in the evidence collector above the
//! decoder; this module provides the checkpointing pass ([`SegmentedCrc32`]),
//! the record type ([`Segment`]) and the composition primitives
//! ([`combine_contiguous`], [`crate::crc::crc32_combine`]).
//!
//! # Intended call pattern
//!
//! The caller declares boundaries in *absolute output offsets* — the file
//! offset the article's decoded bytes land at, plus the PAR2 block size — and
//! then feeds decoded bytes in whatever chunks the driver produces. Checkpoint
//! placement is a function of file offsets alone, so it is invariant to chunk
//! and window boundaries:
//!
//! ```rust
//! use std::num::NonZeroU64;
//! use weaver_yenc::segment::{SegmentedCrc32, combine_contiguous};
//!
//! // Article starts 3 MiB into the file; PAR2 block size is 1 MiB.
//! let mut pass = SegmentedCrc32::new(3 << 20, NonZeroU64::new(1 << 20));
//! pass.update(&[b'a'; 700 * 1024]); // fed in arbitrary chunks by the driver
//! pass.update(&[b'b'; 700 * 1024]);
//! let segments = pass.finish();
//!
//! // Article pcrc32 == the in-order fold of the article's segments.
//! let article = combine_contiguous(&segments).expect("contiguous");
//! assert_eq!(article.file_offset, 3 << 20);
//! assert_eq!(article.len, 1_400 * 1024);
//! ```
//!
//! Before the PAR2 block size is known, construct with `block_size = None`: the
//! pass emits one segment for the whole article. Such segments still compose
//! whenever they happen to tile a block; blocks they do not tile fall back to
//! settle-time read-back. An article is never delayed or re-decoded to obtain
//! checkpoints.

use std::num::NonZeroU64;

use crate::crc::{Crc32, crc32_combine};

/// A contiguous run of decoded output bytes and its standalone CRC32.
///
/// `crc32` is a complete CRC32 over `[file_offset, file_offset + len)` with the
/// standard init and final xor — not a running prefix value — so segments
/// compose in any order-preserving tiling via [`combine_contiguous`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Absolute offset of the first byte within the reconstructed file.
    pub file_offset: u64,
    /// Number of bytes covered.
    pub len: u64,
    /// CRC32 (ISO-HDLC) over exactly those bytes.
    pub crc32: u32,
}

impl Segment {
    /// Absolute offset one past the last byte covered.
    #[inline]
    pub fn end_offset(&self) -> u64 {
        self.file_offset + self.len
    }
}

/// A CRC32 pass over decoded output bytes that checkpoints at caller-declared
/// block boundaries.
///
/// Feeding is chunk-agnostic: `update` may be called with any split of the
/// article's decoded bytes and produces the same segments, because boundaries
/// are derived from absolute file offsets rather than from call boundaries.
#[derive(Debug, Clone)]
pub struct SegmentedCrc32 {
    crc: Crc32,
    /// File offset of the first byte of the segment currently open.
    open_offset: u64,
    /// File offset one past the last byte fed.
    cursor: u64,
    /// Bytes remaining until the next checkpoint; `None` when no block size is
    /// known and the whole article becomes a single segment.
    until_boundary: Option<u64>,
    block_size: Option<NonZeroU64>,
    segments: Vec<Segment>,
}

impl SegmentedCrc32 {
    /// Start a pass whose first byte lands at `file_offset` in the
    /// reconstructed file, checkpointing at every multiple of `block_size`.
    ///
    /// `block_size = None` is the pre-block-size policy: no checkpoints, one
    /// segment for the whole article.
    pub fn new(file_offset: u64, block_size: Option<NonZeroU64>) -> Self {
        Self {
            crc: Crc32::new(),
            open_offset: file_offset,
            cursor: file_offset,
            until_boundary: block_size.map(|size| bytes_to_next_boundary(file_offset, size)),
            block_size,
            segments: Vec::new(),
        }
    }

    /// File offset one past the last byte fed so far.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Segments closed so far. The segment currently open is not included;
    /// call [`Self::finish`] to close it.
    #[inline]
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Feed decoded output bytes, closing a segment at every block boundary
    /// crossed.
    pub fn update(&mut self, mut data: &[u8]) {
        let Some(mut remaining) = self.until_boundary else {
            // No checkpoints: the dominant path stays a single CRC update with
            // no per-call arithmetic beyond the cursor bump.
            self.crc.update(data);
            self.cursor += data.len() as u64;
            return;
        };

        while !data.is_empty() {
            let take = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(data.len());
            self.crc.update(&data[..take]);
            self.cursor += take as u64;
            data = &data[take..];
            remaining -= take as u64;

            if remaining == 0 {
                self.cut();
                // `cut` recomputes the stride from the (block-aligned) cursor.
                remaining = self.until_boundary.expect("block size is known");
            }
        }

        self.until_boundary = Some(remaining);
    }

    /// Close the segment currently open at the current cursor.
    ///
    /// Automatic block-boundary checkpoints call this; callers with boundaries
    /// that are not a fixed stride can drive it directly. A no-op when no bytes
    /// have been fed since the previous checkpoint — zero-length segments are
    /// never emitted.
    pub fn checkpoint(&mut self) {
        if self.cursor > self.open_offset {
            self.cut();
        }
    }

    /// Close the open segment and return every segment in file order.
    pub fn finish(mut self) -> Vec<Segment> {
        self.checkpoint();
        self.segments
    }

    fn cut(&mut self) {
        let crc32 = self.crc.checkpoint();
        self.segments.push(Segment {
            file_offset: self.open_offset,
            len: self.cursor - self.open_offset,
            crc32,
        });
        self.open_offset = self.cursor;
        self.until_boundary = self
            .block_size
            .map(|size| bytes_to_next_boundary(self.cursor, size));
    }
}

/// Bytes from `offset` to the next multiple of `block_size` strictly after it.
#[inline]
fn bytes_to_next_boundary(offset: u64, block_size: NonZeroU64) -> u64 {
    let size = block_size.get();
    size - offset % size
}

/// Fold segments that tile a contiguous file range into the single segment
/// covering that range.
///
/// Returns `None` if `segments` is empty or does not form a gapless,
/// non-overlapping, ascending tiling — a caller assembling a block from
/// several articles' segments uses that to detect an unclaimed block rather
/// than publishing a CRC derived from a broken tiling.
pub fn combine_contiguous(segments: &[Segment]) -> Option<Segment> {
    let (first, rest) = segments.split_first()?;
    let mut folded = *first;
    for segment in rest {
        if segment.file_offset != folded.end_offset() {
            return None;
        }
        folded.crc32 = crc32_combine(folded.crc32, segment.crc32, segment.len);
        folded.len += segment.len;
    }
    Some(folded)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift64* stream: every test input and every random
    /// matrix below is a pure function of its seed.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next_u64() % bound
        }

        fn range(&mut self, low: u64, high: u64) -> u64 {
            low + self.below(high - low + 1)
        }
    }

    fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut rng = Rng(seed | 1);
        (0..len).map(|_| (rng.next_u64() >> 33) as u8).collect()
    }

    fn direct(data: &[u8]) -> u32 {
        crc_fast::crc32_iso_hdlc(data)
    }

    /// Run one article's bytes through the checkpointing pass with a chunking
    /// schedule, returning the emitted segments.
    fn run_pass(
        data: &[u8],
        file_offset: u64,
        block_size: Option<NonZeroU64>,
        chunks: &[usize],
    ) -> Vec<Segment> {
        let mut pass = SegmentedCrc32::new(file_offset, block_size);
        let mut cursor = 0usize;
        let mut idx = 0usize;
        while cursor < data.len() {
            let want = if chunks.is_empty() {
                data.len()
            } else {
                chunks[idx % chunks.len()].max(1)
            };
            let end = (cursor + want).min(data.len());
            pass.update(&data[cursor..end]);
            cursor = end;
            idx += 1;
        }
        pass.finish()
    }

    #[test]
    fn empty_pass_emits_no_segments() {
        let pass = SegmentedCrc32::new(0, NonZeroU64::new(1024));
        assert_eq!(pass.finish(), Vec::new());
    }

    #[test]
    fn segments_tile_the_article_at_block_boundaries() {
        let data = random_bytes(0xa11ce, 5000);
        // Article starts 100 bytes into a 1024-byte block grid: the first
        // segment is short, the rest are block-sized until the tail.
        let segments = run_pass(&data, 1024 * 3 + 100, NonZeroU64::new(1024), &[97]);

        let offsets: Vec<(u64, u64)> = segments.iter().map(|s| (s.file_offset, s.len)).collect();
        assert_eq!(
            offsets,
            vec![
                (3172, 924),
                (4096, 1024),
                (5120, 1024),
                (6144, 1024),
                (7168, 1004),
            ]
        );
        for segment in &segments {
            let start = (segment.file_offset - 3172) as usize;
            let end = start + segment.len as usize;
            assert_eq!(segment.crc32, direct(&data[start..end]));
        }
    }

    #[test]
    fn no_block_size_emits_one_segment_per_article() {
        let data = random_bytes(0xb0b, 4096);
        let segments = run_pass(&data, 777, None, &[13, 900, 1]);
        assert_eq!(
            segments,
            vec![Segment {
                file_offset: 777,
                len: 4096,
                crc32: direct(&data),
            }]
        );
    }

    #[test]
    fn combine_contiguous_rejects_gaps_and_overlaps() {
        let a = Segment {
            file_offset: 0,
            len: 10,
            crc32: 0x1111_1111,
        };
        let gap = Segment {
            file_offset: 11,
            len: 10,
            crc32: 0x2222_2222,
        };
        let overlap = Segment {
            file_offset: 9,
            len: 10,
            crc32: 0x3333_3333,
        };
        assert_eq!(combine_contiguous(&[]), None);
        assert_eq!(combine_contiguous(&[a]), Some(a));
        assert_eq!(combine_contiguous(&[a, gap]), None);
        assert_eq!(combine_contiguous(&[a, overlap]), None);
    }

    #[test]
    fn zero_length_combine_is_the_identity() {
        let data = random_bytes(0xfeed, 512);
        let crc = direct(&data);
        assert_eq!(crc32_combine(crc, direct(&[]), 0), crc);
        assert_eq!(crc32_combine(direct(&[]), crc, data.len() as u64), crc);
    }

    /// Gate 1 + gate 2, exhaustively over a seeded random matrix: random file
    /// bytes x random block sizes x random article tilings (articles that
    /// straddle 0..3 boundaries, first/last block, short final block) x random
    /// chunk schedules.
    ///
    /// Every article's derived pcrc32 must equal its direct whole-article CRC,
    /// and every block's CRC derived from the segments tiling it must equal the
    /// direct CRC over that block's bytes.
    #[test]
    fn derived_block_and_article_crcs_match_direct_over_random_matrix() {
        let mut cases = 0usize;
        let mut blocks_checked = 0usize;
        let mut articles_checked = 0usize;
        let mut straddle_histogram = [0usize; 8];

        for seed in 0..64u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            // Block sizes deliberately include values far below and above the
            // article sizes so articles straddle zero, one, two and three-plus
            // boundaries within the same matrix.
            let block_size = match seed % 8 {
                0 => 1,
                1 => 7,
                2 => 64,
                3 => 256,
                4 => 1000,
                5 => 1024,
                6 => rng.range(2, 4096),
                _ => rng.range(2, 300),
            };
            let block_size = NonZeroU64::new(block_size).expect("non-zero");

            let file_len = rng.range(1, 20_000) as usize;
            let file = random_bytes(seed ^ 0xdead_beef, file_len);

            // Random article tiling of the whole file, so the first and last
            // blocks and the short final block are always covered.
            let mut article_bounds = vec![0usize];
            while *article_bounds.last().expect("seeded") < file_len {
                let start = *article_bounds.last().expect("seeded");
                let step = rng.range(1, 3000) as usize;
                article_bounds.push((start + step).min(file_len));
            }

            // Chunk schedule: how the driver hands decoded bytes to the pass.
            let chunks: Vec<usize> = (0..rng.range(1, 5))
                .map(|_| rng.range(1, 700) as usize)
                .collect();

            // Some articles decode before the block size is known.
            let pre_blocksize = rng.below(4) == 0;

            let mut all_segments: Vec<Segment> = Vec::new();
            for window in article_bounds.windows(2) {
                let (start, end) = (window[0], window[1]);
                let offset = start as u64;
                let article = &file[start..end];
                let known = !(pre_blocksize && rng.below(3) == 0);
                let segments = run_pass(
                    article,
                    offset,
                    known.then_some(block_size),
                    chunks.as_slice(),
                );

                // Gate 2: derived article pcrc32 == direct whole-article CRC.
                let derived = combine_contiguous(&segments).expect("article segments tile");
                assert_eq!(
                    derived.crc32,
                    direct(article),
                    "seed {seed} article [{start},{end}) block {block_size} derived pcrc32"
                );
                assert_eq!(derived.file_offset, offset);
                assert_eq!(derived.len, (end - start) as u64);

                if known {
                    let boundaries = segments.len() - 1;
                    straddle_histogram[boundaries.min(7)] += 1;
                }
                all_segments.extend_from_slice(&segments);
                articles_checked += 1;
            }

            // Segments must tile the file end to end before any block is
            // derived from them.
            let whole = combine_contiguous(&all_segments).expect("file segments tile");
            assert_eq!(whole.crc32, direct(&file), "seed {seed} whole-file fold");

            // Gate 1: every block whose segments tile it exactly.
            let size = block_size.get();
            let block_count = (file_len as u64).div_ceil(size);
            for block in 0..block_count {
                let start = block * size;
                let end = (start + size).min(file_len as u64);
                let tiling: Vec<Segment> = all_segments
                    .iter()
                    .copied()
                    .filter(|s| s.file_offset >= start && s.end_offset() <= end)
                    .collect();
                let Some(folded) = combine_contiguous(&tiling) else {
                    continue;
                };
                if folded.file_offset != start || folded.end_offset() != end {
                    // Boundary orphan: a pre-block-size article's segment
                    // crosses into this block, so it has no in-stream verdict.
                    continue;
                }
                assert_eq!(
                    folded.crc32,
                    direct(&file[start as usize..end as usize]),
                    "seed {seed} block {block} [{start},{end}) block size {block_size}"
                );
                blocks_checked += 1;
            }
            cases += 1;
        }

        // Non-vacuity: the matrix actually exercised the straddle classes the
        // gate names, rather than passing because nothing was checked. Printed
        // as well as asserted so a run's coverage is legible in the log instead
        // of having to be inferred from a green tick.
        eprintln!(
            "derived_block_and_article_crcs: {cases} cases, {articles_checked} articles, \
             {blocks_checked} blocks, articles by boundaries straddled {straddle_histogram:?}"
        );
        assert_eq!(cases, 64);
        assert!(
            articles_checked > 200,
            "articles checked {articles_checked}"
        );
        assert!(blocks_checked > 500, "blocks checked {blocks_checked}");
        for straddled in 0..4usize {
            assert!(
                straddle_histogram[straddled] > 0,
                "no article straddling {straddled} boundaries; histogram {straddle_histogram:?}"
            );
        }
    }

    /// Checkpoint placement is a function of file offsets only: the same
    /// article fed with wildly different chunk schedules must produce
    /// byte-identical segment records.
    #[test]
    fn segments_are_invariant_to_chunking() {
        for seed in 0..24u64 {
            let mut rng = Rng(seed | 1);
            let len = rng.range(1, 9000) as usize;
            let data = random_bytes(seed ^ 0x5eed, len);
            let offset = rng.range(0, 5000);
            let block_size = NonZeroU64::new(rng.range(1, 2048));

            let reference = run_pass(&data, offset, block_size, &[]);
            for schedule in [
                vec![1usize],
                vec![2],
                vec![3, 1, 255, 256, 257],
                vec![64],
                vec![255],
                vec![256],
                vec![1000, 1],
                vec![4096, 7],
                vec![len.max(1)],
            ] {
                assert_eq!(
                    run_pass(&data, offset, block_size, &schedule),
                    reference,
                    "seed {seed} schedule {schedule:?}"
                );
            }
        }
    }

    /// Every split point of a small article, at every offset within a small
    /// block grid — the dense counterpart to the random sweep above.
    #[test]
    fn every_split_point_and_offset_agrees_with_direct() {
        let data = random_bytes(0xc0ffee, 300);
        for block_size in [1u64, 2, 3, 7, 16, 64, 128, 299, 300, 301, 4096] {
            let block_size = NonZeroU64::new(block_size).expect("non-zero");
            for offset in [0u64, 1, 5, 63, 64, 65, 127, 300, 4095] {
                let reference = run_pass(&data, offset, Some(block_size), &[]);
                assert_eq!(
                    combine_contiguous(&reference).expect("tiles").crc32,
                    direct(&data),
                    "block {block_size} offset {offset}"
                );
                for split in 0..=data.len() {
                    let mut pass = SegmentedCrc32::new(offset, Some(block_size));
                    pass.update(&data[..split]);
                    pass.update(&data[split..]);
                    assert_eq!(
                        pass.finish(),
                        reference,
                        "block {block_size} offset {offset} split {split}"
                    );
                }
            }
        }
    }

    /// A checkpoint must be able to cut a segment while the x86 folded-streak
    /// path is carrying state, and while it is not. Sizes straddle the
    /// `VPCLMUL_MIN_UPDATE` threshold in both directions so the cut lands on a
    /// pending folded streak, on a digest-path streak, and on the hand-off.
    #[test]
    fn checkpoint_cuts_across_folded_streak_states() {
        let data = random_bytes(0x51ea4, 64 * 1024);
        let schedules: [&[usize]; 6] = [
            &[255, 1, 256, 300, 4096, 7],
            &[4096, 4096, 1],
            &[256, 256, 256],
            &[1, 1, 1, 4096, 1],
            &[300, 255, 256, 257],
            &[8192, 3, 8192],
        ];
        for block_size in [1u64, 255, 256, 257, 1024, 4096, 12_345] {
            let block_size = NonZeroU64::new(block_size).expect("non-zero");
            for schedule in schedules {
                let total: usize = schedule.iter().sum::<usize>().min(data.len());
                let article = &data[..total];
                let segments = run_pass(article, 0, Some(block_size), schedule);
                for segment in &segments {
                    let start = segment.file_offset as usize;
                    let end = segment.end_offset() as usize;
                    assert_eq!(
                        segment.crc32,
                        direct(&article[start..end]),
                        "block {block_size} schedule {schedule:?} segment [{start},{end})"
                    );
                }
                assert_eq!(
                    combine_contiguous(&segments).expect("tiles").crc32,
                    direct(article),
                    "block {block_size} schedule {schedule:?}"
                );
            }
        }
    }

    /// The checkpoint primitive on the raw CRC state: a cut must restart from
    /// the CRC init state, not carry the closed segment's value forward.
    #[test]
    fn crc_checkpoint_restarts_from_init_state() {
        let data = random_bytes(0x1234_5678, 40_000);
        for cut in [0usize, 1, 255, 256, 257, 1024, 9999, 40_000] {
            let mut crc = Crc32::new();
            crc.update(&data[..cut]);
            assert_eq!(crc.checkpoint(), direct(&data[..cut]), "cut {cut}");
            crc.update(&data[cut..]);
            assert_eq!(crc.finalize(), direct(&data[cut..]), "tail after cut {cut}");
        }
    }
}
