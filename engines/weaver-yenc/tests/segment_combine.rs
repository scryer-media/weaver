//! Pins the CRC32 combine this crate exposes bit-identical to
//! `par2_rs::checksum::Crc32CombineOp` — the combine weaver's fused pipeline
//! already uses to compose part CRCs into a completed-file CRC without
//! re-reading the file.
//!
//! D2 requires one combine, not three. `weaver_yenc::crc32_combine` forwards to
//! `crc-fast`'s `checksum_combine` (already in this crate's dependency set,
//! same Mark Adler GF(2) zeros-operator construction) rather than pulling the
//! PAR2 library down into the yEnc codec. This differential is what makes that
//! substitution safe: the segments produced here are consumed by the evidence
//! collector, which folds them with `Crc32CombineOp`, so the two must agree on
//! every input, not merely be "both correct by construction".
//!
//! par2-rs is a dev-dependency only; nothing in the shipped crate links it.

use std::num::NonZeroU64;

use par2_rs::checksum::{Crc32CombineOp, crc32 as par2_crc32};
use weaver_yenc::segment::{SegmentedCrc32, combine_contiguous};
use weaver_yenc::{Segment, crc32_combine};

/// Deterministic xorshift64* stream.
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
}

fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng(seed | 1);
    (0..len).map(|_| (rng.next_u64() >> 33) as u8).collect()
}

#[test]
fn combine_matches_par2_rs_combine_op() {
    let mut rng = Rng(0x0d2_9001);
    let mut checked = 0usize;

    // Fixed lengths pin the small cases; random ones sweep the zeros-operator's
    // bit decomposition of `len2`.
    let mut lengths: Vec<u64> = vec![1, 2, 3, 7, 8, 63, 64, 255, 256, 257, 1023, 1 << 20];
    lengths.extend((0..64).map(|_| 1 + rng.below(1 << 24)));

    for len_b in lengths {
        let op = Crc32CombineOp::new(len_b);
        for _ in 0..32 {
            let crc_a = rng.next_u64() as u32;
            let crc_b = rng.next_u64() as u32;
            assert_eq!(
                crc32_combine(crc_a, crc_b, len_b),
                op.combine(crc_a, crc_b),
                "crc_a {crc_a:08x} crc_b {crc_b:08x} len_b {len_b}"
            );
            checked += 1;
        }
    }

    eprintln!("combine_matches_par2_rs_combine_op: {checked} (crc_a, crc_b, len_b) triples");
    assert!(checked >= 2_400, "combine pairs checked {checked}");
}

/// The one input on which the two combines disagree, pinned so the difference
/// is a recorded fact rather than a latent surprise for the D2-P2 collector.
///
/// `Crc32CombineOp::new(0)` short-circuits to `crc1`, discarding `crc2`;
/// `crc32_combine(a, b, 0)` returns `a ^ b`. The CRC32 of an empty range is 0,
/// so on every *well-formed* zero-length input the two agree and both are the
/// identity. They can only differ on a zero-length record carrying a non-zero
/// CRC, which is malformed. `SegmentedCrc32` never emits a zero-length segment
/// (`checkpoint` is a no-op with nothing fed), so the collector cannot reach
/// this from decoder output — but a hand-built record could, and the xor
/// semantics would then expose the bug that par2-rs's short-circuit hides.
#[test]
fn zero_length_combine_agrees_on_well_formed_input_only() {
    let empty_crc = par2_crc32(&[]);
    assert_eq!(empty_crc, 0);

    let mut rng = Rng(0x2e40_1e46);
    for _ in 0..256 {
        let crc_a = rng.next_u64() as u32;
        assert_eq!(crc32_combine(crc_a, empty_crc, 0), crc_a);
        assert_eq!(Crc32CombineOp::new(0).combine(crc_a, empty_crc), crc_a);

        // Malformed: zero length, non-zero CRC. Documented divergence.
        let bogus = (rng.next_u64() as u32) | 1;
        assert_eq!(crc32_combine(crc_a, bogus, 0), crc_a ^ bogus);
        assert_eq!(Crc32CombineOp::new(0).combine(crc_a, bogus), crc_a);
    }
}

/// The whole D2 derivation, end to end, with par2-rs as the oracle on both
/// sides: segment CRCs against `par2_rs::checksum::crc32`, and the block/article
/// folds against `Crc32CombineOp`.
#[test]
fn derived_block_crcs_match_par2_rs_over_an_article_tiling() {
    let mut rng = Rng(0x000b_10c5);
    let mut blocks_checked = 0usize;

    for case in 0..24u64 {
        let block_size = NonZeroU64::new(1 + rng.below(2048)).expect("non-zero");
        let file_len = (1 + rng.below(30_000)) as usize;
        let file = random_bytes(case ^ 0xa5a5_5a5a, file_len);

        // Random article tiling of the file.
        let mut bounds = vec![0usize];
        while *bounds.last().expect("seeded") < file_len {
            let start = *bounds.last().expect("seeded");
            let step = (1 + rng.below(4000)) as usize;
            bounds.push((start + step).min(file_len));
        }

        let mut segments: Vec<Segment> = Vec::new();
        for window in bounds.windows(2) {
            let (start, end) = (window[0], window[1]);
            let mut pass = SegmentedCrc32::new(start as u64, Some(block_size));
            let mut cursor = start;
            while cursor < end {
                let chunk = (1 + rng.below(900)) as usize;
                let stop = (cursor + chunk).min(end);
                pass.update(&file[cursor..stop]);
                cursor = stop;
            }
            let article = pass.finish();

            // Segment CRCs against par2-rs's own CRC32.
            for segment in &article {
                let lo = segment.file_offset as usize;
                let hi = segment.end_offset() as usize;
                assert_eq!(segment.crc32, par2_crc32(&file[lo..hi]));
            }

            // Article pcrc32 folded with par2-rs's combine must match ours.
            let ours = combine_contiguous(&article).expect("tiles").crc32;
            let theirs = article.iter().fold(0u32, |acc, segment| {
                Crc32CombineOp::new(segment.len).combine(acc, segment.crc32)
            });
            assert_eq!(ours, theirs, "case {case} article [{start},{end})");
            assert_eq!(ours, par2_crc32(&file[start..end]));

            segments.extend_from_slice(&article);
        }

        // Blocks: segments cut at block boundaries tile every block exactly.
        let size = block_size.get();
        for block in 0..(file_len as u64).div_ceil(size) {
            let lo = block * size;
            let hi = (lo + size).min(file_len as u64);
            let tiling: Vec<Segment> = segments
                .iter()
                .copied()
                .filter(|s| s.file_offset >= lo && s.end_offset() <= hi)
                .collect();
            let folded = combine_contiguous(&tiling).expect("block tiles");
            assert_eq!(folded.file_offset, lo);
            assert_eq!(folded.end_offset(), hi);
            assert_eq!(
                folded.crc32,
                par2_crc32(&file[lo as usize..hi as usize]),
                "case {case} block {block} [{lo},{hi}) block size {block_size}"
            );
            blocks_checked += 1;
        }
    }

    assert!(blocks_checked > 500, "blocks checked {blocks_checked}");
}
