// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Streaming CRC32 for yEnc decode, and the combine that composes segment
// CRCs, both over `crc-turbo`.

const ALGORITHM: crc_turbo::CrcAlgorithm = crc_turbo::CrcAlgorithm::Crc32IsoHdlc;

// A streaming CRC-32/ISO-HDLC.
#[derive(Clone)]
pub struct Crc32 {
    hasher: crc_turbo::Digest,
}

impl Crc32 {
    pub fn new() -> Self {
        Self {
            hasher: crc_turbo::Digest::new(ALGORITHM),
        }
    }

    // Feed a chunk of decoded bytes into the hasher.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    // Finalize and return the CRC32 value. Consumes the hasher.
    pub fn finalize(self) -> u32 {
        self.current()
    }

    // The CRC32 so far, without consuming this wrapper.
    pub fn current(&self) -> u32 {
        self.hasher.finalize() as u32
    }

    // Return the CRC32 of everything fed since the last checkpoint and restart
    // from the initial state, so the next `update` begins a fresh segment.
    //
    // This is the segment-CRC checkpoint primitive: the returned value is a
    // standalone CRC32 over the bytes of the closed segment (standard
    // init/finalize, not a running prefix), which is what makes segments
    // composable with [`crc32_combine`] in any tiling, including block
    // tilings that straddle article boundaries.
    pub fn checkpoint(&mut self) -> u32 {
        self.hasher.finalize_reset() as u32
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Crc32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crc32").finish_non_exhaustive()
    }
}

// Combine two CRC32 values as if their byte ranges were concatenated:
// given `crc_a` over `A`, `crc_b` over `B` and `len_b == B.len()`, returns the
// CRC32 of `A || B`.
//
// `len_b == 0` is the identity on well-formed input: the result is
// `crc_a ^ crc_b`, and the CRC32 of an empty range is 0, hence
// `crc32_combine(a, 0, 0) == a`. `par2_rs::checksum::Crc32CombineOp`
// short-circuits to `a` for any `crc_b` at that length; the two differ only
// on a zero-length record carrying a non-zero CRC, which is malformed and
// unreachable from [`crate::segment::SegmentedCrc32`], which never emits
// zero-length segments. The divergence is pinned by
// `zero_length_combine_agrees_on_well_formed_input_only` in
// `tests/segment_combine.rs`.
//
// Repeated combines over ranges of one length should build a
// [`Crc32Combine`] once and reuse it; that skips the square-and-multiply.
#[inline]
pub fn crc32_combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    crc_turbo::checksum_combine(ALGORITHM, u64::from(crc_a), u64::from(crc_b), len_b) as u32
}

// A CRC32 combine operator for a fixed suffix length: `x^(8*len_b) mod P`,
// computed once, so that combining is a single polynomial multiply.
//
// Segment cuts on a fixed checkpoint stride, and articles of one poster's
// size, combine ranges of the same length over and over; building the
// operator once per length is what keeps those at tens of nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Crc32Combine {
    op: crc_turbo::CrcCombine,
}

impl Crc32Combine {
    // Build the operator for a suffix of `len_b` bytes.
    #[inline]
    pub fn new(len_b: u64) -> Self {
        Self {
            op: crc_turbo::CrcCombine::new(ALGORITHM, len_b),
        }
    }

    // CRC32 of `A || B` from `crc_a` over `A` and `crc_b` over the suffix
    // `B` this operator was built for.
    #[inline]
    pub fn combine(&self, crc_a: u32, crc_b: u32) -> u32 {
        self.op.combine(u64::from(crc_a), u64::from(crc_b)) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::Crc32;

    #[test]
    fn combine_over_real_bytes_matches_a_single_pass() {
        let data: Vec<u8> = (0..300_007u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let whole = crc_turbo::crc32_iso_hdlc(&data);
        for split in [
            0usize,
            1,
            2,
            3,
            64,
            4096,
            65_535,
            150_000,
            data.len() - 1,
            data.len(),
        ] {
            let (a, b) = data.split_at(split);
            let combined = super::crc32_combine(
                crc_turbo::crc32_iso_hdlc(a),
                crc_turbo::crc32_iso_hdlc(b),
                b.len() as u64,
            );
            assert_eq!(combined, whole, "split={split}");
        }
    }

    #[test]
    fn zero_length_suffix_is_the_identity() {
        assert_eq!(super::crc32_combine(0xdead_beef, 0, 0), 0xdead_beef);
        assert_eq!(
            super::Crc32Combine::new(0).combine(0x1234_5678, 0),
            0x1234_5678
        );
        // Xor semantics on a malformed zero-length record, kept.
        assert_eq!(
            super::crc32_combine(0xdead_beef, 0x11, 0),
            0xdead_beef ^ 0x11
        );
    }

    #[test]
    fn crc32_empty() {
        let crc = Crc32::new();
        assert_eq!(crc.finalize(), 0);
    }

    #[test]
    fn crc32_known_value() {
        // CRC32 of "123456789" is 0xCBF43926 (standard test vector).
        let mut crc = Crc32::new();
        crc.update(b"123456789");
        assert_eq!(crc.finalize(), 0xCBF43926);
    }

    #[test]
    fn crc32_streaming() {
        // Feeding data in chunks should produce the same result.
        let mut crc_one_shot = Crc32::new();
        crc_one_shot.update(b"123456789");
        let result_one = crc_one_shot.finalize();

        let mut crc_chunked = Crc32::new();
        crc_chunked.update(b"1234");
        crc_chunked.update(b"56789");
        let result_chunked = crc_chunked.finalize();

        assert_eq!(result_one, result_chunked);
    }

    #[test]
    fn crc32_single_byte_chunks() {
        let mut crc = Crc32::new();
        for &b in b"123456789" {
            crc.update(&[b]);
        }
        assert_eq!(crc.finalize(), 0xCBF43926);
    }

    #[test]
    fn crc32_matches_crc_turbo_across_splits() {
        let mut data = Vec::with_capacity(8192);
        let mut seed = 0x1234_5678u32;
        for _ in 0..8192 {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            data.push((seed >> 24) as u8);
        }

        for len in [
            0usize, 1, 2, 7, 31, 32, 63, 64, 127, 128, 255, 256, 257, 511, 512, 1024, 4095, 8192,
        ] {
            let input = &data[..len];
            let expected = crc_turbo::crc32_iso_hdlc(input);

            let mut one_shot = Crc32::new();
            one_shot.update(input);
            assert_eq!(one_shot.finalize(), expected, "one-shot len {len}");

            for split in [0usize, 1, 3, 17, 63, 127, 255, 256, 511, 1024] {
                let split = split.min(len);
                let mut chunked = Crc32::new();
                chunked.update(&input[..split]);
                chunked.update(&input[split..]);
                assert_eq!(chunked.finalize(), expected, "len {len} split {split}");
            }
        }
    }

    #[test]
    fn crc32_mixed_size_interleaved_updates() {
        const LEN: usize = 32 * 1024;
        let mut data = Vec::with_capacity(LEN);
        let mut seed = 0x9e37_79b9u32;
        for _ in 0..LEN {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            data.push((seed >> 24) as u8);
        }

        // Sizes straddle the kernels' block sizes in both directions, so each
        // sequence crosses between the wide fold and the tail path.
        let sequences: [&[usize]; 6] = [
            &[1, 64, 255, 256, 300, 4096, 7],
            &[4096, 7, 256, 1, 300, 255, 64],
            &[256, 256, 256, 1, 1, 1, 4096],
            &[7, 7, 7, 300, 7, 4096, 255, 256],
            &[300, 1, 4096, 64, 256, 255, 7, 256],
            &[4096, 4096, 1, 4096, 255, 300, 256],
        ];

        for seq in sequences {
            let total: usize = seq.iter().sum();
            assert!(total <= LEN, "sequence {seq:?} exceeds fixture");

            let mut crc = Crc32::new();
            let mut offset = 0usize;
            for &len in seq {
                crc.update(&data[offset..offset + len]);
                offset += len;
                // Every prefix must match a one-shot reference, so a bad carry
                // is caught at the update that introduced it.
                assert_eq!(
                    crc.current(),
                    crc_turbo::crc32_iso_hdlc(&data[..offset]),
                    "sequence {seq:?} prefix {offset}"
                );
            }

            assert_eq!(
                crc.finalize(),
                crc_turbo::crc32_iso_hdlc(&data[..total]),
                "sequence {seq:?}"
            );
        }
    }

    #[test]
    fn crc32_checkpoint_cuts_and_restarts_at_every_streak_state() {
        const LEN: usize = 48 * 1024;
        let mut data = Vec::with_capacity(LEN);
        let mut seed = 0x2545_f491u32;
        for _ in 0..LEN {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            data.push((seed >> 24) as u8);
        }

        // Each sequence crosses the kernels' block sizes so checkpoints land
        // on every streak shape.
        let sequences: [&[usize]; 6] = [
            &[4096, 300, 255, 1, 256, 7],
            &[255, 256, 257, 4096],
            &[1, 1, 8192, 1],
            &[256, 256, 256, 256],
            &[7, 300, 7, 4096, 255],
            &[8192, 8192, 3],
        ];

        for seq in sequences {
            // Checkpoint after every prefix of the sequence: each cut must
            // return the CRC of the bytes since the previous cut, and the CRC
            // must restart from the init state rather than carry that value.
            for cut_after in 0..seq.len() {
                let mut crc = Crc32::new();
                let mut offset = 0usize;
                let mut segment_start = 0usize;
                let mut cut = false;
                for (idx, &len) in seq.iter().enumerate() {
                    crc.update(&data[offset..offset + len]);
                    offset += len;
                    if idx == cut_after {
                        assert_eq!(
                            crc.checkpoint(),
                            crc_turbo::crc32_iso_hdlc(&data[segment_start..offset]),
                            "seq {seq:?} cut after {cut_after} segment [{segment_start},{offset})"
                        );
                        segment_start = offset;
                        cut = true;
                    }
                }
                assert!(cut, "seq {seq:?} never cut");
                assert_eq!(
                    crc.finalize(),
                    crc_turbo::crc32_iso_hdlc(&data[segment_start..offset]),
                    "seq {seq:?} cut after {cut_after} tail [{segment_start},{offset})"
                );
            }
        }
    }
}
