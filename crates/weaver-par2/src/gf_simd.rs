//! SIMD-accelerated GF(2^16) region operations.
//!
//! Provides a bulk multiply-accumulate operation over byte regions interpreted
//! as little-endian u16 words in GF(2^16):
//!
//! ```text
//! dst[i] ^= gf_mul(src[i], factor)    for each u16 word i
//! ```
//!
//! Uses the **split-nibble shuffle** algorithm:
//!
//! 1. Precompute 8 tables of 16 bytes each. Each table maps a 4-bit nibble of
//!    the input to its contribution to one byte of the 16-bit product.
//! 2. Deinterleave input bytes (separate lo/hi bytes of each u16 word).
//! 3. For each nibble, do a PSHUFB/VTBL lookup. XOR the 4 contributions for
//!    the result low byte and the 4 for the result high byte.
//! 4. Reinterleave and XOR-accumulate into the destination.

use crate::gf;

/// Precomputed shuffle tables for a single GF(2^16) multiplication factor.
///
/// For a given `factor`, multiplying an arbitrary 16-bit input `x` by `factor`
/// can be decomposed as:
///
/// ```text
/// factor * x = factor*n0 ^ factor*(n1*16) ^ factor*(n2*256) ^ factor*(n3*4096)
/// ```
///
/// where n0..n3 are the 4 nibbles of x. Each partial product is a 16-bit value
/// with only 16 possible values (one per nibble value 0..15).
///
/// We store these as 8 byte-tables: for each nibble position (4), we store the
/// low byte and high byte of the partial product separately. This maps directly
/// to PSHUFB / VTBL which operate on bytes.
#[derive(Clone)]
pub struct MulTables {
    /// tables[0]: low byte of result contribution from nibble 0 (bits 0-3 of input low byte)
    /// tables[1]: high byte of result contribution from nibble 0
    /// tables[2]: low byte of result contribution from nibble 1 (bits 4-7 of input low byte)
    /// tables[3]: high byte of result contribution from nibble 1
    /// tables[4]: low byte of result contribution from nibble 2 (bits 0-3 of input high byte)
    /// tables[5]: high byte of result contribution from nibble 2
    /// tables[6]: low byte of result contribution from nibble 3 (bits 4-7 of input high byte)
    /// tables[7]: high byte of result contribution from nibble 3
    pub tables: [[u8; 16]; 8],
    /// The original factor, stored for scalar tail processing.
    pub factor: u16,
}

/// Precompute the 8 shuffle tables for a given GF(2^16) multiplication factor.
pub fn precompute_mul_tables(factor: u16) -> MulTables {
    let mut tables = [[0u8; 16]; 8];

    for nibble_val in 0u16..16 {
        // Nibble 0: bits 0-3 of low byte → value is nibble_val
        let prod0 = gf::mul(factor, nibble_val);
        tables[0][nibble_val as usize] = prod0 as u8;
        tables[1][nibble_val as usize] = (prod0 >> 8) as u8;

        // Nibble 1: bits 4-7 of low byte → value is nibble_val << 4
        let prod1 = gf::mul(factor, nibble_val << 4);
        tables[2][nibble_val as usize] = prod1 as u8;
        tables[3][nibble_val as usize] = (prod1 >> 8) as u8;

        // Nibble 2: bits 0-3 of high byte → value is nibble_val << 8
        let prod2 = gf::mul(factor, nibble_val << 8);
        tables[4][nibble_val as usize] = prod2 as u8;
        tables[5][nibble_val as usize] = (prod2 >> 8) as u8;

        // Nibble 3: bits 4-7 of high byte → value is nibble_val << 12
        let prod3 = gf::mul(factor, nibble_val << 12);
        tables[6][nibble_val as usize] = prod3 as u8;
        tables[7][nibble_val as usize] = (prod3 >> 8) as u8;
    }

    MulTables { tables, factor }
}

/// Multiply each u16 word in `src` by `factor` in GF(2^16) and XOR-accumulate
/// into `dst`.
///
/// Both slices are byte slices interpreted as little-endian u16 words.
/// They must have the same length and that length must be even.
///
/// # Panics
///
/// Panics if `src.len() != dst.len()` or if the length is odd.
#[inline]
pub fn mul_acc_region(factor: u16, src: &[u8], dst: &mut [u8]) {
    assert_eq!(src.len(), dst.len(), "src and dst must have equal length");
    assert!(src.len().is_multiple_of(2), "region length must be even");

    if factor == 0 || src.is_empty() {
        return;
    }

    if factor == 1 {
        // factor=1 is just XOR.
        for (d, s) in dst.iter_mut().zip(src.iter()) {
            *d ^= *s;
        }
        return;
    }

    let tables = precompute_mul_tables(factor);

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { mul_acc_region_avx2(&tables, src, dst) };
            return;
        }
        if is_x86_feature_detected!("ssse3") {
            unsafe { mul_acc_region_ssse3(&tables, src, dst) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { mul_acc_region_neon(&tables, src, dst) };
        return;
    }

    #[allow(unreachable_code)]
    mul_acc_region_scalar(factor, src, dst);
}

/// Scalar fallback: one word at a time using gf::mul + gf::add.
fn mul_acc_region_scalar(factor: u16, src: &[u8], dst: &mut [u8]) {
    let word_count = src.len() / 2;
    for w in 0..word_count {
        let s = u16::from_le_bytes([src[w * 2], src[w * 2 + 1]]);
        let d = u16::from_le_bytes([dst[w * 2], dst[w * 2 + 1]]);
        let result = gf::add(d, gf::mul(s, factor));
        let bytes = result.to_le_bytes();
        dst[w * 2] = bytes[0];
        dst[w * 2 + 1] = bytes[1];
    }
}

// ---------------------------------------------------------------------------
// SSSE3 kernel: 16 bytes (8 GF elements) per iteration
//
// Algorithm:
//   1. Deinterleave: separate lo bytes and hi bytes of each u16 word
//   2. Extract 4 nibbles (2 per byte group)
//   3. 8× PSHUFB lookups (4 nibbles × {result_lo, result_hi})
//   4. XOR contributions together
//   5. Reinterleave result lo/hi bytes
//   6. XOR-accumulate into dst
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn mul_acc_region_ssse3(tables: &MulTables, src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        let mask_0f = _mm_set1_epi8(0x0F);

        // Load the 8 shuffle tables into registers.
        let t0 = _mm_loadu_si128(tables.tables[0].as_ptr() as *const __m128i);
        let t1 = _mm_loadu_si128(tables.tables[1].as_ptr() as *const __m128i);
        let t2 = _mm_loadu_si128(tables.tables[2].as_ptr() as *const __m128i);
        let t3 = _mm_loadu_si128(tables.tables[3].as_ptr() as *const __m128i);
        let t4 = _mm_loadu_si128(tables.tables[4].as_ptr() as *const __m128i);
        let t5 = _mm_loadu_si128(tables.tables[5].as_ptr() as *const __m128i);
        let t6 = _mm_loadu_si128(tables.tables[6].as_ptr() as *const __m128i);
        let t7 = _mm_loadu_si128(tables.tables[7].as_ptr() as *const __m128i);

        // Deinterleave masks: extract even-position (lo) and odd-position (hi) bytes.
        // Input: [lo0, hi0, lo1, hi1, ..., lo7, hi7]
        // deint_lo → positions 0-7 = lo bytes, positions 8-15 = zeroed (high bit set → 0)
        // deint_hi → positions 0-7 = hi bytes, positions 8-15 = zeroed
        let deint_lo = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0);
        let deint_hi = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1);

        while offset + 16 <= len {
            let s = _mm_loadu_si128(src.as_ptr().add(offset) as *const __m128i);
            let d = _mm_loadu_si128(dst.as_ptr().add(offset) as *const __m128i);

            // Deinterleave input bytes.
            let lo_bytes = _mm_shuffle_epi8(s, deint_lo); // lo bytes at 0-7
            let hi_bytes = _mm_shuffle_epi8(s, deint_hi); // hi bytes at 0-7

            // Extract nibbles.
            let lo_n0 = _mm_and_si128(lo_bytes, mask_0f);
            let lo_n1 = _mm_and_si128(_mm_srli_epi16(lo_bytes, 4), mask_0f);
            let hi_n0 = _mm_and_si128(hi_bytes, mask_0f);
            let hi_n1 = _mm_and_si128(_mm_srli_epi16(hi_bytes, 4), mask_0f);

            // 8 lookups: each nibble contributes to both result lo and hi bytes.
            let p0_lo = _mm_shuffle_epi8(t0, lo_n0);
            let p0_hi = _mm_shuffle_epi8(t1, lo_n0);
            let p1_lo = _mm_shuffle_epi8(t2, lo_n1);
            let p1_hi = _mm_shuffle_epi8(t3, lo_n1);
            let p2_lo = _mm_shuffle_epi8(t4, hi_n0);
            let p2_hi = _mm_shuffle_epi8(t5, hi_n0);
            let p3_lo = _mm_shuffle_epi8(t6, hi_n1);
            let p3_hi = _mm_shuffle_epi8(t7, hi_n1);

            // XOR contributions for result lo bytes and result hi bytes.
            let result_lo = _mm_xor_si128(_mm_xor_si128(p0_lo, p1_lo), _mm_xor_si128(p2_lo, p3_lo));
            let result_hi = _mm_xor_si128(_mm_xor_si128(p0_hi, p1_hi), _mm_xor_si128(p2_hi, p3_hi));

            // Reinterleave: [rlo0, rhi0, rlo1, rhi1, ..., rlo7, rhi7]
            let product = _mm_unpacklo_epi8(result_lo, result_hi);

            // XOR-accumulate.
            let result = _mm_xor_si128(d, product);
            _mm_storeu_si128(dst.as_mut_ptr().add(offset) as *mut __m128i, result);

            offset += 16;
        }
    }

    // Scalar tail.
    if offset < len {
        mul_acc_region_scalar(tables.factor, &src[offset..], &mut dst[offset..]);
    }
}

// ---------------------------------------------------------------------------
// AVX2 kernel: 32 bytes (16 GF elements) per iteration
//
// Same deinterleave algorithm as SSSE3, but VPSHUFB operates within each
// 128-bit lane independently — so the deinterleave and reinterleave work
// per-lane, producing correct results for 8 words per lane × 2 lanes.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn mul_acc_region_avx2(tables: &MulTables, src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        let mask_0f = _mm256_set1_epi8(0x0F);

        // Broadcast each 16-byte table into both 128-bit lanes.
        let t0 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[0].as_ptr() as *const __m128i
        ));
        let t1 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[1].as_ptr() as *const __m128i
        ));
        let t2 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[2].as_ptr() as *const __m128i
        ));
        let t3 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[3].as_ptr() as *const __m128i
        ));
        let t4 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[4].as_ptr() as *const __m128i
        ));
        let t5 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[5].as_ptr() as *const __m128i
        ));
        let t6 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[6].as_ptr() as *const __m128i
        ));
        let t7 = _mm256_broadcastsi128_si256(_mm_loadu_si128(
            tables.tables[7].as_ptr() as *const __m128i
        ));

        // Deinterleave masks (same pattern in each 128-bit lane).
        let deint_lo_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0);
        let deint_hi_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1);
        let deint_lo = _mm256_broadcastsi128_si256(deint_lo_128);
        let deint_hi = _mm256_broadcastsi128_si256(deint_hi_128);

        while offset + 32 <= len {
            let s = _mm256_loadu_si256(src.as_ptr().add(offset) as *const __m256i);
            let d = _mm256_loadu_si256(dst.as_ptr().add(offset) as *const __m256i);

            // Deinterleave within each 128-bit lane.
            let lo_bytes = _mm256_shuffle_epi8(s, deint_lo);
            let hi_bytes = _mm256_shuffle_epi8(s, deint_hi);

            // Extract nibbles.
            let lo_n0 = _mm256_and_si256(lo_bytes, mask_0f);
            let lo_n1 = _mm256_and_si256(_mm256_srli_epi16(lo_bytes, 4), mask_0f);
            let hi_n0 = _mm256_and_si256(hi_bytes, mask_0f);
            let hi_n1 = _mm256_and_si256(_mm256_srli_epi16(hi_bytes, 4), mask_0f);

            // 8 lookups.
            let p0_lo = _mm256_shuffle_epi8(t0, lo_n0);
            let p0_hi = _mm256_shuffle_epi8(t1, lo_n0);
            let p1_lo = _mm256_shuffle_epi8(t2, lo_n1);
            let p1_hi = _mm256_shuffle_epi8(t3, lo_n1);
            let p2_lo = _mm256_shuffle_epi8(t4, hi_n0);
            let p2_hi = _mm256_shuffle_epi8(t5, hi_n0);
            let p3_lo = _mm256_shuffle_epi8(t6, hi_n1);
            let p3_hi = _mm256_shuffle_epi8(t7, hi_n1);

            // XOR contributions.
            let result_lo = _mm256_xor_si256(
                _mm256_xor_si256(p0_lo, p1_lo),
                _mm256_xor_si256(p2_lo, p3_lo),
            );
            let result_hi = _mm256_xor_si256(
                _mm256_xor_si256(p0_hi, p1_hi),
                _mm256_xor_si256(p2_hi, p3_hi),
            );

            // Reinterleave within each lane.
            let product = _mm256_unpacklo_epi8(result_lo, result_hi);

            // XOR-accumulate.
            let result = _mm256_xor_si256(d, product);
            _mm256_storeu_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, result);

            offset += 32;
        }
    }

    // Tail: fall through to SSSE3 for remaining 16-byte-aligned chunk + scalar.
    if offset < len {
        unsafe { mul_acc_region_ssse3(tables, &src[offset..], &mut dst[offset..]) };
    }
}

// ---------------------------------------------------------------------------
// NEON kernel (aarch64): 16 bytes (8 GF elements) per iteration
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
unsafe fn mul_acc_region_neon(tables: &MulTables, src: &[u8], dst: &mut [u8]) {
    use std::arch::aarch64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        let mask_0f = vdupq_n_u8(0x0F);

        let t0 = vld1q_u8(tables.tables[0].as_ptr());
        let t1 = vld1q_u8(tables.tables[1].as_ptr());
        let t2 = vld1q_u8(tables.tables[2].as_ptr());
        let t3 = vld1q_u8(tables.tables[3].as_ptr());
        let t4 = vld1q_u8(tables.tables[4].as_ptr());
        let t5 = vld1q_u8(tables.tables[5].as_ptr());
        let t6 = vld1q_u8(tables.tables[6].as_ptr());
        let t7 = vld1q_u8(tables.tables[7].as_ptr());

        while offset + 16 <= len {
            let s = vld1q_u8(src.as_ptr().add(offset));
            let d = vld1q_u8(dst.as_ptr().add(offset));

            // Deinterleave: separate lo bytes (even positions) and hi bytes (odd).
            let lo_bytes = vuzp1q_u8(s, s); // positions 0-7: lo bytes of 8 words
            let hi_bytes = vuzp2q_u8(s, s); // positions 0-7: hi bytes of 8 words

            // Extract nibbles.
            let lo_n0 = vandq_u8(lo_bytes, mask_0f);
            let lo_n1 = vandq_u8(vshrq_n_u8(lo_bytes, 4), mask_0f);
            let hi_n0 = vandq_u8(hi_bytes, mask_0f);
            let hi_n1 = vandq_u8(vshrq_n_u8(hi_bytes, 4), mask_0f);

            // 8 lookups.
            let p0_lo = vqtbl1q_u8(t0, lo_n0);
            let p0_hi = vqtbl1q_u8(t1, lo_n0);
            let p1_lo = vqtbl1q_u8(t2, lo_n1);
            let p1_hi = vqtbl1q_u8(t3, lo_n1);
            let p2_lo = vqtbl1q_u8(t4, hi_n0);
            let p2_hi = vqtbl1q_u8(t5, hi_n0);
            let p3_lo = vqtbl1q_u8(t6, hi_n1);
            let p3_hi = vqtbl1q_u8(t7, hi_n1);

            // XOR contributions.
            let result_lo = veorq_u8(veorq_u8(p0_lo, p1_lo), veorq_u8(p2_lo, p3_lo));
            let result_hi = veorq_u8(veorq_u8(p0_hi, p1_hi), veorq_u8(p2_hi, p3_hi));

            // Reinterleave: [rlo0, rhi0, rlo1, rhi1, ...]
            let product = vzip1q_u8(result_lo, result_hi);

            // XOR-accumulate.
            let result = veorq_u8(d, product);
            vst1q_u8(dst.as_mut_ptr().add(offset), result);

            offset += 16;
        }
    }

    // Scalar tail.
    if offset < len {
        mul_acc_region_scalar(tables.factor, &src[offset..], &mut dst[offset..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_matches_gf_mul_add() {
        let factor = 0x1234u16;
        let src: Vec<u8> = (0..64).collect();
        let mut dst_scalar = vec![0xABu8; 64];
        let mut dst_reference = dst_scalar.clone();

        // Reference: manual gf::mul + gf::add
        let word_count = src.len() / 2;
        for w in 0..word_count {
            let s = u16::from_le_bytes([src[w * 2], src[w * 2 + 1]]);
            let d = u16::from_le_bytes([dst_reference[w * 2], dst_reference[w * 2 + 1]]);
            let result = gf::add(d, gf::mul(s, factor));
            let bytes = result.to_le_bytes();
            dst_reference[w * 2] = bytes[0];
            dst_reference[w * 2 + 1] = bytes[1];
        }

        mul_acc_region_scalar(factor, &src, &mut dst_scalar);
        assert_eq!(dst_scalar, dst_reference);
    }

    #[test]
    fn mul_acc_region_factor_zero() {
        let src = vec![0xFF; 32];
        let mut dst = vec![0x42; 32];
        let original = dst.clone();
        mul_acc_region(0, &src, &mut dst);
        assert_eq!(dst, original, "factor=0 should be a no-op");
    }

    #[test]
    fn mul_acc_region_factor_one() {
        let src: Vec<u8> = (0..32).collect();
        let mut dst = vec![0; 32];
        mul_acc_region(1, &src, &mut dst);
        assert_eq!(dst, src, "factor=1 should XOR src into dst");
    }

    #[test]
    fn dispatched_matches_scalar_all_factors() {
        // Test a sweep of factor values.
        let src: Vec<u8> = (0..32).collect();

        for factor in (0..=0xFFFFu16).step_by(257) {
            let mut dst_dispatched = vec![0xCDu8; 32];
            let mut dst_scalar = dst_dispatched.clone();

            mul_acc_region(factor, &src, &mut dst_dispatched);
            if factor == 0 {
                assert_eq!(dst_dispatched, dst_scalar);
                continue;
            }
            mul_acc_region_scalar(factor, &src, &mut dst_scalar);
            assert_eq!(
                dst_dispatched, dst_scalar,
                "mismatch for factor={factor:#06x}"
            );
        }
    }

    #[test]
    fn dispatched_matches_scalar_large_buffer() {
        // Test with a buffer large enough to exercise SIMD main loop + tail.
        let factor = 0xBEEFu16;
        let src: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        let mut dst_dispatched = vec![0x55u8; 8192];
        let mut dst_scalar = dst_dispatched.clone();

        mul_acc_region(factor, &src, &mut dst_dispatched);
        mul_acc_region_scalar(factor, &src, &mut dst_scalar);
        assert_eq!(dst_dispatched, dst_scalar);
    }

    #[test]
    fn dispatched_matches_scalar_odd_sizes() {
        // Test sizes that aren't multiples of 16 or 32.
        let factor = 0x4321u16;
        for size in [2, 4, 6, 14, 18, 30, 34, 50, 62, 66] {
            let src: Vec<u8> = (0..size).map(|i| (i * 7 % 256) as u8).collect();
            let mut dst_dispatched = vec![0xAAu8; size];
            let mut dst_scalar = dst_dispatched.clone();

            mul_acc_region(factor, &src, &mut dst_dispatched);
            mul_acc_region_scalar(factor, &src, &mut dst_scalar);
            assert_eq!(dst_dispatched, dst_scalar, "mismatch for size={size}");
        }
    }

    #[test]
    fn precomputed_tables_correct() {
        let factor = 0xABCDu16;
        let tables = precompute_mul_tables(factor);

        // Verify a few table entries manually.
        for nibble_val in 0u16..16 {
            let prod0 = gf::mul(factor, nibble_val);
            assert_eq!(tables.tables[0][nibble_val as usize], prod0 as u8);
            assert_eq!(tables.tables[1][nibble_val as usize], (prod0 >> 8) as u8);

            let prod2 = gf::mul(factor, nibble_val << 8);
            assert_eq!(tables.tables[4][nibble_val as usize], prod2 as u8);
            assert_eq!(tables.tables[5][nibble_val as usize], (prod2 >> 8) as u8);
        }
    }

    #[test]
    fn exhaustive_factor_sweep() {
        // Test every factor on a small buffer to ensure SIMD matches scalar.
        let src: Vec<u8> = (0..16).collect(); // exactly 1 SIMD iteration

        for factor in 2..=0xFFFFu16 {
            let mut dst_dispatched = vec![0u8; 16];
            let mut dst_scalar = vec![0u8; 16];

            mul_acc_region(factor, &src, &mut dst_dispatched);
            mul_acc_region_scalar(factor, &src, &mut dst_scalar);
            assert_eq!(
                dst_dispatched, dst_scalar,
                "mismatch for factor={factor:#06x}"
            );
        }
    }
}
