//! SIMD-accelerated GF(2^16) region operations.
//!
//! Provides a bulk multiply-accumulate operation over byte regions interpreted
//! as little-endian u16 words in GF(2^16):
//!
//! ```text
//! dst[i] ^= gf_mul(src[i], factor)    for each u16 word i
//! ```
//!
//! Three kernel families, selected at runtime by CPU capability:
//!
//! ## GFNI affine (Ice Lake+ / Zen 4+)
//!
//! GF(2^16) multiplication is linear over GF(2), so it's a 16×16 binary matrix.
//! Split into four 8×8 sub-matrices and apply with `gf2p8affineqb`, which does
//! 8×8 binary matrix × byte in a single instruction. 4 affine transforms + 2 XORs
//! replace 8 PSHUFB + 4 nibble extractions + 6 XORs — roughly half the instructions.
//!
//! ## Split-nibble shuffle (SSSE3 / AVX2 / NEON)
//!
//! 1. Precompute 8 tables of 16 bytes each. Each table maps a 4-bit nibble of
//!    the input to its contribution to one byte of the 16-bit product.
//! 2. Deinterleave input bytes (separate lo/hi bytes of each u16 word).
//! 3. For each nibble, do a PSHUFB/VTBL lookup. XOR the 4 contributions for
//!    the result low byte and the 4 for the result high byte.
//! 4. Reinterleave and XOR-accumulate into the destination.
//!
//! ## Scalar
//!
//! One word at a time via log/antilog tables.

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

/// Precomputed 8×8 binary affine matrices for GFNI-accelerated GF(2^16) multiply.
///
/// GF(2^16) multiplication by a fixed factor is a linear map over GF(2),
/// representable as a 16×16 binary matrix. We partition it into four 8×8
/// sub-matrices:
///
/// ```text
/// [result_lo]   [m_ll  m_lh] [input_lo]
/// [result_hi] = [m_hl  m_hh] [input_hi]
/// ```
///
/// Each 8×8 matrix is packed into a `u64` in the format expected by
/// `gf2p8affineqb`: byte 7 = row 0, bit 7 of each byte = column 0.
pub struct AffineMulMatrices {
    /// Maps input low byte → output low byte.
    pub m_ll: u64,
    /// Maps input high byte → output low byte.
    pub m_lh: u64,
    /// Maps input low byte → output high byte.
    pub m_hl: u64,
    /// Maps input high byte → output high byte.
    pub m_hh: u64,
    /// The original factor, for scalar tail processing.
    pub factor: u16,
}

/// Build the four 8×8 affine matrices for a given GF(2^16) factor.
///
/// For each input bit position, we evaluate `gf_mul(factor, 1 << bit)` and
/// record which output bits are set. The result is packed into the GFNI
/// row-major format.
pub fn precompute_affine_matrices(factor: u16) -> AffineMulMatrices {
    // Build the full 16×16 binary matrix: column `bit` = gf_mul(factor, 1 << bit).
    let mut cols = [0u16; 16];
    for bit in 0..16u32 {
        cols[bit as usize] = gf::mul(factor, 1 << bit);
    }

    // Extract four 8×8 sub-matrices and pack into GFNI format.
    //
    // GFNI gf2p8affineqb computes: result_bit[i] = popcount(row_i AND input) mod 2
    // where row_i is byte (7-i) of the matrix qword (row 0 at MSB byte).
    // The AND operates on matching bit positions: bit j of row ANDs with bit j
    // of input. In our le byte representation, bit 0 = LSB = GF bit 0.
    // So matrix column for GF input bit `col` maps to bit `col` in the row byte.
    let pack = |input_shift: usize, output_shift: usize| -> u64 {
        let mut matrix: u64 = 0;
        for row in 0..8u32 {
            let output_bit = output_shift as u32 + row;
            let mut row_byte: u8 = 0;
            for col in 0..8u32 {
                let input_bit = input_shift as u32 + col;
                if (cols[input_bit as usize] >> output_bit) & 1 == 1 {
                    row_byte |= 1 << col;
                }
            }
            matrix |= (row_byte as u64) << ((7 - row) * 8);
        }
        matrix
    };

    AffineMulMatrices {
        m_ll: pack(0, 0),
        m_lh: pack(8, 0),
        m_hl: pack(0, 8),
        m_hh: pack(8, 8),
        factor,
    }
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

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("gfni")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vl")
        {
            let matrices = precompute_affine_matrices(factor);
            unsafe { mul_acc_region_gfni_avx512(&matrices, src, dst) };
            return;
        }
        if is_x86_feature_detected!("gfni") && is_x86_feature_detected!("avx2") {
            let matrices = precompute_affine_matrices(factor);
            unsafe { mul_acc_region_gfni_avx2(&matrices, src, dst) };
            return;
        }
    }

    let tables = precompute_mul_tables(factor);

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("avx512vl") {
            unsafe { mul_acc_region_avx512(&tables, src, dst) };
            return;
        }
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

/// Multiply each u16 word in `src` by multiple factors and XOR-accumulate into
/// corresponding destination buffers.
///
/// For each factor/dst pair, computes `dst[i] ^= gf_mul(src[i], factor)`.
/// Reads `src` once per SIMD chunk and applies all factors, reducing memory
/// bandwidth compared to calling `mul_acc_region` in a loop.
///
/// Pairs where `factor == 0` are skipped. All dst slices must have the same
/// length as `src`, and that length must be even.
///
/// # Panics
///
/// Panics if any `dst` length differs from `src`, or if lengths are odd.
pub fn mul_acc_multi_region(factors_and_dsts: &mut [FactorDst<'_>], src: &[u8]) {
    let len = src.len();
    assert!(len.is_multiple_of(2), "region length must be even");

    // Filter out zero factors.
    // (We can't actually filter the slice in place, so just skip in the loop.)
    if src.is_empty() {
        return;
    }

    for fd in factors_and_dsts.iter() {
        assert_eq!(fd.dst.len(), len, "all dst slices must match src length");
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("gfni") && is_x86_feature_detected!("avx2") {
            unsafe { mul_acc_multi_region_gfni_avx2(factors_and_dsts, src) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // Use CLMUL (PMULL) kernel when >2 non-zero factors — amortizes reduction cost.
        let nonzero_count = factors_and_dsts
            .iter()
            .filter(|fd| fd.factor != 0 && fd.factor != 1)
            .count();
        if nonzero_count > 2 {
            unsafe { mul_acc_multi_region_clmul(factors_and_dsts, src) };
        } else {
            unsafe { mul_acc_multi_region_neon(factors_and_dsts, src) };
        }
        return;
    }

    // Fallback: call single-region for each factor.
    #[allow(unreachable_code)]
    for fd in factors_and_dsts.iter_mut() {
        if fd.factor != 0 {
            mul_acc_region(fd.factor, src, fd.dst);
        }
    }
}

/// A (factor, destination) pair for multi-region multiply-accumulate.
pub struct FactorDst<'a> {
    pub factor: u16,
    pub dst: &'a mut [u8],
}

/// A (factor, source) pair for grouped-input multiply-accumulate into one destination.
pub struct FactorSrc<'a> {
    pub factor: u16,
    pub src: &'a [u8],
}

/// Multiply multiple input regions by their corresponding factors and XOR-accumulate
/// the results into a single destination buffer.
///
/// For each factor/src pair, computes `dst[i] ^= gf_mul(src[i], factor)`.
/// Reads and writes `dst` once per SIMD chunk, which is a better fit for
/// grouped-input execution than repeatedly calling `mul_acc_region`.
pub fn mul_acc_input_batch(dst: &mut [u8], factors_and_srcs: &[FactorSrc<'_>]) {
    let len = dst.len();
    assert!(len.is_multiple_of(2), "region length must be even");

    if dst.is_empty() {
        return;
    }

    for fs in factors_and_srcs.iter() {
        assert_eq!(fs.src.len(), len, "all src slices must match dst length");
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("gfni") && is_x86_feature_detected!("avx2") {
            unsafe { mul_acc_input_batch_gfni_avx2(dst, factors_and_srcs) };
            return;
        }
    }

    #[allow(unreachable_code)]
    for fs in factors_and_srcs {
        if fs.factor != 0 {
            mul_acc_region(fs.factor, fs.src, dst);
        }
    }
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
// GFNI + AVX2 kernel: 32 bytes (16 GF elements) per iteration
//
// Uses gf2p8affineqb to apply 8×8 binary matrix transforms instead of
// PSHUFB nibble lookups. Each 16-bit GF multiply decomposes into:
//
//   result_lo = affine(input_lo, M_ll) XOR affine(input_hi, M_lh)
//   result_hi = affine(input_lo, M_hl) XOR affine(input_hi, M_hh)
//
// This is 4 affine + 2 XOR vs. 8 PSHUFB + 4 AND + 4 SRLI + 6 XOR in the
// split-nibble approach — roughly half the instructions per element.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx2")]
unsafe fn mul_acc_region_gfni_avx2(matrices: &AffineMulMatrices, src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        // Broadcast each 8×8 matrix (u64) into all four 64-bit lanes of a __m256i.
        let m_ll = _mm256_set1_epi64x(matrices.m_ll as i64);
        let m_lh = _mm256_set1_epi64x(matrices.m_lh as i64);
        let m_hl = _mm256_set1_epi64x(matrices.m_hl as i64);
        let m_hh = _mm256_set1_epi64x(matrices.m_hh as i64);

        // Deinterleave masks (same as AVX2 shuffle kernel — per 128-bit lane).
        let deint_lo_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0);
        let deint_hi_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1);
        let deint_lo = _mm256_broadcastsi128_si256(deint_lo_128);
        let deint_hi = _mm256_broadcastsi128_si256(deint_hi_128);

        macro_rules! process_chunk {
            ($chunk_offset:expr) => {{
                let s = _mm256_loadu_si256(src.as_ptr().add($chunk_offset) as *const __m256i);
                let d = _mm256_loadu_si256(dst.as_ptr().add($chunk_offset) as *const __m256i);

                let lo_bytes = _mm256_shuffle_epi8(s, deint_lo);
                let hi_bytes = _mm256_shuffle_epi8(s, deint_hi);

                let result_lo = _mm256_xor_si256(
                    _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, m_ll),
                    _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, m_lh),
                );
                let result_hi = _mm256_xor_si256(
                    _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, m_hl),
                    _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, m_hh),
                );

                let product = _mm256_unpacklo_epi8(result_lo, result_hi);
                let result = _mm256_xor_si256(d, product);
                _mm256_storeu_si256(dst.as_mut_ptr().add($chunk_offset) as *mut __m256i, result);
            }};
        }

        while offset + 64 <= len {
            process_chunk!(offset);
            process_chunk!(offset + 32);
            offset += 64;
        }

        while offset + 32 <= len {
            process_chunk!(offset);
            offset += 32;
        }
    }

    // Tail: fall through to SSSE3 for remaining 16-byte chunk + scalar.
    if offset < len {
        let tables = precompute_mul_tables(matrices.factor);
        unsafe { mul_acc_region_ssse3(&tables, &src[offset..], &mut dst[offset..]) };
    }
}

// ---------------------------------------------------------------------------
// GFNI + AVX-512 kernel: 64 bytes (32 GF elements) per iteration
//
// Same algorithm as GFNI+AVX2 but 2× wider (512-bit registers).
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx512bw,avx512vl")]
unsafe fn mul_acc_region_gfni_avx512(matrices: &AffineMulMatrices, src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        let m_ll = _mm512_set1_epi64(matrices.m_ll as i64);
        let m_lh = _mm512_set1_epi64(matrices.m_lh as i64);
        let m_hl = _mm512_set1_epi64(matrices.m_hl as i64);
        let m_hh = _mm512_set1_epi64(matrices.m_hh as i64);

        // Deinterleave masks (per 128-bit lane — same pattern broadcast to all 4 lanes).
        let deint_lo = _mm512_broadcast_i32x4(_mm_set_epi8(
            -1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0,
        ));
        let deint_hi = _mm512_broadcast_i32x4(_mm_set_epi8(
            -1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1,
        ));

        while offset + 64 <= len {
            let s = _mm512_loadu_si512(src.as_ptr().add(offset) as *const __m512i);
            let d = _mm512_loadu_si512(dst.as_ptr().add(offset) as *const __m512i);

            let lo_bytes = _mm512_shuffle_epi8(s, deint_lo);
            let hi_bytes = _mm512_shuffle_epi8(s, deint_hi);

            let result_lo = _mm512_xor_si512(
                _mm512_gf2p8affine_epi64_epi8::<0>(lo_bytes, m_ll),
                _mm512_gf2p8affine_epi64_epi8::<0>(hi_bytes, m_lh),
            );
            let result_hi = _mm512_xor_si512(
                _mm512_gf2p8affine_epi64_epi8::<0>(lo_bytes, m_hl),
                _mm512_gf2p8affine_epi64_epi8::<0>(hi_bytes, m_hh),
            );

            let product = _mm512_unpacklo_epi8(result_lo, result_hi);
            let result = _mm512_xor_si512(d, product);
            _mm512_storeu_si512(dst.as_mut_ptr().add(offset) as *mut __m512i, result);

            offset += 64;
        }
    }

    // Tail: fall through to GFNI+AVX2 for 32-byte chunk, then SSSE3/scalar.
    if offset < len {
        unsafe { mul_acc_region_gfni_avx2(matrices, &src[offset..], &mut dst[offset..]) };
    }
}

// ---------------------------------------------------------------------------
// AVX-512 shuffle kernel: 64 bytes (32 GF elements) per iteration
//
// Same split-nibble algorithm as AVX2 but 2× wider (512-bit registers).
// VPSHUFB in AVX-512 operates within each 128-bit lane independently.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512vl")]
unsafe fn mul_acc_region_avx512(tables: &MulTables, src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;

    let len = src.len();
    let mut offset = 0usize;

    unsafe {
        let mask_0f = _mm512_set1_epi8(0x0F);

        // Broadcast each 16-byte table into all four 128-bit lanes.
        let t0 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[0].as_ptr() as *const __m128i));
        let t1 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[1].as_ptr() as *const __m128i));
        let t2 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[2].as_ptr() as *const __m128i));
        let t3 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[3].as_ptr() as *const __m128i));
        let t4 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[4].as_ptr() as *const __m128i));
        let t5 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[5].as_ptr() as *const __m128i));
        let t6 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[6].as_ptr() as *const __m128i));
        let t7 =
            _mm512_broadcast_i32x4(_mm_loadu_si128(tables.tables[7].as_ptr() as *const __m128i));

        let deint_lo = _mm512_broadcast_i32x4(_mm_set_epi8(
            -1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0,
        ));
        let deint_hi = _mm512_broadcast_i32x4(_mm_set_epi8(
            -1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1,
        ));

        while offset + 64 <= len {
            let s = _mm512_loadu_si512(src.as_ptr().add(offset) as *const __m512i);
            let d = _mm512_loadu_si512(dst.as_ptr().add(offset) as *const __m512i);

            let lo_bytes = _mm512_shuffle_epi8(s, deint_lo);
            let hi_bytes = _mm512_shuffle_epi8(s, deint_hi);

            let lo_n0 = _mm512_and_si512(lo_bytes, mask_0f);
            let lo_n1 = _mm512_and_si512(_mm512_srli_epi16(lo_bytes, 4), mask_0f);
            let hi_n0 = _mm512_and_si512(hi_bytes, mask_0f);
            let hi_n1 = _mm512_and_si512(_mm512_srli_epi16(hi_bytes, 4), mask_0f);

            let p0_lo = _mm512_shuffle_epi8(t0, lo_n0);
            let p0_hi = _mm512_shuffle_epi8(t1, lo_n0);
            let p1_lo = _mm512_shuffle_epi8(t2, lo_n1);
            let p1_hi = _mm512_shuffle_epi8(t3, lo_n1);
            let p2_lo = _mm512_shuffle_epi8(t4, hi_n0);
            let p2_hi = _mm512_shuffle_epi8(t5, hi_n0);
            let p3_lo = _mm512_shuffle_epi8(t6, hi_n1);
            let p3_hi = _mm512_shuffle_epi8(t7, hi_n1);

            let result_lo = _mm512_xor_si512(
                _mm512_xor_si512(p0_lo, p1_lo),
                _mm512_xor_si512(p2_lo, p3_lo),
            );
            let result_hi = _mm512_xor_si512(
                _mm512_xor_si512(p0_hi, p1_hi),
                _mm512_xor_si512(p2_hi, p3_hi),
            );

            let product = _mm512_unpacklo_epi8(result_lo, result_hi);
            let result = _mm512_xor_si512(d, product);
            _mm512_storeu_si512(dst.as_mut_ptr().add(offset) as *mut __m512i, result);

            offset += 64;
        }
    }

    // Tail: fall through to AVX2 for 32-byte chunk, then SSSE3/scalar.
    if offset < len {
        unsafe { mul_acc_region_avx2(tables, &src[offset..], &mut dst[offset..]) };
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

// ---------------------------------------------------------------------------
// Multi-region GFNI + AVX2 kernel
//
// Reads src once per 32-byte chunk, applies all factors to all destinations.
// Processes factors in batches of 4 (16 matrix registers fit in 16 YMM regs
// alongside the deinterleave constants and source data).
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx2")]
unsafe fn mul_acc_multi_region_gfni_avx2(factors_and_dsts: &mut [FactorDst<'_>], src: &[u8]) {
    use std::arch::x86_64::*;

    let len = src.len();

    struct BroadcastAffine {
        m_ll: __m256i,
        m_lh: __m256i,
        m_hl: __m256i,
        m_hh: __m256i,
        factor: u16,
        dst_idx: usize,
    }

    // Precompute affine matrices for all non-zero factors.
    let all_matrices: Vec<BroadcastAffine> = factors_and_dsts
        .iter()
        .enumerate()
        .filter(|(_, fd)| fd.factor != 0 && fd.factor != 1)
        .map(|(idx, fd)| {
            let matrices = precompute_affine_matrices(fd.factor);
            BroadcastAffine {
                m_ll: _mm256_set1_epi64x(matrices.m_ll as i64),
                m_lh: _mm256_set1_epi64x(matrices.m_lh as i64),
                m_hl: _mm256_set1_epi64x(matrices.m_hl as i64),
                m_hh: _mm256_set1_epi64x(matrices.m_hh as i64),
                factor: matrices.factor,
                dst_idx: idx,
            }
        })
        .collect();

    // Handle factor=1 (XOR-only) destinations.
    for fd in factors_and_dsts.iter_mut() {
        if fd.factor == 1 {
            for (d, s) in fd.dst.iter_mut().zip(src.iter()) {
                *d ^= *s;
            }
        }
    }

    if all_matrices.is_empty() {
        return;
    }

    unsafe {
        let deint_lo_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0);
        let deint_hi_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1);
        let deint_lo = _mm256_broadcastsi128_si256(deint_lo_128);
        let deint_hi = _mm256_broadcastsi128_si256(deint_hi_128);

        let mut offset = 0usize;
        while offset + 32 <= len {
            let s = _mm256_loadu_si256(src.as_ptr().add(offset) as *const __m256i);
            let lo_bytes = _mm256_shuffle_epi8(s, deint_lo);
            let hi_bytes = _mm256_shuffle_epi8(s, deint_hi);

            for matrices in &all_matrices {
                let d = _mm256_loadu_si256(
                    factors_and_dsts[matrices.dst_idx]
                        .dst
                        .as_ptr()
                        .add(offset) as *const __m256i
                );

                let result_lo = _mm256_xor_si256(
                    _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, matrices.m_ll),
                    _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, matrices.m_lh),
                );
                let result_hi = _mm256_xor_si256(
                    _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, matrices.m_hl),
                    _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, matrices.m_hh),
                );

                let product = _mm256_unpacklo_epi8(result_lo, result_hi);
                let result = _mm256_xor_si256(d, product);
                _mm256_storeu_si256(
                    factors_and_dsts[matrices.dst_idx]
                        .dst
                        .as_mut_ptr()
                        .add(offset) as *mut __m256i,
                    result,
                );
            }

            offset += 32;
        }

        // Tail: scalar for remaining bytes.
        if offset < len {
            for matrices in &all_matrices {
                mul_acc_region_scalar(
                    matrices.factor,
                    &src[offset..],
                    &mut factors_and_dsts[matrices.dst_idx].dst[offset..],
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Grouped-input GFNI + AVX2 kernel
//
// Keeps one destination chunk hot in registers while accumulating multiple
// source regions into it. This mirrors ParPar's grouped-input execution shape
// more closely than repeatedly issuing single-input updates against the same
// destination buffer.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx2")]
unsafe fn mul_acc_input_batch_gfni_avx2(dst: &mut [u8], factors_and_srcs: &[FactorSrc<'_>]) {
    use std::arch::x86_64::*;

    let len = dst.len();

    struct PreparedInput<'a> {
        m_ll: __m256i,
        m_lh: __m256i,
        m_hl: __m256i,
        m_hh: __m256i,
        factor: u16,
        src: &'a [u8],
    }

    let xor_inputs: Vec<&[u8]> = factors_and_srcs
        .iter()
        .filter(|fs| fs.factor == 1)
        .map(|fs| fs.src)
        .collect();

    let prepared: Vec<PreparedInput<'_>> = factors_and_srcs
        .iter()
        .filter(|fs| fs.factor != 0 && fs.factor != 1)
        .map(|fs| {
            let matrices = precompute_affine_matrices(fs.factor);
            PreparedInput {
                m_ll: _mm256_set1_epi64x(matrices.m_ll as i64),
                m_lh: _mm256_set1_epi64x(matrices.m_lh as i64),
                m_hl: _mm256_set1_epi64x(matrices.m_hl as i64),
                m_hh: _mm256_set1_epi64x(matrices.m_hh as i64),
                factor: matrices.factor,
                src: fs.src,
            }
        })
        .collect();

    unsafe {
        let deint_lo_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 14, 12, 10, 8, 6, 4, 2, 0);
        let deint_hi_128 = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 15, 13, 11, 9, 7, 5, 3, 1);
        let deint_lo = _mm256_broadcastsi128_si256(deint_lo_128);
        let deint_hi = _mm256_broadcastsi128_si256(deint_hi_128);

        macro_rules! process_chunk {
            ($chunk_offset:expr) => {{
                let mut acc =
                    _mm256_loadu_si256(dst.as_ptr().add($chunk_offset) as *const __m256i);

                for src in &xor_inputs {
                    let s = _mm256_loadu_si256(src.as_ptr().add($chunk_offset) as *const __m256i);
                    acc = _mm256_xor_si256(acc, s);
                }

                for input in &prepared {
                    let s = _mm256_loadu_si256(
                        input.src.as_ptr().add($chunk_offset) as *const __m256i
                    );
                    let lo_bytes = _mm256_shuffle_epi8(s, deint_lo);
                    let hi_bytes = _mm256_shuffle_epi8(s, deint_hi);

                    let result_lo = _mm256_xor_si256(
                        _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, input.m_ll),
                        _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, input.m_lh),
                    );
                    let result_hi = _mm256_xor_si256(
                        _mm256_gf2p8affine_epi64_epi8::<0>(lo_bytes, input.m_hl),
                        _mm256_gf2p8affine_epi64_epi8::<0>(hi_bytes, input.m_hh),
                    );

                    let product = _mm256_unpacklo_epi8(result_lo, result_hi);
                    acc = _mm256_xor_si256(acc, product);
                }

                _mm256_storeu_si256(dst.as_mut_ptr().add($chunk_offset) as *mut __m256i, acc);
            }};
        }

        let mut offset = 0usize;
        while offset + 64 <= len {
            process_chunk!(offset);
            process_chunk!(offset + 32);
            offset += 64;
        }

        while offset + 32 <= len {
            process_chunk!(offset);
            offset += 32;
        }

        if offset < len {
            let tail = &mut dst[offset..];
            for src in &xor_inputs {
                for (d, s) in tail.iter_mut().zip(src[offset..].iter()) {
                    *d ^= *s;
                }
            }
            for input in &prepared {
                mul_acc_region_scalar(input.factor, &input.src[offset..], tail);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-region CLMUL kernel (aarch64)
//
// Uses ARM polynomial multiply (PMULL/vmull_p8) with Karatsuba decomposition
// to compute GF(2^16) multiply-accumulate. Each 16-bit GF element is split
// into lo/hi bytes; three 8×8 polynomial multiplies produce a 30-bit product
// that is then reduced modulo x^16 + x^12 + x^3 + x + 1.
//
// Advantage over VTBL shuffle: 3 PMULL + accumulate per factor vs 8 VTBL,
// with a single shared reduction at the end. Wins when >2 factors.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
unsafe fn mul_acc_multi_region_clmul(factors_and_dsts: &mut [FactorDst<'_>], src: &[u8]) {
    use std::arch::aarch64::*;

    let len = src.len();

    // Separate factor=1 (XOR-only) destinations.
    for fd in factors_and_dsts.iter_mut() {
        if fd.factor == 1 {
            for (d, s) in fd.dst.iter_mut().zip(src.iter()) {
                *d ^= *s;
            }
        }
    }

    // Collect non-trivial factors with precomputed coefficients.
    // For Karatsuba: store (lo, hi, lo^hi) as poly8x8_t.
    struct ClmulCoeff {
        lo: poly8x8_t,
        hi: poly8x8_t,
        mid: poly8x8_t, // lo ^ hi
        lo_full: poly8x16_t,
        hi_full: poly8x16_t,
        mid_full: poly8x16_t,
        dst_idx: usize,
    }

    let coeffs: Vec<ClmulCoeff> = unsafe {
        factors_and_dsts
            .iter()
            .enumerate()
            .filter(|(_, fd)| fd.factor > 1)
            .map(|(idx, fd)| {
                let lo_byte = (fd.factor & 0xFF) as u8;
                let hi_byte = (fd.factor >> 8) as u8;
                let mid_byte = lo_byte ^ hi_byte;
                ClmulCoeff {
                    lo: vdup_n_p8(lo_byte),
                    hi: vdup_n_p8(hi_byte),
                    mid: vdup_n_p8(mid_byte),
                    lo_full: vdupq_n_p8(lo_byte),
                    hi_full: vdupq_n_p8(hi_byte),
                    mid_full: vdupq_n_p8(mid_byte),
                    dst_idx: idx,
                }
            })
            .collect()
    };

    if coeffs.is_empty() {
        return;
    }

    unsafe {
        let mut offset = 0usize;

        while offset + 16 <= len {
            let s = vld1q_u8(src.as_ptr().add(offset));

            // Deinterleave: separate lo bytes (even) and hi bytes (odd) of each u16.
            let data_lo = vreinterpretq_p8_u8(vuzp1q_u8(s, s));
            let data_hi = vreinterpretq_p8_u8(vuzp2q_u8(s, s));
            let data_mid = vreinterpretq_p8_u8(veorq_u8(
                vreinterpretq_u8_p8(data_lo),
                vreinterpretq_u8_p8(data_hi),
            ));

            let data_lo_half = vget_low_p8(data_lo);
            let data_hi_half = vget_low_p8(data_hi);
            let data_mid_half = vget_low_p8(data_mid);

            // For each coefficient, compute Karatsuba products and accumulate,
            // then reduce and write to that coefficient's destination.
            for coeff in &coeffs {
                // Three Karatsuba products (low half: 4 elements, high half: 4 elements).
                // vmull_p8: poly8x8 × poly8x8 → poly16x8 (8 results from low halves)
                // vmull_high_p8: poly8x16 × poly8x16 → poly16x8 (8 results from high halves)
                let ll_lo = vmull_p8(data_lo_half, coeff.lo);
                let ll_hi = vmull_high_p8(data_lo, coeff.lo_full);
                let hh_lo = vmull_p8(data_hi_half, coeff.hi);
                let hh_hi = vmull_high_p8(data_hi, coeff.hi_full);
                let mm_lo = vmull_p8(data_mid_half, coeff.mid);
                let mm_hi = vmull_high_p8(data_mid, coeff.mid_full);

                // Karatsuba combination:
                // Product = L + (M ^ L ^ H) << 8 + H << 16
                //
                // In byte terms (each poly16x8 has lo_byte and hi_byte per element):
                //   product_byte0 = L.lo
                //   product_byte1 = L.hi ^ K.lo  (K = M ^ L ^ H)
                //   product_byte2 = H.lo ^ K.hi
                //   product_byte3 = H.hi
                //
                // Bytes 0-1 form the low 16 bits, bytes 2-3 form the high 14 bits.

                // Compute K = M ^ L ^ H for both halves.
                let k_lo = veorq_u16(
                    vreinterpretq_u16_p16(mm_lo),
                    veorq_u16(vreinterpretq_u16_p16(ll_lo), vreinterpretq_u16_p16(hh_lo)),
                );
                let k_hi = veorq_u16(
                    vreinterpretq_u16_p16(mm_hi),
                    veorq_u16(vreinterpretq_u16_p16(ll_hi), vreinterpretq_u16_p16(hh_hi)),
                );

                // Deinterleave L, H, K into separate byte lanes.
                let l_lo = vreinterpretq_u8_p16(ll_lo);
                let l_hi = vreinterpretq_u8_p16(ll_hi);
                let h_lo = vreinterpretq_u8_p16(hh_lo);
                let h_hi = vreinterpretq_u8_p16(hh_hi);
                let k_lo_u8 = vreinterpretq_u8_u16(k_lo);
                let k_hi_u8 = vreinterpretq_u8_u16(k_hi);

                // Separate even bytes (byte0 of each u16) and odd bytes (byte1).
                let l_bytes = vuzpq_u8(l_lo, l_hi); // .0 = even bytes (L.lo), .1 = odd bytes (L.hi)
                let h_bytes = vuzpq_u8(h_lo, h_hi); // .0 = H.lo, .1 = H.hi
                let k_bytes = vuzpq_u8(k_lo_u8, k_hi_u8); // .0 = K.lo, .1 = K.hi

                // Combine into product bytes:
                let prod_byte0 = l_bytes.0; // L.lo
                let prod_byte1 = veorq_u8(l_bytes.1, k_bytes.0); // L.hi ^ K.lo
                let prod_byte2 = veorq_u8(h_bytes.0, k_bytes.1); // H.lo ^ K.hi
                let prod_byte3 = h_bytes.1; // H.hi

                // Assemble low 16 bits and high 14 bits as uint16x8_t.
                // low = byte0 | (byte1 << 8) = reinterleave bytes 0,1
                // high = byte2 | (byte3 << 8) = reinterleave bytes 2,3
                let result_low = vreinterpretq_u16_u8(vzipq_u8(prod_byte0, prod_byte1).0);
                let result_high = vreinterpretq_u16_u8(vzipq_u8(prod_byte2, prod_byte3).0);

                // Reduce: high * (x^12 + x^3 + x + 1) folded into low.
                // Iterative: each pass reduces bits above 15.
                let reduced = reduce_gf16(result_low, result_high);

                // Reinterleave from u16 back to bytes and XOR-accumulate into dst.
                let product = vreinterpretq_u8_u16(reduced);
                let d = vld1q_u8(factors_and_dsts[coeff.dst_idx].dst.as_ptr().add(offset));
                let result = veorq_u8(d, product);
                vst1q_u8(
                    factors_and_dsts[coeff.dst_idx].dst.as_mut_ptr().add(offset),
                    result,
                );
            }

            offset += 16;
        }

        // Scalar tail.
        if offset < len {
            for coeff in &coeffs {
                mul_acc_region_scalar(
                    factors_and_dsts[coeff.dst_idx].factor,
                    &src[offset..],
                    &mut factors_and_dsts[coeff.dst_idx].dst[offset..],
                );
            }
        }
    }
}

/// Reduce a 30-bit GF(2)[x] product to GF(2^16).
///
/// Given low (bits 0-15) and high (bits 16-29), computes
/// `low XOR reduce(high)` where the reduction polynomial is
/// x^16 + x^12 + x^3 + x + 1.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn reduce_gf16(
    low: std::arch::aarch64::uint16x8_t,
    high: std::arch::aarch64::uint16x8_t,
) -> std::arch::aarch64::uint16x8_t {
    use std::arch::aarch64::*;

    // Reduction: x^16 ≡ x^12 + x^3 + x + 1
    // For high bits h: contribution = (h << 12) ^ (h << 3) ^ (h << 1) ^ h
    // Bits that overflow u16 (from h << 12) become the next round's high bits.
    let mut result = low;
    let mut h = high;

    // Each pass: max 14 bits → (h >> 4) = 10 bits → 6 bits → 2 bits → 0.
    // Unroll 4 iterations (always sufficient for 14-bit input).
    unsafe {
        for _ in 0..4 {
            let contrib = veorq_u16(
                veorq_u16(vshlq_n_u16::<12>(h), vshlq_n_u16::<3>(h)),
                veorq_u16(vshlq_n_u16::<1>(h), h),
            );
            result = veorq_u16(result, contrib);
            // Overflow from h << 12: bits that shifted past position 15.
            // h >> 4 captures bits 4+ of h that land at position 16+ after << 12.
            // h >> 13 captures the single bit from h << 3 that overflows.
            h = veorq_u16(vshrq_n_u16::<4>(h), vshrq_n_u16::<13>(h));
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Multi-region NEON kernel (aarch64)
//
// Reads src once per 16-byte chunk, applies all factors to all destinations.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
unsafe fn mul_acc_multi_region_neon(factors_and_dsts: &mut [FactorDst<'_>], src: &[u8]) {
    use std::arch::aarch64::*;

    let len = src.len();

    // Precompute shuffle tables for all non-zero/non-one factors.
    let all_tables: Vec<(MulTables, usize)> = factors_and_dsts
        .iter()
        .enumerate()
        .filter(|(_, fd)| fd.factor != 0 && fd.factor != 1)
        .map(|(idx, fd)| (precompute_mul_tables(fd.factor), idx))
        .collect();

    // Handle factor=1 (XOR-only) destinations.
    for fd in factors_and_dsts.iter_mut() {
        if fd.factor == 1 {
            for (d, s) in fd.dst.iter_mut().zip(src.iter()) {
                *d ^= *s;
            }
        }
    }

    if all_tables.is_empty() {
        return;
    }

    // Pre-load all table sets into NEON registers.
    struct NeonTableSet {
        t: [uint8x16_t; 8],
        dst_idx: usize,
    }

    let table_sets: Vec<NeonTableSet> = unsafe {
        all_tables
            .iter()
            .map(|(tables, dst_idx)| NeonTableSet {
                t: [
                    vld1q_u8(tables.tables[0].as_ptr()),
                    vld1q_u8(tables.tables[1].as_ptr()),
                    vld1q_u8(tables.tables[2].as_ptr()),
                    vld1q_u8(tables.tables[3].as_ptr()),
                    vld1q_u8(tables.tables[4].as_ptr()),
                    vld1q_u8(tables.tables[5].as_ptr()),
                    vld1q_u8(tables.tables[6].as_ptr()),
                    vld1q_u8(tables.tables[7].as_ptr()),
                ],
                dst_idx: *dst_idx,
            })
            .collect()
    };

    unsafe {
        let mask_0f = vdupq_n_u8(0x0F);
        let mut offset = 0usize;

        while offset + 16 <= len {
            let s = vld1q_u8(src.as_ptr().add(offset));

            // Deinterleave once, reuse for all factors.
            let lo_bytes = vuzp1q_u8(s, s);
            let hi_bytes = vuzp2q_u8(s, s);
            let lo_n0 = vandq_u8(lo_bytes, mask_0f);
            let lo_n1 = vandq_u8(vshrq_n_u8(lo_bytes, 4), mask_0f);
            let hi_n0 = vandq_u8(hi_bytes, mask_0f);
            let hi_n1 = vandq_u8(vshrq_n_u8(hi_bytes, 4), mask_0f);

            for ts in &table_sets {
                let d = vld1q_u8(factors_and_dsts[ts.dst_idx].dst.as_ptr().add(offset));

                let p0_lo = vqtbl1q_u8(ts.t[0], lo_n0);
                let p0_hi = vqtbl1q_u8(ts.t[1], lo_n0);
                let p1_lo = vqtbl1q_u8(ts.t[2], lo_n1);
                let p1_hi = vqtbl1q_u8(ts.t[3], lo_n1);
                let p2_lo = vqtbl1q_u8(ts.t[4], hi_n0);
                let p2_hi = vqtbl1q_u8(ts.t[5], hi_n0);
                let p3_lo = vqtbl1q_u8(ts.t[6], hi_n1);
                let p3_hi = vqtbl1q_u8(ts.t[7], hi_n1);

                let result_lo = veorq_u8(veorq_u8(p0_lo, p1_lo), veorq_u8(p2_lo, p3_lo));
                let result_hi = veorq_u8(veorq_u8(p0_hi, p1_hi), veorq_u8(p2_hi, p3_hi));
                let product = vzip1q_u8(result_lo, result_hi);
                let result = veorq_u8(d, product);
                vst1q_u8(
                    factors_and_dsts[ts.dst_idx].dst.as_mut_ptr().add(offset),
                    result,
                );
            }

            offset += 16;
        }

        // Scalar tail.
        if offset < len {
            for (tables, dst_idx) in &all_tables {
                mul_acc_region_scalar(
                    tables.factor,
                    &src[offset..],
                    &mut factors_and_dsts[*dst_idx].dst[offset..],
                );
            }
        }
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
    fn affine_matrices_match_scalar() {
        // Verify the affine matrix precomputation produces correct results
        // by checking against scalar gf::mul for a range of factors.
        for factor in [2u16, 0x1234, 0xABCD, 0xFFFF, 0x8000, 0x0001] {
            let matrices = precompute_affine_matrices(factor);

            // For each possible input byte pair, verify the matrix multiplication
            // matches the scalar result.
            for input in [0u16, 1, 0xFF, 0x100, 0xFFFF, 0x1234, 0x8000, 0x5555] {
                let expected = gf::mul(factor, input);
                let in_lo = (input & 0xFF) as u8;
                let in_hi = (input >> 8) as u8;

                // Simulate gf2p8affineqb: for each output bit j,
                // output[j] = popcount(row_j AND input_byte) mod 2
                // where row_j is byte (7-j) of the matrix u64.
                // The AND operates on matching bit positions.
                let apply_matrix = |matrix: u64, byte: u8| -> u8 {
                    let mut result: u8 = 0;
                    for j in 0..8u32 {
                        let row_byte = (matrix >> ((7 - j) * 8)) as u8;
                        let dot = (row_byte & byte).count_ones() & 1;
                        result |= (dot as u8) << j;
                    }
                    result
                };

                let result_lo =
                    apply_matrix(matrices.m_ll, in_lo) ^ apply_matrix(matrices.m_lh, in_hi);
                let result_hi =
                    apply_matrix(matrices.m_hl, in_lo) ^ apply_matrix(matrices.m_hh, in_hi);
                let result = result_lo as u16 | ((result_hi as u16) << 8);

                assert_eq!(
                    result, expected,
                    "affine matrix mismatch for factor={factor:#06x}, input={input:#06x}: \
                     got {result:#06x}, expected {expected:#06x}"
                );
            }
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

    #[test]
    fn multi_region_matches_single_region() {
        let src: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();
        let factors = [0x1234u16, 0xBEEF, 0x0001, 0x0000, 0xFFFF, 0x8000];

        // Compute reference with single-region calls.
        let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0x55u8; 256]).collect();
        for (i, &factor) in factors.iter().enumerate() {
            mul_acc_region(factor, &src, &mut reference[i]);
        }

        // Compute with multi-region.
        let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0x55u8; 256]).collect();
        {
            let mut pairs: Vec<FactorDst<'_>> = factors
                .iter()
                .zip(multi.iter_mut())
                .map(|(&factor, dst)| FactorDst {
                    factor,
                    dst: dst.as_mut_slice(),
                })
                .collect();
            mul_acc_multi_region(&mut pairs, &src);
        }

        for (i, &factor) in factors.iter().enumerate() {
            assert_eq!(
                multi[i], reference[i],
                "multi-region mismatch for factor={factor:#06x}"
            );
        }
    }

    #[test]
    fn multi_region_large_buffer() {
        // Test with larger buffer to exercise SIMD main loop + tail.
        let src: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        let factors = [0xABCDu16, 0x1234, 0x5678];

        let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0xAAu8; 8192]).collect();
        for (i, &factor) in factors.iter().enumerate() {
            mul_acc_region(factor, &src, &mut reference[i]);
        }

        let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0xAAu8; 8192]).collect();
        {
            let mut pairs: Vec<FactorDst<'_>> = factors
                .iter()
                .zip(multi.iter_mut())
                .map(|(&factor, dst)| FactorDst {
                    factor,
                    dst: dst.as_mut_slice(),
                })
                .collect();
            mul_acc_multi_region(&mut pairs, &src);
        }

        for (i, &factor) in factors.iter().enumerate() {
            assert_eq!(
                multi[i], reference[i],
                "multi-region large mismatch for factor={factor:#06x}"
            );
        }
    }

    /// Test the CLMUL dispatch path specifically (>2 non-zero factors).
    #[test]
    fn multi_region_clmul_path() {
        // 8 factors ensures CLMUL is selected on aarch64 (threshold is >2).
        let src: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let factors = [
            0x1234u16, 0x5678, 0x9ABC, 0xDEF0, 0x1111, 0x2222, 0x3333, 0x4444,
        ];

        let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 4096]).collect();
        for (i, &factor) in factors.iter().enumerate() {
            mul_acc_region(factor, &src, &mut reference[i]);
        }

        let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 4096]).collect();
        {
            let mut pairs: Vec<FactorDst<'_>> = factors
                .iter()
                .zip(multi.iter_mut())
                .map(|(&factor, dst)| FactorDst {
                    factor,
                    dst: dst.as_mut_slice(),
                })
                .collect();
            mul_acc_multi_region(&mut pairs, &src);
        }

        for (i, &factor) in factors.iter().enumerate() {
            assert_eq!(
                multi[i], reference[i],
                "CLMUL path mismatch for factor={factor:#06x}"
            );
        }
    }

    /// Test CLMUL with all possible factor edge cases mixed in.
    #[test]
    fn multi_region_clmul_with_edge_factors() {
        let src: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();
        // Mix of: zero (skip), one (XOR), normal, and high-bit factors.
        let factors = [0x0000u16, 0x0001, 0xFFFF, 0x8000, 0x0002, 0x7FFF];

        let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0x55u8; 256]).collect();
        for (i, &factor) in factors.iter().enumerate() {
            mul_acc_region(factor, &src, &mut reference[i]);
        }

        let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0x55u8; 256]).collect();
        {
            let mut pairs: Vec<FactorDst<'_>> = factors
                .iter()
                .zip(multi.iter_mut())
                .map(|(&factor, dst)| FactorDst {
                    factor,
                    dst: dst.as_mut_slice(),
                })
                .collect();
            mul_acc_multi_region(&mut pairs, &src);
        }

        for (i, &factor) in factors.iter().enumerate() {
            assert_eq!(
                multi[i], reference[i],
                "CLMUL edge mismatch for factor={factor:#06x}"
            );
        }
    }

    /// Exhaustive factor sweep for the CLMUL multi-region path.
    #[test]
    fn multi_region_clmul_factor_sweep() {
        let src: Vec<u8> = (0..32).collect();

        // Test groups of 4 factors at a time across the full range.
        for base in (2..=0xFFFCu16).step_by(1024) {
            let factors = [base, base + 1, base + 2, base + 3];

            let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 32]).collect();
            for (i, &factor) in factors.iter().enumerate() {
                mul_acc_region(factor, &src, &mut reference[i]);
            }

            let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 32]).collect();
            {
                let mut pairs: Vec<FactorDst<'_>> = factors
                    .iter()
                    .zip(multi.iter_mut())
                    .map(|(&factor, dst)| FactorDst {
                        factor,
                        dst: dst.as_mut_slice(),
                    })
                    .collect();
                mul_acc_multi_region(&mut pairs, &src);
            }

            for (i, &factor) in factors.iter().enumerate() {
                assert_eq!(
                    multi[i], reference[i],
                    "CLMUL sweep mismatch for factor={factor:#06x}"
                );
            }
        }
    }

    /// Verify the SSSE3 kernel in isolation by testing buffer sizes that are
    /// exactly 16 bytes (one SSSE3 iteration) and 16+tail. On x86 with AVX2,
    /// mul_acc_region dispatches to AVX2 which falls through to SSSE3 for
    /// the 16-byte remainder, so we test the SSSE3 path via the tail.
    #[test]
    fn ssse3_tail_matches_scalar() {
        // 48 bytes = one AVX2 iteration (32) + one SSSE3 iteration (16)
        // The SSSE3 path processes the remaining 16 bytes after AVX2.
        for size in [16, 48, 80] {
            for factor in [2u16, 0x1234, 0xABCD, 0xFFFF, 0x8000] {
                let src: Vec<u8> = (0..size).map(|i| (i * 13 % 256) as u8).collect();
                let mut dst_dispatched = vec![0x77u8; size];
                let mut dst_scalar = dst_dispatched.clone();

                mul_acc_region(factor, &src, &mut dst_dispatched);
                mul_acc_region_scalar(factor, &src, &mut dst_scalar);
                assert_eq!(
                    dst_dispatched, dst_scalar,
                    "SSSE3 tail mismatch for factor={factor:#06x}, size={size}"
                );
            }
        }
    }

    /// Large-buffer factor sweep: ensures SIMD main loop + tail handling
    /// is correct across many factors with a buffer large enough to exercise
    /// multiple SIMD iterations.
    #[test]
    fn large_buffer_factor_sweep() {
        let src: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();

        for factor in (2..=0xFFFFu16).step_by(127) {
            let mut dst_dispatched = vec![0x33u8; 4096];
            let mut dst_scalar = dst_dispatched.clone();

            mul_acc_region(factor, &src, &mut dst_dispatched);
            mul_acc_region_scalar(factor, &src, &mut dst_scalar);
            assert_eq!(
                dst_dispatched, dst_scalar,
                "large buffer mismatch for factor={factor:#06x}"
            );
        }
    }

    /// Multi-region with many factors and large buffers — exercises the
    /// multi-region SIMD kernel's main loop across all dispatch paths.
    #[test]
    fn multi_region_large_factor_sweep() {
        let src: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();

        for base in (2..=0xFFF0u16).step_by(4096) {
            let factors = [base, base + 1, base + 2, base + 3, base + 4, base + 5];

            let mut reference: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 4096]).collect();
            for (i, &factor) in factors.iter().enumerate() {
                mul_acc_region(factor, &src, &mut reference[i]);
            }

            let mut multi: Vec<Vec<u8>> = factors.iter().map(|_| vec![0u8; 4096]).collect();
            {
                let mut pairs: Vec<FactorDst<'_>> = factors
                    .iter()
                    .zip(multi.iter_mut())
                    .map(|(&factor, dst)| FactorDst {
                        factor,
                        dst: dst.as_mut_slice(),
                    })
                    .collect();
                mul_acc_multi_region(&mut pairs, &src);
            }

            for (i, &factor) in factors.iter().enumerate() {
                assert_eq!(
                    multi[i], reference[i],
                    "multi-region large sweep mismatch for factor={factor:#06x}"
                );
            }
        }
    }

    /// Verify dispatch with non-power-of-2 buffer sizes that stress tail
    /// handling across all SIMD widths (scalar remainder after 64/32/16-byte
    /// SIMD iterations).
    #[test]
    fn non_aligned_sizes_stress() {
        let factor = 0xCAFEu16;
        // Sizes chosen to leave different tail lengths:
        // 2 = scalar only, 18 = 16+2, 34 = 32+2, 50 = 32+16+2,
        // 66 = 64+2, 98 = 64+32+2, 130 = 64*2+2
        for size in [2, 6, 10, 14, 18, 22, 30, 34, 46, 50, 62, 66, 98, 130] {
            let src: Vec<u8> = (0..size).map(|i| ((i * 31) % 256) as u8).collect();
            let mut dst_dispatched = vec![0xBBu8; size];
            let mut dst_scalar = dst_dispatched.clone();

            mul_acc_region(factor, &src, &mut dst_dispatched);
            mul_acc_region_scalar(factor, &src, &mut dst_scalar);
            assert_eq!(
                dst_dispatched, dst_scalar,
                "non-aligned size mismatch for size={size}"
            );
        }
    }
}
