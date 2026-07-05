/// Thin wrapper around `crc32fast::Hasher` for streaming CRC32 computation
/// during yEnc decode.
///
/// On x86_64 CPUs with AVX2 + VPCLMULQDQ, large updates may be folded with a
/// 256-bit carry-less multiply path derived from rapidyenc's zlib-ng based CRC
/// folding implementation. `crc32fast` remains the resident state and the
/// fallback/head-tail path, so externally visible CRC semantics stay identical.
#[derive(Clone)]
pub struct Crc32 {
    hasher: crc32fast::Hasher,
    #[cfg(target_arch = "x86_64")]
    use_vpclmul: bool,
}

impl Crc32 {
    #[cfg(target_arch = "x86_64")]
    const VPCLMUL_MIN_UPDATE: usize = 256;

    /// Create a new CRC32 hasher.
    pub fn new() -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            #[cfg(target_arch = "x86_64")]
            use_vpclmul: x86_vpclmul::available(),
        }
    }

    /// Feed a chunk of decoded bytes into the hasher.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        #[cfg(target_arch = "x86_64")]
        if self.use_vpclmul && data.len() >= Self::VPCLMUL_MIN_UPDATE {
            let init = self.hasher.clone().finalize();
            let crc = unsafe { x86_vpclmul::update(init, data) };
            self.hasher = crc32fast::Hasher::new_with_initial(crc);
            return;
        }

        self.hasher.update(data);
    }

    /// Finalize and return the CRC32 value. Consumes the hasher.
    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }

    /// Get the current CRC32 value without consuming this wrapper.
    pub fn current(&self) -> u32 {
        self.hasher.clone().finalize()
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

#[cfg(target_arch = "x86_64")]
mod x86_vpclmul {
    #![allow(unsafe_op_in_unsafe_fn)]

    use std::arch::x86_64::*;
    use std::sync::OnceLock;

    pub(super) fn available() -> bool {
        static AVAILABLE: OnceLock<bool> = OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            is_x86_feature_detected!("avx2")
                && is_x86_feature_detected!("pclmulqdq")
                && is_x86_feature_detected!("sse4.1")
                && is_x86_feature_detected!("vpclmulqdq")
        })
    }

    #[target_feature(enable = "avx2,pclmulqdq,sse4.1,vpclmulqdq")]
    pub(super) unsafe fn update(initial: u32, data: &[u8]) -> u32 {
        unsafe { crc_fold_256(initial, data) }
    }

    #[inline(always)]
    unsafe fn loadu256(data: &[u8]) -> __m256i {
        debug_assert!(data.len() >= 32);
        unsafe { _mm256_loadu_si256(data.as_ptr() as *const __m256i) }
    }

    #[inline(always)]
    unsafe fn load_partial256(data: &[u8]) -> __m256i {
        debug_assert!(data.len() < 32);
        let mut tmp = [0u8; 32];
        tmp[..data.len()].copy_from_slice(data);
        unsafe { _mm256_loadu_si256(tmp.as_ptr() as *const __m256i) }
    }

    #[inline(always)]
    unsafe fn zext128_256(value: __m128i) -> __m256i {
        unsafe { _mm256_inserti128_si256::<0>(_mm256_setzero_si256(), value) }
    }

    #[inline(always)]
    unsafe fn broadcast128(value: __m128i) -> __m256i {
        let out = _mm256_castsi128_si256(value);
        unsafe { _mm256_inserti128_si256::<1>(out, value) }
    }

    #[inline(always)]
    unsafe fn xor3_128(a: __m128i, b: __m128i, c: __m128i) -> __m128i {
        unsafe { _mm_xor_si128(_mm_xor_si128(a, b), c) }
    }

    #[inline(always)]
    unsafe fn setr_epi32(a: u32, b: u32, c: u32, d: u32) -> __m128i {
        unsafe { _mm_set_epi32(d as i32, c as i32, b as i32, a as i32) }
    }

    #[inline(always)]
    unsafe fn do_one_fold(src: __m256i, data: __m256i) -> __m256i {
        let fold4 = _mm256_set_epi32(
            0x0000_0001u32 as i32,
            0x5444_2bd4u32 as i32,
            0x0000_0001u32 as i32,
            0xc6e4_1596u32 as i32,
            0x0000_0001u32 as i32,
            0x5444_2bd4u32 as i32,
            0x0000_0001u32 as i32,
            0xc6e4_1596u32 as i32,
        );
        unsafe {
            _mm256_xor_si256(
                _mm256_xor_si256(data, _mm256_clmulepi64_epi128::<0x01>(src, fold4)),
                _mm256_clmulepi64_epi128::<0x10>(src, fold4),
            )
        }
    }

    #[inline(always)]
    unsafe fn partial_fold(len: usize, crc0: &mut __m256i, crc1: &mut __m256i, crc_part: __m256i) {
        debug_assert!(len < 32);
        const ROT_TABLE: [u8; 32] = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ];

        let shuf128 =
            unsafe { _mm_loadu_si128(ROT_TABLE.as_ptr().add(len & 15) as *const __m128i) };
        let shuf = unsafe { broadcast128(shuf128) };
        let mask = _mm256_cmpgt_epi8(shuf, _mm256_set1_epi8(15));

        *crc0 = _mm256_shuffle_epi8(*crc0, shuf);
        *crc1 = _mm256_shuffle_epi8(*crc1, shuf);
        let crc_part = _mm256_shuffle_epi8(crc_part, shuf);

        let mut crc_out = _mm256_permute2x128_si256::<0x08>(*crc0, *crc0);
        let crc01;
        let crc1p;
        if len >= 16 {
            crc_out = _mm256_blendv_epi8(crc_out, *crc0, mask);
            crc01 = *crc1;
            crc1p = crc_part;
            *crc0 = _mm256_permute2x128_si256::<0x21>(*crc0, *crc1);
            *crc1 = _mm256_permute2x128_si256::<0x21>(*crc1, crc_part);
        } else {
            crc_out = _mm256_and_si256(crc_out, mask);
            crc01 = _mm256_permute2x128_si256::<0x21>(*crc0, *crc1);
            crc1p = _mm256_permute2x128_si256::<0x21>(*crc1, crc_part);
        }

        *crc0 = _mm256_blendv_epi8(*crc0, crc01, mask);
        *crc1 = _mm256_blendv_epi8(*crc1, crc1p, mask);
        *crc1 = unsafe { do_one_fold(crc_out, *crc1) };
    }

    #[inline(always)]
    unsafe fn crc_fold_256(initial: u32, mut data: &[u8]) -> u32 {
        if data.is_empty() {
            return initial;
        }

        let xmm_t0 = unsafe {
            _mm_clmulepi64_si128(
                _mm_cvtsi32_si128((!initial) as i32),
                _mm_cvtsi32_si128(0xdfde_d7ecu32 as i32),
                0,
            )
        };
        let mut crc0 = unsafe { zext128_256(xmm_t0) };
        let mut crc1 = _mm256_setzero_si256();

        if data.len() < 32 {
            let part = unsafe { load_partial256(data) };
            unsafe { partial_fold(data.len(), &mut crc0, &mut crc1, part) };
        } else {
            while data.len() >= 64 {
                crc0 = unsafe { do_one_fold(crc0, loadu256(data)) };
                crc1 = unsafe { do_one_fold(crc1, loadu256(&data[32..])) };
                data = &data[64..];
            }

            if data.len() >= 32 {
                let old = crc1;
                crc1 = unsafe { do_one_fold(crc0, loadu256(data)) };
                crc0 = old;
                data = &data[32..];
            }

            if !data.is_empty() {
                let part = unsafe { load_partial256(data) };
                unsafe { partial_fold(data.len(), &mut crc0, &mut crc1, part) };
            }
        }

        let mask = _mm_set_epi32(-1, -1, -1, 0);
        let mut xmm_crc0 = _mm256_castsi256_si128(crc0);
        let mut xmm_crc1 = _mm256_extracti128_si256::<1>(crc0);
        let mut xmm_crc2 = _mm256_castsi256_si128(crc1);
        let mut xmm_crc3 = _mm256_extracti128_si256::<1>(crc1);

        let mut fold = unsafe { setr_epi32(0xccaa_009e, 0x0000_0000, 0x7519_97d0, 0x0000_0001) };
        let tmp0 = _mm_clmulepi64_si128(xmm_crc0, fold, 0x10);
        xmm_crc0 = _mm_clmulepi64_si128(xmm_crc0, fold, 0x01);
        xmm_crc1 = unsafe { xor3_128(xmm_crc1, tmp0, xmm_crc0) };

        let tmp1 = _mm_clmulepi64_si128(xmm_crc1, fold, 0x10);
        xmm_crc1 = _mm_clmulepi64_si128(xmm_crc1, fold, 0x01);
        xmm_crc2 = unsafe { xor3_128(xmm_crc2, tmp1, xmm_crc1) };

        let tmp2 = _mm_clmulepi64_si128(xmm_crc2, fold, 0x10);
        xmm_crc2 = _mm_clmulepi64_si128(xmm_crc2, fold, 0x01);
        xmm_crc3 = unsafe { xor3_128(xmm_crc3, tmp2, xmm_crc2) };

        fold = unsafe { setr_epi32(0xccaa_009e, 0x0000_0000, 0x63cd_6124, 0x0000_0001) };
        xmm_crc0 = xmm_crc3;
        xmm_crc3 = _mm_clmulepi64_si128(xmm_crc3, fold, 0);
        xmm_crc0 = _mm_srli_si128::<8>(xmm_crc0);
        xmm_crc3 = _mm_xor_si128(xmm_crc3, xmm_crc0);

        xmm_crc0 = xmm_crc3;
        xmm_crc3 = _mm_slli_si128::<4>(xmm_crc3);
        xmm_crc3 = _mm_clmulepi64_si128(xmm_crc3, fold, 0x10);
        xmm_crc0 = _mm_and_si128(xmm_crc0, mask);
        xmm_crc3 = _mm_xor_si128(xmm_crc3, xmm_crc0);

        fold = unsafe { setr_epi32(0xf701_1641, 0x0000_0000, 0xdb71_0640, 0x0000_0001) };
        xmm_crc1 = xmm_crc3;
        xmm_crc3 = _mm_clmulepi64_si128(xmm_crc3, fold, 0);
        xmm_crc3 = _mm_clmulepi64_si128(xmm_crc3, fold, 0x10);
        xmm_crc1 = _mm_xor_si128(xmm_crc1, mask);
        xmm_crc3 = _mm_xor_si128(xmm_crc3, xmm_crc1);

        _mm_extract_epi32::<2>(xmm_crc3) as u32
    }

    #[cfg(test)]
    pub(super) fn test_update_forced(initial: u32, data: &[u8]) -> Option<u32> {
        available().then(|| unsafe { update(initial, data) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn crc32_matches_crc32fast_across_splits() {
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
            let expected = crc32fast::hash(input);

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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn crc32_forced_vpclmul_matches_crc32fast() {
        let mut data = Vec::with_capacity(8192 + 31);
        for idx in 0..data.capacity() {
            data.push(((idx * 31 + 17) & 0xff) as u8);
        }

        for offset in 0..32 {
            for len in [
                0usize, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257, 511, 512, 513, 4096, 8192,
            ] {
                let Some(input) = data.get(offset..offset + len) else {
                    continue;
                };
                let Some(actual) = x86_vpclmul::test_update_forced(0, input) else {
                    return;
                };
                assert_eq!(actual, crc32fast::hash(input), "offset {offset} len {len}");
            }
        }
    }
}
