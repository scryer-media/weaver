//! SIMD and lookup-table accelerated yEnc decoding.
//!
//! The key insight: most yEnc bytes are "normal" (not `=`, `\r`, `\n`, or `.`
//! at line start). For runs of normal bytes, we can batch-subtract 42 using
//! SIMD or an unrolled loop.

/// Decode a run of "normal" yEnc bytes (no special characters) by subtracting 42.
///
/// Scans `input` from position `start`, decoding bytes into `output` at position
/// `dst_start`. Stops at the first special character (`=`, `\r`, `\n`) or end of input.
///
/// Returns `(bytes_consumed_from_input, bytes_written_to_output)`.
#[inline]
pub fn decode_normal_run(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    // Use platform-specific SIMD when available, fall back to scalar.
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { decode_normal_run_avx2(input, start, output, dst_start) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { decode_normal_run_sse2(input, start, output, dst_start) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // NEON is always available on aarch64.
        return unsafe { decode_normal_run_neon(input, start, output, dst_start) };
    }

    // Scalar fallback for other architectures.
    #[allow(unreachable_code)]
    decode_normal_run_scalar(input, start, output, dst_start)
}

/// Scalar fallback: process one byte at a time.
#[inline]
fn decode_normal_run_scalar(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    let mut src = start;
    let mut dst = dst_start;

    while src < input.len() && dst < output.len() {
        let byte = input[src];
        // Stop at special characters.
        if byte == b'=' || byte == b'\r' || byte == b'\n' {
            break;
        }
        output[dst] = byte.wrapping_sub(42);
        src += 1;
        dst += 1;
    }

    (src - start, dst - dst_start)
}

/// SSE2 implementation: process 16 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn decode_normal_run_sse2(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::x86_64::*;

    let mut src = start;
    let mut dst = dst_start;

    let special_eq = _mm_set1_epi8(b'=' as i8);
    let special_cr = _mm_set1_epi8(b'\r' as i8);
    let special_lf = _mm_set1_epi8(b'\n' as i8);
    let sub42 = _mm_set1_epi8(42i8.wrapping_neg());

    // Process 16 bytes at a time.
    while src + 16 <= input.len() && dst + 16 <= output.len() {
        let chunk = _mm_loadu_si128(input.as_ptr().add(src) as *const __m128i);

        // Check for special characters.
        let eq_mask = _mm_cmpeq_epi8(chunk, special_eq);
        let cr_mask = _mm_cmpeq_epi8(chunk, special_cr);
        let lf_mask = _mm_cmpeq_epi8(chunk, special_lf);
        let any_special = _mm_or_si128(_mm_or_si128(eq_mask, cr_mask), lf_mask);

        let mask = _mm_movemask_epi8(any_special);
        if mask != 0 {
            // Found a special character. Decode the clean prefix before it.
            let count = mask.trailing_zeros() as usize;
            if count > 0 {
                let decoded = _mm_add_epi8(chunk, sub42);
                let mut tmp = [0u8; 16];
                _mm_storeu_si128(tmp.as_mut_ptr() as *mut __m128i, decoded);
                output[dst..dst + count].copy_from_slice(&tmp[..count]);
                src += count;
                dst += count;
            }
            break;
        }

        // All 16 bytes are normal -- subtract 42 and store.
        let decoded = _mm_add_epi8(chunk, sub42);
        _mm_storeu_si128(output.as_mut_ptr().add(dst) as *mut __m128i, decoded);
        src += 16;
        dst += 16;
    }

    // Handle remaining bytes with scalar.
    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}

/// AVX2 implementation: process 32 bytes at a time.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn decode_normal_run_avx2(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::x86_64::*;

    let mut src = start;
    let mut dst = dst_start;

    let special_eq = _mm256_set1_epi8(b'=' as i8);
    let special_cr = _mm256_set1_epi8(b'\r' as i8);
    let special_lf = _mm256_set1_epi8(b'\n' as i8);
    let sub42 = _mm256_set1_epi8(42i8.wrapping_neg());

    while src + 32 <= input.len() && dst + 32 <= output.len() {
        let chunk = _mm256_loadu_si256(input.as_ptr().add(src) as *const __m256i);

        let eq_mask = _mm256_cmpeq_epi8(chunk, special_eq);
        let cr_mask = _mm256_cmpeq_epi8(chunk, special_cr);
        let lf_mask = _mm256_cmpeq_epi8(chunk, special_lf);
        let any_special = _mm256_or_si256(_mm256_or_si256(eq_mask, cr_mask), lf_mask);

        let mask = _mm256_movemask_epi8(any_special);
        if mask != 0 {
            let count = mask.trailing_zeros() as usize;
            if count > 0 {
                let decoded = _mm256_add_epi8(chunk, sub42);
                let mut tmp = [0u8; 32];
                _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, decoded);
                output[dst..dst + count].copy_from_slice(&tmp[..count]);
                src += count;
                dst += count;
            }
            break;
        }

        let decoded = _mm256_add_epi8(chunk, sub42);
        _mm256_storeu_si256(output.as_mut_ptr().add(dst) as *mut __m256i, decoded);
        src += 32;
        dst += 32;
    }

    // Tail: fall through to SSE2 for remaining bytes.
    let (extra_src, extra_dst) = unsafe { decode_normal_run_sse2(input, src, output, dst) };
    (src - start + extra_src, dst - dst_start + extra_dst)
}

/// NEON implementation for aarch64: process 16 bytes at a time.
#[cfg(target_arch = "aarch64")]
unsafe fn decode_normal_run_neon(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::aarch64::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = vdupq_n_u8(b'=');
        let special_cr = vdupq_n_u8(b'\r');
        let special_lf = vdupq_n_u8(b'\n');
        let sub42 = vdupq_n_u8(42u8.wrapping_neg()); // 214

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = vld1q_u8(input.as_ptr().add(src));

            let eq_mask = vceqq_u8(chunk, special_eq);
            let cr_mask = vceqq_u8(chunk, special_cr);
            let lf_mask = vceqq_u8(chunk, special_lf);
            let any_special = vorrq_u8(vorrq_u8(eq_mask, cr_mask), lf_mask);

            // Check if any lane is set.
            let max_val = vmaxvq_u8(any_special);
            if max_val != 0 {
                // Found a special character -- find the first one.
                let mut mask_bytes = [0u8; 16];
                vst1q_u8(mask_bytes.as_mut_ptr(), any_special);
                let count = mask_bytes.iter().position(|&b| b != 0).unwrap_or(16);

                if count > 0 {
                    let decoded = vaddq_u8(chunk, sub42);
                    let mut tmp = [0u8; 16];
                    vst1q_u8(tmp.as_mut_ptr(), decoded);
                    output[dst..dst + count].copy_from_slice(&tmp[..count]);
                    src += count;
                    dst += count;
                }
                break;
            }

            let decoded = vaddq_u8(chunk, sub42);
            vst1q_u8(output.as_mut_ptr().add(dst), decoded);
            src += 16;
            dst += 16;
        }
    }

    let (extra_src, extra_dst) = decode_normal_run_scalar(input, src, output, dst);
    (src - start + extra_src, dst - dst_start + extra_dst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_normal_run_basic() {
        let input = b"hello world";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 11);
        assert_eq!(written, 11);
        for i in 0..11 {
            assert_eq!(output[i], input[i].wrapping_sub(42));
        }
    }

    #[test]
    fn scalar_stops_at_equals() {
        let input = b"AB=CD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn scalar_stops_at_cr() {
        let input = b"AB\r\nCD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn scalar_stops_at_lf() {
        let input = b"AB\nCD";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run_scalar(input, 0, &mut output, 0);
        assert_eq!(consumed, 2);
        assert_eq!(written, 2);
    }

    #[test]
    fn decode_normal_run_dispatches() {
        // Tests the dispatch function on whatever platform we're on.
        let input: Vec<u8> = (0..256u16)
            .filter(|&b| b != b'=' as u16 && b != b'\r' as u16 && b != b'\n' as u16)
            .map(|b| b as u8)
            .collect();
        let mut output = vec![0u8; 256];
        let (consumed, written) = decode_normal_run(&input, 0, &mut output, 0);
        assert_eq!(consumed, input.len());
        assert_eq!(written, input.len());
        for i in 0..written {
            assert_eq!(output[i], input[i].wrapping_sub(42));
        }
    }

    #[test]
    fn normal_run_empty() {
        let input = b"";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);
        assert_eq!(consumed, 0);
        assert_eq!(written, 0);
    }

    #[test]
    fn normal_run_starts_with_special() {
        let input = b"=AB";
        let mut output = vec![0u8; 64];
        let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);
        assert_eq!(consumed, 0);
        assert_eq!(written, 0);
    }

    #[test]
    fn normal_run_long_input() {
        // Test with input longer than SIMD register width.
        let input: Vec<u8> = vec![b'A'; 1000];
        let mut output = vec![0u8; 1000];
        let (consumed, written) = decode_normal_run(&input, 0, &mut output, 0);
        assert_eq!(consumed, 1000);
        assert_eq!(written, 1000);
        assert!(output.iter().all(|&b| b == b'A'.wrapping_sub(42)));
    }
}
