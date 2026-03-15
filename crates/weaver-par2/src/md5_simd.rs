//! Multi-buffer MD5: compute up to 4 independent MD5 hashes simultaneously
//! using SIMD instructions.
//!
//! Standard MD5 processes one 64-byte block at a time through 64 rounds. By
//! packing 4 independent MD5 states into SIMD registers (128-bit), we process
//! 4 hashes for the cost of roughly 1. This is the same technique used by
//! par2cmdline-turbo's `md5mb-neon.h`.
//!
//! Used by `verify_slices` to batch 4 slice hashes per rayon task.

#[cfg(test)]
use md5::{Digest, Md5};

// MD5 initial values.
const MD5_A0: u32 = 0x6745_2301;
const MD5_B0: u32 = 0xEFCD_AB89;
const MD5_C0: u32 = 0x98BA_DCFE;
const MD5_D0: u32 = 0x1032_5476;

// MD5 per-round shift amounts.
const S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, // rounds 0-15
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, // rounds 16-31
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, // rounds 32-47
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, // rounds 48-63
];

// MD5 per-round constants (floor(2^32 * abs(sin(i+1)))).
const K: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
    0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
    0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
    0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
    0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
    0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
    0xeb86d391,
];

// MD5 per-round message schedule index.
const G: [usize; 64] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // rounds 0-15: g=i
    1, 6, 11, 0, 5, 10, 15, 4, 9, 14, 3, 8, 13, 2, 7, 12, // rounds 16-31: g=(5i+1)%16
    5, 8, 11, 14, 1, 4, 7, 10, 13, 0, 3, 6, 9, 12, 15, 2, // rounds 32-47: g=(3i+5)%16
    0, 7, 14, 5, 12, 3, 10, 1, 8, 15, 6, 13, 4, 11, 2, 9, // rounds 48-63: g=(7i)%16
];

/// Compute MD5 hashes of up to 4 input buffers simultaneously.
///
/// Each input is hashed independently. If `pad_to` is `Some(n)`, each input
/// is logically zero-padded to `n` bytes before finalizing (PAR2 last-slice
/// semantics). Returns one digest per input, in order.
pub fn md5_multi(inputs: &[&[u8]], pad_to: Option<u64>) -> Vec<[u8; 16]> {
    assert!(!inputs.is_empty() && inputs.len() <= 4);

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { md5_multi_neon(inputs, pad_to) };
    }

    #[allow(unreachable_code)]
    md5_multi_scalar(inputs, pad_to)
}

/// Scalar fallback: hash each input independently using `md5_pad` + scalar rounds.
fn md5_multi_scalar(inputs: &[&[u8]], pad_to: Option<u64>) -> Vec<[u8; 16]> {
    inputs
        .iter()
        .map(|inp| {
            let effective_len = match pad_to {
                Some(p) if p > inp.len() as u64 => p,
                _ => inp.len() as u64,
            };
            md5_single_scalar(inp, effective_len)
        })
        .collect()
}

/// Compute a single MD5 hash using the scalar round implementation.
fn md5_single_scalar(data: &[u8], effective_len: u64) -> [u8; 16] {
    let padded = md5_pad(data, effective_len);
    let num_blocks = padded.len() / 64;

    let mut a = MD5_A0;
    let mut b = MD5_B0;
    let mut c = MD5_C0;
    let mut d = MD5_D0;

    for block_idx in 0..num_blocks {
        let block = &padded[block_idx * 64..(block_idx + 1) * 64];
        let mut m = [0u32; 16];
        for w in 0..16 {
            m[w] = u32::from_le_bytes(block[w * 4..w * 4 + 4].try_into().unwrap());
        }

        let (oa, ob, oc, od) = (a, b, c, d);

        for r in 0..64 {
            let f = match r {
                0..16 => (b & c) | (!b & d),
                16..32 => (d & b) | (!d & c),
                32..48 => b ^ c ^ d,
                _ => c ^ (b | !d),
            };

            let tmp = f
                .wrapping_add(a)
                .wrapping_add(K[r])
                .wrapping_add(m[G[r]]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(tmp.rotate_left(S[r]));
        }

        a = a.wrapping_add(oa);
        b = b.wrapping_add(ob);
        c = c.wrapping_add(oc);
        d = d.wrapping_add(od);
    }

    let mut digest = [0u8; 16];
    digest[0..4].copy_from_slice(&a.to_le_bytes());
    digest[4..8].copy_from_slice(&b.to_le_bytes());
    digest[8..12].copy_from_slice(&c.to_le_bytes());
    digest[12..16].copy_from_slice(&d.to_le_bytes());
    digest
}

/// Apply MD5 padding to data: append 0x80, then zeros, then 64-bit length.
/// `effective_len` is the logical length (including any zero-padding before MD5 padding).
fn md5_pad(data: &[u8], effective_len: u64) -> Vec<u8> {
    let bit_len = effective_len * 8;

    // Start with actual data.
    let mut padded = Vec::with_capacity((effective_len as usize + 72) & !63);
    padded.extend_from_slice(data);

    // Zero-pad to effective_len if needed (PAR2 last-slice padding).
    if (data.len() as u64) < effective_len {
        padded.resize(effective_len as usize, 0);
    }

    // MD5 padding: 0x80 byte.
    padded.push(0x80);

    // Pad with zeros until length ≡ 56 (mod 64).
    while padded.len() % 64 != 56 {
        padded.push(0);
    }

    // Append original length in bits as 64-bit little-endian.
    padded.extend_from_slice(&bit_len.to_le_bytes());

    padded
}

// ---------------------------------------------------------------------------
// NEON kernel (aarch64): 4 independent MD5 hashes in uint32x4_t registers
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn md5_multi_neon(inputs: &[&[u8]], pad_to: Option<u64>) -> Vec<[u8; 16]> {
    use std::arch::aarch64::*;

    let n = inputs.len();

    // Determine effective length per lane.
    let effective_lens: Vec<u64> = inputs
        .iter()
        .map(|inp| {
            let raw = inp.len() as u64;
            match pad_to {
                Some(p) if p > raw => p,
                _ => raw,
            }
        })
        .collect();

    // Build padded messages.
    let padded: Vec<Vec<u8>> = (0..n)
        .map(|i| md5_pad(inputs[i], effective_lens[i]))
        .collect();

    // All lanes must have the same block count for SIMD processing.
    // If they differ, fall back to scalar.
    let block_counts: Vec<usize> = padded.iter().map(|p| p.len() / 64).collect();
    let uniform_blocks = block_counts.iter().all(|&c| c == block_counts[0]);
    if !uniform_blocks {
        return md5_multi_scalar(inputs, pad_to);
    }

    let num_blocks = block_counts[0];

    // Pad to 4 lanes (unused lanes replicate lane 0's data — we discard
    // their results, and they need the same block count).
    let lane_ptrs: [&[u8]; 4] = [
        &padded[0],
        if n > 1 { &padded[1] } else { &padded[0] },
        if n > 2 { &padded[2] } else { &padded[0] },
        if n > 3 { &padded[3] } else { &padded[0] },
    ];

    unsafe {
        let mut a = vdupq_n_u32(MD5_A0);
        let mut b = vdupq_n_u32(MD5_B0);
        let mut c = vdupq_n_u32(MD5_C0);
        let mut d = vdupq_n_u32(MD5_D0);

        for block_idx in 0..num_blocks {
            // Load and transpose: 16 words from each of 4 lanes.
            let mut m = [vdupq_n_u32(0); 16];
            let off = block_idx * 64;

            for w in 0..4 {
                // Load 4 consecutive u32 words from each lane (16 bytes each).
                let idx = w * 4;
                let w_off = idx * 4;

                let in0 = vreinterpretq_u32_u8(vld1q_u8(lane_ptrs[0].as_ptr().add(off + w_off)));
                let in1 = vreinterpretq_u32_u8(vld1q_u8(lane_ptrs[1].as_ptr().add(off + w_off)));
                let in2 = vreinterpretq_u32_u8(vld1q_u8(lane_ptrs[2].as_ptr().add(off + w_off)));
                let in3 = vreinterpretq_u32_u8(vld1q_u8(lane_ptrs[3].as_ptr().add(off + w_off)));

                // Transpose: zip pairs then combine halves.
                let z01 = vzipq_u32(in0, in1);
                let z23 = vzipq_u32(in2, in3);

                m[idx] = vcombine_u32(
                    vget_low_u32(z01.0),
                    vget_low_u32(z23.0),
                );
                m[idx + 1] = vcombine_u32(
                    vget_high_u32(z01.0),
                    vget_high_u32(z23.0),
                );
                m[idx + 2] = vcombine_u32(
                    vget_low_u32(z01.1),
                    vget_low_u32(z23.1),
                );
                m[idx + 3] = vcombine_u32(
                    vget_high_u32(z01.1),
                    vget_high_u32(z23.1),
                );
            }

            let oa = a;
            let ob = b;
            let oc = c;
            let od = d;

            // General rotate for variable shift amounts.
            #[inline(always)]
            fn neon_rotl(v: uint32x4_t, amount: u32) -> uint32x4_t {
                unsafe {
                    let left = vshlq_u32(v, vdupq_n_s32(amount as i32));
                    let right = vshlq_u32(v, vdupq_n_s32(-(32i32 - amount as i32)));
                    vorrq_u32(left, right)
                }
            }

            // Rounds 0-15: F(b,c,d) = (b & c) | (~b & d) = vbslq(b, c, d)
            for r in 0..16 {
                let f = vbslq_u32(b, c, d);
                let tmp = vaddq_u32(
                    vaddq_u32(a, f),
                    vaddq_u32(vdupq_n_u32(K[r]), m[G[r]]),
                );
                a = d;
                d = c;
                c = b;
                b = vaddq_u32(b, neon_rotl(tmp, S[r]));
            }

            // Rounds 16-31: G(b,c,d) = (d & b) | (~d & c) = vbslq(d, b, c)
            for r in 16..32 {
                let f = vbslq_u32(d, b, c);
                let tmp = vaddq_u32(
                    vaddq_u32(a, f),
                    vaddq_u32(vdupq_n_u32(K[r]), m[G[r]]),
                );
                a = d;
                d = c;
                c = b;
                b = vaddq_u32(b, neon_rotl(tmp, S[r]));
            }

            // Rounds 32-47: H(b,c,d) = b ^ c ^ d
            for r in 32..48 {
                let f = veorq_u32(veorq_u32(b, c), d);
                let tmp = vaddq_u32(
                    vaddq_u32(a, f),
                    vaddq_u32(vdupq_n_u32(K[r]), m[G[r]]),
                );
                a = d;
                d = c;
                c = b;
                b = vaddq_u32(b, neon_rotl(tmp, S[r]));
            }

            // Rounds 48-63: I(b,c,d) = c ^ (b | ~d)
            for r in 48..64 {
                let f = veorq_u32(c, vornq_u32(b, d));
                let tmp = vaddq_u32(
                    vaddq_u32(a, f),
                    vaddq_u32(vdupq_n_u32(K[r]), m[G[r]]),
                );
                a = d;
                d = c;
                c = b;
                b = vaddq_u32(b, neon_rotl(tmp, S[r]));
            }

            a = vaddq_u32(a, oa);
            b = vaddq_u32(b, ob);
            c = vaddq_u32(c, oc);
            d = vaddq_u32(d, od);
        }

        // Extract digests from SIMD lanes.
        let mut results = Vec::with_capacity(n);
        for lane in 0..n {
            let mut digest = [0u8; 16];
            digest[0..4].copy_from_slice(&extract_lane(a, lane).to_le_bytes());
            digest[4..8].copy_from_slice(&extract_lane(b, lane).to_le_bytes());
            digest[8..12].copy_from_slice(&extract_lane(c, lane).to_le_bytes());
            digest[12..16].copy_from_slice(&extract_lane(d, lane).to_le_bytes());
            results.push(digest);
        }

        results
    }
}

/// Extract a u32 from a specific lane of a uint32x4_t.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn extract_lane(v: std::arch::aarch64::uint32x4_t, lane: usize) -> u32 {
    unsafe {
        use std::arch::aarch64::*;
        match lane {
            0 => vgetq_lane_u32::<0>(v),
            1 => vgetq_lane_u32::<1>(v),
            2 => vgetq_lane_u32::<2>(v),
            3 => vgetq_lane_u32::<3>(v),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_md5(data: &[u8]) -> [u8; 16] {
        Md5::digest(data).into()
    }

    fn reference_md5_padded(data: &[u8], pad_to: u64) -> [u8; 16] {
        let mut padded = data.to_vec();
        if (padded.len() as u64) < pad_to {
            padded.resize(pad_to as usize, 0);
        }
        Md5::digest(&padded).into()
    }

    #[test]
    fn single_input_matches_reference() {
        let data = b"hello world";
        let result = md5_multi(&[data], None);
        assert_eq!(result[0], reference_md5(data));
    }

    #[test]
    fn four_inputs_match_reference() {
        let inputs: Vec<Vec<u8>> = (0..4)
            .map(|i| (0..256u32).map(|j| ((j * 7 + i * 13) % 256) as u8).collect())
            .collect();
        let refs: Vec<&[u8]> = inputs.iter().map(|v| v.as_slice()).collect();

        let results = md5_multi(&refs, None);
        for (i, input) in inputs.iter().enumerate() {
            assert_eq!(results[i], reference_md5(input), "mismatch for input {i}");
        }
    }

    #[test]
    fn variable_input_counts() {
        let data: Vec<Vec<u8>> = (0..4)
            .map(|i| vec![(i * 37 % 256) as u8; 100 + i * 50])
            .collect();

        for count in 1..=4 {
            let refs: Vec<&[u8]> = data[..count].iter().map(|v| v.as_slice()).collect();
            let results = md5_multi(&refs, None);
            assert_eq!(results.len(), count);
            for (i, input) in data[..count].iter().enumerate() {
                assert_eq!(
                    results[i],
                    reference_md5(input),
                    "mismatch for count={count}, input {i}"
                );
            }
        }
    }

    #[test]
    fn pad_to_semantics() {
        let data = b"short data";
        let pad_to = 128u64;

        let result = md5_multi(&[data], Some(pad_to));
        assert_eq!(result[0], reference_md5_padded(data, pad_to));
    }

    #[test]
    fn pad_to_multiple_inputs() {
        let inputs: Vec<Vec<u8>> = vec![
            vec![0xAA; 50],
            vec![0xBB; 64],
            vec![0xCC; 100],
            vec![0xDD; 128],
        ];
        let pad_to = 128u64;
        let refs: Vec<&[u8]> = inputs.iter().map(|v| v.as_slice()).collect();

        let results = md5_multi(&refs, Some(pad_to));
        for (i, input) in inputs.iter().enumerate() {
            assert_eq!(
                results[i],
                reference_md5_padded(input, pad_to),
                "pad_to mismatch for input {i} (len={})",
                input.len()
            );
        }
    }

    #[test]
    fn empty_input() {
        let data: &[u8] = b"";
        let result = md5_multi(&[data], None);
        assert_eq!(result[0], reference_md5(data));
    }

    #[test]
    fn exact_block_sizes() {
        for size in [64, 128, 192, 256] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let result = md5_multi(&[&data], None);
            assert_eq!(result[0], reference_md5(&data), "mismatch for size={size}");
        }
    }

    #[test]
    fn non_block_aligned_sizes() {
        for size in [1, 7, 55, 56, 63, 65, 100, 119, 120, 127, 129] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let result = md5_multi(&[&data], None);
            assert_eq!(result[0], reference_md5(&data), "mismatch for size={size}");
        }
    }

    #[test]
    fn large_input() {
        let data: Vec<u8> = (0..65536u32).map(|i| (i % 256) as u8).collect();
        let result = md5_multi(&[&data], None);
        assert_eq!(result[0], reference_md5(&data));
    }

    #[test]
    fn four_large_inputs() {
        let inputs: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                (0..8192u32)
                    .map(|j| ((j * 7 + i * 1337) % 256) as u8)
                    .collect()
            })
            .collect();
        let refs: Vec<&[u8]> = inputs.iter().map(|v| v.as_slice()).collect();

        let results = md5_multi(&refs, None);
        for (i, input) in inputs.iter().enumerate() {
            assert_eq!(results[i], reference_md5(input), "mismatch for input {i}");
        }
    }

    #[test]
    fn pad_to_with_exact_length_is_noop() {
        let data = vec![0xAB; 256];
        let result_no_pad = md5_multi(&[&data], None);
        let result_pad = md5_multi(&[&data], Some(256));
        assert_eq!(result_no_pad[0], result_pad[0]);
    }

    #[test]
    fn scalar_matches_neon() {
        // Ensure the scalar and dispatched paths produce identical results.
        let inputs: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                (0..500u32)
                    .map(|j| ((j * 11 + i * 97) % 256) as u8)
                    .collect()
            })
            .collect();
        let refs: Vec<&[u8]> = inputs.iter().map(|v| v.as_slice()).collect();

        let scalar = md5_multi_scalar(&refs, None);
        let dispatched = md5_multi(&refs, None);

        for i in 0..4 {
            assert_eq!(
                scalar[i], dispatched[i],
                "scalar vs dispatched mismatch for input {i}"
            );
        }
    }
}
