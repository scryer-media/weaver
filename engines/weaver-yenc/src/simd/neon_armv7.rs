use super::*;

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) unsafe fn decode_kernel_arm_neon(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    unsafe {
        decode_kernel_simd32(
            input,
            output,
            state,
            dot_unstuffing,
            preserve_pending,
            search_end,
            try_decode_arm_neon_block,
        )
    }
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) type DecodeBlock32 = unsafe fn(
    &[u8],
    usize,
    &mut [u8],
    &mut usize,
    &mut KernelState,
    bool,
) -> Result<Option<usize>, YencError>;

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) unsafe fn decode_kernel_simd32(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
    block: DecodeBlock32,
) -> Result<KernelOutcome, YencError> {
    const WIDTH: usize = 32;

    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };
    let tail_buffer = if dot_unstuffing {
        WIDTH - 1 + 4
    } else {
        WIDTH - 1
    };
    let simd_limit = input.len().saturating_sub(tail_buffer);

    if input.len() > WIDTH * 2 {
        while (!search_end || state.end == DecodeEnd::None) && src + WIDTH <= simd_limit {
            if let Some(consumed) =
                unsafe { block(input, src, output, &mut dst, state, dot_unstuffing)? }
            {
                src += consumed;
                continue;
            }

            if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
                break;
            }
        }
    }

    while (!search_end || state.end == DecodeEnd::None) && src < input.len() {
        if !decode_scalar_step(input, &mut src, output, &mut dst, state, mode)? {
            break;
        }
    }

    Ok(KernelOutcome {
        consumed: src,
        written: dst,
        end: state.end.into(),
    })
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) unsafe fn try_decode_arm_neon_block(
    input: &[u8],
    src: usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    dot_unstuffing: bool,
) -> Result<Option<usize>, YencError> {
    use std::arch::arm::*;

    if input.len().saturating_sub(src) < 32 || output.len().saturating_sub(*dst) < 32 {
        return Ok(None);
    }

    if !matches!(state.state, DecoderState::None | DecoderState::CrLf) {
        return Ok(None);
    }

    let a = unsafe { vld1q_u8(input.as_ptr().add(src)) };
    let b = unsafe { vld1q_u8(input.as_ptr().add(src + 16)) };
    let eq_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'=')) };
    let eq_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'=')) };
    let cr_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\r')) };
    let cr_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\r')) };
    let lf_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'\n')) };
    let lf_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'\n')) };
    let dot_a = unsafe { vceqq_u8(a, vdupq_n_u8(b'.')) };
    let dot_b = unsafe { vceqq_u8(b, vdupq_n_u8(b'.')) };
    let eq = unsafe { arm_neon_compare_mask32([eq_a, eq_b]) };
    let cr = unsafe { arm_neon_compare_mask32([cr_a, cr_b]) };
    let lf = unsafe { arm_neon_compare_mask32([lf_a, lf_b]) };
    let dot = unsafe { arm_neon_compare_mask32([dot_a, dot_b]) };

    let fixed_eq = fix_eq_mask(eq, eq << 1);
    if fixed_eq & (1u64 << 31) != 0 {
        return Ok(None);
    }

    let escaped = fixed_eq << 1;
    let raw_cr = cr & !escaped;
    let raw_lf = lf & !escaped;
    let raw_breaks = raw_cr | raw_lf;
    // NNTP line boundaries exist in the raw stream even when yEnc escaped the
    // '\r' (the scalar machine re-enters Cr after an escaped CR when
    // dot-unstuffing), so pair detection uses the unmasked '\r' bits.
    let pair_cr = if dot_unstuffing { cr } else { raw_cr };
    let crlf = pair_cr & (lf >> 1);
    let line_start = if state.state == DecoderState::CrLf {
        1
    } else {
        0
    } | (crlf << 2);
    let dot_start = if dot_unstuffing {
        dot & !escaped & line_start
    } else {
        0
    };

    if dot_start & (1u64 << 31) != 0 {
        return Ok(None);
    }

    let dot_before_break = dot_start & (raw_breaks >> 1);
    let dot_before_eq = dot_start & (eq >> 1);
    let line_start_eq = if dot_unstuffing { eq & line_start } else { 0 };
    if dot_before_break != 0 || dot_before_eq != 0 || line_start_eq != 0 {
        return Ok(None);
    }

    let skip = fixed_eq | raw_breaks | dot_start;
    let sub42 = unsafe { vdupq_n_u8(42u8.wrapping_neg()) };

    if skip == 0 {
        unsafe {
            vst1q_u8(output.as_mut_ptr().add(*dst), vaddq_u8(a, sub42));
            vst1q_u8(output.as_mut_ptr().add(*dst + 16), vaddq_u8(b, sub42));
        }
        *dst += 32;
        state.state = DecoderState::None;
        return Ok(Some(32));
    }

    let keep = 32 - skip.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let [decoded_a, decoded_b] = unsafe { arm_neon_decode_with_escape_mask([a, b], escaped) };
    unsafe { compact_store_8_arm_neon(vget_low_u8(decoded_a), (skip & 0xff) as u8, output, dst)? };
    unsafe {
        compact_store_8_arm_neon(
            vget_high_u8(decoded_a),
            ((skip >> 8) & 0xff) as u8,
            output,
            dst,
        )?
    };
    unsafe {
        compact_store_8_arm_neon(
            vget_low_u8(decoded_b),
            ((skip >> 16) & 0xff) as u8,
            output,
            dst,
        )?
    };
    unsafe {
        compact_store_8_arm_neon(
            vget_high_u8(decoded_b),
            ((skip >> 24) & 0xff) as u8,
            output,
            dst,
        )?
    };

    let output_mask = (!skip) & 0xffff_ffff;
    state.state = final_state_after_block(raw_breaks, raw_cr, crlf << 1, output_mask);
    Ok(Some(32))
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
// ARMv7 companion to the 32-byte NEON decoder mask path.
pub(super) unsafe fn arm_neon_compare_mask32(vectors: [std::arch::arm::uint8x16_t; 2]) -> u64 {
    use std::arch::arm::*;

    let mut mask = 0u64;
    for (chunk, vector) in vectors.into_iter().enumerate() {
        let mut lanes = [0u8; 16];
        unsafe { vst1q_u8(lanes.as_mut_ptr(), vector) };
        for (lane, value) in lanes.into_iter().enumerate() {
            if value != 0 {
                mask |= 1u64 << (chunk * 16 + lane);
            }
        }
    }
    mask
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
// ARMv7 companion to the NEON maskEqTemp/vtbl escaped-byte offset path.
pub(super) unsafe fn arm_neon_decode_with_escape_mask(
    vectors: [std::arch::arm::uint8x16_t; 2],
    escaped: u64,
) -> [std::arch::arm::uint8x16_t; 2] {
    use std::arch::arm::*;

    let mut offset_a = [42u8; 16];
    let mut offset_b = [42u8; 16];
    for lane in 0..16usize {
        if escaped & (1u64 << lane) != 0 {
            offset_a[lane] = 106;
        }
        if escaped & (1u64 << (lane + 16)) != 0 {
            offset_b[lane] = 106;
        }
    }

    let offset_a = unsafe { vld1q_u8(offset_a.as_ptr()) };
    let offset_b = unsafe { vld1q_u8(offset_b.as_ptr()) };
    [unsafe { vsubq_u8(vectors[0], offset_a) }, unsafe {
        vsubq_u8(vectors[1], offset_b)
    }]
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) fn compact_table_8() -> &'static [[u8; 8]; 256] {
    use std::sync::OnceLock;

    static TABLE: OnceLock<Box<[[u8; 8]; 256]>> = OnceLock::new();
    TABLE
        .get_or_init(|| {
            let mut table = [[0xffu8; 8]; 256];
            for (mask, row) in table.iter_mut().enumerate() {
                let mut out = 0usize;
                for lane in 0..8usize {
                    if mask & (1 << lane) == 0 {
                        row[out] = lane as u8;
                        out += 1;
                    }
                }
            }
            Box::new(table)
        })
        .as_ref()
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) unsafe fn compact_store_8_arm_neon(
    decoded: std::arch::arm::uint8x8_t,
    skip_mask: u8,
    output: &mut [u8],
    dst: &mut usize,
) -> Result<(), YencError> {
    use std::arch::arm::*;

    let keep = 8 - skip_mask.count_ones() as usize;
    if output.len().saturating_sub(*dst) < keep {
        return Err(YencError::BufferTooSmall {
            needed: *dst + keep,
            available: output.len(),
        });
    }

    let shuffle = unsafe { vld1_u8(compact_table_8()[skip_mask as usize].as_ptr()) };
    let packed = unsafe { vtbl1_u8(decoded, shuffle) };
    let mut tmp = [0u8; 8];
    unsafe { vst1_u8(tmp.as_mut_ptr(), packed) };
    output[*dst..*dst + keep].copy_from_slice(&tmp[..keep]);
    *dst += keep;
    Ok(())
}

#[cfg(all(target_arch = "arm", target_feature = "neon"))]
pub(super) unsafe fn decode_normal_run_arm_neon(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    use std::arch::arm::*;

    let mut src = start;
    let mut dst = dst_start;

    unsafe {
        let special_eq = vdupq_n_u8(b'=');
        let special_cr = vdupq_n_u8(b'\r');
        let special_lf = vdupq_n_u8(b'\n');
        let sub42 = vdupq_n_u8(42u8.wrapping_neg());

        while src + 16 <= input.len() && dst + 16 <= output.len() {
            let chunk = vld1q_u8(input.as_ptr().add(src));
            let any_special = vorrq_u8(
                vorrq_u8(vceqq_u8(chunk, special_eq), vceqq_u8(chunk, special_cr)),
                vceqq_u8(chunk, special_lf),
            );

            let mut lanes = [0u8; 16];
            vst1q_u8(lanes.as_mut_ptr(), any_special);
            if let Some(count) = lanes.iter().position(|&lane| lane != 0) {
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
