use super::*;

#[cfg(any(test, not(target_arch = "aarch64")))]
pub(super) fn decode_kernel_scalar(
    input: &[u8],
    output: &mut [u8],
    state: &mut KernelState,
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
) -> Result<KernelOutcome, YencError> {
    let mut src = 0usize;
    let mut dst = 0usize;
    let mode = DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    };

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

pub(super) fn decode_scalar_step(
    input: &[u8],
    src: &mut usize,
    output: &mut [u8],
    dst: &mut usize,
    state: &mut KernelState,
    mode: DecodeStepMode,
) -> Result<bool, YencError> {
    let DecodeStepMode {
        dot_unstuffing,
        preserve_pending,
        search_end,
    } = mode;

    loop {
        if *src >= input.len() {
            return Ok(false);
        }

        match state.state {
            DecoderState::Eq => {
                let byte = input[*src];
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::CrLfEq => {
                let byte = input[*src];
                if search_end && dot_unstuffing && byte == b'y' {
                    *src += 1;
                    state.state = DecoderState::None;
                    state.end = DecodeEnd::Control;
                    return Ok(false);
                }
                write_decoded(output, dst, byte.wrapping_sub(106))?;
                *src += 1;
                state.state = if dot_unstuffing && byte == b'\r' {
                    DecoderState::Cr
                } else {
                    DecoderState::None
                };
                return Ok(true);
            }
            DecoderState::Cr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLfDot => {
                let byte = input[*src];
                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = if search_end {
                            DecoderState::CrLfDotCr
                        } else {
                            DecoderState::Cr
                        };
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' if dot_unstuffing && search_end => {
                        *src += 1;
                        state.state = DecoderState::CrLfEq;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = DecoderState::Eq;
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    b'.' => {
                        write_decoded(output, dst, b'.'.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    _ => {
                        state.state = DecoderState::None;
                    }
                }
            }
            DecoderState::CrLfDotCr => {
                if input[*src] == b'\n' {
                    *src += 1;
                    state.state = DecoderState::CrLf;
                    if search_end {
                        state.end = DecodeEnd::Article;
                        return Ok(false);
                    }
                    return Ok(true);
                }
                state.state = DecoderState::None;
            }
            DecoderState::CrLf | DecoderState::None => {
                let at_line_start = state.state == DecoderState::CrLf;
                let byte = input[*src];

                if dot_unstuffing && at_line_start && byte == b'.' {
                    *src += 1;
                    state.state = DecoderState::CrLfDot;
                    return Ok(true);
                }

                match byte {
                    b'\r' => {
                        *src += 1;
                        state.state = DecoderState::Cr;
                        return Ok(true);
                    }
                    b'\n' => {
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                    b'=' => {
                        *src += 1;
                        if *src >= input.len() {
                            if preserve_pending {
                                state.state = if dot_unstuffing && at_line_start {
                                    DecoderState::CrLfEq
                                } else {
                                    DecoderState::Eq
                                };
                                return Ok(false);
                            }
                            return Err(YencError::MalformedEscape(src.saturating_sub(1)));
                        }
                        if search_end && dot_unstuffing && at_line_start && input[*src] == b'y' {
                            *src += 1;
                            state.state = DecoderState::None;
                            state.end = DecodeEnd::Control;
                            return Ok(false);
                        }
                        let escaped = input[*src];
                        write_decoded(output, dst, escaped.wrapping_sub(106))?;
                        *src += 1;
                        state.state = if dot_unstuffing && escaped == b'\r' {
                            DecoderState::Cr
                        } else {
                            DecoderState::None
                        };
                        return Ok(true);
                    }
                    _ => {
                        write_decoded(output, dst, byte.wrapping_sub(42))?;
                        *src += 1;
                        state.state = DecoderState::None;
                        return Ok(true);
                    }
                }
            }
        }
    }
}

#[cfg(target_arch = "riscv64")]
#[inline]
pub(super) fn decode_normal_run_riscv64(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    // RVV runtime dispatch is not exposed through stable Rust's portable
    // detection surface yet, so RISC-V stays on the scalar kernel for now.
    decode_normal_run_scalar(input, start, output, dst_start)
}

/// Scalar fallback: process one byte at a time.
#[inline]
pub(super) fn decode_normal_run_scalar(
    input: &[u8],
    start: usize,
    output: &mut [u8],
    dst_start: usize,
) -> (usize, usize) {
    let max_len = input
        .len()
        .saturating_sub(start)
        .min(output.len().saturating_sub(dst_start));
    let input = &input[start..start + max_len];
    let run_len = memchr::memchr3(b'=', b'\r', b'\n', input).unwrap_or(input.len());
    let output = &mut output[dst_start..dst_start + run_len];

    let mut i = 0usize;
    while i + 8 <= run_len {
        output[i] = input[i].wrapping_sub(42);
        output[i + 1] = input[i + 1].wrapping_sub(42);
        output[i + 2] = input[i + 2].wrapping_sub(42);
        output[i + 3] = input[i + 3].wrapping_sub(42);
        output[i + 4] = input[i + 4].wrapping_sub(42);
        output[i + 5] = input[i + 5].wrapping_sub(42);
        output[i + 6] = input[i + 6].wrapping_sub(42);
        output[i + 7] = input[i + 7].wrapping_sub(42);
        i += 8;
    }
    while i < run_len {
        output[i] = input[i].wrapping_sub(42);
        i += 1;
    }

    (run_len, run_len)
}
