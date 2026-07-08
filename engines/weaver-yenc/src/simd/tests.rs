use super::*;
use crate::decode::DecodeState;

fn scalar_body_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> (Vec<u8>, KernelState) {
    let mut output = vec![0u8; input.len() + 64];
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let outcome =
        decode_kernel_scalar(input, &mut output, &mut state, dot_unstuffing, false, true).unwrap();
    output.truncate(outcome.written);
    (output, state)
}

#[cfg(target_arch = "x86_64")]
unsafe fn sse2_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        try_decode_sse2_block(
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("sse2 block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn ssse3_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    if !is_x86_feature_detected!("ssse3") {
        return None;
    }

    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        try_decode_ssse3_block(
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("ssse3 block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn sse41_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    if !(is_x86_feature_detected!("sse4.1") && is_x86_feature_detected!("ssse3")) {
        return None;
    }

    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        try_decode_sse41_block(
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("sse4.1 block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn avx_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    if !(is_x86_feature_detected!("avx")
        && is_x86_feature_detected!("popcnt")
        && is_x86_feature_detected!("sse4.1")
        && is_x86_feature_detected!("ssse3"))
    {
        return None;
    }

    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        try_decode_avx_block(
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("avx block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn avx2_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    if !is_x86_feature_detected!("avx2") {
        return None;
    }

    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(input.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(input.as_ptr().add(32) as *const __m256i);
        let specials = avx2_special_mask64(a, b);
        try_decode_avx2_block(
            a,
            b,
            specials,
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("avx2 block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn avx512_vbmi2_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    if !(is_x86_feature_detected!("avx512vbmi2")
        && is_x86_feature_detected!("avx512vl")
        && is_x86_feature_detected!("avx512bw")
        && is_x86_feature_detected!("avx512f")
        && is_x86_feature_detected!("avx2"))
    {
        return None;
    }

    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let consumed = unsafe {
        try_decode_avx512_vbmi2_block(
            input,
            0,
            &mut output,
            &mut dst,
            &mut state,
            dot_unstuffing,
            true,
        )
        .unwrap()
    }
    .expect("avx512-vbmi2 block should consume");
    assert_eq!(consumed, 64);
    output.truncate(dst);
    Some((output, state))
}

#[cfg(target_arch = "x86_64")]
unsafe fn assert_x86_blocks_produce(
    input: &[u8; 64],
    initial_state: DecoderState,
    dot_unstuffing: bool,
    expected_output: &[u8],
    expected_state: DecoderState,
) {
    let expected_state = KernelState {
        state: expected_state,
        ..KernelState::body()
    };

    let check = |name: &str, result: Option<(Vec<u8>, KernelState)>| {
        if let Some((actual_output, actual_state)) = result {
            assert_eq!(actual_output, expected_output, "{name} output");
            assert_eq!(actual_state, expected_state, "{name} state");
        }
    };

    check("sse2", unsafe {
        sse2_block_with_state(input, initial_state, dot_unstuffing)
    });
    check("ssse3", unsafe {
        ssse3_block_with_state(input, initial_state, dot_unstuffing)
    });
    check("sse4.1", unsafe {
        sse41_block_with_state(input, initial_state, dot_unstuffing)
    });
    check("avx", unsafe {
        avx_block_with_state(input, initial_state, dot_unstuffing)
    });
    check("avx2", unsafe {
        avx2_block_with_state(input, initial_state, dot_unstuffing)
    });
    check("avx512-vbmi2", unsafe {
        avx512_vbmi2_block_with_state(input, initial_state, dot_unstuffing)
    });
}

#[cfg(target_arch = "aarch64")]
unsafe fn neon_block_with_state(
    input: &[u8],
    initial_state: DecoderState,
    dot_unstuffing: bool,
) -> Option<(Vec<u8>, KernelState)> {
    // The block reads one byte past the 64-byte window for control-line
    // lookahead; give it the same slack the kernel driver guarantees.
    let mut padded = input.to_vec();
    padded.extend_from_slice(&[b'A'; 8]);
    let mut output = vec![0u8; input.len() + 64];
    let mut dst = 0usize;
    let mut state = KernelState {
        state: initial_state,
        ..KernelState::body()
    };
    let mut src = 0usize;
    let ctx = Neon64Ctx {
        dot_unstuffing,
        search_end: true,
        constants: unsafe { Neon64Constants::new() },
        table: compact_table_16(),
    };
    match unsafe {
        decode_neon64_span_block(&padded, &mut src, &mut output, &mut dst, &mut state, &ctx)
            .unwrap()
    } {
        SpanBlockOutcome::Consumed => {}
        SpanBlockOutcome::ScalarThrough(_) => return None,
    }
    assert_eq!(src, 64);
    output.truncate(dst);
    Some((output, state))
}

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
    let input: Vec<u8> = vec![b'A'; 1000];
    let mut output = vec![0u8; 1000];
    let (consumed, written) = decode_normal_run(&input, 0, &mut output, 0);
    assert_eq!(consumed, 1000);
    assert_eq!(written, 1000);
    assert!(output.iter().all(|&b| b == b'A'.wrapping_sub(42)));
}

#[test]
fn body_kernel_handles_escape_crlf_and_dot_unstuffing() {
    let input = b"AB=C\r\n..D";
    let mut output = vec![0u8; 64];
    let written = decode_body_into(input, &mut output, true).unwrap();

    assert_eq!(
        &output[..written],
        &[
            b'A'.wrapping_sub(42),
            b'B'.wrapping_sub(42),
            b'C'.wrapping_sub(106),
            b'.'.wrapping_sub(42),
            b'D'.wrapping_sub(42),
        ]
    );
}

fn encoded_body_for(input: &[u8], line_length: usize) -> Vec<u8> {
    let mut article = Vec::new();
    crate::encode::encode(input, &mut article, line_length, "line-aware.bin").unwrap();
    let parsed = crate::header::parse_headers(&article).unwrap();
    article[parsed.data_start..parsed.data_end].to_vec()
}

fn assert_line_hint_matches_general(body: &[u8], line_length: u32, dot_unstuffing: bool) {
    let mut general = vec![0u8; body.len() + 128];
    let general_written = decode_body_into(body, &mut general, dot_unstuffing).unwrap();
    general.truncate(general_written);

    let mut hinted = vec![0u8; body.len() + 128];
    let hinted_written =
        decode_body_into_with_line_length(body, &mut hinted, dot_unstuffing, Some(line_length))
            .unwrap();
    hinted.truncate(hinted_written);

    assert_eq!(hinted, general);
}

#[test]
fn line_hint_matches_general_for_128_column_article() {
    let input: Vec<u8> = (0..32_768)
        .map(|idx| ((idx * 31 + 17) & 0xff) as u8)
        .collect();
    let body = encoded_body_for(&input, 128);

    assert_line_hint_matches_general(&body, 128, false);
}

#[test]
fn line_hint_matches_general_for_escape_heavy_lines() {
    let input = vec![0x13; 512];
    let body = encoded_body_for(&input, 128);

    assert_line_hint_matches_general(&body, 128, false);
}

#[test]
fn line_hint_handles_escape_at_predicted_boundary() {
    let mut input = vec![b'A'; 126];
    input.push(0x13);
    input.extend((0..512).map(|idx| ((idx * 19 + 3) & 0xff) as u8));
    let body = encoded_body_for(&input, 128);

    assert_eq!(&body[126..128], b"=}");
    assert_eq!(&body[128..130], b"\r\n");
    assert_line_hint_matches_general(&body, 128, false);
}

#[test]
fn line_hint_falls_back_on_wrong_line_length() {
    let input: Vec<u8> = (0..4096)
        .map(|idx| ((idx * 13 + 91) & 0xff) as u8)
        .collect();
    let body = encoded_body_for(&input, 128);

    assert_line_hint_matches_general(&body, 64, false);
}

#[test]
fn line_hint_falls_back_on_dot_stuffed_line() {
    let mut body = Vec::new();
    body.push(b'.');
    body.extend(std::iter::repeat_n(b'A', 128));
    body.extend_from_slice(b"\r\n");
    body.extend(std::iter::repeat_n(b'B', 128));

    assert_line_hint_matches_general(&body, 128, true);
}

#[test]
fn chunk_kernel_preserves_pending_escape() {
    let mut state = DecodeState::new();
    let mut output = vec![0u8; 64];
    let n1 = decode_chunk_into(b"AB=", &mut output, &mut state, false).unwrap();
    assert_eq!(n1, 2);
    assert!(state.escape_pending);

    let n2 = decode_chunk_into(b"C", &mut output[n1..], &mut state, false).unwrap();
    assert_eq!(n2, 1);
    assert!(!state.escape_pending);
    assert_eq!(output[n1], b'C'.wrapping_sub(106));
}

#[test]
fn chunk_kernel_preserves_line_start_escape_control_boundary() {
    let mut state = DecodeState::new();
    let mut output = vec![0u8; 64];
    let n1 = decode_chunk_into(b"AB\r\n=", &mut output, &mut state, true).unwrap();
    assert_eq!(n1, 2);
    assert!(state.escape_pending);
    assert!(state.at_line_start);

    let n2 = decode_chunk_into(b"yignored", &mut output[n1..], &mut state, true).unwrap();
    assert_eq!(n2, 8);
    assert!(!state.escape_pending);
    assert_eq!(
        &output[..n1 + n2],
        &[
            b'A'.wrapping_sub(42),
            b'B'.wrapping_sub(42),
            b'y'.wrapping_sub(106),
            b'i'.wrapping_sub(42),
            b'g'.wrapping_sub(42),
            b'n'.wrapping_sub(42),
            b'o'.wrapping_sub(42),
            b'r'.wrapping_sub(42),
            b'e'.wrapping_sub(42),
            b'd'.wrapping_sub(42),
        ]
    );
}

#[test]
fn chunk_kernel_preserves_bare_cr_pending() {
    let input = b"AB\r.CD";
    let mut whole = vec![0u8; 64];
    let whole_written = decode_body_into(input, &mut whole, true).unwrap();

    let mut state = DecodeState::new();
    let mut chunked = vec![0u8; 64];
    let n1 = decode_chunk_into(&input[..3], &mut chunked, &mut state, true).unwrap();
    let n2 = decode_chunk_into(&input[3..], &mut chunked[n1..], &mut state, true).unwrap();

    assert_eq!(&chunked[..n1 + n2], &whole[..whole_written]);
}

#[test]
fn chunk_kernel_preserves_dot_cr_pending() {
    let input = b"AB\r\n.\r.CD";
    let mut whole = vec![0u8; 64];
    let whole_written = decode_body_into(input, &mut whole, true).unwrap();

    let mut state = DecodeState::new();
    let mut chunked = vec![0u8; 64];
    let n1 = decode_chunk_into(&input[..6], &mut chunked, &mut state, true).unwrap();
    let n2 = decode_chunk_into(&input[6..], &mut chunked[n1..], &mut state, true).unwrap();

    assert_eq!(&chunked[..n1 + n2], &whole[..whole_written]);
}

#[test]
fn normal_run_does_not_mutate_past_written_prefix() {
    let input = b"AB=DEFGHIJKLMNOPQRST";
    let mut output = [0xaau8; 64];
    let (consumed, written) = decode_normal_run(input, 0, &mut output, 0);

    assert_eq!(consumed, 2);
    assert_eq!(written, 2);
    assert_eq!(
        &output[..2],
        &[b'A'.wrapping_sub(42), b'B'.wrapping_sub(42)]
    );
    assert!(output[2..32].iter().all(|&byte| byte == 0xaa));
}

#[test]
fn body_kernel_reports_trailing_escape() {
    let mut output = vec![0u8; 64];
    let result = decode_body_into(b"AB=", &mut output, false);
    assert!(matches!(result, Err(YencError::MalformedEscape(2))));
}

#[cfg(target_arch = "x86_64")]
#[test]
fn fix_eq_mask_keeps_alternating_escape_starts() {
    assert_eq!(fix_eq_mask(0b1111, 0b11110), 0b0101);
    assert_eq!(fix_eq_mask(0b1110, 0b11100), 0b1010);
    assert_eq!(fix_eq_mask(0b1010, 0b10100), 0b1010);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_dispatch_guardrails_match_reference_model_lists() {
    for model in [
        0x1c, 0x26, 0x27, 0x35, 0x36, 0x37, 0x4a, 0x4c, 0x4d, 0x5a, 0x5d,
    ] {
        assert!(x86_family_model_prefers_sse2_over_ssse3(6, model));
    }
    for model in [0, 1, 2] {
        assert!(x86_family_model_prefers_sse2_over_ssse3(0x5f, model));
    }
    for model in [0x5c, 0x5f, 0x7a, 0x9c] {
        assert!(x86_family_model_prefers_ssse3_over_sse41(6, model));
        assert!(!x86_family_model_prefers_sse2_over_ssse3(6, model));
    }
    assert!(!x86_family_model_prefers_sse2_over_ssse3(6, 0x3c));
    assert!(!x86_family_model_prefers_ssse3_over_sse41(6, 0x3c));
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_cpu_family_model_uses_extended_amd_family_bits() {
    let eax = (0xf << 8) | (0x5 << 20);
    assert_eq!(x86_cpu_family_model_from_eax(eax), (0x5f, 0));
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_blocks_consume_clean_window() {
    let input = [b'A'; 64];
    let expected = vec![b'A'.wrapping_sub(42); 64];

    unsafe {
        assert_x86_blocks_produce(
            &input,
            DecoderState::None,
            false,
            &expected,
            DecoderState::None,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_blocks_carry_trailing_escape() {
    let mut input = [b'A'; 64];
    input[63] = b'=';
    let expected = vec![b'A'.wrapping_sub(42); 63];

    unsafe {
        assert_x86_blocks_produce(
            &input,
            DecoderState::None,
            false,
            &expected,
            DecoderState::Eq,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_blocks_carry_line_start_trailing_escape() {
    let mut input = [b'A'; 64];
    input[61] = b'\r';
    input[62] = b'\n';
    input[63] = b'=';
    let expected = vec![b'A'.wrapping_sub(42); 61];

    unsafe {
        assert_x86_blocks_produce(
            &input,
            DecoderState::None,
            true,
            &expected,
            DecoderState::CrLfEq,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_blocks_carry_line_start_trailing_dot() {
    let mut input = [b'A'; 64];
    input[61] = b'\r';
    input[62] = b'\n';
    input[63] = b'.';
    let expected = vec![b'A'.wrapping_sub(42); 61];

    unsafe {
        assert_x86_blocks_produce(
            &input,
            DecoderState::None,
            true,
            &expected,
            DecoderState::CrLfDot,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn x86_blocks_decode_entry_escape_carry() {
    let input = [b'C'; 64];
    let mut expected = vec![b'C'.wrapping_sub(42); 64];
    expected[0] = b'C'.wrapping_sub(106);

    unsafe {
        assert_x86_blocks_produce(
            &input,
            DecoderState::Eq,
            false,
            &expected,
            DecoderState::None,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx2_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((avx2, avx_state)) =
        (unsafe { avx2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(avx2, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse2_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((sse2, sse_state)) =
        (unsafe { sse2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(sse2, scalar);
    assert_eq!(sse_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse2_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((sse2, sse_state)) =
        (unsafe { sse2_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(sse2, scalar);
    assert_eq!(sse_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse2_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((sse2, sse_state)) =
        (unsafe { sse2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(sse2, scalar);
    assert_eq!(sse_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn ssse3_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((ssse3, ssse3_state)) =
        (unsafe { ssse3_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(ssse3, scalar);
    assert_eq!(ssse3_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn ssse3_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((ssse3, ssse3_state)) =
        (unsafe { ssse3_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(ssse3, scalar);
    assert_eq!(ssse3_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn ssse3_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((ssse3, ssse3_state)) =
        (unsafe { ssse3_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(ssse3, scalar);
    assert_eq!(ssse3_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse41_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((sse41, sse41_state)) =
        (unsafe { sse41_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(sse41, scalar);
    assert_eq!(sse41_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse41_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((sse41, sse41_state)) =
        (unsafe { sse41_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(sse41, scalar);
    assert_eq!(sse41_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse41_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((sse41, sse41_state)) =
        (unsafe { sse41_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(sse41, scalar);
    assert_eq!(sse41_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((avx, avx_state)) =
        (unsafe { avx_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(avx, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((avx, avx_state)) =
        (unsafe { avx_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(avx, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((avx, avx_state)) =
        (unsafe { avx_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(avx, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx2_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((avx2, avx_state)) =
        (unsafe { avx2_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(avx2, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx2_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((avx2, avx_state)) =
        (unsafe { avx2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(avx2, scalar);
    assert_eq!(avx_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx512_vbmi2_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((vbmi2, vbmi2_state)) =
        (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(vbmi2, scalar);
    assert_eq!(vbmi2_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx512_vbmi2_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((vbmi2, vbmi2_state)) =
        (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(vbmi2, scalar);
    assert_eq!(vbmi2_state, scalar_state);
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx512_vbmi2_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((vbmi2, vbmi2_state)) =
        (unsafe { avx512_vbmi2_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(vbmi2, scalar);
    assert_eq!(vbmi2_state, scalar_state);
}

#[cfg(target_arch = "aarch64")]
#[test]
fn neon_block_compacts_escapes_and_line_breaks() {
    let mut input = [b'A'; 64];
    input[3] = b'=';
    input[4] = b'C';
    input[10] = b'\r';
    input[11] = b'\n';
    input[20] = b'=';
    input[21] = b'=';
    input[22] = b'B';
    input[37] = b'\n';
    input[52] = b'=';
    input[53] = b'Z';

    let Some((neon, neon_state)) =
        (unsafe { neon_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(neon, scalar);
    assert_eq!(neon_state, scalar_state);
}

#[cfg(target_arch = "aarch64")]
#[test]
fn neon_block_handles_dot_stuffed_line_start() {
    let mut input = [b'A'; 64];
    input[0] = b'.';
    input[1] = b'.';
    input[16] = b'\r';
    input[17] = b'\n';
    input[18] = b'.';
    input[19] = b'.';

    let Some((neon, neon_state)) =
        (unsafe { neon_block_with_state(&input, DecoderState::CrLf, true) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::CrLf, true);

    assert_eq!(neon, scalar);
    assert_eq!(neon_state, scalar_state);
}

#[cfg(target_arch = "aarch64")]
#[test]
fn neon_block_preserves_trailing_cr_state() {
    let mut input = [b'A'; 64];
    input[63] = b'\r';

    let Some((neon, neon_state)) =
        (unsafe { neon_block_with_state(&input, DecoderState::None, false) })
    else {
        return;
    };
    let (scalar, scalar_state) = scalar_body_with_state(&input, DecoderState::None, false);

    assert_eq!(neon, scalar);
    assert_eq!(neon_state, scalar_state);
}

#[test]
fn scalar_kernel_detects_control_boundaries() {
    let (output, state) = scalar_body_with_state(b"A\r\n=yignored", DecoderState::CrLf, true);
    assert_eq!(output, vec![b'A'.wrapping_sub(42)]);
    assert_eq!(state.end, DecodeEnd::Control);

    let (output, state) = scalar_body_with_state(b"A\r\n.=yignored", DecoderState::CrLf, true);
    assert_eq!(output, vec![b'A'.wrapping_sub(42)]);
    assert_eq!(state.end, DecodeEnd::Control);
}

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed >> 33
}

/// Build an adversarial raw NNTP/yEnc stream: normal runs interleaved
/// with escapes, chains, malformed escapes, bare breaks, line-start dots
/// and escapes, and (rarely) control/terminator shapes.
fn adversarial_stream(seed: &mut u64, len_target: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len_target + 16);
    while out.len() < len_target {
        match lcg(seed) % 20 {
            0..=7 => {
                let run = (lcg(seed) % 90) as usize + 1;
                for _ in 0..run {
                    let byte = (lcg(seed) % 256) as u8;
                    if !matches!(byte, b'=' | b'\r' | b'\n') {
                        out.push(byte);
                    }
                }
            }
            8..=10 => out.extend_from_slice(b"\r\n"),
            11 => {
                out.push(b'=');
                out.push((lcg(seed) % 256) as u8);
            }
            12 => out.extend_from_slice(b"=\r"),
            13 => out.extend_from_slice(b"=\n"),
            14 => out.extend_from_slice(b"=="),
            15 => out.extend_from_slice(b"==="),
            16 => out.extend_from_slice(b"\r\n."),
            17 => out.extend_from_slice(b"\r\n.."),
            18 => out.extend_from_slice(b"\r\n=J"),
            19 => match lcg(seed) % 6 {
                0 => out.extend_from_slice(b"\r"),
                1 => out.extend_from_slice(b"\n"),
                2 => out.extend_from_slice(b"\r\r"),
                3 => out.extend_from_slice(b"\n\n"),
                4 => out.extend_from_slice(b"\r\n.\r\n"),
                _ => out.extend_from_slice(b"\r\n=y"),
            },
            _ => unreachable!(),
        }
    }
    // Avoid a trailing escape so non-preserving modes do not error and
    // every mode combination can run over the same stream.
    while matches!(out.last(), Some(b'=')) {
        out.pop();
    }
    out
}

#[allow(clippy::type_complexity)]
fn run_kernel_whole(
    input: &[u8],
    dot_unstuffing: bool,
    preserve_pending: bool,
    search_end: bool,
    scalar: bool,
    line_length: Option<u32>,
) -> Result<(Vec<u8>, usize, RapidyencDecodeEnd, KernelState), YencError> {
    let mut output = vec![0u8; input.len() + 64];
    let mut state = KernelState::body_with_line_length(line_length);
    let outcome = if scalar {
        decode_kernel_scalar(
            input,
            &mut output,
            &mut state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )?
    } else {
        decode_kernel(
            input,
            &mut output,
            &mut state,
            dot_unstuffing,
            preserve_pending,
            search_end,
        )?
    };
    output.truncate(outcome.written);
    Ok((output, outcome.consumed, outcome.end, state))
}

#[allow(clippy::type_complexity)]
fn run_kernel_chunked(
    input: &[u8],
    chunk_len: usize,
    dot_unstuffing: bool,
    search_end: bool,
    scalar: bool,
    line_length: Option<u32>,
) -> (Vec<u8>, usize, RapidyencDecodeEnd, KernelState) {
    let mut output = vec![0u8; input.len() + 64];
    let mut state = KernelState::body_with_line_length(line_length);
    let mut consumed = 0usize;
    let mut written = 0usize;
    let mut end = RapidyencDecodeEnd::None;
    while consumed < input.len() && end == RapidyencDecodeEnd::None {
        let chunk_end = (consumed + chunk_len).min(input.len());
        let chunk = &input[consumed..chunk_end];
        let outcome = if scalar {
            decode_kernel_scalar(
                chunk,
                &mut output[written..],
                &mut state,
                dot_unstuffing,
                true,
                search_end,
            )
        } else {
            decode_kernel(
                chunk,
                &mut output[written..],
                &mut state,
                dot_unstuffing,
                true,
                search_end,
            )
        }
        .expect("chunked decode with preserve_pending cannot error");
        written += outcome.written;
        end = outcome.end;
        if outcome.consumed == 0 && outcome.end == RapidyencDecodeEnd::None {
            break;
        }
        consumed += outcome.consumed;
    }
    output.truncate(written);
    (output, consumed, end, state)
}

#[test]
fn dispatch_kernel_matches_scalar_on_adversarial_streams() {
    let mut seed = 0x00c0ffee_d15ea5e5u64;
    for round in 0..64 {
        let len = 256 + (lcg(&mut seed) % 4096) as usize;
        let input = adversarial_stream(&mut seed, len);
        for &hint in &[None, Some(128u32)] {
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, true, false),
                (true, false, true),
                (false, true, false),
                (false, false, false),
            ] {
                let simd = run_kernel_whole(&input, dot, preserve, search, false, hint);
                let reference = run_kernel_whole(&input, dot, preserve, search, true, hint);
                match (simd, reference) {
                    (Ok(simd), Ok(reference)) => {
                        assert_eq!(
                            simd, reference,
                            "round {round} hint={hint:?} dot={dot} preserve={preserve} \
                                 search={search}"
                        );
                    }
                    (Err(simd), Err(reference)) => {
                        assert_eq!(
                            simd.to_string(),
                            reference.to_string(),
                            "round {round} hint={hint:?} dot={dot} preserve={preserve} \
                                 search={search}"
                        );
                    }
                    (simd, reference) => panic!(
                        "kernel disagreement round {round} hint={hint:?} dot={dot} \
                             preserve={preserve} search={search}: simd={simd:?} \
                             reference={reference:?}"
                    ),
                }
            }
        }
    }
}

#[test]
fn dispatch_kernel_matches_scalar_across_chunk_splits() {
    let mut seed = 0xfeedface_0badf00du64;
    for round in 0..12 {
        let len = 1024 + (lcg(&mut seed) % 2048) as usize;
        let input = adversarial_stream(&mut seed, len);
        for &chunk_len in &[1usize, 2, 3, 63, 64, 65, 127, 128, 129, 130, 257] {
            for &hint in &[None, Some(128u32)] {
                for &(dot, search) in &[(true, true), (true, false), (false, false)] {
                    let simd = run_kernel_chunked(&input, chunk_len, dot, search, false, hint);
                    let reference = run_kernel_chunked(&input, chunk_len, dot, search, true, hint);
                    assert_eq!(
                        simd, reference,
                        "round {round} chunk={chunk_len} hint={hint:?} dot={dot} \
                             search={search}"
                    );
                }
            }
        }
    }
}

#[test]
fn dispatch_zero_window_preserves_crlf_entry_state() {
    // Regression: an input of length 129 or 130 passes the flat raw-kernel gate
    // (len > 128) but the SIMD loop consumes ZERO windows (simd_limit = len - 67
    // < WIDTH = 64), leaving src == 0. The kernel must NOT clobber the CrLf entry
    // state on that zero-window exit — body_with_line_length starts at CrLf, so a
    // leading '.' is a stuffed dot that must be stripped. Before the fix the exit
    // overwrote state to None and the scalar epilogue kept the '.' as data.
    // Runs against whichever tier the CPU dispatches (AVX2 / AVX-512 / SSE / NEON).
    for len in [129usize, 130] {
        let mut input = vec![b'x'; len - 2];
        input[0] = b'.'; // stuffed dot at the carried CrLf line start
        input.extend_from_slice(b"\r\n");
        assert_eq!(input.len(), len);
        let simd = run_kernel_whole(&input, true, false, false, false, None).unwrap();
        let reference = run_kernel_whole(&input, true, false, false, true, None).unwrap();
        assert_eq!(simd, reference, "zero-window CrLf clobber at len={len}");
    }
}

// Deterministic windows that pin the block-boundary carries: escapes,
// line-start escapes and dots, and escaped CRs at the 64-byte edge, plus
// control lines and terminators mid-window. Shared by the differential
// assertion and the divergence-dump diagnostic.
fn boundary_special_cases() -> Vec<Vec<u8>> {
    let mut cases: Vec<Vec<u8>> = Vec::new();

    // '=' as the final byte of the first 64-byte window.
    let mut case = vec![b'A'; 63];
    case.push(b'=');
    case.extend_from_slice(b"J then more data follows here");
    case.extend_from_slice(&[b'B'; 96]);
    cases.push(case);

    // Line-start '=' at the window edge: "\r\n" at 61..63, '=' at 63.
    let mut case = vec![b'A'; 61];
    case.extend_from_slice(b"\r\n=");
    case.extend_from_slice(b"Jrest-of-line");
    case.extend_from_slice(&[b'C'; 96]);
    cases.push(case);

    // Line-start '=y' split across the window edge (control line).
    let mut case = vec![b'A'; 61];
    case.extend_from_slice(b"\r\n=");
    case.extend_from_slice(b"yend size=128");
    case.extend_from_slice(&[b'D'; 96]);
    cases.push(case);

    // Line-start dot at the window edge, stuffed-data continuation.
    let mut case = vec![b'A'; 61];
    case.extend_from_slice(b"\r\n.");
    case.extend_from_slice(b".data-after-stuffed-dot");
    case.extend_from_slice(&[b'E'; 96]);
    cases.push(case);

    // Line-start dot at the window edge, terminator continuation.
    let mut case = vec![b'A'; 61];
    case.extend_from_slice(b"\r\n.");
    case.extend_from_slice(b"\r\nignored-after-terminator");
    case.extend_from_slice(&[b'F'; 96]);
    cases.push(case);

    // Escaped CR at the window edge, dot-stuffed continuation.
    let mut case = vec![b'A'; 62];
    case.extend_from_slice(b"=\r");
    case.extend_from_slice(b"\n.more-data");
    case.extend_from_slice(&[b'G'; 96]);
    cases.push(case);

    // Escaped CR mid-window opening a line boundary ("=\r\n." strips).
    let mut case = vec![b'H'; 32];
    case.extend_from_slice(b"=\r\n.stuffed");
    case.extend_from_slice(&[b'I'; 128]);
    cases.push(case);

    // '=' chains crossing the window edge.
    let mut case = vec![b'J'; 62];
    case.extend_from_slice(b"===");
    case.extend_from_slice(b"KLMNOP");
    case.extend_from_slice(&[b'K'; 96]);
    cases.push(case);

    // Control line "=y" mid-window with a line-start escape before it.
    let mut case = vec![b'L'; 20];
    case.extend_from_slice(b"\r\n=Mescaped-line-start\r\n=yend trailer");
    case.extend_from_slice(&[b'M'; 96]);
    cases.push(case);

    // Plain terminator mid-window.
    let mut case = vec![b'N'; 40];
    case.extend_from_slice(b"\r\n.\r\nleftover");
    case.extend_from_slice(&[b'O'; 96]);
    cases.push(case);

    // Dot-stuffed control terminator "\r\n.=y" (CrLfDot→=y path).
    let mut case = vec![b'P'; 40];
    case.extend_from_slice(b"\r\n.=yend after dot-stuffed control");
    case.extend_from_slice(&[b'Q'; 96]);
    cases.push(case);

    // Escaped LF "=\n" at the window edge (data, not a break).
    let mut case = vec![b'R'; 62];
    case.extend_from_slice(b"=\n");
    case.extend_from_slice(b"continues-same-line");
    case.extend_from_slice(&[b'S'; 96]);
    cases.push(case);

    // Pending escape as the final byte of the whole stream.
    let mut case = vec![b'T'; 130];
    case.push(b'=');
    cases.push(case);

    // Pending CR as the final byte of the whole stream.
    let mut case = vec![b'U'; 130];
    case.push(b'\r');
    cases.push(case);

    // "\r\n." exactly at the window/simd-limit tail edge.
    let mut case = vec![b'V'; 125];
    case.extend_from_slice(b"\r\n.tail");
    cases.push(case);

    cases
}

#[test]
fn dispatch_kernel_handles_boundary_specials_like_scalar() {
    let cases = boundary_special_cases();
    for (idx, input) in cases.iter().enumerate() {
        for &hint in &[None, Some(64u32)] {
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, true, false),
                (true, false, true),
                (false, true, false),
            ] {
                let simd = run_kernel_whole(input, dot, preserve, search, false, hint);
                let reference = run_kernel_whole(input, dot, preserve, search, true, hint);
                match (simd, reference) {
                    (Ok(simd), Ok(reference)) => assert_eq!(
                        simd, reference,
                        "case {idx} hint={hint:?} dot={dot} preserve={preserve} \
                             search={search}"
                    ),
                    (Err(simd), Err(reference)) => assert_eq!(
                        simd.to_string(),
                        reference.to_string(),
                        "case {idx} hint={hint:?} dot={dot} preserve={preserve} \
                             search={search}"
                    ),
                    (simd, reference) => panic!(
                        "kernel disagreement case {idx} hint={hint:?} dot={dot} \
                             preserve={preserve} search={search}: simd={simd:?} \
                             reference={reference:?}"
                    ),
                }
            }
        }
    }
}

/// Diagnostic (run with `--ignored --nocapture`): dumps the first byte
/// where the AVX2 kernel diverges from the scalar oracle across the same
/// boundary corpus, with surrounding context, to pinpoint port bugs.
#[cfg(target_arch = "x86_64")]
#[test]
#[ignore = "diagnostic; run explicitly"]
fn dump_avx2_divergence() {
    if !is_x86_feature_detected!("avx2") {
        eprintln!("no avx2; skipping");
        return;
    }
    let cases = boundary_special_cases();
    for (idx, input) in cases.iter().enumerate() {
        for &hint in &[None, Some(64u32)] {
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, true, false),
                (true, false, true),
                (false, true, false),
            ] {
                let simd = run_kernel_whole(input, dot, preserve, search, false, hint);
                let reference = run_kernel_whole(input, dot, preserve, search, true, hint);
                if let (Ok(s), Ok(r)) = (&simd, &reference)
                    && s.0 != r.0
                {
                    let at =
                        s.0.iter()
                            .zip(&r.0)
                            .position(|(a, b)| a != b)
                            .unwrap_or(s.0.len().min(r.0.len()));
                    let lo = at.saturating_sub(4);
                    eprintln!(
                        "DIVERGE case={idx} hint={hint:?} dot={dot} preserve={preserve} search={search}\n  \
                             inlen={} first_diff_out={at} simd_len={} ref_len={}\n  \
                             simd_out[{lo}..]={:?}\n  ref_out [{lo}..]={:?}\n  \
                             simd_state={:?} ref_state={:?} simd_end={:?} ref_end={:?}",
                        input.len(),
                        s.0.len(),
                        r.0.len(),
                        &s.0[lo..(at + 8).min(s.0.len())],
                        &r.0[lo..(at + 8).min(r.0.len())],
                        s.3.state,
                        r.3.state,
                        s.2,
                        r.2,
                    );
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn forced_tier_kernels_match_scalar_with_line_hints() {
    // Every x86 tier kernel (not just the one dispatch picks on this
    // machine) against the scalar oracle, hint-less and hinted, so the
    // per-tier line-aware paths keep differential coverage. Tiers whose
    // features are missing self-skip.
    type KernelFn = unsafe fn(
        &[u8],
        &mut [u8],
        &mut KernelState,
        bool,
        bool,
        bool,
    ) -> Result<KernelOutcome, YencError>;
    let tiers: &[(&str, bool, KernelFn)] = &[
        (
            "ssse3",
            is_x86_feature_detected!("ssse3"),
            decode_kernel_ssse3 as KernelFn,
        ),
        (
            "sse4.1",
            is_x86_feature_detected!("sse4.1") && is_x86_feature_detected!("ssse3"),
            decode_kernel_sse41 as KernelFn,
        ),
        (
            "avx",
            is_x86_feature_detected!("avx")
                && is_x86_feature_detected!("popcnt")
                && is_x86_feature_detected!("sse4.1")
                && is_x86_feature_detected!("ssse3"),
            decode_kernel_avx as KernelFn,
        ),
        (
            "avx2",
            is_x86_feature_detected!("avx2"),
            decode_kernel_avx2 as KernelFn,
        ),
        (
            "avx512-vbmi2",
            is_x86_feature_detected!("avx512vbmi2")
                && is_x86_feature_detected!("avx512vl")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx2"),
            decode_kernel_avx512_vbmi2 as KernelFn,
        ),
    ];

    let mut seed = 0x71e2_ca5e_0b5e_55edu64;
    for round in 0..8 {
        let len = 1024 + (lcg(&mut seed) % 4096) as usize;
        let payload: Vec<u8> = (0..len).map(|_| (lcg(&mut seed) % 256) as u8).collect();
        let body = encoded_body_for(&payload, 128);
        for &hint in &[None, Some(128u32), Some(64)] {
            let mut reference = vec![0u8; body.len() + 64];
            let mut reference_state = KernelState::body_with_line_length(hint);
            let reference_outcome = decode_kernel_scalar(
                &body,
                &mut reference,
                &mut reference_state,
                true,
                false,
                false,
            )
            .unwrap();
            reference.truncate(reference_outcome.written);

            for &(name, available, kernel) in tiers {
                if !available {
                    continue;
                }
                let mut output = vec![0u8; body.len() + 64];
                let mut state = KernelState::body_with_line_length(hint);
                let outcome =
                    unsafe { kernel(&body, &mut output, &mut state, true, false, false) }.unwrap();
                output.truncate(outcome.written);
                assert_eq!(
                    (output, outcome.consumed, outcome.end, state),
                    (
                        reference.clone(),
                        reference_outcome.consumed,
                        reference_outcome.end,
                        reference_state,
                    ),
                    "tier {name} round {round} hint={hint:?}"
                );
            }
        }
    }
}

#[test]
fn dispatch_kernel_matches_scalar_on_hinted_encoded_streams() {
    // Well-formed fixed-column articles decoded WITH the line hint set, so
    // the line-aware fast path actually engages (the adversarial streams
    // above mostly bail out of it). Right and wrong hints, whole and
    // chunked, against the scalar oracle carrying the same hint.
    let mut seed = 0x5eed_1a7e_c0de_beefu64;
    for round in 0..24 {
        let len = 512 + (lcg(&mut seed) % 8192) as usize;
        let payload: Vec<u8> = (0..len).map(|_| (lcg(&mut seed) % 256) as u8).collect();
        let body = encoded_body_for(&payload, 128);
        for &hint in &[Some(128u32), Some(64), Some(256)] {
            for &(dot, preserve, search) in &[
                (true, true, true),
                (true, false, false),
                (false, true, false),
            ] {
                let simd = run_kernel_whole(&body, dot, preserve, search, false, hint);
                let reference = run_kernel_whole(&body, dot, preserve, search, true, hint);
                assert_eq!(
                    simd.as_ref().map_err(ToString::to_string),
                    reference.as_ref().map_err(ToString::to_string),
                    "round {round} hint={hint:?} dot={dot} preserve={preserve} \
                         search={search}"
                );
            }
            for &chunk_len in &[63usize, 128, 130, 257, 1024] {
                let simd = run_kernel_chunked(&body, chunk_len, true, false, false, hint);
                let reference = run_kernel_chunked(&body, chunk_len, true, false, true, hint);
                assert_eq!(
                    simd, reference,
                    "round {round} chunk={chunk_len} hint={hint:?}"
                );
            }
        }
    }
}
