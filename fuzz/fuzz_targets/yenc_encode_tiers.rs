// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every yEnc encode tier this CPU runs, forced, against the scalar kernel.
//
// Input layout: 10 configuration bytes, then the bytes to encode.
//   0: bit 0 is_end on the last call, bit 1 output is an exact allocation
//      (else the budget is followed by canary bytes), bits 2-3 line size
//      kind, bits 4-5 column kind.
//   1-4: u32 for the line size.  5-8: u32 for the column.
//   9: number of chunk-length bytes that follow the header (mod 32); those
//      bytes split the input across calls that carry the column.
//
// Line size kinds: 0 small (1 + u32 % 256: every short line and the
// `line_size < 12` cut-over), 1 1 + u32 % 4096, 2 one of the edge values
// (1, 2, 11, 12, 13, 127, 128, 129, 255, 256, MAX_LINE_SIZE - 1,
// MAX_LINE_SIZE, MAX_LINE_SIZE + 1, 0), 3 the raw u32 (0 and values past
// MAX_LINE_SIZE are refused).
// Column kinds: 0 u32 % (line_size + 3), 1 0, 2 one of line_size - 1,
// line_size, line_size + 1, MAX_COLUMN, MAX_COLUMN + 1, 3 the raw u32.
//
// Asserted per tier and per call: the same result (bytes written or the
// same error) and column as the scalar tier, the same output bytes, and no
// write past `encode_max_len(len, line_size)` (canary bytes, or the end of
// an exact allocation for ASan). A refused call writes nothing. The input is
// its own exact allocation at a fuzz-chosen misalignment, so ASan reports a
// read past its end.

#![no_main]

#[path = "common/buffers.rs"]
mod buffers;

use std::mem::MaybeUninit;

use buffers::{Config, Exact, chunk_lengths};
use libfuzzer_sys::fuzz_target;
use weaver_yenc::encode::{MAX_COLUMN, MAX_LINE_SIZE};
use weaver_yenc::{EncodeError, EncoderTier, encode_body_with_tier, encode_max_len};

const CANARY: u8 = 0x5a;
const PAD: usize = 128;

fn as_uninit(b: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    // SAFETY: `MaybeUninit<u8>` has the layout of `u8`.
    unsafe { std::slice::from_raw_parts_mut(b.as_mut_ptr().cast(), b.len()) }
}

// One call on one tier; returns the result, the column after, and the
// output bytes (the whole budget, so callers compare the written prefix).
fn call(
    tier: EncoderTier,
    line_size: usize,
    column: usize,
    src: &[u8],
    head: usize,
    exact: bool,
    is_end: bool,
) -> (Result<usize, EncodeError>, usize, Vec<u8>) {
    let input = Exact::from_bytes(head, src);
    let budget = if (1..=MAX_LINE_SIZE).contains(&line_size) {
        encode_max_len(src.len(), line_size)
    } else {
        64
    };
    let pad = if exact { 0 } else { PAD };
    let mut out = Exact::new(0, budget + pad, CANARY);
    let mut col = column;
    let res = encode_body_with_tier(
        tier,
        line_size,
        &mut col,
        input.as_ref(),
        as_uninit(out.as_mut()),
        is_end,
    );
    let out = out.as_ref();
    let ctx = || {
        format!(
            "{tier} line_size {line_size} column {column} len {} head {head} is_end {is_end}",
            src.len()
        )
    };
    if let Some(p) = out[budget..].iter().position(|&b| b != CANARY) {
        panic!(
            "{}: write past encode_max_len ({budget}) at {}",
            ctx(),
            budget + p
        );
    }
    match res {
        Ok(n) => assert!(
            n <= budget,
            "{}: returned {n} past the budget {budget}",
            ctx()
        ),
        Err(_) => {
            assert!(
                out.iter().all(|&b| b == CANARY),
                "{}: a refused call wrote",
                ctx()
            );
            assert_eq!(col, column, "{}: a refused call moved the column", ctx());
        }
    }
    (res, col, out[..budget].to_vec())
}

fuzz_target!(|bytes: &[u8]| {
    static ANNOUNCE: std::sync::Once = std::sync::Once::new();
    ANNOUNCE.call_once(|| {
        let names: Vec<_> = EncoderTier::available()
            .into_iter()
            .map(|t| t.as_str())
            .collect();
        eprintln!("yenc_encode_tiers: encode tiers run here: {names:?}");
    });
    let mut cfg = Config::new(bytes);
    let flags = cfg.u8();
    let ls_raw = cfg.u32();
    let col_raw = cfg.u32();
    let n_chunk = (cfg.u8() % 32) as usize;
    let rest = bytes.get(10..).unwrap_or(&[]);
    let (chunk_seeds, data) = rest.split_at(n_chunk.min(rest.len()));

    let is_end = flags & 1 != 0;
    let exact = flags & 2 != 0;
    const LS_EDGE: [usize; 14] = [
        1,
        2,
        11,
        12,
        13,
        127,
        128,
        129,
        255,
        256,
        MAX_LINE_SIZE - 1,
        MAX_LINE_SIZE,
        MAX_LINE_SIZE + 1,
        0,
    ];
    let line_size = match (flags >> 2) & 3 {
        0 => 1 + (ls_raw % 256) as usize,
        1 => 1 + (ls_raw % 4096) as usize,
        2 => LS_EDGE[ls_raw as usize % LS_EDGE.len()],
        _ => ls_raw as usize,
    };
    let column = match (flags >> 4) & 3 {
        0 => (col_raw as usize) % (line_size.saturating_add(3)),
        1 => 0,
        2 => [
            line_size.saturating_sub(1),
            line_size,
            line_size.saturating_add(1),
            MAX_COLUMN,
            MAX_COLUMN + 1,
        ][col_raw as usize % 5],
        _ => col_raw as usize,
    };
    let head = (col_raw >> 24) as usize % 64;

    let chunks = chunk_lengths(data.len(), chunk_seeds);
    let chunks: Vec<usize> = if chunks.is_empty() { vec![0] } else { chunks };
    let tiers = EncoderTier::available();
    let mut cols: Vec<usize> = vec![column; tiers.len()];
    let mut at = 0;
    for (k, &n) in chunks.iter().enumerate() {
        let src = &data[at..at + n];
        let end = is_end && k + 1 == chunks.len();
        let h = (head + at) % 64;
        let (want_res, want_col, want_out) =
            call(EncoderTier::Scalar, line_size, cols[0], src, h, exact, end);
        cols[0] = want_col;
        for (t, &tier) in tiers.iter().enumerate().skip(1) {
            let (res, col, out) = call(tier, line_size, cols[t], src, h, exact, end);
            let ctx = format!(
                "{tier} chunk {k} of {chunks:?} line_size {line_size} column {} is_end {end}",
                cols[t]
            );
            assert_eq!(res, want_res, "{ctx}: result");
            assert_eq!(col, want_col, "{ctx}: column");
            if let Ok(n) = res {
                assert!(out[..n] == want_out[..n], "{ctx}: output bytes differ");
            }
            cols[t] = col;
        }
        at += n;
    }
});
