// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every yEnc decode tier this CPU runs, forced, against the scalar state
// machine (`decode_scalar_step` one byte at a time).
//
// Input layout: 8 configuration bytes, then the encoded bytes.
//   0: bit 0 dot-unstuffing (raw), bit 1 keep a trailing escape pending,
//      bit 2 search for the end marker, bits 3-5 entry state (mod 7),
//      bits 6-7 output size: 0 exactly the input length, 1 roomy,
//      2 compact (shorter than the input), 3 exactly the input length.
//   1: input head misalignment (mod 64).
//   2: line length hint: 0 none, 1 the u16 below, 2 the u16 below * 64,
//      3 the u16 below * 16, else the u16 below + 1.
//   3-4: u16 for the hint.
//   5: roomy extra bytes / compact shortfall.
//   6: stretch length for the CRC-folding whole-body path (* 64, at least 64).
//   7: number of chunk-length bytes that follow the header (mod 32); those
//      bytes split the input for the streaming check.
//
// What is asserted, per tier, against the reference on the same input:
//   * the result: Ok (consumed, written, end) or the same error;
//   * the carried state (line state, end, line hint);
//   * the decoded bytes `output[..written]`;
//   * the allowed-write contract. The output is its own heap allocation:
//     with `output.len() >= input.len()` a tier may store anywhere in
//     `output[..input.len()]` (full-width stores; simd/mod.rs `decode_kernel`
//     and simd/scalar.rs `decode_kernel_scalar`) and nothing at or past
//     `input.len()`: those bytes are a canary, and with an output of exactly
//     `input.len()` the allocation ends there, so ASan reports the write.
//     With a compact output (shorter than the input) nothing past the decoded
//     bytes is written: the whole buffer must equal the reference's.
//   * the input is its own exact heap allocation, so ASan reports any read
//     past its end (or, at misalignment 0, before its start).
//   * streaming: with a pending escape kept, the input fed in the chosen
//     chunks (each its own exact allocation, carrying the state) gives the
//     reference's whole-call result.

#![no_main]

#[path = "common/buffers.rs"]
mod buffers;

use buffers::{Config, Exact, chunk_lengths};
use libfuzzer_sys::fuzz_target;
use weaver_yenc::RapidyencDecodeState;
use weaver_yenc::YencError;
use weaver_yenc::simd::fuzzing::{
    ChunkState, Mode, Outcome, State, TIERS, Tier, chunk_until_end_tier, decode_dispatched,
    decode_reference, decode_tier, rapidyenc_incremental_tier, rapidyenc_oracle,
    raw_body_until_end_crc, raw_body_until_end_crc_avx2, runs, set_and_add_select,
    set_avx2_raw_asm, set_zen4,
};

const CANARY: u8 = 0xa5;

#[derive(Clone, Copy, Debug)]
enum OutSize {
    Exact,
    Roomy(usize),
    Compact(usize),
}

impl OutSize {
    fn cap(self, len: usize) -> usize {
        match self {
            OutSize::Exact => len,
            OutSize::Roomy(extra) => len + extra,
            OutSize::Compact(short) => len - short.min(len),
        }
    }
}

type Res = Result<Outcome, YencError>;

fn same(a: &Res, b: &Res) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

struct Case<'a> {
    data: &'a [u8],
    head: usize,
    mode: Mode,
    entry: u8,
    hint: Option<u32>,
    out: OutSize,
}

struct Want {
    res: Res,
    state: State,
    out: Vec<u8>,
}

fn reference(c: &Case) -> Want {
    let mut state = State::new(c.entry, c.hint);
    let mut out = vec![CANARY; c.out.cap(c.data.len())];
    let res = decode_reference(c.data, &mut out, &mut state, c.mode);
    Want { res, state, out }
}

fn check_one(
    c: &Case,
    want: &Want,
    name: &str,
    run: &mut dyn FnMut(&[u8], &mut [u8], &mut State) -> Res,
) {
    let input = Exact::from_bytes(c.head, c.data);
    let cap = c.out.cap(c.data.len());
    let mut out = Exact::new(0, cap, CANARY);
    let mut state = State::new(c.entry, c.hint);
    let res = run(input.as_ref(), out.as_mut(), &mut state);
    let out = out.as_ref();
    let ctx = || {
        format!(
            "{name} mode {:?} entry {} hint {:?} out {:?} head {} len {}",
            c.mode,
            c.entry,
            c.hint,
            c.out,
            c.head,
            c.data.len()
        )
    };
    assert!(
        same(&res, &want.res),
        "{}: result {res:?} want {:?}",
        ctx(),
        want.res
    );
    assert_eq!(state, want.state, "{}: state", ctx());
    if cap < c.data.len() {
        assert!(out == &want.out[..], "{}: compact output differs", ctx());
    } else {
        if let Ok(o) = &res {
            assert!(
                out[..o.written] == want.out[..o.written],
                "{}: decoded bytes differ",
                ctx()
            );
        }
        if let Some(at) = out[c.data.len()..].iter().position(|&b| b != CANARY) {
            panic!(
                "{}: write past input.len() at output offset {}",
                ctx(),
                c.data.len() + at
            );
        }
    }
}

// The input fed in chunks, each its own exact allocation and its own output
// allocation of exactly the chunk length (plus `extra` canary bytes).
fn check_chunked(
    c: &Case,
    want: &Want,
    chunks: &[usize],
    name: &str,
    run: &mut dyn FnMut(&[u8], &mut [u8], &mut State) -> Res,
) {
    let extra = match c.out {
        OutSize::Roomy(e) => e,
        _ => 0,
    };
    let mut state = State::new(c.entry, c.hint);
    let mut decoded = Vec::new();
    let mut consumed = 0usize;
    let mut res: Res = Ok(Outcome {
        consumed: 0,
        written: 0,
        end: weaver_yenc::RapidyencDecodeEnd::None,
    });
    let mut at = 0usize;
    for (k, &n) in chunks.iter().enumerate() {
        let input = Exact::from_bytes((c.head + at) % 64, &c.data[at..at + n]);
        let mut out = Exact::new(0, n + extra, CANARY);
        let r = run(input.as_ref(), out.as_mut(), &mut state);
        let out = out.as_ref();
        if let Some(p) = out[n..].iter().position(|&b| b != CANARY) {
            panic!(
                "{name} chunk {k} ({n} bytes at {at}): write past the chunk length at {}",
                n + p
            );
        }
        match r {
            Ok(o) => {
                decoded.extend_from_slice(&out[..o.written]);
                consumed += o.consumed;
                res = Ok(Outcome {
                    consumed,
                    written: decoded.len(),
                    end: o.end,
                });
                if c.mode.search_end && state.ended() {
                    break;
                }
                // A chunk call stops early only at an end marker.
                assert_eq!(
                    o.consumed, n,
                    "{name} chunk {k}: stopped early without an end ({o:?})"
                );
            }
            Err(e) => {
                res = Err(e);
                break;
            }
        }
        at += n;
    }
    let ctx = format!(
        "{name} chunked {chunks:?} mode {:?} entry {} hint {:?} len {}",
        c.mode,
        c.entry,
        c.hint,
        c.data.len()
    );
    assert!(
        same(&res, &want.res),
        "{ctx}: result {res:?} want {:?}",
        want.res
    );
    assert_eq!(state, want.state, "{ctx}: state");
    if let Ok(o) = &want.res {
        assert!(
            decoded[..] == want.out[..o.written],
            "{ctx}: decoded bytes differ"
        );
    }
}

// The forced kernel forms a tier has: the AVX2 raw path's asm and intrinsic
// kernels, each with both escape selects; the AVX-512 tier's Zen 4 and plain
// loops. Every form runs on any CPU with the tier.
#[derive(Clone, Copy)]
struct Variant {
    asm: Option<bool>,
    and_add: Option<bool>,
    zen4: Option<bool>,
}

fn variants(tier: Tier) -> Vec<Variant> {
    let none = Variant {
        asm: None,
        and_add: None,
        zen4: None,
    };
    match tier {
        Tier::Avx2 => [(true, true), (true, false), (false, true), (false, false)]
            .into_iter()
            .map(|(a, s)| Variant {
                asm: Some(a),
                and_add: Some(s),
                zen4: None,
            })
            .collect(),
        Tier::Avx512Vbmi2 => vec![
            Variant {
                zen4: Some(true),
                ..none
            },
            Variant {
                zen4: Some(false),
                ..none
            },
        ],
        _ => vec![none],
    }
}

fn set_variant(v: Variant) {
    set_avx2_raw_asm(v.asm);
    set_and_add_select(v.and_add);
    set_zen4(v.zen4);
}

fn variant_name(tier: Tier, v: Variant) -> String {
    let mut n = tier.as_str().to_string();
    if let Some(a) = v.asm {
        n += if a { "+asm" } else { "-asm" };
    }
    if let Some(s) = v.and_add {
        n += if s { "+andadd" } else { "+blend" };
    }
    if let Some(z) = v.zen4 {
        n += if z { "+zen4" } else { "+plain" };
    }
    n
}

// The fused NNTP decoder's body path (`decode_chunk_until_end_into`) as
// production drives it: the body from its start (CRLF, `line=` hint), split
// at socket-read boundaries, each chunk its own exact input allocation and an
// output of exactly its length (as `decode_body_chunk_until_end` trims it),
// state carried through `DecodeState`. The joined result must be the
// reference's whole-body result.
fn check_production(
    data: &[u8],
    head: usize,
    hint: Option<u32>,
    want: &Want,
    splits: &[Vec<usize>],
    tier: Tier,
    name: &str,
) {
    for chunks in splits {
        let mut state = ChunkState::body(hint);
        let mut decoded = Vec::new();
        let (mut consumed, mut at) = (0usize, 0usize);
        let mut res: Res = Ok(Outcome {
            consumed: 0,
            written: 0,
            end: weaver_yenc::RapidyencDecodeEnd::None,
        });
        for (k, &n) in chunks.iter().enumerate() {
            let input = Exact::from_bytes((head + at) % 64, &data[at..at + n]);
            let mut out = Exact::new(0, n, CANARY);
            let r = chunk_until_end_tier(tier, input.as_ref(), out.as_mut(), &mut state);
            match r {
                Ok(o) => {
                    decoded.extend_from_slice(&out.as_ref()[..o.written]);
                    consumed += o.consumed;
                    res = Ok(Outcome {
                        consumed,
                        written: decoded.len(),
                        end: o.end,
                    });
                    if o.end != weaver_yenc::RapidyencDecodeEnd::None {
                        break;
                    }
                    assert_eq!(
                        o.consumed, n,
                        "{name} production chunk {k} of {chunks:?}: stopped early without an end ({o:?})"
                    );
                }
                Err(e) => {
                    res = Err(e);
                    break;
                }
            }
            at += n;
        }
        let ctx = format!(
            "{name} production {chunks:?} hint {hint:?} len {}",
            data.len()
        );
        assert!(
            same(&res, &want.res),
            "{ctx}: result {res:?} want {:?}",
            want.res
        );
        assert_eq!(state.line_state(), want.state.without_end(), "{ctx}: state");
        if let Ok(o) = &want.res {
            assert!(
                decoded[..] == want.out[..o.written],
                "{ctx}: decoded bytes differ"
            );
        }
    }
}

// Split lists for `check_production`: two-chunk splits at every offset in the
// first and last 256 bytes (at most 64 of them, rotated by `pick` so every
// offset is reached across runs), and the fuzzer's own chunk list.
fn production_splits(len: usize, pick: u8, fuzz_chunks: &[usize]) -> Vec<Vec<usize>> {
    let mut offs: Vec<usize> = (1..len.min(257)).collect();
    offs.extend(len.saturating_sub(256).max(257)..len);
    let step = offs.len().div_ceil(64).max(1);
    let mut out: Vec<Vec<usize>> = offs
        .into_iter()
        .skip(pick as usize % step)
        .step_by(step)
        .map(|k| vec![k, len - k])
        .collect();
    if fuzz_chunks.len() > 1 {
        out.push(fuzz_chunks.to_vec());
    }
    out
}

// rapidyenc's `decode_end` (the C library), whole and in the fuzzer's chunks,
// against `decode_rapidyenc_incremental` with the tier forced. Only when the
// fuzz build linked rapidyenc (`WEAVER_RAPIDYENC_SRC`).
fn check_rapidyenc(
    data: &[u8],
    head: usize,
    entry: RapidyencDecodeState,
    chunks: &[usize],
    tier: Tier,
    name: &str,
) -> bool {
    let run =
        |f: &mut dyn FnMut(&[u8], &mut [u8], &mut RapidyencDecodeState) -> Option<Outcome>,
         chunks: &[usize]| {
            let mut st = entry;
            let mut decoded = Vec::new();
            let (mut consumed, mut at) = (0usize, 0usize);
            let mut last = Outcome {
                consumed: 0,
                written: 0,
                end: weaver_yenc::RapidyencDecodeEnd::None,
            };
            for &n in chunks {
                let input = Exact::from_bytes((head + at) % 64, &data[at..at + n]);
                let mut out = Exact::new(0, n, CANARY);
                let o = f(input.as_ref(), out.as_mut(), &mut st)?;
                decoded.extend_from_slice(&out.as_ref()[..o.written]);
                consumed += o.consumed;
                last = Outcome {
                    consumed,
                    written: decoded.len(),
                    end: o.end,
                };
                if o.end != weaver_yenc::RapidyencDecodeEnd::None {
                    break;
                }
                at += n;
            }
            Some((last, st, decoded))
        };
    let whole = [data.len()];
    for split in [&whole[..], chunks] {
        let Some(want) = run(&mut |i, o, s| rapidyenc_oracle(i, o, s), split) else {
            return false;
        };
        let got = run(
            &mut |i, o, s| {
                Some(
                    rapidyenc_incremental_tier(tier, i, o, s)
                        .expect("incremental decode never fails"),
                )
            },
            split,
        )
        .unwrap();
        let ctx = format!(
            "{name} vs rapidyenc entry {entry:?} chunks {split:?} len {}",
            data.len()
        );
        assert_eq!(got.0, want.0, "{ctx}: outcome");
        assert_eq!(got.1, want.1, "{ctx}: state");
        assert!(got.2 == want.2, "{ctx}: decoded bytes differ");
    }
    true
}

fuzz_target!(|bytes: &[u8]| {
    static ANNOUNCE: std::sync::Once = std::sync::Once::new();
    ANNOUNCE.call_once(|| {
        let names: Vec<_> = TIERS
            .into_iter()
            .filter(|&t| runs(t))
            .map(|t| t.as_str())
            .collect();
        let ry = rapidyenc_oracle(&[], &mut [], &mut RapidyencDecodeState::CrLf).is_some();
        eprintln!(
            "yenc_decode_tiers: decode tiers run here: {names:?}; rapidyenc oracle linked: {ry}"
        );
    });
    let mut cfg = Config::new(bytes);
    let flags = cfg.u8();
    let head = (cfg.u8() % 64) as usize;
    let hint_kind = cfg.u8();
    let hint_raw = cfg.u16() as u32;
    let size_byte = cfg.u8() as usize;
    let stretch = (cfg.u8() as usize).max(1) * 64;
    let n_chunk = (cfg.u8() % 32) as usize;
    let rest = bytes.get(8..).unwrap_or(&[]);
    let (chunk_seeds, data) = rest.split_at(n_chunk.min(rest.len()));

    let mode = Mode {
        dot_unstuffing: flags & 1 != 0,
        preserve_pending: flags & 2 != 0,
        search_end: flags & 4 != 0,
    };
    let entry = (flags >> 3) & 7;
    let hint = match hint_kind {
        0 => None,
        1 => Some(hint_raw),
        2 => Some(hint_raw * 64),
        3 => Some(hint_raw * 16),
        _ => Some(hint_raw + 1),
    };
    let out = match flags >> 6 {
        1 => OutSize::Roomy(size_byte + 1),
        2 => OutSize::Compact(size_byte + 1),
        _ => OutSize::Exact,
    };
    let c = Case {
        data,
        head,
        mode,
        entry,
        hint,
        out,
    };
    let want = reference(&c);

    // The reference itself is chunk-invariant when a pending escape is kept.
    let chunks = chunk_lengths(data.len(), chunk_seeds);
    let chunked = mode.preserve_pending && chunks.len() > 1;
    // Chunks get outputs of their own length, so the whole call they are
    // compared with has a non-compact output too.
    let roomy = Case {
        out: if matches!(out, OutSize::Compact(_)) {
            OutSize::Exact
        } else {
            out
        },
        ..c
    };
    let chunk_want = reference(&roomy);
    if chunked {
        check_chunked(&roomy, &chunk_want, &chunks, "reference", &mut |i, o, s| {
            decode_reference(i, o, s, mode)
        });
    }

    // The production body path's reference: from the body start, whole.
    let prod_mode = Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: true,
    };
    let prod_want = reference(&Case {
        data,
        head,
        mode: prod_mode,
        entry: 0,
        hint,
        out: OutSize::Exact,
    });
    let prod_splits = production_splits(data.len(), size_byte as u8, &chunks);
    const RY_STATES: [RapidyencDecodeState; 7] = [
        RapidyencDecodeState::CrLf,
        RapidyencDecodeState::Eq,
        RapidyencDecodeState::Cr,
        RapidyencDecodeState::None,
        RapidyencDecodeState::CrLfDot,
        RapidyencDecodeState::CrLfDotCr,
        RapidyencDecodeState::CrLfEq,
    ];
    let ry_entry = RY_STATES[entry as usize % 7];

    for tier in TIERS {
        if !runs(tier) {
            continue;
        }
        for v in variants(tier) {
            set_variant(v);
            let name = variant_name(tier, v);
            check_one(&c, &want, &name, &mut |i, o, s| {
                decode_tier(tier, i, o, s, mode)
            });
            if chunked {
                check_chunked(&roomy, &chunk_want, &chunks, &name, &mut |i, o, s| {
                    decode_tier(tier, i, o, s, mode)
                });
            }
            check_production(data, head, hint, &prod_want, &prod_splits, tier, &name);
            check_rapidyenc(data, head, ry_entry, &chunks, tier, &name);
        }
        set_variant(Variant {
            asm: None,
            and_add: None,
            zen4: None,
        });
    }
    check_one(&c, &want, "dispatched", &mut |i, o, s| {
        decode_dispatched(i, o, s, mode)
    });

    // The whole-body raw end-searching path with CRC folding, from the start
    // of a body (CRLF state), against the reference in the same mode.
    let body_mode = Mode {
        dot_unstuffing: true,
        preserve_pending: false,
        search_end: true,
    };
    let bc = Case {
        data,
        head,
        mode: body_mode,
        entry: 0,
        hint,
        out,
    };
    let bwant = reference(&bc);
    let bcrc = match &bwant.res {
        Ok(o) => {
            let mut h = weaver_yenc::crc::Crc32::new();
            h.update(&bwant.out[..o.written]);
            Some(h.finalize())
        }
        Err(_) => None,
    };
    let mut crc_checks: Vec<(&str, Option<bool>, bool)> = vec![("crc-body", None, false)];
    // The forced AVX2 stretch loop is not entered with a compact output (the
    // production path sends those to the scalar kernel, checked above).
    if runs(Tier::Avx2) && out.cap(data.len()) >= data.len() {
        crc_checks.push(("crc-body-avx2+asm", Some(true), true));
        crc_checks.push(("crc-body-avx2-asm", Some(false), true));
    }
    for (name, asm, forced) in crc_checks {
        set_avx2_raw_asm(asm);
        let mut crc = None;
        check_one(&bc, &bwant, name, &mut |i, o, s| {
            let (r, h) = if forced {
                raw_body_until_end_crc_avx2(i, o, hint, stretch).expect("avx2 runs")
            } else {
                raw_body_until_end_crc(i, o, hint, stretch)
            };
            // The state is not returned by this path; mirror the reference's
            // so `check_one` compares results and bytes only.
            *s = bwant.state;
            crc = Some(h);
            r
        });
        if let Some(want_crc) = bcrc {
            assert_eq!(crc, Some(want_crc), "{name}: crc of the decoded bytes");
        }
        set_avx2_raw_asm(None);
    }
});
