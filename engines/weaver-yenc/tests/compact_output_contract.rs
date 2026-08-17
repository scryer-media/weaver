//! The output-bounds contract of the safe decode APIs, exercised from outside
//! the crate.
//!
//! `weaver-yenc` splits its public decode surface into three tiers, and each
//! tier promises something different about the destination buffer:
//!
//! 1. The whole-article entries (`decode`, `decode_nntp`, `decode_with_options`)
//!    reject `output.len() < input.len()` up front with
//!    [`YencError::BufferTooSmall`], *before* parsing — the contract is a
//!    property of the call, not of how compressible the article turned out.
//! 2. The self-sizing entries (`decode_nntp_append`,
//!    `decode_body_chunk_until_control`) reserve `input.len()` of spare capacity
//!    themselves and write into it.
//! 3. The byte-level entries (`decode_body`, `decode_chunk`, and the
//!    `decode_rapidyenc*` family) *accept* a compact destination and quietly
//!    drop from the full-width SIMD kernels to a checked scalar one.
//!
//! Tier 3 is the interesting one: it is the only place where the same input can
//! be decoded by two different kernels, so it is the only place where the two
//! can disagree. Every test here therefore runs the same input twice — once
//! into `vec![0; input.len()]` (the roomy, SIMD-eligible oracle) and once into a
//! compact window — and demands one of exactly two outcomes: byte-for-byte
//! agreement, or a typed `BufferTooSmall`. A short write, a silent truncation,
//! or a store outside the caller's slice is a contract violation.
//!
//! Every compact call runs inside a 0xAB canary buffer whose window is a
//! sub-slice, so an over-store in either direction is caught by the padding
//! rather than by luck. Only safe, publicly re-exported APIs are used; nothing
//! here reaches into `simd::` or constructs decoder state by hand.

use weaver_yenc::crc::Crc32;
use weaver_yenc::{
    DecodeOptions, DecodeResult, DecodeState, RapidyencDecodeEnd, RapidyencDecodeState, YencError,
    decode, decode_body, decode_body_chunk_until_control, decode_chunk, decode_nntp,
    decode_nntp_append, decode_rapidyenc, decode_rapidyenc_ex, decode_rapidyenc_incremental,
    decode_with_options, encode, encode_part, max_decoded_len,
};

// ── canary scaffolding ──────────────────────────────────────────────────────

/// Fill byte for the guard region around (and inside) every output window.
const CANARY: u8 = 0xAB;

/// Guard bytes on each side of the output window. Wider than the widest SIMD
/// store in the crate (64 bytes) so a single over-wide store cannot step clean
/// over the guard and land in untouched memory.
const PAD: usize = 96;

/// One decode call, reduced to everything the contract says must be reproducible.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    written: usize,
    consumed: usize,
    end: RapidyencDecodeEnd,
    crc: u32,
    bytes: Vec<u8>,
    /// Decoder carry state after the call, for the APIs that expose one.
    carry: Option<RapidyencDecodeState>,
}

impl Run {
    fn new(written: usize, consumed: usize) -> Self {
        Self {
            written,
            consumed,
            end: RapidyencDecodeEnd::None,
            crc: 0,
            bytes: Vec::new(),
            carry: None,
        }
    }

    fn end(mut self, end: RapidyencDecodeEnd) -> Self {
        self.end = end;
        self
    }

    fn crc(mut self, crc: u32) -> Self {
        self.crc = crc;
        self
    }

    fn carry(mut self, carry: RapidyencDecodeState) -> Self {
        self.carry = Some(carry);
        self
    }
}

/// Run one decode into a `out_len`-byte window carved out of a canary-filled
/// buffer, then prove the call stayed inside that window.
///
/// The guards on both sides are checked unconditionally, including on the error
/// path — a decode that overflows must not have scribbled outside the slice on
/// its way to reporting that. The *tail* of the window (past `bytes_written`)
/// is checked only on the compact path: the SIMD kernels legitimately store
/// full vectors inside the caller's slice past the last decoded byte, and the
/// crate only promises they stay within it.
fn canary_run(
    label: &str,
    input_len: usize,
    out_len: usize,
    f: impl FnOnce(&mut [u8]) -> Result<Run, YencError>,
) -> Result<Run, YencError> {
    let mut buf = vec![CANARY; PAD + out_len + PAD];
    let outcome = f(&mut buf[PAD..PAD + out_len]);

    assert!(
        buf[..PAD].iter().all(|&b| b == CANARY),
        "{label}: wrote before the {out_len}-byte output window"
    );
    assert!(
        buf[PAD + out_len..].iter().all(|&b| b == CANARY),
        "{label}: wrote past the {out_len}-byte output window"
    );

    let mut run = outcome?;
    assert!(
        run.written <= out_len,
        "{label}: reported {} bytes written into a {out_len}-byte window",
        run.written
    );
    let window = &buf[PAD..PAD + out_len];
    if out_len < input_len {
        assert!(
            window[run.written..].iter().all(|&b| b == CANARY),
            "{label}: compact kernel touched the window past bytes_written={}",
            run.written
        );
    }
    run.bytes = window[..run.written].to_vec();
    Ok(run)
}

/// Which arm of the contract a comparison landed on.
///
/// Reported so a sweep can prove it actually drove the scalar fallback, rather
/// than quietly comparing the SIMD kernel against itself: a window is only
/// compact when it is shorter than the input, and for an all-plain body the
/// "exactly the decoded length" window is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// A window shorter than the input decoded successfully — the scalar
    /// fallback did real work and agreed with the SIMD kernels byte for byte.
    CompactOk,
    /// A window at or above `input.len()`, where the SIMD kernels are eligible.
    RoomyOk,
    /// A window too small for the output, correctly refused.
    Refused,
}

/// The whole compact contract in one assertion.
///
/// A window that can hold the roomy run's output must reproduce it exactly. A
/// window that cannot must refuse with `BufferTooSmall` naming its own size —
/// never a partial success.
fn assert_compact_run(
    compact: Result<Run, YencError>,
    roomy: &Run,
    input_len: usize,
    out_len: usize,
    label: &str,
) -> Verdict {
    if out_len >= roomy.written {
        match compact {
            Ok(run) => {
                assert_eq!(
                    run, *roomy,
                    "{label}: compact run into {out_len} bytes diverged from the roomy run"
                );
                if out_len < input_len {
                    Verdict::CompactOk
                } else {
                    Verdict::RoomyOk
                }
            }
            Err(err) => panic!(
                "{label}: {out_len} bytes hold all {} decoded bytes, but the decode refused: {err:?}",
                roomy.written
            ),
        }
    } else {
        match compact {
            Err(YencError::BufferTooSmall { needed, available }) => {
                assert_eq!(
                    available, out_len,
                    "{label}: BufferTooSmall reported available={available} for a {out_len}-byte window"
                );
                assert!(
                    needed > available,
                    "{label}: BufferTooSmall reported needed={needed} <= available={available}"
                );
                Verdict::Refused
            }
            other => panic!(
                "{label}: a {out_len}-byte window cannot hold {} decoded bytes, but got {other:?}",
                roomy.written
            ),
        }
    }
}

// ── deterministic encoded-body fixtures ─────────────────────────────────────

/// Shapes of *encoded* yEnc body bytes. Each one drives a different arm of the
/// decoder: the flat copy loop, the escape-compaction path, the line-boundary
/// machine, and the NNTP dot-unstuffing machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// No specials at all: the widest SIMD fast path.
    Plain,
    /// Every payload byte escaped (`=` + escapee): pure compaction.
    DenseEscapes,
    /// Short encoded lines separated by CRLF.
    Lines,
    /// CRLF lines whose first byte is a stuffed dot (`..`).
    DotLines,
    /// Plain runs, escapes, line breaks and stuffed dots interleaved.
    Mixed,
}

const SHAPES: [Shape; 5] = [
    Shape::Plain,
    Shape::DenseEscapes,
    Shape::Lines,
    Shape::DotLines,
    Shape::Mixed,
];

/// An encoded byte that is never a yEnc special (`=`, CR, LF), never `.`
/// (which would collide with dot-stuffing at a line start) and never `y`
/// (which would turn a preceding `=` at a line start into a `=y` control line).
fn plain_byte(i: usize) -> u8 {
    const ALPHABET: &[u8] = b"AZ09az!~Bb-_MmQq{}Ww";
    ALPHABET[i % ALPHABET.len()]
}

/// Exactly `len` bytes of encoded yEnc body in the requested shape.
///
/// Tokens are emitted whole or not at all, so a body never ends on a dangling
/// `=` or a lone CR. That keeps one fixture legal for both the entry points
/// that carry a pending escape across calls and the ones that reject it.
fn encoded_body(shape: Shape, len: usize) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(len);
    let mut i = 0usize;
    while out.len() < len {
        let room = len - out.len();
        let token: Vec<u8> = match shape {
            Shape::Plain => vec![plain_byte(i)],
            Shape::DenseEscapes => vec![b'=', plain_byte(i + 7)],
            Shape::Lines => {
                if i % 9 == 8 {
                    b"\r\n".to_vec()
                } else {
                    vec![plain_byte(i)]
                }
            }
            Shape::DotLines => match i % 6 {
                0 => b"\r\n".to_vec(),
                1 => b"..".to_vec(),
                _ => vec![plain_byte(i)],
            },
            Shape::Mixed => match i % 7 {
                0 => b"\r\n".to_vec(),
                1 => b"..".to_vec(),
                2 | 5 => vec![b'=', plain_byte(i + 3)],
                _ => vec![plain_byte(i)],
            },
        };
        if token.len() <= room {
            out.extend_from_slice(&token);
        } else {
            out.push(plain_byte(i));
        }
        i += 1;
    }
    debug_assert_eq!(out.len(), len);
    out
}

/// Sizes that bracket every vector width the crate dispatches on (16/32/64) and
/// the ~128-byte point where the raw kernels have enough runway to engage at
/// all (`WIDTH + tail`), so both the SIMD loop and its scalar prologue/epilogue
/// are exercised on each side of the switch.
const BOUNDARY_SIZES: [usize; 14] = [15, 16, 17, 31, 32, 33, 63, 64, 65, 126, 127, 128, 129, 130];

/// Sizes well past the point where the SIMD loop runs many full iterations.
const BULK_SIZES: [usize; 4] = [257, 512, 1000, 4096];

fn with_suffix(body: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = body.to_vec();
    out.extend_from_slice(suffix);
    out
}

/// The raw NNTP article terminator.
const TERMINATOR: &[u8] = b"\r\n.\r\n";

/// A `=yend` control line, the other thing an end-detecting decode stops on.
const CONTROL: &[u8] = b"\r\n=yend size=7 crc32=00000000\r\n";

// ── per-API runners ─────────────────────────────────────────────────────────

fn run_decode_body(input: &[u8], out_len: usize, dot: bool) -> Result<Run, YencError> {
    let label = format!("decode_body(dot={dot})");
    canary_run(&label, input.len(), out_len, |out| {
        let mut crc = Crc32::new();
        let written = decode_body(
            input,
            out,
            &mut crc,
            DecodeOptions {
                dot_unstuffing: dot,
            },
        )?;
        Ok(Run::new(written, input.len()).crc(crc.finalize()))
    })
}

fn run_decode_chunk(
    input: &[u8],
    out_len: usize,
    state: &mut DecodeState,
    dot: bool,
) -> Result<Run, YencError> {
    let label = format!("decode_chunk(dot={dot})");
    canary_run(&label, input.len(), out_len, |out| {
        let written = decode_chunk(
            input,
            out,
            state,
            DecodeOptions {
                dot_unstuffing: dot,
            },
        )?;
        Ok(Run::new(written, input.len()).crc(state.current_crc()))
    })
}

fn run_decode_rapidyenc(input: &[u8], out_len: usize) -> Result<Run, YencError> {
    canary_run("decode_rapidyenc", input.len(), out_len, |out| {
        let written = decode_rapidyenc(input, out)?;
        Ok(Run::new(written, input.len()))
    })
}

fn run_decode_rapidyenc_ex(
    is_raw: bool,
    carry_in: RapidyencDecodeState,
    input: &[u8],
    out_len: usize,
) -> Result<Run, YencError> {
    let label = format!("decode_rapidyenc_ex(is_raw={is_raw}, state={carry_in:?})");
    canary_run(&label, input.len(), out_len, |out| {
        let mut state = carry_in;
        let written = decode_rapidyenc_ex(is_raw, input, out, &mut state)?;
        Ok(Run::new(written, input.len()).carry(state))
    })
}

fn run_decode_rapidyenc_incremental(
    input: &[u8],
    out_len: usize,
    state: &mut RapidyencDecodeState,
) -> Result<Run, YencError> {
    canary_run(
        "decode_rapidyenc_incremental",
        input.len(),
        out_len,
        |out| {
            let progress = decode_rapidyenc_incremental(input, out, state)?;
            Ok(Run::new(progress.bytes_written, progress.source_consumed)
                .end(progress.end)
                .carry(*state))
        },
    )
}

/// The compact window sizes the contract calls out, deduped and ordered.
fn output_ladder(input_len: usize, decoded_len: usize) -> Vec<usize> {
    let mut lens = vec![
        0,
        decoded_len.saturating_sub(1),
        decoded_len,
        input_len.saturating_sub(1),
    ];
    lens.sort_unstable();
    lens.dedup();
    lens.retain(|&len| len <= input_len);
    lens
}

/// A `DecodeState`'s observable state, for comparing two streaming decodes.
#[derive(Debug, PartialEq, Eq)]
struct ChunkCarry {
    escape_pending: bool,
    at_line_start: bool,
    dot_pending: bool,
    bytes_decoded: u64,
    crc: u32,
}

fn chunk_carry(state: &DecodeState) -> ChunkCarry {
    ChunkCarry {
        escape_pending: state.escape_pending,
        at_line_start: state.at_line_start,
        dot_pending: state.dot_pending,
        bytes_decoded: state.bytes_decoded,
        crc: state.current_crc(),
    }
}

// ── §1  the rapidyenc family, compact vs roomy ──────────────────────────────

/// `decode_rapidyenc` into every window size the contract names, over every
/// body shape and every size that brackets a vector width, must either
/// reproduce the roomy decode byte for byte or refuse with `BufferTooSmall`.
#[test]
fn rapidyenc_compact_windows_match_the_roomy_run() {
    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BOUNDARY_SIZES.into_iter().chain(BULK_SIZES) {
            for body in [
                encoded_body(shape, len),
                with_suffix(&encoded_body(shape, len), TERMINATOR),
                with_suffix(&encoded_body(shape, len), CONTROL),
            ] {
                let roomy = run_decode_rapidyenc(&body, body.len())
                    .unwrap_or_else(|err| panic!("{shape:?}/{len}: roomy run failed: {err:?}"));
                for out_len in output_ladder(body.len(), roomy.written) {
                    let label = format!("decode_rapidyenc {shape:?} len={len} out={out_len}");
                    tally.push(assert_compact_run(
                        run_decode_rapidyenc(&body, out_len),
                        &roomy,
                        body.len(),
                        out_len,
                        &label,
                    ));
                }
            }
        }
    }
    assert_verdicts(&tally, 500, "decode_rapidyenc");
    assert_saw_refusals(&tally, "decode_rapidyenc");
}

fn count_verdicts(tally: &[Verdict], want: Verdict) -> usize {
    tally.iter().filter(|&&verdict| verdict == want).count()
}

/// A sweep only means something if a real share of it drove the scalar
/// fallback. Without this the whole battery could degenerate into comparing the
/// SIMD kernel against itself and still pass.
fn assert_verdicts(tally: &[Verdict], min_total: usize, label: &str) {
    let compact_ok = count_verdicts(tally, Verdict::CompactOk);
    eprintln!(
        "{label}: {} comparisons ({compact_ok} compact ok, {} refused, {} roomy)",
        tally.len(),
        count_verdicts(tally, Verdict::Refused),
        count_verdicts(tally, Verdict::RoomyOk)
    );
    assert!(
        tally.len() >= min_total,
        "{label}: only {} comparisons",
        tally.len()
    );
    assert!(
        compact_ok * 4 >= tally.len(),
        "{label}: only {compact_ok} of {} comparisons actually exercised the scalar fallback",
        tally.len()
    );
}

/// The ladder sweeps also have to prove the refusal arm is reachable.
fn assert_saw_refusals(tally: &[Verdict], label: &str) {
    assert!(
        count_verdicts(tally, Verdict::Refused) > 0,
        "{label}: no window was ever refused"
    );
}

/// The same ladder for `decode_rapidyenc_ex`, across both `is_raw` modes and
/// every carry-in state — the carry-out state must survive the compact kernel
/// unchanged too, not just the bytes.
#[test]
fn rapidyenc_ex_compact_windows_match_the_roomy_run() {
    const STATES: [RapidyencDecodeState; 7] = [
        RapidyencDecodeState::CrLf,
        RapidyencDecodeState::Eq,
        RapidyencDecodeState::Cr,
        RapidyencDecodeState::None,
        RapidyencDecodeState::CrLfDot,
        RapidyencDecodeState::CrLfDotCr,
        RapidyencDecodeState::CrLfEq,
    ];

    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BOUNDARY_SIZES {
            let body = encoded_body(shape, len);
            for is_raw in [true, false] {
                for carry_in in STATES {
                    let roomy = run_decode_rapidyenc_ex(is_raw, carry_in, &body, body.len())
                        .unwrap_or_else(|err| {
                            panic!(
                                "{shape:?}/{len}/{is_raw}/{carry_in:?}: roomy run failed: {err:?}"
                            )
                        });
                    for out_len in output_ladder(body.len(), roomy.written) {
                        let label = format!(
                            "decode_rapidyenc_ex {shape:?} len={len} raw={is_raw} \
                             carry={carry_in:?} out={out_len}"
                        );
                        tally.push(assert_compact_run(
                            run_decode_rapidyenc_ex(is_raw, carry_in, &body, out_len),
                            &roomy,
                            body.len(),
                            out_len,
                            &label,
                        ));
                    }
                }
            }
        }
    }
    assert_verdicts(&tally, 1_000, "decode_rapidyenc_ex");
    assert_saw_refusals(&tally, "decode_rapidyenc_ex");
}

// ── §2  incremental decode across chunk boundaries ──────────────────────────

/// Feed a body to `decode_rapidyenc_incremental` in several pieces, each into a
/// window sized to exactly that piece's decoded length, and require the
/// concatenation — bytes, total consumed, and end detection — to equal one
/// roomy whole-input call.
#[test]
fn rapidyenc_incremental_compact_chunks_match_one_roomy_call() {
    let mut checked = 0usize;
    for shape in SHAPES {
        for len in BOUNDARY_SIZES {
            for body in [
                encoded_body(shape, len),
                with_suffix(&encoded_body(shape, len), TERMINATOR),
                with_suffix(&encoded_body(shape, len), CONTROL),
            ] {
                let mut whole_state = RapidyencDecodeState::default();
                let whole =
                    run_decode_rapidyenc_incremental(&body, body.len(), &mut whole_state).unwrap();

                for parts in [2usize, 3, 5, 7] {
                    let step = body.len().div_ceil(parts).max(1);
                    let mut state = RapidyencDecodeState::default();
                    let mut bytes = Vec::new();
                    let mut consumed = 0usize;
                    let mut end = RapidyencDecodeEnd::None;
                    let mut offset = 0usize;

                    while offset < body.len() && end == RapidyencDecodeEnd::None {
                        let stop = (offset + step).min(body.len());
                        let chunk = &body[offset..stop];

                        // Size this call's window to exactly what this chunk
                        // decodes to, measured with a roomy probe on a clone of
                        // the live carry state so the probe cannot perturb it.
                        let mut probe_state = state;
                        let probe =
                            run_decode_rapidyenc_incremental(chunk, chunk.len(), &mut probe_state)
                                .unwrap();

                        let run =
                            run_decode_rapidyenc_incremental(chunk, probe.written, &mut state)
                                .unwrap_or_else(|err| {
                                    panic!(
                                        "{shape:?} len={len} parts={parts} offset={offset}: \
                                 compact chunk refused an exactly-sized window: {err:?}"
                                    )
                                });
                        assert_eq!(
                            run, probe,
                            "{shape:?} len={len} parts={parts} offset={offset}: \
                             compact chunk diverged from the roomy chunk"
                        );

                        if run.end == RapidyencDecodeEnd::None {
                            assert_eq!(
                                run.consumed,
                                chunk.len(),
                                "{shape:?} len={len} parts={parts} offset={offset}: \
                                 stopped short without reporting an end"
                            );
                        }
                        bytes.extend_from_slice(&run.bytes);
                        consumed += run.consumed;
                        end = run.end;
                        offset += run.consumed;
                        checked += 1;
                    }

                    assert_eq!(
                        bytes, whole.bytes,
                        "{shape:?} len={len} parts={parts}: chunked bytes differ from one call"
                    );
                    assert_eq!(
                        consumed, whole.consumed,
                        "{shape:?} len={len} parts={parts}: chunked consumed differs"
                    );
                    assert_eq!(
                        end, whole.end,
                        "{shape:?} len={len} parts={parts}: end detection differs"
                    );
                    assert_eq!(
                        state, whole_state,
                        "{shape:?} len={len} parts={parts}: carry state differs"
                    );
                }
            }
        }
    }
    assert!(checked > 500, "incremental chunk calls checked {checked}");
}

// ── §3  decode_chunk and the until-control hook ─────────────────────────────

/// `decode_chunk` fed whole, into every window on the ladder, must match the
/// roomy run — including the state it leaves behind for the next chunk.
#[test]
fn decode_chunk_compact_windows_match_the_roomy_run() {
    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BOUNDARY_SIZES.into_iter().chain(BULK_SIZES) {
            let body = encoded_body(shape, len);
            for dot in [true, false] {
                let mut roomy_state = DecodeState::new();
                let roomy = run_decode_chunk(&body, body.len(), &mut roomy_state, dot).unwrap();
                let roomy_carry = chunk_carry(&roomy_state);

                for out_len in output_ladder(body.len(), roomy.written) {
                    let label = format!("decode_chunk {shape:?} len={len} dot={dot} out={out_len}");
                    let mut state = DecodeState::new();
                    let compact = run_decode_chunk(&body, out_len, &mut state, dot);
                    let verdict = assert_compact_run(compact, &roomy, body.len(), out_len, &label);
                    if verdict != Verdict::Refused {
                        assert_eq!(chunk_carry(&state), roomy_carry, "{label}: carry differs");
                    }
                    tally.push(verdict);
                }
            }
        }
    }
    assert_verdicts(&tally, 300, "decode_chunk");
    assert_saw_refusals(&tally, "decode_chunk");
}

/// `decode_body_chunk_until_control` sizes its own destination, so its
/// "compact" dimension is the destination vector's spare capacity rather than a
/// slice length. A vector with nothing to spare must produce exactly what a
/// generously pre-reserved one does, and must stop at the same `=y` control
/// line either way.
#[test]
fn until_control_agrees_whatever_spare_capacity_the_destination_has() {
    let mut checked = 0usize;
    for shape in SHAPES {
        for len in BOUNDARY_SIZES {
            let plain = encoded_body(shape, len);
            for body in [
                plain.clone(),
                with_suffix(&plain, CONTROL),
                with_suffix(&plain, TERMINATOR),
            ] {
                // Roomy: destination pre-reserved far beyond what is needed.
                let mut roomy_state = DecodeState::new();
                let mut roomy_out = Vec::with_capacity(body.len() * 4 + 1024);
                let roomy =
                    decode_body_chunk_until_control(&mut roomy_state, &body, &mut roomy_out)
                        .unwrap();

                // Compact: a destination that already holds bytes and has zero
                // spare capacity, so the reservation inside must reallocate.
                let mut tight_state = DecodeState::new();
                let mut tight_out = b"KEEP".to_vec();
                tight_out.shrink_to_fit();
                assert_eq!(
                    tight_out.capacity(),
                    tight_out.len(),
                    "destination still has spare capacity"
                );
                let tight =
                    decode_body_chunk_until_control(&mut tight_state, &body, &mut tight_out)
                        .unwrap();

                let label = format!("until_control {shape:?} len={len} body={}", body.len());
                assert_eq!(&tight_out[..4], b"KEEP", "{label}: clobbered the prefix");
                assert_eq!(&tight_out[4..], &roomy_out[..], "{label}: bytes differ");
                assert_eq!(tight.bytes_written, roomy.bytes_written, "{label}: written");
                assert_eq!(
                    tight.source_consumed, roomy.source_consumed,
                    "{label}: consumed"
                );
                assert_eq!(tight.end, roomy.end, "{label}: end");
                assert_eq!(
                    chunk_carry(&tight_state),
                    chunk_carry(&roomy_state),
                    "{label}: carry"
                );
                assert_eq!(
                    roomy_out.len(),
                    roomy.bytes_written,
                    "{label}: appended length disagrees with bytes_written"
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 100, "until-control cases checked {checked}");
}

/// The `=y` control line is where an end-detecting body decode must stop, and
/// it must stop at exactly the same source offset whether it is the SIMD or the
/// scalar kernel doing the looking.
#[test]
fn until_control_stops_at_the_control_line() {
    let body = with_suffix(&encoded_body(Shape::Mixed, 200), CONTROL);
    let mut state = DecodeState::new();
    let mut out = Vec::new();
    let progress = decode_body_chunk_until_control(&mut state, &body, &mut out).unwrap();

    assert_eq!(progress.end, RapidyencDecodeEnd::Control);
    // The decode consumed the body plus the `\r\n=y` that opened the control
    // line, and nothing beyond it.
    let control_at = body.len() - CONTROL.len();
    assert_eq!(
        progress.source_consumed,
        control_at + 4,
        "stopped at {} rather than just past the `=y` at {control_at}",
        progress.source_consumed
    );
    assert!(progress.source_consumed < body.len());

    // And what it produced is exactly what the same prefix produces on the
    // roomy whole-body path.
    let mut roomy = vec![0u8; control_at];
    let mut crc = Crc32::new();
    let written = decode_body(
        &body[..control_at],
        &mut roomy,
        &mut crc,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )
    .unwrap();
    assert_eq!(out, roomy[..written]);
}

// ── §4  appending into a non-empty destination ──────────────────────────────

/// `decode_nntp_append` writes into the destination's spare capacity. It must
/// leave everything already in the vector alone, append exactly what a fresh
/// decode produces, and do so even when the vector had no spare capacity at all
/// and the reservation inside had to move it.
#[test]
fn nntp_append_preserves_a_nonempty_destination() {
    for len in [0usize, 1, 63, 128, 1000] {
        for shape in SHAPES {
            let article = nntp_article(&encoded_body(shape, len), "append.bin");

            // Ground truth: the slice entry point with a roomy destination.
            let mut fresh = vec![0u8; max_decoded_len(article.len())];
            let expected = decode_nntp(&article, &mut fresh).unwrap();
            let expected_bytes = fresh[..expected.bytes_written].to_vec();

            for prefix_len in [1usize, 7, 64, 300] {
                let prefix: Vec<u8> = (0..prefix_len).map(|i| (i as u8) ^ 0x5A).collect();

                // Zero spare capacity before the call.
                let mut dest = prefix.clone();
                dest.shrink_to_fit();
                assert_eq!(
                    dest.capacity(),
                    dest.len(),
                    "{shape:?}/{len}: destination still had spare capacity"
                );

                let result = decode_nntp_append(&article, &mut dest).unwrap();
                assert_eq!(
                    &dest[..prefix_len],
                    &prefix[..],
                    "{shape:?}/{len}: prefix was modified"
                );
                assert_eq!(
                    &dest[prefix_len..],
                    &expected_bytes[..],
                    "{shape:?}/{len}: appended bytes differ from a fresh decode"
                );
                assert_eq!(dest.len(), prefix_len + expected.bytes_written);
                assert_eq!(result.bytes_written, expected.bytes_written);
                assert_eq!(result.part_crc, expected.part_crc);
                assert_eq!(result.crc_status, expected.crc_status);

                // And again into a vector that already has room to spare, to
                // pin that the reservation path is not what makes it correct.
                let mut roomy_dest = prefix.clone();
                roomy_dest.reserve(article.len() * 2 + 4096);
                decode_nntp_append(&article, &mut roomy_dest).unwrap();
                assert_eq!(
                    roomy_dest, dest,
                    "{shape:?}/{len}: reservation changed the result"
                );
            }
        }
    }
}

/// A raw-NNTP article wrapped around an already-encoded, already-dot-stuffed
/// body, with `=ybegin`/`=yend` fields taken from the roomy decode of that body
/// so the article is self-consistent by construction.
fn nntp_article(body: &[u8], name: &str) -> Vec<u8> {
    let mut scratch = vec![0u8; max_decoded_len(body.len())];
    let mut crc = Crc32::new();
    let written = decode_body(
        body,
        &mut scratch,
        &mut crc,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )
    .expect("fixture body decodes");
    let crc = crc.finalize();

    let mut article = Vec::new();
    article
        .extend_from_slice(format!("=ybegin line=128 size={written} name={name}\r\n").as_bytes());
    article.extend_from_slice(body);
    article.extend_from_slice(b"\r\n");
    article.extend_from_slice(format!("=yend size={written} crc32={crc:08x}\r\n").as_bytes());
    article
}

// ── §5  sizes around the vector and raw-kernel boundaries ───────────────────

/// One sweep that puts every byte-level entry point through the same body at
/// every size that brackets a SIMD vector width or the raw kernel's activation
/// runway, compact against roomy.
#[test]
fn boundary_sizes_decode_identically_compact_and_roomy() {
    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BOUNDARY_SIZES {
            let body = encoded_body(shape, len);
            assert_every_byte_level_api_agrees(&body, &format!("{shape:?}/{len}"), &mut tally);
        }
    }
    assert_verdicts(&tally, 500, "boundary sizes");
}

/// The same sweep at sizes where the SIMD loop runs many full iterations, so a
/// compact/roomy divergence in the steady state cannot hide behind the
/// prologue.
#[test]
fn bulk_sizes_decode_identically_compact_and_roomy() {
    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BULK_SIZES {
            let body = encoded_body(shape, len);
            assert_every_byte_level_api_agrees(&body, &format!("{shape:?}/{len}"), &mut tally);
        }
    }
    assert_verdicts(&tally, 100, "bulk sizes");
}

/// Run `body` through every compact-tolerant entry point at an exactly-sized
/// window and compare against the roomy run.
fn assert_every_byte_level_api_agrees(body: &[u8], label: &str, tally: &mut Vec<Verdict>) {
    let n = body.len();

    for dot in [true, false] {
        let roomy = run_decode_body(body, n, dot).unwrap();
        let exact = run_decode_body(body, roomy.written, dot);
        tally.push(assert_compact_run(
            exact,
            &roomy,
            n,
            roomy.written,
            &format!("{label} decode_body"),
        ));

        let mut roomy_state = DecodeState::new();
        let roomy_chunk = run_decode_chunk(body, n, &mut roomy_state, dot).unwrap();
        let mut exact_state = DecodeState::new();
        let exact_chunk = run_decode_chunk(body, roomy_chunk.written, &mut exact_state, dot);
        tally.push(assert_compact_run(
            exact_chunk,
            &roomy_chunk,
            n,
            roomy_chunk.written,
            &format!("{label} decode_chunk"),
        ));
        assert_eq!(
            chunk_carry(&exact_state),
            chunk_carry(&roomy_state),
            "{label} decode_chunk: carry differs"
        );
    }

    let roomy = run_decode_rapidyenc(body, n).unwrap();
    tally.push(assert_compact_run(
        run_decode_rapidyenc(body, roomy.written),
        &roomy,
        n,
        roomy.written,
        &format!("{label} decode_rapidyenc"),
    ));

    for is_raw in [true, false] {
        let roomy = run_decode_rapidyenc_ex(is_raw, RapidyencDecodeState::CrLf, body, n).unwrap();
        tally.push(assert_compact_run(
            run_decode_rapidyenc_ex(is_raw, RapidyencDecodeState::CrLf, body, roomy.written),
            &roomy,
            n,
            roomy.written,
            &format!("{label} decode_rapidyenc_ex raw={is_raw}"),
        ));
    }

    let mut roomy_state = RapidyencDecodeState::default();
    let roomy = run_decode_rapidyenc_incremental(body, n, &mut roomy_state).unwrap();
    let mut exact_state = RapidyencDecodeState::default();
    tally.push(assert_compact_run(
        run_decode_rapidyenc_incremental(body, roomy.written, &mut exact_state),
        &roomy,
        n,
        roomy.written,
        &format!("{label} decode_rapidyenc_incremental"),
    ));
    assert_eq!(
        exact_state, roomy_state,
        "{label} incremental: carry differs"
    );
}

// ── §6  carried state across a chunk boundary ───────────────────────────────

/// A body containing, in order, every carry shape a chunk boundary can land on:
/// a pending `=`, a lone CR, a completed CRLF, and a CRLF followed by a
/// line-start dot.
const CARRY_BODY: &[u8] = b"Aa=Jbb\r\ncc\r\n..dd=K=L\r\nee\r\n.ff\r\ngg";

fn find_at(body: &[u8], needle: &[u8]) -> usize {
    body.windows(needle.len())
        .position(|window| window == needle)
        .unwrap_or_else(|| panic!("fixture is missing {needle:?}"))
}

/// The four boundaries the contract names, as `(name, split offset)`.
fn named_carry_splits() -> Vec<(&'static str, usize)> {
    vec![
        ("after a pending `=`", find_at(CARRY_BODY, b"=") + 1),
        ("after a lone CR", find_at(CARRY_BODY, b"\r\n") + 1),
        ("after a CRLF", find_at(CARRY_BODY, b"\r\n") + 2),
        ("after a CRLF and a dot", find_at(CARRY_BODY, b"\r\n.") + 3),
    ]
}

/// Splitting a body at a boundary that leaves decoder state pending must not
/// change what comes out — with each half decoded into a window sized to
/// exactly that half's output.
#[test]
fn carried_state_splits_agree_with_the_unsplit_decode() {
    // Every named boundary is distinct, so the sweep below really does cover
    // four different carries and not the same one four times.
    let named = named_carry_splits();
    for (i, (name_a, split_a)) in named.iter().enumerate() {
        for (name_b, split_b) in named.iter().skip(i + 1) {
            assert_ne!(
                split_a, split_b,
                "`{name_a}` and `{name_b}` are the same split"
            );
        }
    }
    assert_eq!(
        &CARRY_BODY[named[0].1 - 1..named[0].1],
        b"=",
        "the pending-escape split does not end on `=`"
    );
    assert_eq!(&CARRY_BODY[named[3].1 - 1..named[3].1], b".");

    let mut checked = 0usize;
    for shape_body in [
        CARRY_BODY.to_vec(),
        with_suffix(CARRY_BODY, TERMINATOR),
        with_suffix(CARRY_BODY, CONTROL),
    ] {
        // Named boundaries first, then every split point, so a regression at one
        // of the four carries names itself before the sweep buries it.
        let sweep = (0..=shape_body.len()).map(|split| ("sweep", split));
        for (name, split) in named_carry_splits().into_iter().chain(sweep) {
            if split > shape_body.len() {
                continue;
            }
            assert_chunk_split_agrees(&shape_body, split, name);
            assert_incremental_split_agrees(&shape_body, split, name);
            checked += 1;
        }
    }
    assert!(checked > 100, "carry splits checked {checked}");
}

fn assert_chunk_split_agrees(body: &[u8], split: usize, name: &str) {
    for dot in [true, false] {
        let mut whole_state = DecodeState::new();
        let whole = run_decode_chunk(body, body.len(), &mut whole_state, dot).unwrap();

        let (head, tail) = body.split_at(split);
        let mut state = DecodeState::new();

        let mut probe_state = state.clone();
        let head_probe = run_decode_chunk(head, head.len(), &mut probe_state, dot).unwrap();
        let head_run = run_decode_chunk(head, head_probe.written, &mut state, dot)
            .unwrap_or_else(|err| panic!("decode_chunk head {name} split={split}: {err:?}"));

        let mut probe_state = state.clone();
        let tail_probe = run_decode_chunk(tail, tail.len(), &mut probe_state, dot).unwrap();
        let tail_run = run_decode_chunk(tail, tail_probe.written, &mut state, dot)
            .unwrap_or_else(|err| panic!("decode_chunk tail {name} split={split}: {err:?}"));

        let mut joined = head_run.bytes.clone();
        joined.extend_from_slice(&tail_run.bytes);
        assert_eq!(
            joined, whole.bytes,
            "decode_chunk(dot={dot}) {name} split={split}: bytes differ from the unsplit decode"
        );
        assert_eq!(
            chunk_carry(&state),
            chunk_carry(&whole_state),
            "decode_chunk(dot={dot}) {name} split={split}: carry differs"
        );
    }
}

fn assert_incremental_split_agrees(body: &[u8], split: usize, name: &str) {
    let mut whole_state = RapidyencDecodeState::default();
    let whole = run_decode_rapidyenc_incremental(body, body.len(), &mut whole_state).unwrap();

    let (head, tail) = body.split_at(split);
    let mut state = RapidyencDecodeState::default();

    let mut probe_state = state;
    let head_probe = run_decode_rapidyenc_incremental(head, head.len(), &mut probe_state).unwrap();
    let head_run = run_decode_rapidyenc_incremental(head, head_probe.written, &mut state)
        .unwrap_or_else(|err| panic!("incremental head {name} split={split}: {err:?}"));
    assert_eq!(
        head_run, head_probe,
        "incremental head {name} split={split}"
    );

    let mut bytes = head_run.bytes.clone();
    let mut consumed = head_run.consumed;
    let mut end = head_run.end;

    if end == RapidyencDecodeEnd::None {
        let mut probe_state = state;
        let tail_probe =
            run_decode_rapidyenc_incremental(tail, tail.len(), &mut probe_state).unwrap();
        let tail_run = run_decode_rapidyenc_incremental(tail, tail_probe.written, &mut state)
            .unwrap_or_else(|err| panic!("incremental tail {name} split={split}: {err:?}"));
        assert_eq!(
            tail_run, tail_probe,
            "incremental tail {name} split={split}"
        );
        bytes.extend_from_slice(&tail_run.bytes);
        consumed += tail_run.consumed;
        end = tail_run.end;
        assert_eq!(
            state, whole_state,
            "incremental {name} split={split}: carry differs"
        );
    }

    assert_eq!(
        bytes, whole.bytes,
        "incremental {name} split={split}: bytes differ from the unsplit decode"
    );
    assert_eq!(
        consumed, whole.consumed,
        "incremental {name} split={split}: consumed differs"
    );
    assert_eq!(
        end, whole.end,
        "incremental {name} split={split}: end differs"
    );
}

// ── §7  terminators and trailers ────────────────────────────────────────────

/// `\r\n.\r\n` ends a raw NNTP body, and it must be found at the same offset by
/// the compact and the roomy kernel — including when the terminator itself is
/// what the window has no room to reach past.
#[test]
fn raw_terminator_is_detected_identically_compact_and_roomy() {
    let mut tally = Vec::new();
    for shape in SHAPES {
        for len in BOUNDARY_SIZES {
            let body = with_suffix(&encoded_body(shape, len), TERMINATOR);
            // Trailing bytes after the terminator must never be reached.
            let stream = with_suffix(&body, b"THIS MUST NOT BE DECODED");

            let mut roomy_state = RapidyencDecodeState::default();
            let roomy =
                run_decode_rapidyenc_incremental(&stream, stream.len(), &mut roomy_state).unwrap();
            assert_eq!(
                roomy.end,
                RapidyencDecodeEnd::Article,
                "{shape:?}/{len}: terminator not detected on the roomy path"
            );
            assert_eq!(
                roomy.consumed,
                body.len(),
                "{shape:?}/{len}: roomy run ran past the terminator"
            );

            for out_len in output_ladder(stream.len(), roomy.written) {
                let mut state = RapidyencDecodeState::default();
                tally.push(assert_compact_run(
                    run_decode_rapidyenc_incremental(&stream, out_len, &mut state),
                    &roomy,
                    stream.len(),
                    out_len,
                    &format!("terminator {shape:?}/{len} out={out_len}"),
                ));
            }

            // The body-level hook must agree about where the article ended.
            let mut chunk_state = DecodeState::new();
            let mut out = Vec::new();
            let progress =
                decode_body_chunk_until_control(&mut chunk_state, &stream, &mut out).unwrap();
            assert_eq!(progress.end, RapidyencDecodeEnd::Article);
            assert_eq!(progress.source_consumed, body.len());
            assert_eq!(out, roomy.bytes);
        }
    }
    assert_verdicts(&tally, 100, "raw terminator");
    assert_saw_refusals(&tally, "raw terminator");
}

/// A complete article with a `=yend` trailer decodes to the same bytes and the
/// same verified CRC through every whole-article entry point, single-part and
/// multi-part alike.
#[test]
fn article_trailers_agree_across_the_whole_article_entry_points() {
    for len in [0usize, 1, 15, 16, 17, 127, 128, 129, 1000, 4096] {
        let payload: Vec<u8> = (0..len).map(|i| ((i * 31 + 7) % 256) as u8).collect();

        let mut single = Vec::new();
        encode(&payload, &mut single, 128, "single.bin").unwrap();
        let mut out = vec![0u8; max_decoded_len(single.len())];
        let result = decode(&single, &mut out).unwrap();
        assert_eq!(&out[..result.bytes_written], &payload[..], "len={len}");
        assert!(result.has_trailer, "len={len}: trailer not seen");

        // The same article via the NNTP entries (the encoder escapes a
        // line-start `.`, so raw and dot-unstuffed reads coincide here).
        let mut nntp_out = vec![0u8; max_decoded_len(single.len())];
        let nntp = decode_nntp(&single, &mut nntp_out).unwrap();
        assert_eq!(&nntp_out[..nntp.bytes_written], &payload[..], "len={len}");

        let mut appended = Vec::new();
        decode_nntp_append(&single, &mut appended).unwrap();
        assert_eq!(appended, payload, "len={len}");

        if len > 0 {
            let mut multi = Vec::new();
            encode_part(
                &payload,
                &mut multi,
                128,
                "multi.bin",
                1,
                1,
                1,
                len as u64,
                len as u64,
            )
            .unwrap();
            let mut multi_out = vec![0u8; max_decoded_len(multi.len())];
            let multi_result = decode(&multi, &mut multi_out).unwrap();
            assert_eq!(
                &multi_out[..multi_result.bytes_written],
                &payload[..],
                "len={len}: multipart"
            );
            assert!(multi_result.has_trailer);
        }
    }
}

/// The shared shape of every tier-1 entry point.
type WholeArticleEntry = fn(&[u8], &mut [u8]) -> Result<DecodeResult, YencError>;

/// Tier 1: the whole-article entries refuse a compact destination before they
/// parse anything, and refuse it without touching the buffer they were handed.
#[test]
fn whole_article_entries_reject_a_compact_destination() {
    let payload: Vec<u8> = (0..600).map(|i| ((i * 17 + 3) % 256) as u8).collect();
    let mut article = Vec::new();
    encode(&payload, &mut article, 64, "reject.bin").unwrap();

    let decoded_len = payload.len();
    assert!(decoded_len < article.len(), "fixture is not compressible");

    let entries: [(&str, WholeArticleEntry); 3] = [
        ("decode", decode),
        ("decode_nntp", decode_nntp),
        ("decode_with_options", |input, output| {
            decode_with_options(
                input,
                output,
                DecodeOptions {
                    dot_unstuffing: true,
                },
            )
        }),
    ];

    for out_len in [0, decoded_len, article.len() - 1] {
        for (name, call) in entries {
            let mut buf = vec![CANARY; PAD + out_len + PAD];
            let err = call(&article, &mut buf[PAD..PAD + out_len]).unwrap_err();
            match err {
                YencError::BufferTooSmall { needed, available } => {
                    assert_eq!(needed, max_decoded_len(article.len()), "{name}");
                    assert_eq!(available, out_len, "{name}");
                }
                other => panic!("{name} at out={out_len} returned {other:?}"),
            }
            assert!(
                buf.iter().all(|&b| b == CANARY),
                "{name} at out={out_len} wrote into a buffer it rejected"
            );
        }
    }

    // Exactly `input.len()` is the smallest accepted size, and it works.
    let mut exact = vec![0u8; max_decoded_len(article.len())];
    let result = decode(&article, &mut exact).unwrap();
    assert_eq!(&exact[..result.bytes_written], &payload[..]);
}

// ── §9  zero-length output windows ──────────────────────────────────────────

/// A zero-length destination with non-empty input is a legitimate call. It must
/// answer with an honest `Ok(0)` when the input really decodes to nothing, or a
/// typed `BufferTooSmall` when it does not — never a panic, never a write,
/// never a silent short decode.
///
/// Which of the two applies is not a property of the input alone: `\r\n.\r\n`
/// decodes to nothing under NNTP dot-unstuffing but to one `.` byte without it,
/// and `\r\n=y` is an end marker to the end-detecting entries and an ordinary
/// escape to the rest. So each API is judged against its own roomy oracle
/// rather than against a hand-declared expectation.
#[test]
fn zero_length_windows_are_honest_never_a_panic() {
    const INPUTS: [&[u8]; 12] = [
        b"\r\n",
        b"\r\n\r\n",
        b"\r",
        b"\n",
        b"\r\n.\r\n",
        b"\r\n=y",
        b"\r\n=yend size=0\r\n",
        b"A",
        b"=J",
        b"\r\n..",
        b"AB\r\nCD",
        b"\r\n.A",
    ];

    let mut accepted = 0usize;
    let mut refused = 0usize;

    for input in INPUTS {
        let mut cases: Vec<(String, Run, Result<Run, YencError>)> = Vec::new();

        for dot in [true, false] {
            cases.push((
                format!("decode_body(dot={dot})"),
                run_decode_body(input, input.len(), dot).unwrap(),
                run_decode_body(input, 0, dot),
            ));

            let mut roomy_state = DecodeState::new();
            let roomy = run_decode_chunk(input, input.len(), &mut roomy_state, dot).unwrap();
            let mut state = DecodeState::new();
            cases.push((
                format!("decode_chunk(dot={dot})"),
                roomy,
                run_decode_chunk(input, 0, &mut state, dot),
            ));
        }

        cases.push((
            "decode_rapidyenc".to_string(),
            run_decode_rapidyenc(input, input.len()).unwrap(),
            run_decode_rapidyenc(input, 0),
        ));

        for is_raw in [true, false] {
            let carry = RapidyencDecodeState::CrLf;
            cases.push((
                format!("decode_rapidyenc_ex(is_raw={is_raw})"),
                run_decode_rapidyenc_ex(is_raw, carry, input, input.len()).unwrap(),
                run_decode_rapidyenc_ex(is_raw, carry, input, 0),
            ));
        }

        let mut roomy_state = RapidyencDecodeState::default();
        let roomy = run_decode_rapidyenc_incremental(input, input.len(), &mut roomy_state).unwrap();
        let mut state = RapidyencDecodeState::default();
        let compact = run_decode_rapidyenc_incremental(input, 0, &mut state);
        if let Ok(run) = &compact {
            // Consumption is reported honestly even with nowhere to write.
            assert_eq!(
                run.consumed, roomy.consumed,
                "incremental({input:?}): consumed differs at a zero-length window"
            );
            if run.end == RapidyencDecodeEnd::None {
                assert_eq!(
                    run.consumed,
                    input.len(),
                    "incremental({input:?}): stopped short without reporting an end"
                );
            }
        }
        cases.push(("decode_rapidyenc_incremental".to_string(), roomy, compact));

        for (name, roomy, compact) in cases {
            if compact.is_ok() {
                accepted += 1;
            } else {
                refused += 1;
            }
            assert_compact_run(
                compact,
                &roomy,
                input.len(),
                0,
                &format!("{name} on {input:?}"),
            );
        }
    }

    // Both halves of the contract really are exercised by this input set.
    assert!(accepted > 10, "no zero-length Ok(0) cases ({accepted})");
    assert!(refused > 10, "no zero-length refusals ({refused})");
}

/// An empty input is a no-op for every compact-tolerant entry point, whatever
/// the destination looks like.
#[test]
fn empty_input_writes_nothing() {
    for out_len in [0usize, 1, 64] {
        let run = run_decode_rapidyenc(b"", out_len).unwrap();
        assert_eq!(run.written, 0);
        assert_eq!(run_decode_body(b"", out_len, true).unwrap().written, 0);

        let mut state = DecodeState::new();
        assert_eq!(
            run_decode_chunk(b"", out_len, &mut state, true)
                .unwrap()
                .written,
            0
        );
        assert_eq!(state.bytes_decoded, 0);

        let mut state = RapidyencDecodeState::default();
        let progress = run_decode_rapidyenc_incremental(b"", out_len, &mut state).unwrap();
        assert_eq!((progress.written, progress.consumed), (0, 0));
        assert_eq!(progress.end, RapidyencDecodeEnd::None);

        let mut chunk_state = DecodeState::new();
        let mut out = b"KEEP".to_vec();
        let progress = decode_body_chunk_until_control(&mut chunk_state, b"", &mut out).unwrap();
        assert_eq!((progress.bytes_written, progress.source_consumed), (0, 0));
        assert_eq!(out, b"KEEP");
    }
}
