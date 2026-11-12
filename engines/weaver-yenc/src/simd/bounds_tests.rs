// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Memory bounds and carried state of every decode tier this CPU runs, forced,
// against the scalar state machine (`fuzzing::decode_reference`).
//
// Every input length 0..=300, every head offset 0..64, four modes (body;
// raw with a pending escape kept; raw searching for the end; raw searching
// with a pending escape kept), every entry state in turn. Each case decodes
// three times, into buffers next to inaccessible pages:
//
//   A. the input ends exactly at a PROT_NONE page (a read past its end
//      faults) and the output is exactly `input.len()` bytes ending at one
//      (a store at or past `input.len()` faults, the `decode_kernel`
//      contract);
//   B. the input starts `head` bytes after a PROT_NONE page (head 0: a read
//      before its start faults) and the output starts exactly after one (a
//      store before its start faults), followed by `PAD` canary bytes past
//      `input.len()`, which must stay intact;
//   C. a compact output (half the input) ending at a PROT_NONE page: only
//      decoded bytes are stored, so the whole buffer equals the reference's.
//
// A second test runs A with the end 1..=63 bytes before the guard page, on
// the input (the bytes after its end change the decode if a kernel takes
// them) and then on the output (canary bytes after its end), so the end is
// not where a window or the page ends.
//
// Each run must give the reference's result, carried state and decoded
// bytes. AVX2 runs with the frozen `asm!` raw kernel and with the intrinsic
// one. Deterministic: fixed seeds, no timing.
//
// Unit tests rather than `tests/` because the forced-tier entry points exist
// only under `cfg(fuzzing)` or `cfg(test)`.

use super::fuzzing::{
    Mode, Outcome, State, TIERS, Tier, decode_reference, decode_tier, runs, set_and_add_select,
    set_avx2_raw_asm, set_zen4,
};
use crate::YencError;

const MAX_LEN: usize = 300;
const PAD: usize = 64;
const FILL: u8 = 0xa5;

const MODES: [Mode; 4] = [
    Mode {
        dot_unstuffing: false,
        preserve_pending: false,
        search_end: false,
    },
    Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: false,
    },
    Mode {
        dot_unstuffing: true,
        preserve_pending: false,
        search_end: true,
    },
    Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: true,
    },
];

// ---- guarded memory ---------------------------------------------------------

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod guard {
    use std::ffi::c_void;

    unsafe extern "C" {
        fn mmap(
            addr: *mut c_void,
            len: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            off: i64,
        ) -> *mut c_void;
        fn mprotect(addr: *mut c_void, len: usize, prot: i32) -> i32;
        fn munmap(addr: *mut c_void, len: usize) -> i32;
        fn sysconf(name: i32) -> i64;
    }

    const PROT_NONE: i32 = 0;
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 2;
    #[cfg(target_os = "macos")]
    const MAP_ANON: i32 = 0x1000;
    #[cfg(target_os = "linux")]
    const MAP_ANON: i32 = 0x20;
    #[cfg(target_os = "macos")]
    const SC_PAGESIZE: i32 = 29;
    #[cfg(target_os = "linux")]
    const SC_PAGESIZE: i32 = 30;

    // `rw_len` read-write bytes between two PROT_NONE pages.
    pub struct Arena {
        base: *mut u8,
        total: usize,
        rw: *mut u8,
        rw_len: usize,
    }

    impl Arena {
        pub fn new(min_len: usize) -> Self {
            // SAFETY: plain libc calls with valid arguments; results checked.
            unsafe {
                let page = sysconf(SC_PAGESIZE) as usize;
                assert!(page.is_power_of_two(), "page size {page}");
                let rw_len = min_len.div_ceil(page) * page;
                let total = rw_len + 2 * page;
                let base = mmap(
                    std::ptr::null_mut(),
                    total,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANON,
                    -1,
                    0,
                );
                assert!(base as isize != -1 && !base.is_null(), "mmap failed");
                let base = base.cast::<u8>();
                assert_eq!(mprotect(base.cast(), page, PROT_NONE), 0);
                assert_eq!(mprotect(base.add(page + rw_len).cast(), page, PROT_NONE), 0);
                Arena {
                    base,
                    total,
                    rw: base.add(page),
                    rw_len,
                }
            }
        }

        // `n` bytes starting `off` bytes after the leading guard page.
        pub fn head(&mut self, off: usize, n: usize) -> &mut [u8] {
            assert!(off + n <= self.rw_len);
            // SAFETY: inside the read-write part of the mapping.
            unsafe { std::slice::from_raw_parts_mut(self.rw.add(off), n) }
        }

        // `n` bytes ending right at the trailing guard page.
        pub fn tail(&mut self, n: usize) -> &mut [u8] {
            assert!(n <= self.rw_len);
            // SAFETY: inside the read-write part of the mapping.
            unsafe { std::slice::from_raw_parts_mut(self.rw.add(self.rw_len - n), n) }
        }
    }

    impl Drop for Arena {
        fn drop(&mut self) {
            // SAFETY: the mapping made in `new`.
            unsafe {
                munmap(self.base.cast(), self.total);
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod guard {
    // No guard pages here: a heap buffer with slack on both sides. The
    // canary and reference checks still run.
    const SLACK: usize = 4096;

    pub struct Arena {
        buf: Vec<u8>,
        rw_len: usize,
    }

    impl Arena {
        pub fn new(min_len: usize) -> Self {
            Arena {
                buf: vec![0u8; min_len + 2 * SLACK],
                rw_len: min_len,
            }
        }
        pub fn head(&mut self, off: usize, n: usize) -> &mut [u8] {
            assert!(off + n <= self.rw_len);
            &mut self.buf[SLACK + off..SLACK + off + n]
        }
        pub fn tail(&mut self, n: usize) -> &mut [u8] {
            assert!(n <= self.rw_len);
            let end = SLACK + self.rw_len;
            &mut self.buf[end - n..end]
        }
    }
}

use guard::Arena;

// ---- inputs -----------------------------------------------------------------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// Encoded-looking bytes dense in the decoder's special shapes: escapes, bare
// and paired line breaks, line-start dots, `=y` controls and terminators.
fn content(len: usize, seed: u64) -> Vec<u8> {
    let mut s = seed;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        let r = splitmix(&mut s);
        let token: &[u8] = match r % 100 {
            0..=49 => {
                let b = (r >> 8) as u8;
                out.push(if matches!(b, b'=' | b'\r' | b'\n' | b'.') {
                    b'a'
                } else {
                    b
                });
                continue;
            }
            50..=59 => b"=",
            60..=66 => b"\r",
            67..=73 => b"\n",
            74..=81 => b"\r\n",
            82..=87 => b".",
            88..=91 => b"y",
            92..=95 => b"\r\n.",
            96..=98 => b"\r\n=y",
            _ => b"\r\n.\r\n",
        };
        out.extend_from_slice(token);
    }
    out.truncate(len);
    out
}

// A forced kernel form: the AVX2 raw kernel (asm or intrinsic) and escape
// select (and-add or blend), or the AVX-512 loop (Zen 4 or plain). `None`
// leaves the CPU's choice.
#[derive(Clone, Copy)]
struct Form {
    asm: Option<bool>,
    and_add: Option<bool>,
    zen4: Option<bool>,
}

const CPU_FORM: Form = Form {
    asm: None,
    and_add: None,
    zen4: None,
};

fn set_form(f: Form) {
    set_avx2_raw_asm(f.asm);
    set_and_add_select(f.and_add);
    set_zen4(f.zen4);
}

// Each tier that runs here, AVX2 once per raw kernel and escape select, and
// AVX-512 once per loop form: every form runs on any CPU with the tier.
fn variants() -> Vec<(Tier, Form, String)> {
    let mut v = Vec::new();
    for tier in TIERS {
        if !runs(tier) {
            continue;
        }
        match tier {
            Tier::Avx2 => {
                for (asm, and_add) in [(true, true), (true, false), (false, true), (false, false)] {
                    let name = format!(
                        "avx2{}{}",
                        if asm { "+asm" } else { "-asm" },
                        if and_add { "+andadd" } else { "+blend" }
                    );
                    v.push((
                        tier,
                        Form {
                            asm: Some(asm),
                            and_add: Some(and_add),
                            zen4: None,
                        },
                        name,
                    ));
                }
            }
            Tier::Avx512Vbmi2 => {
                for zen4 in [true, false] {
                    let name = format!("avx512-vbmi2{}", if zen4 { "+zen4" } else { "+plain" });
                    v.push((
                        tier,
                        Form {
                            zen4: Some(zen4),
                            ..CPU_FORM
                        },
                        name,
                    ));
                }
            }
            _ => v.push((tier, CPU_FORM, tier.as_str().to_string())),
        }
    }
    v
}

fn same(a: &Result<Outcome, YencError>, b: &Result<Outcome, YencError>) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

// ---- tests ------------------------------------------------------------------

#[test]
fn every_tier_stays_in_bounds_and_matches_the_reference() {
    let variants = variants();
    let mut input_arena = Arena::new(MAX_LEN + 64);
    let mut output_arena = Arena::new(MAX_LEN + 64 + PAD);
    let mut cases = 0usize;
    for len in 0..=MAX_LEN {
        for head in 0..64 {
            for (m, &mode) in MODES.iter().enumerate() {
                let seed = ((len as u64) << 16) | ((head as u64) << 4) | m as u64;
                let data = content(len, seed);
                let entry = ((len + head + m) % 7) as u8;
                let compact = len / 2;

                let mut want_state = State::new(entry, None);
                let mut want_out = vec![FILL; len];
                let want = decode_reference(&data, &mut want_out, &mut want_state, mode);
                let mut want_c_state = State::new(entry, None);
                let mut want_c_out = vec![FILL; compact];
                let want_c = decode_reference(&data, &mut want_c_out, &mut want_c_state, mode);

                for (tier, asm, name) in &variants {
                    set_form(*asm);
                    let ctx = |run: &str| {
                        format!(
                            "{name} run {run} len {len} head {head} mode {mode:?} entry {entry}"
                        )
                    };

                    // A: input and exact output both end at a guard page.
                    input_arena.tail(len).copy_from_slice(&data);
                    output_arena.tail(len).fill(FILL);
                    let mut state = State::new(entry, None);
                    let input = input_arena.tail(len);
                    let output = output_arena.tail(len);
                    let res = decode_tier(*tier, input, output, &mut state, mode);
                    assert!(
                        same(&res, &want),
                        "{}: result {res:?} want {want:?}",
                        ctx("A")
                    );
                    assert_eq!(state, want_state, "{}: state", ctx("A"));
                    if let Ok(o) = &res {
                        assert!(
                            output[..o.written] == want_out[..o.written],
                            "{}: bytes",
                            ctx("A")
                        );
                    }

                    // B: input `head` bytes after a guard page, output right
                    // after one with canary bytes past `input.len()`.
                    input_arena.head(head, len).copy_from_slice(&data);
                    output_arena.head(0, len + PAD).fill(FILL);
                    let mut state = State::new(entry, None);
                    let input = input_arena.head(head, len);
                    let output = output_arena.head(0, len + PAD);
                    let res = decode_tier(*tier, input, output, &mut state, mode);
                    assert!(
                        same(&res, &want),
                        "{}: result {res:?} want {want:?}",
                        ctx("B")
                    );
                    assert_eq!(state, want_state, "{}: state", ctx("B"));
                    if let Ok(o) = &res {
                        assert!(
                            output[..o.written] == want_out[..o.written],
                            "{}: bytes",
                            ctx("B")
                        );
                    }
                    if let Some(at) = output[len..].iter().position(|&b| b != FILL) {
                        panic!("{}: store past input.len() at {}", ctx("B"), len + at);
                    }

                    // C: compact output ending at a guard page.
                    if compact < len {
                        input_arena.tail(len).copy_from_slice(&data);
                        output_arena.tail(compact).fill(FILL);
                        let mut state = State::new(entry, None);
                        let input = input_arena.tail(len);
                        let output = output_arena.tail(compact);
                        let res = decode_tier(*tier, input, output, &mut state, mode);
                        assert!(
                            same(&res, &want_c),
                            "{}: result {res:?} want {want_c:?}",
                            ctx("C")
                        );
                        assert_eq!(state, want_c_state, "{}: state", ctx("C"));
                        assert!(*output == want_c_out[..], "{}: compact bytes", ctx("C"));
                    }
                    cases += 1;
                }
            }
        }
    }
    set_form(CPU_FORM);
    let names: Vec<_> = variants.iter().map(|v| v.2.as_str()).collect();
    eprintln!("decode bounds: {cases} cases over {names:?}");
}

// Bytes past the input's end that change the decode if a kernel takes them:
// escapes, line breaks, a stuffed dot and `=y`.
const PAST_END: &[u8] = b"=\r\n.=y\r\n.\r\n=";

// One case with an end `gap` bytes (1..=63) before a guard page, so the end
// is not where a window or the page ends. `input_gap`: the input ends `gap`
// bytes before the page, `PAST_END` bytes fill the rest of it (a read there
// is allowed, but must not change the result), and the exact output ends at
// a guard page. Otherwise the input ends at a guard page and the exact output
// ends `gap` bytes before one, with `FILL` canary bytes in the gap, which
// must stay intact.
#[allow(clippy::too_many_arguments)]
fn gap_case(
    ctx: &str,
    tier: Tier,
    data: &[u8],
    gap: usize,
    input_gap: bool,
    entry: u8,
    line: Option<u32>,
    mode: Mode,
    input_arena: &mut Arena,
    output_arena: &mut Arena,
) -> (Result<Outcome, YencError>, State, Vec<u8>) {
    let len = data.len();
    let mut state = State::new(entry, line);
    if input_gap {
        let buf = input_arena.tail(len + gap);
        buf[..len].copy_from_slice(data);
        for (i, b) in buf[len..].iter_mut().enumerate() {
            *b = PAST_END[i % PAST_END.len()];
        }
        output_arena.tail(len).fill(FILL);
        let input = &input_arena.tail(len + gap)[..len];
        let output = output_arena.tail(len);
        let res = decode_tier(tier, input, output, &mut state, mode);
        (res, state, output.to_vec())
    } else {
        input_arena.tail(len).copy_from_slice(data);
        output_arena.tail(len + gap).fill(FILL);
        let input = input_arena.tail(len);
        let buf = output_arena.tail(len + gap);
        let res = decode_tier(tier, input, &mut buf[..len], &mut state, mode);
        if let Some(at) = buf[len..].iter().position(|&b| b != FILL) {
            panic!("{ctx}: store past the output's end at {}", len + at);
        }
        (res, state, buf[..len].to_vec())
    }
}

// Run A with the end off the page: every input length 0..=300, every gap
// 1..=63 bytes before the guard page, on the input's end and then on the
// output's, the four modes, every variant.
#[test]
fn every_tier_stays_in_bounds_with_the_end_before_the_page() {
    let variants = variants();
    let mut input_arena = Arena::new(MAX_LEN + 64);
    let mut output_arena = Arena::new(MAX_LEN + 64);
    let mut cases = 0usize;
    for len in 0..=MAX_LEN {
        for gap in 1..64 {
            for (m, &mode) in MODES.iter().enumerate() {
                let seed = 0x6a70_0000_0000 | ((len as u64) << 16) | ((gap as u64) << 4) | m as u64;
                let data = content(len, seed);
                let entry = ((len + gap + m) % 7) as u8;
                let mut want_state = State::new(entry, None);
                let mut want_out = vec![FILL; len];
                let want = decode_reference(&data, &mut want_out, &mut want_state, mode);
                for (tier, form, name) in &variants {
                    set_form(*form);
                    for input_gap in [true, false] {
                        let ctx = format!(
                            "{name} len {len} {} gap {gap} mode {mode:?} entry {entry}",
                            if input_gap { "input" } else { "output" }
                        );
                        let (res, state, out) = gap_case(
                            &ctx,
                            *tier,
                            &data,
                            gap,
                            input_gap,
                            entry,
                            None,
                            mode,
                            &mut input_arena,
                            &mut output_arena,
                        );
                        assert!(same(&res, &want), "{ctx}: result {res:?} want {want:?}");
                        assert_eq!(state, want_state, "{ctx}: state");
                        if let Ok(o) = &res {
                            assert!(out[..o.written] == want_out[..o.written], "{ctx}: bytes");
                        }
                        cases += 1;
                    }
                }
            }
        }
    }
    set_form(CPU_FORM);
    let names: Vec<_> = variants.iter().map(|v| v.2.as_str()).collect();
    eprintln!("decode bounds, end before the page: {cases} cases over {names:?}");
}

// A window that ends between the `\r` and `\n` of a line break, before a
// line-start `=` that is the last input byte: the carried state must be
// `CrLfEq` (the scalar machine's), not `Eq`, so a following end-searching
// call sees `\r\n=y` as the control marker instead of decoding the `y` as
// escaped data. The SSE-class raw kernel (`sse_raw_body`, x86_sse.rs) and the
// AVX-512 raw kernel (`decode_kernel_avx512_raw`, x86_avx512.rs) kept only
// the oracle's `nextMask` (a `CrLf`/`Cr` before a `.`) and left `None`, and
// the scalar tail then read the `\n` as a bare line feed.
// Smallest repro (SSE2, head 1): 62 x `a`, then `\r\n=`.
#[test]
fn line_start_escape_after_a_window_keeps_crlfeq() {
    let carry = Mode {
        dot_unstuffing: true,
        preserve_pending: true,
        search_end: false,
    };
    let search = Mode {
        dot_unstuffing: true,
        preserve_pending: false,
        search_end: true,
    };
    let crlf_eq = State::new(6, None);
    let mut input_arena = Arena::new(512);
    let mut output_arena = Arena::new(512);
    for (tier, asm, name) in variants() {
        set_form(asm);
        for k in 0..=260 {
            for head in 0..64 {
                let mut data = vec![b'a'; k];
                data.extend_from_slice(b"\r\n=");
                let len = data.len();
                input_arena.head(head, len).copy_from_slice(&data);
                let mut state = State::new(0, None);
                let input = input_arena.head(head, len);
                let output = output_arena.tail(len);
                let o = decode_tier(tier, input, output, &mut state, carry).unwrap();
                assert_eq!(
                    (o.consumed, o.written),
                    (len, k),
                    "{name} k {k} head {head}"
                );
                assert_eq!(state, crlf_eq, "{name} k {k} head {head}: carried state");

                // The next call, searching for the end, starts at the `y`.
                let next = b"ybegin";
                input_arena.tail(next.len()).copy_from_slice(next);
                let input = input_arena.tail(next.len());
                let output = output_arena.tail(next.len());
                let o = decode_tier(tier, input, output, &mut state, search).unwrap();
                assert_eq!(
                    (o.consumed, o.written, o.end),
                    (1, 0, crate::RapidyencDecodeEnd::Control),
                    "{name} k {k} head {head}: `=y` across calls"
                );
            }
        }
    }
    set_form(CPU_FORM);
}

// A window whose last three bytes are `=\r\n`, in raw mode, before a stuffed
// line-start `.`: the escaped `\r` still ends a line (`decode_scalar_step`'s
// `Eq` arm maps `=\r` to `Cr`), so the window must carry `CrLf` and the `.`
// is dropped. `span_end_state` (x86_common.rs) required an unescaped `\r`
// and left `None`, so the AVX2 general kernel decoded the `.` as a data
// byte: one extra output byte. Entry `CrLfDot` sends AVX2 to that kernel.
// Smallest repro (AVX2, head 0, entry `CrLfDot`): 62 x `a`, `=\r\n.b`,
// 70 x `a`; 135 bytes written where the reference writes 134.
#[test]
fn escaped_cr_before_a_stuffed_dot_at_a_window_end_is_a_line_break() {
    let input_modes = [
        Mode {
            dot_unstuffing: true,
            preserve_pending: false,
            search_end: false,
        },
        Mode {
            dot_unstuffing: true,
            preserve_pending: true,
            search_end: false,
        },
    ];
    let mut input_arena = Arena::new(512);
    let mut output_arena = Arena::new(512);
    for (tier, asm, name) in variants() {
        set_form(asm);
        for mode in input_modes {
            for entry in 0..7u8 {
                for k in 0..=200 {
                    for head in [0usize, 1, 17, 31, 32, 63] {
                        let mut data = vec![b'a'; k];
                        data.extend_from_slice(b"=\r\n.b");
                        data.extend_from_slice(&[b'a'; 70]);
                        let len = data.len();
                        let mut want_state = State::new(entry, None);
                        let mut want_out = vec![FILL; len];
                        let want = decode_reference(&data, &mut want_out, &mut want_state, mode);
                        input_arena.head(head, len).copy_from_slice(&data);
                        let mut state = State::new(entry, None);
                        let input = input_arena.head(head, len);
                        let output = output_arena.tail(len);
                        let res = decode_tier(tier, input, output, &mut state, mode);
                        let ctx = format!("{name} mode {mode:?} entry {entry} k {k} head {head}");
                        assert!(same(&res, &want), "{ctx}: result {res:?} want {want:?}");
                        assert_eq!(state, want_state, "{ctx}: state");
                        let n = want.as_ref().map(|o| o.written).unwrap_or(0);
                        assert!(output[..n] == want_out[..n], "{ctx}: bytes");
                    }
                }
            }
        }
    }
    set_form(CPU_FORM);
}

// Long end-searching inputs whose last window chunks (`END_CHUNK` = 32
// windows of 64 bytes in the AVX-512 end loop) switch between the mode for
// input with `=` and the one for input without, ending at a guard page with
// an exact output ending at one. ASan does not see the `asm!` loops' loads
// and stores; the guard pages do. Each part: plain 128-byte lines with no
// `=` (the clean mode), lines dense in escapes, then the special shapes, in
// an order the seed picks, with the input cut at every length in a band
// around two and three chunks past the 128-byte entry gate. Every eighth
// length, and the two longest, also run with the end 1..=63 bytes before the
// guard page, on the input and then on the output (`gap_case`).
#[test]
fn long_end_search_chunks_stay_in_bounds() {
    let modes = [
        Mode {
            dot_unstuffing: true,
            preserve_pending: true,
            search_end: true,
        },
        Mode {
            dot_unstuffing: true,
            preserve_pending: false,
            search_end: true,
        },
        Mode {
            dot_unstuffing: true,
            preserve_pending: true,
            search_end: false,
        },
    ];
    let plain = |n: usize, s: &mut u64| -> Vec<u8> {
        let mut out = Vec::new();
        while out.len() < n {
            for _ in 0..126 {
                let b = splitmix(s) as u8;
                out.push(if matches!(b, b'=' | b'\r' | b'\n' | 0) {
                    b'q'
                } else {
                    b
                });
            }
            out.extend_from_slice(b"\r\n");
        }
        out.truncate(n);
        out
    };
    let dense = |n: usize, s: &mut u64| -> Vec<u8> {
        let mut out = Vec::new();
        while out.len() < n {
            let r = splitmix(s);
            out.extend_from_slice(match r % 8 {
                0 => b"=J".as_slice(),
                1 => b"=}",
                2 => b"\r\n",
                3 => b"\r\n..",
                4 => b"==",
                _ => b"abc",
            });
        }
        out.truncate(n);
        out
    };
    const MAX: usize = 3 * 2048 + 512;
    let mut input_arena = Arena::new(MAX + 64);
    let mut output_arena = Arena::new(MAX + 64);
    let variants = variants();
    let (mut cases, mut gap_cases) = (0usize, 0usize);
    for order in 0..4u64 {
        let mut s = 0x10e5_c4a1 ^ order;
        let mut data = match order {
            0 => [
                plain(2048, &mut s),
                dense(2048, &mut s),
                plain(2048, &mut s),
            ]
            .concat(),
            1 => [dense(2048, &mut s), plain(4096, &mut s)].concat(),
            2 => [plain(4096, &mut s), dense(1024, &mut s)].concat(),
            _ => [plain(2100, &mut s), dense(64, &mut s), plain(4000, &mut s)].concat(),
        };
        data.extend_from_slice(&content(MAX - data.len().min(MAX), s));
        data.truncate(MAX);
        let mut lens: Vec<usize> = (2048 + 64..2048 + 64 + 140).collect();
        lens.extend(4096 + 64..4096 + 64 + 140);
        lens.extend([MAX - 1, MAX]);
        for &len in &lens {
            let data = &data[..len];
            for (m, &mode) in modes.iter().enumerate() {
                let entry = ((len + m) % 7) as u8;
                let mut want_state = State::new(entry, Some(128));
                let mut want_out = vec![FILL; len];
                let want = decode_reference(data, &mut want_out, &mut want_state, mode);
                for (tier, form, name) in &variants {
                    set_form(*form);
                    input_arena.tail(len).copy_from_slice(data);
                    output_arena.tail(len).fill(FILL);
                    let mut state = State::new(entry, Some(128));
                    let input = input_arena.tail(len);
                    let output = output_arena.tail(len);
                    let res = decode_tier(*tier, input, output, &mut state, mode);
                    let ctx = format!("{name} order {order} len {len} mode {mode:?} entry {entry}");
                    assert!(same(&res, &want), "{ctx}: result {res:?} want {want:?}");
                    assert_eq!(state, want_state, "{ctx}: state");
                    if let Ok(o) = &res {
                        assert!(output[..o.written] == want_out[..o.written], "{ctx}: bytes");
                    }
                    cases += 1;
                    // Every eighth length, and the two longest, again with the
                    // input's end, then the output's, 1..=63 bytes before the
                    // guard page.
                    if len % 8 != 0 && len < MAX - 1 {
                        continue;
                    }
                    for gap in 1..64 {
                        for input_gap in [true, false] {
                            let ctx = format!(
                                "{ctx} {} gap {gap}",
                                if input_gap { "input" } else { "output" }
                            );
                            let (res, state, out) = gap_case(
                                &ctx,
                                *tier,
                                data,
                                gap,
                                input_gap,
                                entry,
                                Some(128),
                                mode,
                                &mut input_arena,
                                &mut output_arena,
                            );
                            assert!(same(&res, &want), "{ctx}: result {res:?} want {want:?}");
                            assert_eq!(state, want_state, "{ctx}: state");
                            if let Ok(o) = &res {
                                assert!(out[..o.written] == want_out[..o.written], "{ctx}: bytes");
                            }
                            gap_cases += 1;
                        }
                    }
                }
            }
        }
    }
    set_form(CPU_FORM);
    eprintln!("long end-search bounds: {cases} cases, {gap_cases} with the end before the page");
}
