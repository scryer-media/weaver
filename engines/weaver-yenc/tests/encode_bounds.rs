// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Memory bounds of the encode kernels, on every tier this CPU runs.
//
// Each case encodes twice, into buffers next to inaccessible pages:
//
//   1. the input ends exactly at a PROT_NONE page (a read past its end
//      faults) and the output starts exactly after one (a write before its
//      start faults); the output budget is followed by `PAD` canary bytes,
//      so a write past `encode_max_len` fails with the case named;
//   2. the input starts exactly after a PROT_NONE page (a read before its
//      start faults) and the output is exactly `encode_max_len` bytes,
//      ending at a PROT_NONE page (a write past the budget faults).
//
// Both runs must give the scalar tier's bytes and column. Run 1 also
// records, per tier, the furthest byte a kernel stored past its returned
// length and the least budget it left unused (both lower bounds: a stored
// byte equal to the canary is not seen), printed at the end of each test.
//
// The guard pages come from the C library's `mmap` / `mprotect`, declared
// here (the C library is linked anyway; no crate dependency). Other targets
// keep the canary checks without guard pages.
//
// The rapidyenc oracle, when linked (`cfg(rapidyenc_linked)`), gets the same
// guard-page treatment against its own `rapidyenc_encode_max_length`.
//
// Deterministic: fixed seeds, no timing.

use std::mem::MaybeUninit;

use weaver_yenc::encode::{MAX_COLUMN, MAX_LINE_SIZE};
use weaver_yenc::{EncodeError, EncoderTier, encode_body_with_tier, encode_max_len};

// Canary bytes after the budget in run 1.
const PAD: usize = 256;
const FILL: u8 = 0xa5;
// Largest input any sweep uses, and room for its worst-case output.
const MAX_INPUT: usize = 4608;
const IN_ARENA: usize = MAX_INPUT;
const OUT_ARENA: usize = 6 * MAX_INPUT + 1024 + PAD;

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

    pub const GUARDED: bool = true;

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
        // `n` bytes starting right after the leading guard page.
        pub fn head(&mut self, n: usize) -> *mut u8 {
            assert!(n <= self.rw_len);
            self.rw
        }
        // `n` bytes ending right at the trailing guard page.
        pub fn tail(&mut self, n: usize) -> *mut u8 {
            assert!(n <= self.rw_len);
            // SAFETY: inside the mapping.
            unsafe { self.rw.add(self.rw_len - n) }
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
    // canary checks still run.
    pub const GUARDED: bool = false;
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
        pub fn head(&mut self, n: usize) -> *mut u8 {
            assert!(n <= self.rw_len);
            // SAFETY: inside the buffer.
            unsafe { self.buf.as_mut_ptr().add(SLACK) }
        }
        pub fn tail(&mut self, n: usize) -> *mut u8 {
            assert!(n <= self.rw_len);
            // SAFETY: inside the buffer.
            unsafe { self.buf.as_mut_ptr().add(SLACK + self.rw_len - n) }
        }
    }
}

use guard::Arena;

fn as_uninit(b: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    // SAFETY: `MaybeUninit<u8>` has the layout of `u8`; every byte written
    // through it is a valid `u8`.
    unsafe { std::slice::from_raw_parts_mut(b.as_mut_ptr().cast(), b.len()) }
}

// ---- inputs -----------------------------------------------------------------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// Source bytes that encode to NUL, LF, CR, '=' (always escaped).
const ALWAYS: [u8; 4] = [214, 224, 227, 19];
// Source bytes that encode to TAB, SPACE, '.' (escaped at line edges).
const EDGE: [u8; 3] = [223, 246, 4];
// A source byte that never escapes ('Z' after the +42).
const PLAIN: u8 = b'Z' - 42;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Zeros,
    AllCritical,
    AllEdge,
    Random,
    // One always-escaped byte everywhere: output exactly 2x plus breaks.
    EveryEscaped,
    // Plain bytes, escapes at the first and last byte of 16-, 32- and
    // 64-byte blocks.
    BlockEdges,
    // Plain bytes, escapes and line-edge bytes at the first and last byte of
    // every line (positions exact while the line holds no escape, then
    // shifted, which varies them further).
    LineEdges,
    // Half hot bytes, half random.
    HotMix,
    // Plain with a critical byte now and then.
    Sparse,
}

fn make_input(kind: Kind, len: usize, ls: usize, seed: u64) -> Vec<u8> {
    let mut st = seed ^ ((len as u64) << 20) ^ ((ls as u64) << 40) ^ kind as u64;
    let mut r = move || splitmix(&mut st);
    (0..len)
        .map(|i| {
            let x = r();
            match kind {
                Kind::Zeros => 0,
                Kind::AllCritical => ALWAYS[(x >> 8) as usize % 4],
                Kind::AllEdge => EDGE[(x >> 8) as usize % 3],
                Kind::Random => x as u8,
                Kind::EveryEscaped => 214,
                Kind::BlockEdges => {
                    if matches!(i % 64, 0 | 15 | 16 | 31 | 32 | 47 | 48 | 63) {
                        ALWAYS[(x >> 8) as usize % 4]
                    } else {
                        PLAIN
                    }
                }
                Kind::LineEdges => {
                    let c = i % ls.max(1);
                    if c == 0 || c + 1 == ls || c + 2 == ls {
                        if x & 1 == 0 {
                            EDGE[(x >> 8) as usize % 3]
                        } else {
                            ALWAYS[(x >> 8) as usize % 4]
                        }
                    } else {
                        PLAIN
                    }
                }
                Kind::HotMix => {
                    if x & 1 == 0 {
                        [214, 224, 227, 19, 223, 246, 4][(x >> 8) as usize % 7]
                    } else {
                        (x >> 16) as u8
                    }
                }
                Kind::Sparse => {
                    if x.is_multiple_of(61) {
                        ALWAYS[(x >> 8) as usize % 4]
                    } else {
                        PLAIN
                    }
                }
            }
        })
        .collect()
}

const LINE_SIZES: [usize; 24] = [
    1,
    2,
    3,
    4,
    11,
    12,
    13,
    16,
    31,
    32,
    33,
    63,
    64,
    65,
    66,
    127,
    128,
    129,
    255,
    256,
    257,
    997,
    4096,
    MAX_LINE_SIZE,
];

// 0..=300, around block multiples, every end 0..=64 bytes past two large
// block boundaries, and around line-length multiples.
fn lengths(ls: usize) -> Vec<usize> {
    let mut v: Vec<usize> = (0..=300).collect();
    for m in [
        320usize, 384, 448, 512, 576, 640, 768, 1024, 1536, 2048, 4096,
    ] {
        v.extend([m - 1, m, m + 1]);
    }
    for b in [1024usize, 4096] {
        v.extend((0..=64).map(|d| b + d));
    }
    for k in 1..=3usize {
        if let Some(x) = ls.checked_mul(k) {
            for l in [x.saturating_sub(1), x, x + 1] {
                if l <= MAX_INPUT {
                    v.push(l);
                }
            }
        }
    }
    v.sort_unstable();
    v.dedup();
    v
}

fn columns(ls: usize, seed: u64) -> Vec<usize> {
    let mut st = seed;
    let rnd = (splitmix(&mut st) % (ls as u64 + 2)) as usize;
    let mut v = vec![
        0,
        1,
        2,
        ls / 2,
        ls.saturating_sub(2),
        ls - 1,
        ls,
        ls + 1,
        rnd,
        MAX_COLUMN,
    ];
    v.retain(|&c| c <= MAX_COLUMN);
    v.sort_unstable();
    v.dedup();
    v
}

// ---- the checker ------------------------------------------------------------

struct Stats {
    cases: u64,
    max_past: usize,
    max_past_at: String,
    min_slack: usize,
    min_slack_at: String,
}

struct Checker {
    tiers: Vec<EncoderTier>,
    stats: Vec<Stats>,
    inp: Arena,
    out: Arena,
    want: Vec<u8>,
}

impl Checker {
    fn new() -> Self {
        let tiers = EncoderTier::available();
        let stats = tiers
            .iter()
            .map(|_| Stats {
                cases: 0,
                max_past: 0,
                max_past_at: String::new(),
                min_slack: usize::MAX,
                min_slack_at: String::new(),
            })
            .collect();
        Checker {
            tiers,
            stats,
            inp: Arena::new(IN_ARENA),
            out: Arena::new(OUT_ARENA),
            want: Vec::new(),
        }
    }

    fn case(&mut self, kind: Kind, ls: usize, col0: usize, src: &[u8], is_end: bool) {
        let len = src.len();
        let need = encode_max_len(len, ls);
        assert!(need + PAD <= OUT_ARENA && len <= IN_ARENA);
        let at = |tier: EncoderTier, run: u32| {
            format!("{tier} run {run}: {kind:?} len {len} line {ls} col {col0} end {is_end}")
        };
        self.want.clear();
        self.want.resize(need, 0);
        let mut wcol = col0;
        let wn = encode_body_with_tier(
            EncoderTier::Scalar,
            ls,
            &mut wcol,
            src,
            as_uninit(&mut self.want),
            is_end,
        )
        .expect("scalar encode");
        for (ti, &tier) in self.tiers.iter().enumerate() {
            // Run 1: input ends at a guard page, output starts after one,
            // canaries past the budget.
            // SAFETY: arena regions of the asserted sizes; input and output
            // live in different mappings.
            let (s, o) = unsafe {
                let ip = self.inp.tail(len);
                std::ptr::copy_nonoverlapping(src.as_ptr(), ip, len);
                let op = self.out.head(need + PAD);
                std::ptr::write_bytes(op, FILL, need + PAD);
                (
                    std::slice::from_raw_parts(ip, len),
                    std::slice::from_raw_parts_mut(op, need + PAD),
                )
            };
            let mut col = col0;
            let n = encode_body_with_tier(tier, ls, &mut col, s, as_uninit(&mut o[..need]), is_end)
                .unwrap_or_else(|e| panic!("{}: {e}", at(tier, 1)));
            assert!(
                n == wn && col == wcol && o[..n] == self.want[..wn],
                "{}: differs from scalar (len {n} vs {wn}, column {col} vs {wcol})",
                at(tier, 1)
            );
            if let Some(i) = o[need..].iter().rposition(|&b| b != FILL) {
                panic!(
                    "{}: wrote {} bytes past the encode_max_len budget of {need}",
                    at(tier, 1),
                    i + 1
                );
            }
            let extent = o[..need]
                .iter()
                .rposition(|&b| b != FILL)
                .map_or(0, |i| i + 1)
                .max(n);
            let st = &mut self.stats[ti];
            st.cases += 1;
            if extent - n > st.max_past {
                st.max_past = extent - n;
                st.max_past_at = at(tier, 1);
            }
            if need - extent < st.min_slack {
                st.min_slack = need - extent;
                st.min_slack_at = format!("{} (wrote {n}, budget {need})", at(tier, 1));
            }

            // Run 2: input starts after a guard page, output is exactly the
            // budget and ends at one.
            // SAFETY: as above.
            let (s, o) = unsafe {
                let ip = self.inp.head(len);
                std::ptr::copy_nonoverlapping(src.as_ptr(), ip, len);
                let op = self.out.tail(need);
                (
                    std::slice::from_raw_parts(ip, len),
                    std::slice::from_raw_parts_mut(op, need),
                )
            };
            let mut col = col0;
            let n = encode_body_with_tier(tier, ls, &mut col, s, as_uninit(o), is_end)
                .unwrap_or_else(|e| panic!("{}: {e}", at(tier, 2)));
            assert!(
                n == wn && col == wcol && o[..n] == self.want[..wn],
                "{}: differs from scalar (len {n} vs {wn}, column {col} vs {wcol})",
                at(tier, 2)
            );
        }
    }

    fn report(&self, label: &str) {
        eprintln!("encode_bounds {label}: guard pages {}", guard::GUARDED);
        for (tier, st) in self.tiers.iter().zip(&self.stats) {
            eprintln!(
                "  {tier:>12}: {} cases; max stored past returned length {} B ({}); least unused budget {} B ({})",
                st.cases, st.max_past, st.max_past_at, st.min_slack, st.min_slack_at
            );
        }
    }
}

fn sweep(kind: Kind) {
    let mut ck = Checker::new();
    for &ls in &LINE_SIZES {
        for len in lengths(ls) {
            let src = make_input(kind, len, ls, 0xb0_0d5);
            let cols = columns(ls, 0xc0_1000 ^ (len as u64) << 8 ^ ls as u64);
            for &col in &cols {
                for is_end in [false, true] {
                    ck.case(kind, ls, col, &src, is_end);
                }
            }
        }
    }
    ck.report(&format!("{kind:?}"));
}

#[test]
fn bounds_zeros() {
    sweep(Kind::Zeros);
}

#[test]
fn bounds_all_critical() {
    sweep(Kind::AllCritical);
}

#[test]
fn bounds_all_edge() {
    sweep(Kind::AllEdge);
}

#[test]
fn bounds_random() {
    sweep(Kind::Random);
}

#[test]
fn bounds_every_escaped() {
    sweep(Kind::EveryEscaped);
}

#[test]
fn bounds_block_edges() {
    sweep(Kind::BlockEdges);
}

#[test]
fn bounds_line_edges() {
    sweep(Kind::LineEdges);
}

#[test]
fn bounds_hot_mix() {
    sweep(Kind::HotMix);
}

#[test]
fn bounds_sparse() {
    sweep(Kind::Sparse);
}

// Every start column across a whole line (and two past it), for line sizes
// at and around the SIMD block widths, over lengths at the main loops'
// entry thresholds.
#[test]
fn bounds_every_column() {
    let mut ck = Checker::new();
    let lens = [
        1usize, 31, 32, 33, 63, 64, 65, 66, 67, 127, 128, 129, 130, 159, 160, 161, 192, 193, 200,
        257, 300, 513, 1000,
    ];
    for &ls in &[12usize, 13, 63, 64, 65, 127, 128, 129, 255, 256] {
        for &len in &lens {
            for kind in [
                Kind::Random,
                Kind::AllCritical,
                Kind::AllEdge,
                Kind::LineEdges,
            ] {
                let src = make_input(kind, len, ls, 0xe7e7);
                for col in 0..=ls + 1 {
                    for is_end in [false, true] {
                        ck.case(kind, ls, col, &src, is_end);
                    }
                }
            }
        }
    }
    ck.report("every column");
}

// The public API refuses a destination one byte short of `encode_max_len`
// on every tier, writing nothing, so no caller can hand a kernel less than
// the budget it relies on.
#[test]
fn short_destination_is_refused() {
    let src = make_input(Kind::HotMix, 1000, 128, 7);
    for tier in EncoderTier::available() {
        for &(len, ls) in &[
            (0usize, 1usize),
            (1, 1),
            (65, 12),
            (129, 128),
            (1000, 128),
            (1000, 1),
        ] {
            let need = encode_max_len(len, ls);
            let mut dst = vec![FILL; need - 1];
            let mut col = 5;
            assert_eq!(
                encode_body_with_tier(tier, ls, &mut col, &src[..len], as_uninit(&mut dst), true),
                Err(EncodeError::DestinationTooSmall {
                    needed: need,
                    available: need - 1
                }),
                "{tier} len {len} line {ls}"
            );
            assert_eq!(col, 5);
            assert!(
                dst.iter().all(|&b| b == FILL),
                "{tier}: wrote into a refused destination"
            );
        }
    }
}

// ---- rapidyenc under the same guards ---------------------------------------

#[cfg(rapidyenc_linked)]
mod rapidyenc_guarded {
    use super::*;
    use std::sync::Mutex;

    unsafe extern "C" {
        fn weaver_rapidyenc_encode_init();
        fn weaver_rapidyenc_encode_ex(
            line_size: i32,
            column: *mut i32,
            src: *const core::ffi::c_void,
            dest: *mut core::ffi::c_void,
            len: u64,
            is_end: i32,
        ) -> u64;
        fn weaver_rapidyenc_encode_max_length(len: u64, line_size: i32) -> u64;
        fn weaver_rapidyenc_encode_force(kernel: i32) -> i32;
    }

    // rapidyenc's kernel is a process-global pointer.
    static RAPIDYENC: Mutex<()> = Mutex::new(());

    const KERNELS: [(i32, &str); 7] = [
        (0, "generic"),
        (0x100, "SSE2"),
        (0x200, "SSSE3"),
        (0x381, "AVX"),
        (0x403, "AVX2"),
        (0x603, "VBMI2"),
        (0x1000, "NEON"),
    ];

    // Every rapidyenc kernel this host runs, into an output of exactly its
    // own `rapidyenc_encode_max_length` ending at a guard page and, again,
    // starting right after one, with the input against a guard page on the
    // other side. Its bytes must be weaver scalar's. Empty input is skipped
    // for the generic kernel (it reads out of bounds there, see
    // `kernel/scalar.rs`).
    #[test]
    fn rapidyenc_kernels_stay_in_their_own_budget() {
        let _g = RAPIDYENC.lock().unwrap_or_else(|p| p.into_inner());
        let mut inp = Arena::new(IN_ARENA);
        let mut out = Arena::new(OUT_ARENA);
        let mut ran = Vec::new();
        for (kid, name) in KERNELS {
            // SAFETY: init is idempotent; force checks CPU support itself.
            if unsafe {
                weaver_rapidyenc_encode_init();
                weaver_rapidyenc_encode_force(kid)
            } != 1
            {
                continue;
            }
            ran.push(name);
            let mut cases = 0u64;
            let mut min_slack = usize::MAX;
            for &ls in &[
                1usize, 2, 4, 11, 12, 13, 16, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 997,
            ] {
                let mut lens: Vec<usize> = (0..=300).collect();
                lens.extend((0..=64).map(|d| 1024 + d));
                lens.extend([511, 512, 513, 2047, 2048, 2049, 4095, 4096, 4097]);
                for len in lens {
                    if len == 0 && kid == 0 {
                        continue;
                    }
                    for kind in [
                        Kind::AllCritical,
                        Kind::Random,
                        Kind::AllEdge,
                        Kind::LineEdges,
                        Kind::BlockEdges,
                        Kind::Zeros,
                    ] {
                        let src = make_input(kind, len, ls, 0x4a9d);
                        for col0 in [0usize, 1, ls / 2, ls - 1, ls, ls + 1] {
                            for is_end in [false, true] {
                                let mut wcol = col0;
                                let mut want = vec![0u8; encode_max_len(len, ls)];
                                let wn = encode_body_with_tier(
                                    EncoderTier::Scalar,
                                    ls,
                                    &mut wcol,
                                    &src,
                                    as_uninit(&mut want),
                                    is_end,
                                )
                                .unwrap();
                                // SAFETY: plain call.
                                let cap = unsafe {
                                    weaver_rapidyenc_encode_max_length(len as u64, ls as i32)
                                } as usize;
                                assert_eq!(cap, encode_max_len(len, ls));
                                for run in 0..2 {
                                    // SAFETY: arena regions of the asserted
                                    // sizes; distinct mappings; the lock is
                                    // held.
                                    let (n, col, o) = unsafe {
                                        let ip = if run == 0 {
                                            inp.tail(len)
                                        } else {
                                            inp.head(len)
                                        };
                                        std::ptr::copy_nonoverlapping(src.as_ptr(), ip, len);
                                        let op = if run == 0 {
                                            out.tail(cap)
                                        } else {
                                            out.head(cap + PAD)
                                        };
                                        std::ptr::write_bytes(
                                            op,
                                            FILL,
                                            cap + if run == 0 { 0 } else { PAD },
                                        );
                                        let mut col = col0 as i32;
                                        let n = weaver_rapidyenc_encode_ex(
                                            ls as i32,
                                            &mut col,
                                            ip.cast(),
                                            op.cast(),
                                            len as u64,
                                            is_end as i32,
                                        ) as usize;
                                        let o = std::slice::from_raw_parts(
                                            op,
                                            cap + if run == 0 { 0 } else { PAD },
                                        );
                                        (n, col as usize, o)
                                    };
                                    let at = format!(
                                        "rapidyenc {name} run {run}: {kind:?} len {len} line {ls} col {col0} end {is_end}"
                                    );
                                    assert!(
                                        n == wn && col == wcol && o[..n] == want[..wn],
                                        "{at}: differs from weaver scalar"
                                    );
                                    if run == 1 {
                                        if let Some(i) = o[cap..].iter().rposition(|&b| b != FILL) {
                                            panic!(
                                                "{at}: wrote {} bytes past its own budget {cap}",
                                                i + 1
                                            );
                                        }
                                        let ext = o[..cap]
                                            .iter()
                                            .rposition(|&b| b != FILL)
                                            .map_or(0, |i| i + 1)
                                            .max(n);
                                        min_slack = min_slack.min(cap - ext);
                                    }
                                    cases += 1;
                                }
                            }
                        }
                    }
                }
            }
            eprintln!("rapidyenc {name}: {cases} guarded runs; least unused budget {min_slack} B");
        }
        // SAFETY: as above.
        unsafe { weaver_rapidyenc_encode_force(0) };
        eprintln!("rapidyenc kernels run under guard pages: {ran:?}");
    }
}
