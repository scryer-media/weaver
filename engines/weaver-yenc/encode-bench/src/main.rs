// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// yEnc encode bench: each weaver encoder tier against the matching rapidyenc
// kernel (forced), same process, interleaved samples.
//
//   WEAVER_RAPIDYENC_SRC=/path/to/rapidyenc \
//     cargo run --locked --release -- [--quick] [--tier NAME] [--fixture NAME]
//
// For every fixture and every rapidyenc kernel this CPU runs, the output of
// both sides is first checked byte for byte (and length and final column);
// then both are timed, A,B,A,B..., each sample a calibrated batch of calls.
// `ratio` is rapidyenc time / weaver time: above 1 means weaver is faster.
// A row whose weaver tier is not implemented yet times rapidyenc alone: that
// is the number the tier has to beat.
//
// Buffers sit at fixed page offsets on both sides (source at a page start,
// each destination half a page past one), so a run's layout, and with it
// 4 KiB store-to-load aliasing on small fixtures, does not change between
// processes or favour one side.
//
// Code placement is pinned too: every function on both sides starts on a
// 64-byte boundary (.cargo/config.toml, rapidyenc_encode_build.rs), so a
// change on one side does not move the other's loops. Run it under
// `setarch -R`; the `aslr:` header line says whether that was done.
//
// Every row gives each side's own min and median time next to the ratios,
// so a moved ratio shows which side moved. Each table row is repeated as one
// `TSV` line for collecting results from several hosts. No timing
// assertions anywhere.

#[path = "../../src/encode/kernel/mod.rs"]
#[allow(dead_code)]
mod kernel;

// `--verify` runs the shared test generators; they name the crate
// `weaver_yenc`, so this crate stands in for it.
extern crate self as weaver_yenc;
mod encode {
    pub(crate) use crate::kernel::{MAX_COLUMN, MAX_LINE_SIZE};
}
#[path = "../../tests/encode_support/mod.rs"]
mod encode_support;
mod verify;

use std::hint::black_box;
use std::mem::MaybeUninit;
use std::time::Instant;

use kernel::{EncoderTier, encode_body_with_tier, encode_max_len};

const LINE_SIZE: usize = 128;

// Destination offset past a page boundary; the source starts on one.
const DST_PAGE_OFFSET: usize = 2048;

// `len` bytes starting `off` bytes past a 4096-byte boundary.
struct Paged {
    buf: Vec<u8>,
    start: usize,
    len: usize,
}

impl Paged {
    fn new(len: usize, off: usize) -> Self {
        let buf = vec![0u8; len + 8192];
        let start = off.wrapping_sub(buf.as_ptr() as usize) & 4095;
        Paged { buf, start, len }
    }
    fn bytes(&mut self) -> &mut [u8] {
        &mut self.buf[self.start..self.start + self.len]
    }
    fn uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        let b = self.bytes();
        // SAFETY: `MaybeUninit<u8>` has the layout of `u8`, and every value
        // written through it is a valid `u8`.
        unsafe { std::slice::from_raw_parts_mut(b.as_mut_ptr().cast(), b.len()) }
    }
}

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
    fn weaver_rapidyenc_encode_kernel() -> i32;
    fn weaver_rapidyenc_encode_force(kernel: i32) -> i32;
}

// rapidyenc kernels by RYKERN id, with the weaver tier that matches each.
const PAIRS: [(i32, &str, EncoderTier); 7] = [
    (0, "generic", EncoderTier::Scalar),
    (0x1000, "NEON", EncoderTier::Neon),
    (0x100, "SSE2", EncoderTier::Sse2),
    (0x200, "SSSE3", EncoderTier::Ssse3),
    (0x381, "AVX", EncoderTier::Avx),
    (0x403, "AVX2", EncoderTier::Avx2),
    (0x603, "VBMI2", EncoderTier::Avx512Vbmi2),
];

struct Options {
    quick: bool,
    tier: Option<String>,
    fixture: Option<String>,
    // Run one side only, for `perf stat` / `perf record`.
    perf: Option<String>,
}

fn parse_args() -> Options {
    let mut opts = Options {
        quick: false,
        tier: None,
        fixture: None,
        perf: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--quick" => opts.quick = true,
            "--verify" => {
                verify::run();
                std::process::exit(0);
            }
            "--tier" => opts.tier = Some(args.next().expect("--tier needs a name")),
            "--fixture" => opts.fixture = Some(args.next().expect("--fixture needs a name")),
            "--perf" => opts.perf = Some(args.next().expect("--perf needs weaver, rapidyenc or none")),
            "-h" | "--help" => {
                println!(
                    "usage: encode-bench [--quick] [--tier NAME] [--fixture NAME]\n\
                     tiers: {} (or a rapidyenc kernel name: {})\n\
                     fixtures: {}",
                    EncoderTier::ALL.map(|t| t.as_str()).join(", "),
                    PAIRS.map(|p| p.1).join(", "),
                    FIXTURES.map(|f| f.0).into_iter().chain(STREAMS.map(|s| s.0)).collect::<Vec<_>>().join(", ")
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument {other:?} (try --help)"),
        }
    }
    opts
}

// ---- fixtures -------------------------------------------------------------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn random(len: usize) -> Vec<u8> {
    let mut state = 0x7e57_0000_0001;
    (0..len).map(|_| splitmix(&mut state) as u8).collect()
}

// Every byte encodes to NUL, LF, CR or '=': every byte is escaped.
fn all_critical(len: usize) -> Vec<u8> {
    const ALWAYS: [u8; 4] = [214, 224, 227, 19];
    let mut state = 0xc217;
    (0..len)
        .map(|_| ALWAYS[(splitmix(&mut state) >> 8) as usize % 4])
        .collect()
}

// Half the bytes encode to TAB, SPACE or '.', which are escaped only at line
// edges: stresses the line-start and line-end rules.
fn edge_heavy(len: usize) -> Vec<u8> {
    const EDGE: [u8; 3] = [223, 246, 4];
    let mut state = 0xed6e;
    (0..len)
        .map(|_| {
            let r = splitmix(&mut state);
            if r & 1 == 0 {
                EDGE[(r >> 8) as usize % 3]
            } else {
                (r >> 16) as u8
            }
        })
        .collect()
}

type Fixture = (&'static str, fn() -> Vec<u8>);

const FIXTURES: [Fixture; 6] = [
    // rapidyenc tool/bench.cc's shape: 768000 random bytes.
    ("random", || random(768_000)),
    ("random-64k", || random(65_536)),
    ("random-4k", || random(4096)),
    // Zero encodes to '*': no escapes at all.
    ("zeros", || vec![0u8; 768_000]),
    ("all-critical", || all_critical(768_000)),
    ("edge-heavy", || edge_heavy(768_000)),
];

// Non-repeating fixtures: consecutive articles of `len` bytes walking a
// 256 MiB seeded pool, each article starting on a page boundary, so a timed
// batch never sees an article twice and nothing repeats for the branch
// predictor or the caches to learn (real posts never repeat). Pool 0 is
// random bytes, pool 1 has the `edge-heavy` mix.
const STREAMS: [(&str, usize, usize); 4] = [
    ("random-stream", 768_000, 0),
    ("random-stream-64k", 65_536, 0),
    ("random-stream-4k", 4096, 0),
    ("edge-heavy-stream", 768_000, 1),
];
const STREAM_POOL: usize = 256 << 20;

fn stream_pool(kind: usize) -> Paged {
    let mut pool = Paged::new(STREAM_POOL, 0);
    if kind == 0 {
        let mut state = 0x5eed_57ea_0001;
        for c in pool.bytes().chunks_exact_mut(8) {
            c.copy_from_slice(&splitmix(&mut state).to_le_bytes());
        }
    } else {
        const EDGE: [u8; 3] = [223, 246, 4];
        let mut state = 0x5eed_57ea_0002;
        for b in pool.bytes() {
            // Branch-free select (bytes as `edge_heavy`), so building the
            // pool adds no branch misses to `--perf` counts.
            let r = splitmix(&mut state);
            let m = ((r & 1) as u8).wrapping_sub(1);
            *b = (EDGE[(r >> 8) as usize % 3] & m) | ((r >> 16) as u8 & !m);
        }
    }
    pool
}

// Article `k` of a stream of `len`-byte articles starts at `k * stride`.
fn stream_stride(len: usize) -> usize {
    len.div_ceil(4096) * 4096
}

// ---- the two encoders -----------------------------------------------------

fn weaver_once(tier: EncoderTier, src: &[u8], dst: &mut [MaybeUninit<u8>]) -> (usize, usize) {
    let mut column = 0usize;
    let n =
        encode_body_with_tier(tier, LINE_SIZE, &mut column, src, dst, true).expect("weaver encode");
    (n, column)
}

fn rapidyenc_once(src: &[u8], dst: &mut [u8]) -> (usize, usize) {
    let mut column = 0i32;
    // SAFETY: `dst` holds rapidyenc's own maximum for `src` at LINE_SIZE;
    // the two buffers are distinct.
    let n = unsafe {
        weaver_rapidyenc_encode_ex(
            LINE_SIZE as i32,
            &mut column,
            src.as_ptr().cast(),
            dst.as_mut_ptr().cast(),
            src.len() as u64,
            1,
        )
    };
    (n as usize, column as usize)
}

// ---- timing ---------------------------------------------------------------

// Calls per sample so a sample takes at least `target_ns`.
fn calibrate(target_ns: u128, mut f: impl FnMut()) -> u64 {
    f();
    let mut iters = 1u64;
    loop {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        if t.elapsed().as_nanos() >= target_ns || iters >= 1 << 30 {
            return iters;
        }
        iters *= 2;
    }
}

fn sample(iters: u64, f: &mut impl FnMut()) -> f64 {
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_nanos() as f64 / iters as f64
}

struct Stats {
    min: f64,
    median: f64,
}

fn stats(mut ns: Vec<f64>) -> Stats {
    ns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Stats {
        min: ns[0],
        median: ns[ns.len() / 2],
    }
}

fn mbps(bytes: usize, ns: f64) -> f64 {
    bytes as f64 / ns * 1000.0
}

// ---- host description -----------------------------------------------------

fn cpu_model() -> String {
    #[cfg(target_arch = "x86_64")]
    {
        let mut bytes = Vec::new();
        for leaf in 0x8000_0002u32..=0x8000_0004 {
            // SAFETY: CPUID exists on every x86_64 CPU; leaves 0x80000002-4
            // hold the brand string wherever the extended range does.
            #[allow(unused_unsafe)]
            let r = unsafe { std::arch::x86_64::__cpuid(leaf) };
            for word in [r.eax, r.ebx, r.ecx, r.edx] {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
        }
        let s = String::from_utf8_lossy(&bytes)
            .trim_matches(char::from(0))
            .trim()
            .to_string();
        if !s.is_empty() {
            return s;
        }
    }
    if cfg!(target_os = "macos")
        && let Ok(out) = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
    {
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !s.is_empty() {
            return s;
        }
    }
    if let Ok(info) = std::fs::read_to_string("/proc/cpuinfo") {
        let field = |name: &str| {
            info.lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.split(':').nth(1))
                .map(|v| v.trim().to_string())
        };
        if let Some(model) = field("model name") {
            return model;
        }
        if let (Some(imp), Some(part)) = (field("CPU implementer"), field("CPU part")) {
            return format!("arm implementer {imp} part {part}");
        }
    }
    "unknown".to_string()
}

fn features() -> String {
    let mut list: Vec<&str> = Vec::new();
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::is_x86_feature_detected as has;
        let all = [
            ("sse2", has!("sse2")),
            ("ssse3", has!("ssse3")),
            ("sse4.1", has!("sse4.1")),
            ("popcnt", has!("popcnt")),
            ("avx", has!("avx")),
            ("avx2", has!("avx2")),
            ("bmi1", has!("bmi1")),
            ("bmi2", has!("bmi2")),
            ("lzcnt", has!("lzcnt")),
            ("avx512f", has!("avx512f")),
            ("avx512bw", has!("avx512bw")),
            ("avx512vl", has!("avx512vl")),
            ("avx512vbmi2", has!("avx512vbmi2")),
        ];
        list.extend(all.iter().filter(|f| f.1).map(|f| f.0));
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::is_aarch64_feature_detected as has;
        let all = [
            ("neon", has!("neon")),
            ("crc", has!("crc")),
            ("sha3", has!("sha3")),
            ("sve", has!("sve")),
            ("sve2", has!("sve2")),
        ];
        list.extend(all.iter().filter(|f| f.1).map(|f| f.0));
    }
    list.join(" ")
}

// Whether this process runs with address randomisation off (`setarch -R`).
// The binary is position-independent, so with it on, code and data move per
// run, and so do branch-predictor and cache-set placement.
fn aslr_state() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        unsafe extern "C" {
            fn personality(persona: core::ffi::c_ulong) -> core::ffi::c_int;
        }
        const ADDR_NO_RANDOMIZE: core::ffi::c_int = 0x0040000;
        // SAFETY: 0xffffffff only queries the current persona.
        let p = unsafe { personality(0xffff_ffff) };
        if p >= 0 && p & ADDR_NO_RANDOMIZE != 0 {
            return "off (ADDR_NO_RANDOMIZE, e.g. setarch -R)";
        }
        return "ON: load addresses change per run; run under setarch -R";
    }
    #[allow(unreachable_code)]
    "not checked on this OS"
}

fn kernel_name(id: i32) -> String {
    PAIRS
        .iter()
        .find(|p| p.0 == id)
        .map(|p| p.1.to_string())
        .unwrap_or_else(|| format!("{id:#x}"))
}

// Interleaved samples, weaver first; rapidyenc alone when weaver has no tier.
fn time_sides(
    weaver: bool,
    n_samples: usize,
    target_ns: u128,
    run_w: &mut impl FnMut(),
    run_r: &mut impl FnMut(),
) -> (Vec<f64>, Vec<f64>) {
    let r_iters = calibrate(target_ns, &mut *run_r);
    let mut r_ns = Vec::with_capacity(n_samples);
    let mut w_ns = Vec::with_capacity(n_samples);
    if weaver {
        let w_iters = calibrate(target_ns, &mut *run_w);
        for _ in 0..n_samples {
            w_ns.push(sample(w_iters, run_w));
            r_ns.push(sample(r_iters, run_r));
        }
    } else {
        for _ in 0..n_samples {
            r_ns.push(sample(r_iters, run_r));
        }
    }
    (w_ns, r_ns)
}

// ---- main -----------------------------------------------------------------

fn main() {
    let opts = parse_args();
    // SAFETY: plain init call.
    unsafe { weaver_rapidyenc_encode_init() };
    let default_kernel = unsafe { weaver_rapidyenc_encode_kernel() };

    let cpu = cpu_model();
    let arch = std::env::consts::ARCH;
    println!("weaver-yenc encode bench (line size {LINE_SIZE}, is_end, column 0)");
    println!("host:      {arch} {} / {cpu}", std::env::consts::OS);
    println!("features:  {}", features());
    let tiers: Vec<String> = EncoderTier::ALL
        .iter()
        .filter(|t| t.cpu_supported())
        .map(|t| {
            if t.implemented() {
                t.as_str().to_string()
            } else {
                format!("{} (not implemented)", t.as_str())
            }
        })
        .collect();
    println!(
        "weaver:    CPU supports {}; dispatch picks {}",
        tiers.join(", "),
        EncoderTier::detect()
    );
    println!("rapidyenc: default kernel {}", kernel_name(default_kernel));
    println!("aslr:      {}", aslr_state());
    println!(
        "samples:   {} per side, interleaved; each a batch of >= {} ms\n",
        if opts.quick { 5 } else { 21 },
        if opts.quick { 1 } else { 5 }
    );

    let (n_samples, target_ns) = if opts.quick {
        (5usize, 1_000_000u128)
    } else {
        (21, 5_000_000)
    };

    println!(
        "{:<13} {:>7} | {:<12} {:>11} {:>11} {:>8} | {:<9} {:>10} {:>11} {:>8} | {:>6} {:>6}",
        "fixture",
        "bytes",
        "weaver",
        "min ns",
        "median ns",
        "MB/s",
        "rapidyenc",
        "min ns",
        "median ns",
        "MB/s",
        "r/w",
        "r/w med"
    );
    println!(
        "TSVHEAD\tarch\tcpu\tfixture\tbytes\tweaver_tier\tweaver_min_ns\tweaver_median_ns\trapidyenc_kernel\trapidyenc_min_ns\trapidyenc_median_ns\tratio_min\tratio_median"
    );

    let names = FIXTURES
        .iter()
        .map(|&(n, m)| (n, Some(m), (0, 0)))
        .chain(STREAMS.iter().map(|&(n, l, k)| (n, None, (l, k))));
    let mut pool_buf: [Option<Paged>; 2] = [None, None];
    for (fname, make, (slen, kind)) in names {
        if opts.fixture.as_deref().is_some_and(|f| f != fname) {
            continue;
        }
        let mut src_buf: Paged;
        // `n` articles of `len` bytes, `stride` apart; a fixed fixture is one.
        let (pool, len, stride, n): (&[u8], usize, usize, usize) = match make {
            Some(make) => {
                let data = make();
                src_buf = Paged::new(data.len(), 0);
                src_buf.bytes().copy_from_slice(&data);
                (src_buf.bytes(), data.len(), data.len(), 1)
            }
            None => {
                let st = stream_stride(slen);
                let pool = pool_buf[kind].get_or_insert_with(|| stream_pool(kind));
                (pool.bytes(), slen, st, STREAM_POOL / st)
            }
        };
        let src: &[u8] = &pool[..len];
        let art = |k: usize| &pool[k * stride..k * stride + len];
        let wcap = encode_max_len(src.len(), LINE_SIZE);
        let mut wdst_buf = Paged::new(wcap, DST_PAGE_OFFSET);
        let wdst = wdst_buf.uninit();
        let rcap =
            unsafe { weaver_rapidyenc_encode_max_length(src.len() as u64, LINE_SIZE as i32) };
        assert_eq!(rcap as usize, wcap, "encode_max_len differs from rapidyenc");
        let mut rdst_buf = Paged::new(rcap as usize, DST_PAGE_OFFSET);
        let rdst = rdst_buf.bytes();

        for (kid, kname, tier) in PAIRS {
            if let Some(want) = opts.tier.as_deref()
                && want != tier.as_str()
                && !want.eq_ignore_ascii_case(kname)
            {
                continue;
            }
            // SAFETY: force checks CPU support itself; single-threaded.
            if unsafe { weaver_rapidyenc_encode_force(kid) } != 1 {
                continue;
            }
            let weaver = tier.is_available();

            // Byte equality first.
            let (rn, rcol) = rapidyenc_once(src, rdst);
            if weaver {
                let (wn, wcol) = weaver_once(tier, src, wdst);
                // SAFETY: the encoder initialised the first `wn` bytes.
                let wbytes = unsafe { std::slice::from_raw_parts(wdst.as_ptr().cast::<u8>(), wn) };
                assert!(
                    wn == rn && wcol == rcol && wbytes == &rdst[..rn],
                    "{fname}: weaver {tier} output differs from rapidyenc {kname} \
                     (lengths {wn}/{rn}, columns {wcol}/{rcol})"
                );
            }

            // One side only, the same loop for both: about 4 GB of input,
            // so the counters cover the kernel and not the set-up. `none`
            // runs the set-up alone, to subtract from the other two.
            if let Some(side) = opts.perf.as_deref() {
                let iters = (4usize << 30) / src.len();
                for i in 0..iters {
                    let a = art(i % n);
                    if side == "weaver" {
                        black_box(weaver_once(tier, black_box(a), wdst));
                    } else if side == "rapidyenc" {
                        black_box(rapidyenc_once(black_box(a), rdst));
                    }
                }
                println!("PERF\t{side}\t{fname}\t{}\t{iters}\t{}", tier.as_str(), src.len());
                continue;
            }
            let (w_ns, r_ns) = if n == 1 {
                let mut run_w = || {
                    black_box(weaver_once(tier, black_box(src), wdst));
                };
                let mut run_r = || {
                    black_box(rapidyenc_once(black_box(src), rdst));
                };
                time_sides(weaver, n_samples, target_ns, &mut run_w, &mut run_r)
            } else {
                // Each side walks the stream from article 0, one article per
                // call, wrapping only after the whole pool.
                let (mut kw, mut kr) = (0usize, 0usize);
                let mut run_w = || {
                    black_box(weaver_once(tier, black_box(art(kw)), wdst));
                    kw = if kw + 1 == n { 0 } else { kw + 1 };
                };
                let mut run_r = || {
                    black_box(rapidyenc_once(black_box(art(kr)), rdst));
                    kr = if kr + 1 == n { 0 } else { kr + 1 };
                };
                time_sides(weaver, n_samples, target_ns, &mut run_w, &mut run_r)
            };
            let r = stats(r_ns);
            let bytes = src.len();
            if weaver {
                let w = stats(w_ns);
                println!(
                    "{fname:<17} {bytes:>7} | {:<12} {:>11.0} {:>11.0} {:>8.0} | {kname:<9} {:>10.0} {:>11.0} {:>8.0} | {:>6.3} {:>6.3}",
                    tier.as_str(),
                    w.min,
                    w.median,
                    mbps(bytes, w.min),
                    r.min,
                    r.median,
                    mbps(bytes, r.min),
                    r.min / w.min,
                    r.median / w.median
                );
                println!(
                    "TSV\t{arch}\t{cpu}\t{fname}\t{bytes}\t{}\t{:.1}\t{:.1}\t{kname}\t{:.1}\t{:.1}\t{:.4}\t{:.4}",
                    tier.as_str(),
                    w.min,
                    w.median,
                    r.min,
                    r.median,
                    r.min / w.min,
                    r.median / w.median
                );
            } else {
                println!(
                    "{fname:<17} {bytes:>7} | {:<12} {:>11} {:>11} {:>8} | {kname:<9} {:>10.0} {:>11.0} {:>8.0} | {:>6} {:>6}",
                    format!("({})", tier.as_str()),
                    "-",
                    "-",
                    "-",
                    r.min,
                    r.median,
                    mbps(bytes, r.min),
                    "-",
                    "-"
                );
                println!(
                    "TSV\t{arch}\t{cpu}\t{fname}\t{bytes}\t{}\t-\t-\t{kname}\t{:.1}\t{:.1}\t-\t-",
                    tier.as_str(),
                    r.min,
                    r.median
                );
            }
        }
    }
    // Leave rapidyenc on its own default.
    unsafe { weaver_rapidyenc_encode_force(default_kernel) };
}
