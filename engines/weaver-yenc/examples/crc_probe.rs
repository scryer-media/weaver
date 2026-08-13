//! CRC32 attribution + throughput probe.
//!
//! Evidence-gathering only: nothing here changes a production path. It answers
//! one question — on a given CPU, *which* CRC32 code path actually executes for
//! weaver's yEnc decode, and how fast is it relative to the alternatives.
//!
//!   cargo run --release --example crc_probe
//!
//! Optional env:
//!   CRC_PROBE_GHZ    nominal core clock used for the cycles/byte column
//!                    (default 2.1, the Atom C3538 / Denverton base clock)
//!   CRC_PROBE_SECS   wall-clock budget per lane, seconds (default 1.5)
//!
//! Lanes:
//!   a. weaver `Crc32` — the production wrapper, driven exactly like
//!      `decode_body_with_line_length` drives it (one `update()` over the whole
//!      decoded run, then `finalize()`; see src/decode.rs:666-671).
//!   b. `crc_fast::Digest` called directly (same streaming shape).
//!   c. a self-contained slice-by-16 table CRC32 (ISO-HDLC poly), no deps.
//!   d. rapidyenc's own `RapidYenc::crc32`, via the shim (only when
//!      `WEAVER_RAPIDYENC_SRC` was set at build time -> cfg(rapidyenc_linked)).

use std::time::{Duration, Instant};

use crc_fast::{CrcAlgorithm, Digest};
use weaver_yenc::crc::Crc32;

#[cfg(rapidyenc_linked)]
unsafe extern "C" {
    fn weaver_rapidyenc_crc32_init();
    fn weaver_rapidyenc_crc32(data: *const core::ffi::c_void, len: u64, init: u32) -> u32;
    fn weaver_rapidyenc_crc32_isa() -> i32;
}

// ---------------------------------------------------------------------------
// Lane (c): self-contained slice-by-16, CRC-32/ISO-HDLC (reflected, poly
// 0xEDB88320, init/xorout 0xFFFFFFFF). 16 x 256 x u32 = 16 KiB of tables.
// ---------------------------------------------------------------------------

const ISO_HDLC_REFLECTED_POLY: u32 = 0xEDB8_8320;

struct SliceBy16 {
    tables: [[u32; 256]; 16],
}

impl SliceBy16 {
    fn new() -> Box<Self> {
        let mut tables = [[0u32; 256]; 16];
        for (byte, slot) in tables[0].iter_mut().enumerate() {
            let mut crc = byte as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ ISO_HDLC_REFLECTED_POLY
                } else {
                    crc >> 1
                };
            }
            *slot = crc;
        }
        let t0 = tables[0];
        for k in 1..16 {
            let prev = tables[k - 1];
            for (byte, slot) in tables[k].iter_mut().enumerate() {
                let p = prev[byte];
                *slot = (p >> 8) ^ t0[(p & 0xff) as usize];
            }
        }
        Box::new(Self { tables })
    }

    fn checksum(&self, data: &[u8]) -> u32 {
        let t = &self.tables;
        let mut c = 0xFFFF_FFFFu32;
        let mut chunks = data.chunks_exact(16);
        for ch in &mut chunks {
            let w0 = u32::from_le_bytes([ch[0], ch[1], ch[2], ch[3]]) ^ c;
            let w1 = u32::from_le_bytes([ch[4], ch[5], ch[6], ch[7]]);
            let w2 = u32::from_le_bytes([ch[8], ch[9], ch[10], ch[11]]);
            let w3 = u32::from_le_bytes([ch[12], ch[13], ch[14], ch[15]]);
            // Balanced XOR tree (depth 4), not the left-associative chain a
            // plain `a ^ b ^ c ^ ...` would compile to. On every core measured
            // the naive chain is loop-carried-latency bound rather than
            // load-throughput bound, which would understate the table lane.
            let g0 = (t[15][(w0 & 0xff) as usize] ^ t[14][((w0 >> 8) & 0xff) as usize])
                ^ (t[13][((w0 >> 16) & 0xff) as usize] ^ t[12][(w0 >> 24) as usize]);
            let g1 = (t[11][(w1 & 0xff) as usize] ^ t[10][((w1 >> 8) & 0xff) as usize])
                ^ (t[9][((w1 >> 16) & 0xff) as usize] ^ t[8][(w1 >> 24) as usize]);
            let g2 = (t[7][(w2 & 0xff) as usize] ^ t[6][((w2 >> 8) & 0xff) as usize])
                ^ (t[5][((w2 >> 16) & 0xff) as usize] ^ t[4][(w2 >> 24) as usize]);
            let g3 = (t[3][(w3 & 0xff) as usize] ^ t[2][((w3 >> 8) & 0xff) as usize])
                ^ (t[1][((w3 >> 16) & 0xff) as usize] ^ t[0][(w3 >> 24) as usize]);
            c = (g0 ^ g1) ^ (g2 ^ g3);
        }
        for &b in chunks.remainder() {
            c = (c >> 8) ^ t[0][((c ^ u32::from(b)) & 0xff) as usize];
        }
        !c
    }
}

// ---------------------------------------------------------------------------
// 1. Feature report
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
fn feature_report() {
    println!("[1] CPU feature detection (std::is_x86_feature_detected)");
    for name in [
        "sse4.1",
        "sse4.2",
        "ssse3",
        "pclmulqdq",
        "avx",
        "avx2",
        "vpclmulqdq",
        "avx512f",
        "avx512vl",
    ] {
        let present = match name {
            "sse4.1" => is_x86_feature_detected!("sse4.1"),
            "sse4.2" => is_x86_feature_detected!("sse4.2"),
            "ssse3" => is_x86_feature_detected!("ssse3"),
            "pclmulqdq" => is_x86_feature_detected!("pclmulqdq"),
            "avx" => is_x86_feature_detected!("avx"),
            "avx2" => is_x86_feature_detected!("avx2"),
            "vpclmulqdq" => is_x86_feature_detected!("vpclmulqdq"),
            "avx512f" => is_x86_feature_detected!("avx512f"),
            "avx512vl" => is_x86_feature_detected!("avx512vl"),
            _ => unreachable!(),
        };
        println!("    {:<12} {}", name, if present { "yes" } else { "NO" });
    }
    println!();
}

#[cfg(not(target_arch = "x86_64"))]
fn feature_report() {
    println!("[1] CPU feature detection");
    println!(
        "    target_arch = {} (not x86_64); x86 feature gates are compiled out",
        std::env::consts::ARCH
    );
    println!();
}

// ---------------------------------------------------------------------------
// 2. Static + runtime attribution
// ---------------------------------------------------------------------------

// Replicates crc-fast 1.10.0's own gates verbatim so the selected branch can be
// named without guessing. Sources (cargo registry checkout
// `crc-fast-1.10.0/src/...`):
//
//   feature_detection.rs:186-208  detect_x86_features()
//       has_sse41      = is_x86_feature_detected!("sse4.1")
//       has_pclmulqdq  = has_sse41     && is_x86_feature_detected!("pclmulqdq")
//       has_avx512vl   = has_pclmulqdq && is_x86_feature_detected!("avx512vl")
//       has_vpclmulqdq = has_avx512vl  && is_x86_feature_detected!("vpclmulqdq")
//     Note the chaining: has_vpclmulqdq is gated behind AVX512VL, so a CPU with
//     VPCLMULQDQ but no AVX512VL reports has_vpclmulqdq = false.
//
//   feature_detection.rs:234-268  select_performance_tier()
//       vpclmulqdq -> X86_64Avx512Vpclmulqdq
//       avx512vl   -> X86_64Avx512Pclmulqdq
//       pclmulqdq  -> X86_64SsePclmulqdq
//       else       -> SoftwareTable
//
//   arch/mod.rs:100-136  update() dispatch -> update_x86_sse_pclmulqdq, which
//     carries #[target_feature(enable = "sse4.1,pclmulqdq")].
//
//   lib.rs:1329-1345  crc32_iso_hdlc_calculator() — the aarch64 CRC32 fusion arm
//     is #[cfg(target_arch = "aarch64")], so on x86 ISO-HDLC always falls
//     through to Calculator::calculate (the SIMD folding path).
//
//   algorithm.rs:85-100  update() — inputs < 128 bytes go to the
//     DataChunkProcessor short paths; >= 128 bytes go to process_large_aligned
//     -> process_simd_chunks (8 x 16 B accumulators, 128 B/iteration).
//     arch/x86/sse.rs does not override process_enhanced_simd_blocks
//     (traits.rs:45-59 default returns false), so the SSE tier always uses the
//     128 B/iter loop.
#[cfg(target_arch = "x86_64")]
fn crc_fast_attribution() {
    println!("[2a] crc-fast 1.10.0 dispatch attribution (gates replicated verbatim)");

    let has_sse41 = is_x86_feature_detected!("sse4.1");
    let has_pclmulqdq = has_sse41 && is_x86_feature_detected!("pclmulqdq");
    let has_avx512vl = has_pclmulqdq && is_x86_feature_detected!("avx512vl");
    let has_vpclmulqdq = has_avx512vl && is_x86_feature_detected!("vpclmulqdq");
    let has_sse42 = is_x86_feature_detected!("sse4.2");

    println!(
        "    ArchCapabilities {{ sse41: {has_sse41}, sse42: {has_sse42}, pclmulqdq: {has_pclmulqdq}, avx512vl: {has_avx512vl}, vpclmulqdq: {has_vpclmulqdq} }}"
    );

    let (tier, target, why) = if has_vpclmulqdq {
        (
            "X86_64Avx512Vpclmulqdq",
            "x86_64-avx512-vpclmulqdq",
            "has_vpclmulqdq (= avx512vl && vpclmulqdq) is true",
        )
    } else if has_avx512vl {
        (
            "X86_64Avx512Pclmulqdq",
            "x86_64-avx512-pclmulqdq",
            "has_avx512vl is true, has_vpclmulqdq is false",
        )
    } else if has_pclmulqdq {
        (
            "X86_64SsePclmulqdq",
            "x86_64-sse-pclmulqdq",
            "has_pclmulqdq is true, has_avx512vl is false",
        )
    } else {
        (
            "SoftwareTable",
            "software-fallback-tables",
            "has_pclmulqdq is false (no SSE4.1 and/or no PCLMULQDQ)",
        )
    };

    println!("    crc-fast selects: {tier} because {why}");
    println!("      -> arch::update() arm: {}", dispatch_arm(tier));
    println!("      -> ISO-HDLC has no x86 fusion arm (lib.rs:1331 is cfg(aarch64)),");
    println!("         so Crc32IsoHdlc runs Calculator::calculate -> the tier above.");

    // Runtime confirmation from crc-fast's own public introspection API
    // (`lib.rs:1131-1134` get_calculator_target -> ArchOpsInstance::get_target_string).
    let reported = crc_fast::get_calculator_target(CrcAlgorithm::Crc32IsoHdlc);
    println!("    crc-fast get_calculator_target(Crc32IsoHdlc) = \"{reported}\"");
    if reported == target {
        println!("    CONFIRMED: replicated gate == crc-fast's own report");
    } else if target == "x86_64-sse-pclmulqdq" && reported == "x86-sse-pclmulqdq" {
        // Known cosmetic round-trip loss inside crc-fast, NOT a different kernel:
        // select_performance_tier() returns PerformanceTier::X86_64SsePclmulqdq
        // (feature_detection.rs:254-256), but create_arch_ops_from_tier() folds
        // both x86 and x86_64 SSE tiers into the single ArchOpsInstance variant
        // X86SsePclmulqdq (feature_detection.rs:377-380, 394-398), whose
        // get_tier() maps back to PerformanceTier::X86SsePclmulqdq
        // (feature_detection.rs:298-299) -> the 32-bit label. Same struct, same
        // update_x86_sse_pclmulqdq arm, same kernel.
        println!("    CONFIRMED (label caveat): crc-fast reports the 32-bit spelling because its");
        println!(
            "      ArchOpsInstance round-trip collapses X86_64SsePclmulqdq -> X86SsePclmulqdq"
        );
        println!("      (feature_detection.rs:377-380 + :298-299). Identical kernel either way.");
    } else {
        panic!("replicated gate ({target}) disagrees with crc-fast's own report ({reported})");
    }
    if tier == "X86_64SsePclmulqdq" {
        println!(
            "    Kernel shape: 128-bit XMM PCLMULQDQ folding, 8 accumulators, 128 B/iteration"
        );
        println!("      (algorithm.rs process_simd_chunks; NOT scalar, NOT a table).");
    } else if tier == "SoftwareTable" {
        println!(
            "    Kernel shape: arch/software.rs byte/word table loop (NO carry-less multiply)."
        );
    }
    println!();
}

#[cfg(target_arch = "x86_64")]
fn dispatch_arm(tier: &str) -> &'static str {
    match tier {
        "X86_64Avx512Vpclmulqdq" => {
            "update_x86_64_avx512_vpclmulqdq (target_feature avx512vl,vpclmulqdq)"
        }
        "X86_64Avx512Pclmulqdq" => {
            "update_x86_64_avx512_pclmulqdq (target_feature avx512vl,pclmulqdq)"
        }
        "X86_64SsePclmulqdq" => "update_x86_sse_pclmulqdq (target_feature sse4.1,pclmulqdq)",
        _ => "crate::arch::software::update (table fallback)",
    }
}

#[cfg(not(target_arch = "x86_64"))]
fn crc_fast_attribution() {
    println!("[2a] crc-fast 1.10.0 dispatch attribution");
    println!(
        "    crc-fast get_calculator_target(Crc32IsoHdlc) = \"{}\"",
        crc_fast::get_calculator_target(CrcAlgorithm::Crc32IsoHdlc)
    );
    println!("    (x86 gate replication is compiled out on this arch)");
    println!();
}

/// Replicates weaver-yenc's own wrapper gate. Source:
/// `engines/weaver-yenc/src/crc.rs:108-124` `x86_vpclmul::available()`:
///
///     avx2 && pclmulqdq && sse4.1 && vpclmulqdq && !avx512vl
///
/// and `src/crc.rs:29` `VPCLMUL_MIN_UPDATE = 256` — updates below that byte
/// count never take the folded path (`src/crc.rs:47`).
#[cfg(target_arch = "x86_64")]
fn weaver_attribution() {
    println!("[2b] weaver-yenc Crc32 wrapper attribution (src/crc.rs)");

    let avx2 = is_x86_feature_detected!("avx2");
    let pclmul = is_x86_feature_detected!("pclmulqdq");
    let sse41 = is_x86_feature_detected!("sse4.1");
    let vpclmul = is_x86_feature_detected!("vpclmulqdq");
    let avx512vl = is_x86_feature_detected!("avx512vl");
    let available = avx2 && pclmul && sse41 && vpclmul && !avx512vl;

    println!(
        "    x86_vpclmul::available() = avx2({avx2}) && pclmulqdq({pclmul}) && sse4.1({sse41}) && vpclmulqdq({vpclmul}) && !avx512vl({})  =>  {available}",
        !avx512vl
    );
    if available {
        println!(
            "    weaver runs: Y2 folded 2x256-bit VPCLMUL streak kernel (crc.rs crc_fold_256)"
        );
        println!(
            "      for every update >= {} bytes; smaller updates fall through to crc-fast.",
            256
        );
    } else {
        let missing = [
            ("avx2", avx2),
            ("pclmulqdq", pclmul),
            ("sse4.1", sse41),
            ("vpclmulqdq", vpclmul),
        ]
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(n, _)| *n)
        .collect::<Vec<_>>();
        let reason = if avx512vl {
            "avx512vl is present (weaver deliberately stands aside for crc-fast's ZMM tier)"
                .to_string()
        } else {
            format!("missing: {}", missing.join(", "))
        };
        println!("    weaver runs: PASS-THROUGH to crc_fast::Digest on every update ({reason})");
        println!("      -> the weaver lane and the crc-fast lane execute the SAME kernel here.");
    }
    println!(
        "    Production call shape (src/decode.rs:666-671): one update() over the whole decoded"
    );
    println!("      run, then finalize(). Lane (a) below reproduces exactly that.");
    println!();
}

#[cfg(not(target_arch = "x86_64"))]
fn weaver_attribution() {
    println!("[2b] weaver-yenc Crc32 wrapper attribution (src/crc.rs)");
    println!("    The VPCLMUL streak path is cfg(target_arch = \"x86_64\") only; on this arch");
    println!("    Crc32 is an unconditional pass-through to crc_fast::Digest.");
    println!();
}

#[cfg(rapidyenc_linked)]
fn rapidyenc_attribution() {
    // Values from rapidyenc src/common.h:231-244 (YEncDecIsaLevel) and
    // src/crc.cc:231-240 crc32_init() -> cpu_supports_crc_isa()
    // (src/platform.cc:170-187): 2 => crc_clmul256_set_funcs (VPCLMUL, needs
    // AVX2 + VPCLMULQDQ + OS AVX state), 1 => crc_clmul_set_funcs (SSE
    // PCLMUL folding, needs SSE4.1 + SSSE3 + CLMUL), 0 => generic table.
    let isa = unsafe { weaver_rapidyenc_crc32_isa() };
    let name = match isa {
        0x440 => "ISA_LEVEL_VPCLMUL — crc_folding_256.cc (256-bit VPCLMULQDQ fold)",
        0x340 => "ISA_LEVEL_PCLMUL — crc_folding.cc (128-bit PCLMULQDQ fold, zlib-ng/Intel)",
        0x8 => "ISA_FEATURE_CRC — crc_arm.cc (ARMv8 CRC32 instructions)",
        0x48 => "ISA_FEATURE_CRC|PMULL — crc_arm.cc + crc_arm_pmull.cc",
        0 => "ISA_GENERIC — crc.cc slice-by-4 x 4-chain table loop",
        _ => "unrecognised ISA level",
    };
    println!("[2c] rapidyenc CRC attribution");
    println!("    RapidYenc::crc32_isa_level() = {isa:#x} => {name}");
    println!("    (this build defines YENC_DISABLE_CRCUTIL=1, so the *generic* fallback is");
    println!("     rapidyenc's own table loop rather than crcutil; irrelevant whenever a");
    println!("     PCLMUL/VPCLMUL/ARM level is reported above.)");
    println!();
}

#[cfg(not(rapidyenc_linked))]
fn rapidyenc_attribution() {
    println!("[2c] rapidyenc CRC attribution");
    println!("    not linked (build without WEAVER_RAPIDYENC_SRC); lane (d) is skipped");
    println!();
}

// ---------------------------------------------------------------------------
// 3. Throughput lanes
// ---------------------------------------------------------------------------

struct Lane {
    name: &'static str,
    min: Duration,
    median: Duration,
    iters: usize,
    crc: u32,
}

fn bench<F>(name: &'static str, data: &[u8], budget: Duration, mut f: F) -> Lane
where
    F: FnMut(&[u8]) -> u32,
{
    // Calibrate: three untimed-ish passes to size the run at ~`budget`.
    let probe_start = Instant::now();
    let mut crc = 0u32;
    for _ in 0..3 {
        crc = f(std::hint::black_box(data));
        std::hint::black_box(crc);
    }
    let per_iter = probe_start.elapsed().as_nanos().max(1) as f64 / 3.0;
    let iters = ((budget.as_nanos() as f64 / per_iter) as usize).clamp(5, 200_000);

    // Warmup (10% of the run, capped) so caches and any OnceLock are hot.
    for _ in 0..(iters / 10).clamp(1, 200) {
        std::hint::black_box(f(std::hint::black_box(data)));
    }

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let v = f(std::hint::black_box(data));
        let dt = t.elapsed();
        std::hint::black_box(v);
        samples.push(dt);
    }
    samples.sort_unstable();

    Lane {
        name,
        min: samples[0],
        median: samples[iters / 2],
        iters,
        crc,
    }
}

fn pseudo_random(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut seed = 0x9E37_79B9u32;
    for _ in 0..len {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        out.push((seed >> 24) as u8);
    }
    out
}

fn main() {
    #[cfg(rapidyenc_linked)]
    unsafe {
        weaver_rapidyenc_crc32_init();
    }

    let ghz: f64 = std::env::var("CRC_PROBE_GHZ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2.1);
    let budget = Duration::from_secs_f64(
        std::env::var("CRC_PROBE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.5),
    );

    println!("weaver-yenc CRC32 attribution probe");
    println!(
        "arch={} os={} rapidyenc_linked={} clock_assumed={ghz} GHz budget={:?}/lane\n",
        std::env::consts::ARCH,
        std::env::consts::OS,
        cfg!(rapidyenc_linked),
        budget
    );

    feature_report();
    crc_fast_attribution();
    weaver_attribution();
    rapidyenc_attribution();

    let table = SliceBy16::new();

    println!("[3] Throughput (min of N, pseudo-random data)");
    println!(
        "    {:<22} {:>9} {:>12} {:>12} {:>9} {:>9}",
        "lane", "iters", "min us", "median us", "GB/s", "cyc/B"
    );

    for &size in &[64 * 1024usize, 512 * 1024, 4 * 1024 * 1024] {
        let data = pseudo_random(size);
        println!("  -- {} KiB --", size / 1024);

        let mut lanes = Vec::new();

        lanes.push(bench("a weaver Crc32", &data, budget, |d| {
            let mut c = Crc32::new();
            c.update(d);
            c.finalize()
        }));

        lanes.push(bench("b crc-fast Digest", &data, budget, |d| {
            let mut dg = Digest::new(CrcAlgorithm::Crc32IsoHdlc);
            dg.update(d);
            dg.finalize() as u32
        }));

        lanes.push(bench("c slice-by-16 table", &data, budget, |d| {
            table.checksum(d)
        }));

        #[cfg(rapidyenc_linked)]
        lanes.push(bench("d rapidyenc crc32", &data, budget, |d| unsafe {
            weaver_rapidyenc_crc32(d.as_ptr() as *const core::ffi::c_void, d.len() as u64, 0)
        }));

        for lane in &lanes {
            let min_s = lane.min.as_secs_f64();
            let gbps = size as f64 / min_s / 1e9;
            let cpb = ghz * 1e9 * min_s / size as f64;
            println!(
                "    {:<22} {:>9} {:>12.3} {:>12.3} {:>9.3} {:>9.3}",
                lane.name,
                lane.iters,
                min_s * 1e6,
                lane.median.as_secs_f64() * 1e6,
                gbps,
                cpb
            );
        }

        // 4. Correctness cross-check: every lane must agree.
        let expected = lanes[0].crc;
        for lane in &lanes {
            assert_eq!(
                lane.crc, expected,
                "CRC mismatch at {size} bytes: {} produced {:#010x}, expected {expected:#010x}",
                lane.name, lane.crc
            );
        }
        println!("    all lanes agree: crc32 = {expected:#010x}");
    }
}
