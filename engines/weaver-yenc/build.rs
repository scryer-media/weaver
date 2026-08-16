//! Optional: when `WEAVER_RAPIDYENC_SRC` points at a rapidyenc checkout, compile
//! its decode sources + a tiny extern-"C" shim via `cc` and link them, and emit
//! `cfg(rapidyenc_linked)` so the timing harness can A/B against the real
//! library in-process. When the env var is unset (normal builds, CI, everyone
//! else) this is a complete no-op — no rapidyenc dependency, no behavior change.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(rapidyenc_linked)");
    println!("cargo:rustc-check-cfg=cfg(weaver_yenc_raw_asm)");
    println!("cargo:rerun-if-env-changed=WEAVER_RAPIDYENC_SRC");
    println!("cargo:rerun-if-env-changed=WEAVER_YENC_RAW_ASM");
    println!("cargo:rerun-if-changed=rapidyenc_shim.cc");

    // The oracle-model `asm!` decode kernel (`avx2_raw_kernel_oracle`) is the
    // DEFAULT `SEARCH_END=false` path on x86_64: measured on Alder Lake and
    // Zen2/Windows it matches or beats rapidyenc on every decode fixture
    // (realshape 1.013/1.127, crlf 1.111/1.048; >1 = weaver faster), where
    // the intrinsic loop trailed on realshape. `WEAVER_YENC_RAW_ASM=0` is the
    // escape hatch back to the intrinsic loop (A/B runs, or triage on a
    // microarchitecture that disagrees with the measured set). Both forms
    // stay compiled on x86_64 so the differential tests can compare them on
    // real hardware regardless of the cfg.
    if !matches!(env::var("WEAVER_YENC_RAW_ASM").as_deref(), Ok("0")) {
        println!("cargo:rustc-cfg=weaver_yenc_raw_asm");
    }

    let Some(root) = env::var_os("WEAVER_RAPIDYENC_SRC") else {
        return;
    };
    let root = PathBuf::from(root);
    let src = root.join("src");
    if !src.join("decoder.h").exists() {
        println!(
            "cargo:warning=WEAVER_RAPIDYENC_SRC set but {}/decoder.h not found; skipping rapidyenc link",
            src.display()
        );
        return;
    }

    // On aarch64 (Apple Silicon / Graviton) rapidyenc's decode dispatches to the
    // NEON tier; NEON is baseline on ARMv8 so no -march flag is needed. Compile
    // the dispatch + the aarch64 NEON decoder + the shim, then stop.
    if env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("aarch64") {
        cc::Build::new()
            .cpp(true)
            .include(&root)
            .file(src.join("platform.cc"))
            .file(src.join("decoder.cc"))
            .file(src.join("decoder_neon64.cc"))
            .file("rapidyenc_shim.cc")
            .compile("rapidyenc_arm");

        // CRC dispatch + the ARM kernels it installs (needed by the
        // crc_probe example's rapidyenc lane). `crc_arm*.cc` self-stub when the
        // required ISA extension is not enabled, so `flag_if_supported` is safe.
        cc::Build::new()
            .cpp(true)
            .include(&root)
            .define("YENC_DISABLE_CRCUTIL", "1")
            .file(src.join("crc.cc"))
            .compile("rapidyenc_crc");
        cc::Build::new()
            .cpp(true)
            .include(&root)
            .flag_if_supported("-march=armv8-a+crc")
            .file(src.join("crc_arm.cc"))
            .compile("rapidyenc_crc_arm");
        cc::Build::new()
            .cpp(true)
            .include(&root)
            .flag_if_supported("-march=armv8-a+crypto+crc")
            .file(src.join("crc_arm_pmull.cc"))
            .compile("rapidyenc_crc_arm_pmull");

        println!("cargo:rustc-cfg=rapidyenc_linked");
        return;
    }

    // Baseline group (x64 MSVC baseline is SSE2; SSSE3 intrinsics need no arch
    // flag on MSVC) + the shim.
    cc::Build::new()
        .cpp(true)
        .include(&root)
        .file(src.join("platform.cc"))
        .file(src.join("decoder.cc"))
        .file(src.join("decoder_sse2.cc"))
        .file(src.join("decoder_ssse3.cc"))
        .file("rapidyenc_shim.cc")
        .compile("rapidyenc_base");

    let groups: [(&str, &str, &[&str]); 3] = [
        ("decoder_avx.cc", "/arch:AVX", &["-mavx"]),
        ("decoder_avx2.cc", "/arch:AVX2", &["-mavx2", "-mbmi2"]),
        (
            "decoder_vbmi2.cc",
            "/arch:AVX512",
            &["-mavx512vbmi2", "-mavx512vl", "-mavx512bw"],
        ),
    ];
    for (file, msvc_flag, gnu_flags) in groups {
        let mut b = cc::Build::new();
        b.cpp(true).include(&root).file(src.join(file));
        if b.get_compiler().is_like_msvc() {
            b.flag(msvc_flag);
        } else {
            for f in gnu_flags {
                b.flag(f);
            }
        }
        let name = file.trim_end_matches(".cc");
        b.compile(&format!("rapidyenc_{name}"));
    }

    // CRC dispatch + its x86 folding kernels, for the crc_probe example's
    // rapidyenc lane. `YENC_DISABLE_CRCUTIL` keeps the *generic* fallback as
    // rapidyenc's own slice table instead of pulling in the whole crcutil-1.0
    // tree; on any CPU with SSE4.1+SSSE3+PCLMUL the generic path is overwritten
    // by crc_clmul_set_funcs() anyway (rapidyenc src/crc.cc:231-240).
    cc::Build::new()
        .cpp(true)
        .include(&root)
        .define("YENC_DISABLE_CRCUTIL", "1")
        .file(src.join("crc.cc"))
        .compile("rapidyenc_crc");

    let crc_groups: [(&str, &str, &[&str]); 2] = [
        (
            "crc_folding.cc",
            "/arch:SSE2",
            &["-mssse3", "-msse4.1", "-mpclmul"],
        ),
        (
            "crc_folding_256.cc",
            "/arch:AVX2",
            &["-mavx2", "-mvpclmulqdq", "-mpclmul"],
        ),
    ];
    for (file, msvc_flag, gnu_flags) in crc_groups {
        let mut b = cc::Build::new();
        b.cpp(true).include(&root).file(src.join(file));
        if b.get_compiler().is_like_msvc() {
            b.flag(msvc_flag);
        } else {
            for f in gnu_flags {
                b.flag(f);
            }
        }
        let name = file.trim_end_matches(".cc");
        b.compile(&format!("rapidyenc_{name}"));
    }

    println!("cargo:rustc-cfg=rapidyenc_linked");
}
