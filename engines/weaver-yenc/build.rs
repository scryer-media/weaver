//! Optional: when `WEAVER_RAPIDYENC_SRC` points at a rapidyenc checkout, compile
//! its decode sources + a tiny extern-"C" shim via `cc` and link them, and emit
//! `cfg(rapidyenc_linked)` so the timing harness can A/B against the real
//! library in-process. When the env var is unset (normal builds, CI, everyone
//! else) this is a complete no-op — no rapidyenc dependency, no behavior change.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(rapidyenc_linked)");
    println!("cargo:rerun-if-env-changed=WEAVER_RAPIDYENC_SRC");
    println!("cargo:rerun-if-changed=rapidyenc_shim.cc");

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

    println!("cargo:rustc-cfg=rapidyenc_linked");
}
