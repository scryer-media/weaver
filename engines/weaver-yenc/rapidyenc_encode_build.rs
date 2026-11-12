// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Build-script helper shared by `build.rs` and `encode-bench/build.rs` (each
// pulls it in with `include!`): compile rapidyenc's ENCODER sources plus
// `rapidyenc_encode_shim.cc`, with the per-file ISA flags from rapidyenc's
// CMakeLists.txt:175-214. `YENC_DISABLE_AVX256` stays undefined, matching
// rapidyenc's default build, so `rapidyenc_encode_max_length` carries the
// 64-byte vector slack `encode_max_len` mirrors.
//
// `with_platform` compiles rapidyenc's `platform.cc` (CPU detection) too; the
// main crate already links it from its decode build and must not link it
// twice.

// Every rapidyenc function starts on a 64-byte boundary. rapidyenc is linked
// after the code that uses it, so without this its kernels sit wherever the
// size of that code puts them, and their speed moves with every change to
// it, as a loop's offset within a 64-byte line moves with them. The
// padding sits before each function and never runs. Loops inside a function
// keep the alignment the compiler gives them, now at a fixed offset.
#[allow(dead_code)]
fn pin_functions(b: &mut cc::Build) {
    if !b.get_compiler().is_like_msvc() {
        b.flag("-falign-functions=64");
    }
}

#[allow(dead_code)]
fn compile_rapidyenc_encoder(
    root: &std::path::Path,
    shim: &std::path::Path,
    with_platform: bool,
) {
    let src = root.join("src");
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    let mut base = cc::Build::new();
    base.cpp(true)
        .include(root)
        // `rapidyenc.cc` for the public `rapidyenc_encode_ex` /
        // `rapidyenc_encode_max_length`, without its decode and CRC halves.
        .define("RAPIDYENC_DISABLE_DECODE", "1")
        .define("RAPIDYENC_DISABLE_CRC", "1")
        .file(root.join("rapidyenc.cc"))
        .file(src.join("encoder.cc"))
        .file(shim);
    if with_platform {
        base.file(src.join("platform.cc"));
    }
    pin_functions(&mut base);
    if arch == "aarch64" || arch == "arm" {
        // NEON is baseline on AArch64; no flag needed.
        base.file(src.join("encoder_neon.cc"));
    }
    base.compile("rapidyenc_encode_base");

    if arch != "x86_64" && arch != "x86" {
        return;
    }
    let groups: [(&str, Option<&str>, &[&str]); 5] = [
        ("encoder_sse2.cc", None, &["-msse2"]),
        ("encoder_ssse3.cc", None, &["-mssse3"]),
        ("encoder_avx.cc", Some("/arch:AVX"), &["-mavx", "-mpopcnt"]),
        (
            "encoder_avx2.cc",
            Some("/arch:AVX2"),
            &["-mavx2", "-mpopcnt", "-mbmi", "-mbmi2", "-mlzcnt"],
        ),
        (
            "encoder_vbmi2.cc",
            Some("/arch:AVX512"),
            &[
                "-mavx512vbmi2",
                "-mavx512vl",
                "-mavx512bw",
                "-mpopcnt",
                "-mbmi",
                "-mbmi2",
                "-mlzcnt",
            ],
        ),
    ];
    for (file, msvc_flag, gnu_flags) in groups {
        let mut b = cc::Build::new();
        b.cpp(true).include(root).file(src.join(file));
        if b.get_compiler().is_like_msvc() {
            if let Some(flag) = msvc_flag {
                b.flag(flag);
            }
        } else {
            for f in gnu_flags {
                b.flag(f);
            }
        }
        pin_functions(&mut b);
        let name = file.trim_end_matches(".cc");
        b.compile(&format!("rapidyenc_{name}"));
    }
}
