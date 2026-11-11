// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Compile rapidyenc's encoder (with its CPU detection) and the shared encode
// shim. Requires WEAVER_RAPIDYENC_SRC: this bench exists to compare against
// rapidyenc.

use std::path::PathBuf;

include!("../rapidyenc_encode_build.rs");

fn main() {
    println!("cargo:rerun-if-env-changed=WEAVER_RAPIDYENC_SRC");
    println!("cargo:rerun-if-changed=../rapidyenc_encode_shim.cc");
    println!("cargo:rerun-if-changed=../rapidyenc_encode_build.rs");
    let Some(root) = std::env::var_os("WEAVER_RAPIDYENC_SRC") else {
        panic!("set WEAVER_RAPIDYENC_SRC to a rapidyenc checkout to build the encode bench");
    };
    let root = PathBuf::from(root);
    if !root.join("src").join("encoder.h").exists() {
        panic!(
            "WEAVER_RAPIDYENC_SRC={} has no src/encoder.h",
            root.display()
        );
    }
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let shim = manifest.join("..").join("rapidyenc_encode_shim.cc");
    compile_rapidyenc_encoder(&root, &shim, true);
}
