// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Names aws-lc's SHA-256 block function for the merkle tree. aws-lc-sys
//! comes in only through aws-lc-rs and exports each symbol under its
//! version, `aws_lc_0_47_0_SHA256_TransformBlocks` for 0.47.0, so the name
//! is read from the one aws-lc-sys in Cargo.lock. Two of them in the lock
//! fail the build: the tree would carry two copies of aws-lc.

use std::path::{Path, PathBuf};

fn main() {
    let lock = lockfile();
    println!("cargo:rerun-if-changed={}", lock.display());
    let text = std::fs::read_to_string(&lock).expect("read Cargo.lock");
    let versions: Vec<&str> = text
        .split("[[package]]")
        .filter(|entry| {
            entry
                .lines()
                .any(|line| line.trim() == r#"name = "aws-lc-sys""#)
        })
        .filter_map(|entry| {
            entry
                .lines()
                .find_map(|line| line.trim().strip_prefix("version = \""))
                .and_then(|rest| rest.strip_suffix('"'))
        })
        .collect();
    let [version] = versions[..] else {
        panic!("Cargo.lock must hold exactly one aws-lc-sys, holds {versions:?}");
    };
    println!(
        "cargo:rustc-env=WEAVER_AWS_LC_SHA256_TRANSFORM_BLOCKS=aws_lc_{}_SHA256_TransformBlocks",
        version.replace('.', "_")
    );
}

/// The workspace's Cargo.lock, the nearest one above this crate.
fn lockfile() -> PathBuf {
    let dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    Path::new(&dir)
        .ancestors()
        .map(|dir| dir.join("Cargo.lock"))
        .find(|lock| lock.is_file())
        .expect("a Cargo.lock above the crate")
}
