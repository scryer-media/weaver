// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The encode oracle: every weaver encoder tier available on this host
// against every rapidyenc encode kernel this host can run, in process.
// Bytes, returned length and final column must match for every case.
//
// Built only when WEAVER_RAPIDYENC_SRC points at a rapidyenc checkout
// (cfg `rapidyenc_linked`, see build.rs):
//
//   WEAVER_RAPIDYENC_SRC=/path/to/rapidyenc \
//     cargo nextest run --locked -p weaver-yenc rapidyenc_encode_diff
//
// rapidyenc's encode kernel is a process-global pointer, so every test here
// takes `RAPIDYENC` before forcing a kernel and holds it while encoding.
//
// One rapidyenc limit shapes the cases: its scalar kernel
// (`do_encode_generic`) reads out of bounds on empty input, so empty input is
// only compared against its SIMD kernels, whose wrapper returns 0 first.

#![cfg(rapidyenc_linked)]

mod encode_support;

use std::sync::{Mutex, MutexGuard};

use encode_support::*;
use weaver_yenc::{EncoderTier, encode_max_len};

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

static RAPIDYENC: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    RAPIDYENC
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// The rapidyenc kernels, by RYKERN id, with names.
const KERNELS: [(i32, &str); 7] = [
    (0, "generic"),
    (0x100, "SSE2"),
    (0x200, "SSSE3"),
    (0x381, "AVX"),
    (0x403, "AVX2"),
    (0x603, "VBMI2"),
    (0x1000, "NEON"),
];

// Force a rapidyenc kernel; false when this host cannot run it.
fn force(kernel: i32) -> bool {
    // SAFETY: init is idempotent; force checks CPU support itself and the
    // caller holds `RAPIDYENC`.
    unsafe {
        weaver_rapidyenc_encode_init();
        weaver_rapidyenc_encode_force(kernel) == 1
    }
}

fn rapidyenc(line_size: usize, column: &mut usize, src: &[u8], is_end: bool) -> Vec<u8> {
    let cap = unsafe { weaver_rapidyenc_encode_max_length(src.len() as u64, line_size as i32) };
    let mut out = vec![0u8; cap as usize];
    let mut col = i32::try_from(*column).unwrap();
    // SAFETY: `out` holds rapidyenc's own maximum for this input; `src` and
    // `out` are distinct live buffers; the caller holds `RAPIDYENC`.
    let n = unsafe {
        weaver_rapidyenc_encode_ex(
            i32::try_from(line_size).unwrap(),
            &mut col,
            src.as_ptr().cast(),
            out.as_mut_ptr().cast(),
            src.len() as u64,
            is_end as i32,
        )
    } as usize;
    assert!(n <= out.len());
    out.truncate(n);
    *column = usize::try_from(col).expect("rapidyenc returned a negative column");
    out
}

// Run `f(label, weaver, rapidyenc, min_len)` for every weaver tier against
// every rapidyenc kernel this host runs; returns the pairs it covered.
fn for_each_pair(mut f: impl FnMut(&str, EncFn, EncFn, usize)) -> Vec<String> {
    let _guard = lock();
    let mut covered = Vec::new();
    for (kernel, kname) in KERNELS {
        if !force(kernel) {
            continue;
        }
        let min_len = if kernel == 0 { 1 } else { 0 };
        for tier in EncoderTier::available() {
            let enc = move |ls: usize, col: &mut usize, src: &[u8], end: bool| {
                weaver_encode(tier, ls, col, src, end)
            };
            let label = format!("weaver {tier} vs rapidyenc {kname}");
            f(&label, &enc, &rapidyenc, min_len);
            covered.push(label);
        }
    }
    assert!(force(0), "the generic kernel can always be forced");
    eprintln!("covered: {covered:?}");
    assert!(
        covered.len() >= 2,
        "expected at least generic and one SIMD kernel"
    );
    covered
}

#[test]
fn rapidyenc_kernel_ids_match_tiers() {
    let _guard = lock();
    for tier in EncoderTier::ALL {
        assert!(KERNELS.iter().any(|(id, _)| *id == tier.rapidyenc_kernel()));
    }
    for (kernel, kname) in KERNELS {
        if force(kernel) {
            assert_eq!(
                unsafe { weaver_rapidyenc_encode_kernel() },
                kernel,
                "{kname}"
            );
        }
    }
}

#[test]
fn max_len_matches_rapidyenc() {
    for len in [0usize, 1, 63, 64, 65, 1000, 768_000, 10_000_000] {
        for line_size in [1usize, 2, 3, 12, 64, 127, 128, 129, 1000, 1 << 30] {
            let theirs =
                unsafe { weaver_rapidyenc_encode_max_length(len as u64, line_size as i32) };
            assert_eq!(
                encode_max_len(len, line_size) as u64,
                theirs,
                "len {len} line {line_size}"
            );
        }
    }
}

// rapidyenc against itself: every SIMD kernel against its scalar kernel.
// A failure here is a finding about rapidyenc, not about weaver.
#[test]
fn rapidyenc_kernels_agree_with_its_generic() {
    let _guard = lock();
    let generic = |ls: usize, col: &mut usize, src: &[u8], end: bool| {
        assert!(force(0));
        rapidyenc(ls, col, src, end)
    };
    for (kernel, kname) in KERNELS.into_iter().skip(1) {
        if !force(kernel) {
            continue;
        }
        let simd = move |ls: usize, col: &mut usize, src: &[u8], end: bool| {
            assert!(force(kernel));
            rapidyenc(ls, col, src, end)
        };
        let label = format!("rapidyenc {kname} vs rapidyenc generic");
        check_exhaustive_from(
            &label,
            &exhaustive_line_sizes(),
            &DISTS[..2],
            1,
            &simd,
            &generic,
        );
        check_random(&label, 0x5e1f, 1500, 1, &simd, &generic);
        check_chunked(&label, 0x5e2f, 200, &simd, &generic);
    }
    assert!(force(0));
}

#[test]
fn rapidyenc_diff_exhaustive() {
    for_each_pair(|label, w, r, min_len| {
        check_exhaustive_from(label, &exhaustive_line_sizes(), &DISTS, min_len, w, r)
    });
}

#[test]
fn rapidyenc_diff_random() {
    for_each_pair(|label, w, r, min_len| check_random(label, 0xd1ff, 3000, min_len, w, r));
}

#[test]
fn rapidyenc_diff_chunked() {
    for_each_pair(|label, w, r, _| check_chunked(label, 0xd2ff, 400, w, r));
}

#[test]
fn rapidyenc_diff_extremes() {
    for_each_pair(|label, w, r, min_len| check_extremes(label, min_len, w, r));
}

// The golden digests in `encode_support` are rapidyenc's output: check them
// against every rapidyenc kernel here, and print the table if they drift.
#[test]
fn golden_vectors_match_rapidyenc() {
    let _guard = lock();
    for (kernel, kname) in KERNELS {
        if !force(kernel) {
            continue;
        }
        let measured: Vec<_> = GOLDEN.iter().map(|g| run_golden(g, &rapidyenc)).collect();
        let expected: Vec<_> = GOLDEN
            .iter()
            .map(|g| (g.written, g.final_column, g.digest))
            .collect();
        if measured != expected {
            print_golden(&measured);
            panic!("rapidyenc {kname}: output differs from the pinned digests");
        }
    }
    assert!(force(0));
}
