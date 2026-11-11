// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `--verify`: the encode differential on the host that runs the bench, for
// hosts that cannot build the weaver test suite. Every available weaver tier
// against weaver scalar and against the matching forced rapidyenc kernel,
// with the generators of `tests/encode_support` (the same cases as
// `tests/encode_kernel.rs` and `tests/rapidyenc_encode_diff.rs`), plus the
// golden digests. Panics on the first difference.

use std::time::Instant;

use crate::encode_support::*;
use crate::kernel::{EncoderTier, encode_max_len};

fn force(kernel: i32) -> bool {
    // SAFETY: init is idempotent; force checks CPU support itself;
    // single-threaded.
    unsafe {
        crate::weaver_rapidyenc_encode_init();
        crate::weaver_rapidyenc_encode_force(kernel) == 1
    }
}

fn rapidyenc(line_size: usize, column: &mut usize, src: &[u8], is_end: bool) -> Vec<u8> {
    let cap =
        unsafe { crate::weaver_rapidyenc_encode_max_length(src.len() as u64, line_size as i32) };
    let mut out = vec![0u8; cap as usize];
    let mut col = i32::try_from(*column).unwrap();
    // SAFETY: `out` holds rapidyenc's own maximum; distinct live buffers.
    let n = unsafe {
        crate::weaver_rapidyenc_encode_ex(
            i32::try_from(line_size).unwrap(),
            &mut col,
            src.as_ptr().cast(),
            out.as_mut_ptr().cast(),
            src.len() as u64,
            is_end as i32,
        )
    } as usize;
    assert!(n <= out.len());
    assert_eq!(cap as usize, encode_max_len(src.len(), line_size));
    out.truncate(n);
    *column = usize::try_from(col).expect("rapidyenc returned a negative column");
    out
}

fn all_checks(label: &str, a: EncFn, b: EncFn) {
    let t = Instant::now();
    check_exhaustive(label, &exhaustive_line_sizes(), &DISTS, a, b);
    check_random(label, 0xd1ff, 3000, 0, a, b);
    check_random(label, 0x7a11, 3000, 0, a, b);
    check_chunked(label, 0xd2ff, 400, a, b);
    check_chunked(label, 0xc4a2, 400, a, b);
    check_extremes(label, 0, a, b);
    println!("VERIFY ok   {label} ({:.1} s)", t.elapsed().as_secs_f64());
}

// The bench's stream fixtures as the bench encodes them (one article per
// call, column 0, last chunk): every article of the 768,000-byte walks, the
// first 512 of the 64 KiB and 4 KiB walks. Needs the rapidyenc kernel forced.
fn check_streams(tier: EncoderTier, pools: &mut [Option<crate::Paged>; 2]) {
    let t = Instant::now();
    for (name, len, kind) in crate::STREAMS {
        let pool = pools[kind].get_or_insert_with(|| crate::stream_pool(kind)).bytes();
        let stride = crate::stream_stride(len);
        let n = (crate::STREAM_POOL / stride).min(if len >= 768_000 { usize::MAX } else { 512 });
        let cap = encode_max_len(len, crate::LINE_SIZE);
        let mut wbuf = crate::Paged::new(cap, crate::DST_PAGE_OFFSET);
        let mut rbuf = crate::Paged::new(cap, crate::DST_PAGE_OFFSET);
        for k in 0..n {
            let a = &pool[k * stride..k * stride + len];
            let (rn, rcol) = crate::rapidyenc_once(a, rbuf.bytes());
            let (wn, wcol) = crate::weaver_once(tier, a, wbuf.uninit());
            assert!(
                wn == rn && wcol == rcol && wbuf.bytes()[..wn] == rbuf.bytes()[..rn],
                "{name}: weaver {tier} differs from rapidyenc at article {k}"
            );
        }
    }
    println!("VERIFY ok   {tier} streams ({:.1} s)", t.elapsed().as_secs_f64());
}

pub fn run() {
    let scalar = |ls: usize, col: &mut usize, src: &[u8], end: bool| {
        weaver_encode(EncoderTier::Scalar, ls, col, src, end)
    };
    let mut verified = Vec::new();
    let mut pools: [Option<crate::Paged>; 2] = [None, None];
    for tier in EncoderTier::available() {
        let enc = move |ls: usize, col: &mut usize, src: &[u8], end: bool| {
            weaver_encode(tier, ls, col, src, end)
        };
        let measured: Vec<_> = GOLDEN.iter().map(|g| run_golden(g, &enc)).collect();
        let expected: Vec<_> = GOLDEN
            .iter()
            .map(|g| (g.written, g.final_column, g.digest))
            .collect();
        if measured != expected {
            print_golden(&measured);
            panic!("{tier}: output differs from the pinned rapidyenc digests");
        }
        println!("VERIFY ok   {tier} golden digests");
        if tier != EncoderTier::Scalar {
            all_checks(&format!("weaver {tier} vs weaver scalar"), &enc, &scalar);
        }
        let kernel = tier.rapidyenc_kernel();
        // rapidyenc's scalar kernel reads out of bounds on empty input, and
        // the generators include it; scalar is covered by the golden digests.
        if tier == EncoderTier::Scalar {
            continue;
        }
        if force(kernel) {
            all_checks(
                &format!("weaver {tier} vs rapidyenc kernel {kernel:#x}"),
                &enc,
                &rapidyenc,
            );
            check_streams(tier, &mut pools);
            verified.push(tier.as_str());
        } else {
            println!("VERIFY skip {tier}: rapidyenc kernel {kernel:#x} not runnable here");
        }
    }
    force(0);
    println!("VERIFY PASS tiers verified against rapidyenc: {}", verified.join(", "));
}
