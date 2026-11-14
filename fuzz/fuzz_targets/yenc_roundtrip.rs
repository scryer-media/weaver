// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Encode on any tier, decode on any tier: the original bytes come back.
//
// Input layout: 6 configuration bytes, then the bytes to encode.
//   0: encode tier (mod the available tiers).   1: decode tier (mod the
//      decode tiers that run here).   2-3: u16 line size (0 means 128).
//   4: bit 0 encode in two calls carrying the column (split at byte 5),
//      bit 1 asm choice for AVX2.
//   5: split point (scaled to the input) and re-wrap seed.
//
// Checks, each giving back the original bytes:
//   1. the encoded body, decoded without dot-unstuffing (body mode);
//   2. the body re-wrapped (CRLFs inserted at seed-chosen points, never
//      inside an escape) and NNTP dot-stuffed (a line-start `.` doubled),
//      decoded with dot-unstuffing, then again with end detection followed
//      by `\r\n=yend`, which must stop at the `=y` (Control);
//   3. the same through the public article API: `encode` wraps the body in
//      `=ybegin`/`=yend` with its CRC, the body is re-wrapped and
//      dot-stuffed, and `decode_nntp_append` must return the original bytes
//      with the CRC verified.

#![no_main]

#[path = "common/buffers.rs"]
mod buffers;

use buffers::{Config, Exact};
use libfuzzer_sys::fuzz_target;
use weaver_yenc::simd::fuzzing::{Mode, State, TIERS, decode_tier, runs, set_avx2_raw_asm};
use weaver_yenc::{
    CrcVerification, EncoderTier, RapidyencDecodeEnd, decode_nntp_append, encode,
    encode_body_append, encode_body_with_tier, encode_max_len,
};

fn encode_with(tier: EncoderTier, ls: usize, data: &[u8], split: Option<usize>) -> Vec<u8> {
    let mut out = Vec::new();
    let mut column = 0;
    let pieces: Vec<&[u8]> = match split {
        Some(s) => vec![&data[..s], &data[s..]],
        None => vec![data],
    };
    let last = pieces.len() - 1;
    for (k, piece) in pieces.into_iter().enumerate() {
        let start = out.len();
        out.resize(start + encode_max_len(piece.len(), ls), 0);
        // SAFETY: `MaybeUninit<u8>` has the layout of `u8`.
        let dst = unsafe {
            std::slice::from_raw_parts_mut(out[start..].as_mut_ptr().cast(), out.len() - start)
        };
        let n = encode_body_with_tier(tier, ls, &mut column, piece, dst, k == last).unwrap();
        out.truncate(start + n);
    }
    out
}

// Insert CRLFs at seed-chosen points outside escapes, then dot-stuff.
fn rewrap_and_stuff(body: &[u8], seed: u8) -> Vec<u8> {
    let mut wrapped = Vec::with_capacity(body.len() * 2);
    let mut x = seed as u32 | 0x100;
    let mut in_escape = false;
    for &b in body {
        x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        if !in_escape && (x >> 16).is_multiple_of(17) {
            wrapped.extend_from_slice(b"\r\n");
        }
        wrapped.push(b);
        in_escape = !in_escape && b == b'=';
    }
    let mut out = Vec::with_capacity(wrapped.len() + wrapped.len() / 8 + 2);
    let mut line_start = true;
    let mut prev = 0u8;
    for &b in &wrapped {
        if line_start && b == b'.' {
            out.push(b'.');
        }
        out.push(b);
        line_start = prev == b'\r' && b == b'\n';
        prev = b;
    }
    out
}

fn decode_on(tier_idx: usize, body: &[u8], mode: Mode) -> (Vec<u8>, RapidyencDecodeEnd, usize) {
    let tiers: Vec<_> = TIERS.into_iter().filter(|&t| runs(t)).collect();
    let tier = tiers[tier_idx % tiers.len()];
    let input = Exact::from_bytes(0, body);
    let mut out = Exact::new(0, body.len(), 0);
    let mut state = State::new(0, None);
    let o = decode_tier(tier, input.as_ref(), out.as_mut(), &mut state, mode)
        .unwrap_or_else(|e| panic!("{}: decode failed: {e:?}", tier.as_str()));
    (out.as_ref()[..o.written].to_vec(), o.end, o.consumed)
}

fuzz_target!(|bytes: &[u8]| {
    let mut cfg = Config::new(bytes);
    let enc_idx = cfg.u8() as usize;
    let dec_idx = cfg.u8() as usize;
    let ls = match cfg.u16() as usize {
        0 => 128,
        n => n,
    };
    let flags = cfg.u8();
    let seed = cfg.u8();
    let data = bytes.get(6..).unwrap_or(&[]);

    let enc_tiers = EncoderTier::available();
    let enc_tier = enc_tiers[enc_idx % enc_tiers.len()];
    let split = (flags & 1 != 0).then(|| data.len() * seed as usize / 256);
    set_avx2_raw_asm(Some(flags & 2 != 0));

    // 1. Body mode.
    let body = encode_with(enc_tier, ls, data, split);
    let plain = Mode {
        dot_unstuffing: false,
        preserve_pending: false,
        search_end: false,
    };
    let (got, _, _) = decode_on(dec_idx, &body, plain);
    assert!(
        got == data,
        "{enc_tier} body round trip (ls {ls}, split {split:?})"
    );

    // 2. Raw mode, re-wrapped and dot-stuffed; then with end detection.
    let raw = rewrap_and_stuff(&body, seed);
    let unstuff = Mode {
        dot_unstuffing: true,
        preserve_pending: false,
        search_end: false,
    };
    let (got, _, _) = decode_on(dec_idx, &raw, unstuff);
    assert!(
        got == data,
        "{enc_tier} raw round trip (ls {ls}, seed {seed})"
    );
    let mut ended = raw.clone();
    ended.extend_from_slice(b"\r\n=yend size=0\r\n");
    let search = Mode {
        dot_unstuffing: true,
        preserve_pending: false,
        search_end: true,
    };
    let (got, end, consumed) = decode_on(dec_idx, &ended, search);
    assert!(
        got == data,
        "{enc_tier} raw end-search round trip (ls {ls}, seed {seed})"
    );
    assert_eq!(end, RapidyencDecodeEnd::Control);
    assert_eq!(consumed, raw.len() + 4, "stops just past the `=y`");

    // 3. The article API.
    let mut article = Vec::new();
    encode(data, &mut article, ls, "f.bin").unwrap();
    let hdr = article.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    let mut body_only = Vec::new();
    let mut column = 0;
    encode_body_append(ls, &mut column, data, &mut body_only, true).unwrap();
    let trailer = &article[hdr + body_only.len()..];
    let mut nntp = article[..hdr].to_vec();
    nntp.extend_from_slice(&rewrap_and_stuff(&body_only, seed));
    nntp.extend_from_slice(trailer);
    let mut out = Vec::new();
    let r = decode_nntp_append(&nntp, &mut out).unwrap_or_else(|e| panic!("article decode: {e:?}"));
    assert!(out == data, "article round trip (ls {ls}, seed {seed})");
    assert_eq!(r.crc_status, CrcVerification::Verified);
    set_avx2_raw_asm(None);
});
