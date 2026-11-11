// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Always-on encoder tests (no rapidyenc needed):
//   * every available tier against the scalar tier, over the shared
//     generators (new tiers are picked up from `EncoderTier::available()`);
//   * round trip through the crate's decoder;
//   * golden digests of rapidyenc's output, pinning byte identity;
//   * `encode_max_len` is never exceeded, checked with a canary.

mod encode_support;

use std::mem::MaybeUninit;

use encode_support::*;
use weaver_yenc::encode::{MAX_COLUMN, MAX_LINE_SIZE};
use weaver_yenc::{
    EncodeError, EncoderTier, RapidyencDecodeState, decode_rapidyenc_ex, encode_body,
    encode_body_append, encode_body_with_tier, encode_max_len,
};

fn scalar(line_size: usize, column: &mut usize, src: &[u8], is_end: bool) -> Vec<u8> {
    weaver_encode(EncoderTier::Scalar, line_size, column, src, is_end)
}

fn auto(line_size: usize, column: &mut usize, src: &[u8], is_end: bool) -> Vec<u8> {
    let mut out = Vec::new();
    encode_body_append(line_size, column, src, &mut out, is_end).unwrap();
    out
}

// Every available tier other than scalar, plus the auto-dispatched path.
fn for_each_candidate(mut f: impl FnMut(&str, EncFn)) {
    for tier in EncoderTier::available() {
        if tier == EncoderTier::Scalar {
            continue;
        }
        let enc = move |ls: usize, col: &mut usize, src: &[u8], end: bool| {
            weaver_encode(tier, ls, col, src, end)
        };
        f(tier.as_str(), &enc);
    }
    f("auto", &auto);
}

#[test]
fn scalar_is_always_available_and_detect_picks_an_available_tier() {
    assert!(EncoderTier::Scalar.is_available());
    assert!(EncoderTier::detect().is_available());
    let available = EncoderTier::available();
    assert!(available.contains(&EncoderTier::detect()));
    for tier in EncoderTier::ALL {
        assert_eq!(EncoderTier::from_name(tier.as_str()), Some(tier));
        if !tier.is_available() {
            let mut col = 0;
            let mut dst = [MaybeUninit::<u8>::uninit(); 128];
            assert_eq!(
                encode_body_with_tier(tier, 128, &mut col, b"x", &mut dst, true),
                Err(EncodeError::TierUnavailable { tier })
            );
        }
    }
    eprintln!(
        "encoder tiers available here: {:?}, detected {}",
        available,
        EncoderTier::detect()
    );
}

#[test]
fn tiers_match_scalar_exhaustive() {
    for_each_candidate(|label, enc| {
        check_exhaustive(label, &exhaustive_line_sizes(), &DISTS, &scalar, enc)
    });
}

#[test]
fn tiers_match_scalar_random() {
    for_each_candidate(|label, enc| check_random(label, 0x7a11, 3000, 0, &scalar, enc));
}

#[test]
fn tiers_match_scalar_chunked() {
    for_each_candidate(|label, enc| check_chunked(label, 0xc4a2, 400, &scalar, enc));
}

#[test]
fn tiers_match_scalar_extremes() {
    for_each_candidate(|label, enc| check_extremes(label, 0, &scalar, enc));
}

// Every available tier: golden digests produced by rapidyenc.
#[test]
fn golden_vectors() {
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
    }
}

// Decode with the crate's rapidyenc-semantics decoder, NNTP dot-unstuffing
// on: an encoded body must survive an NNTP transfer and decode back.
fn decode_body(encoded: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; encoded.len()];
    let mut state = RapidyencDecodeState::default();
    let n = decode_rapidyenc_ex(true, encoded, &mut out, &mut state).unwrap();
    out.truncate(n);
    out
}

#[test]
fn round_trip_through_decoder() {
    let mut rng = Rng::new(0x707d);
    for tier in EncoderTier::available() {
        for case in 0..400 {
            let dist = DISTS[case % DISTS.len()];
            let len = rng.below(10_000);
            let line_size = [1usize, 2, 5, 12, 64, 127, 128, 129, 256][rng.below(9)];
            let data = gen_input(rng.next(), len, dist);
            // One call.
            let mut col = 0;
            let encoded = weaver_encode(tier, line_size, &mut col, &data, true);
            assert_eq!(decode_body(&encoded), data, "{tier} case {case} one call");
            // No line may start with '.', no line may end in TAB or SPACE,
            // and no bare NUL, CR or LF.
            for line in encoded.split(|&b| b == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                assert!(!line.contains(&b'\r') && !line.contains(&0));
                assert_ne!(line.first(), Some(&b'.'), "{tier} case {case}");
                assert!(!matches!(line.first(), Some(b'\t' | b' ')));
                assert!(!matches!(line.last(), Some(b'\t' | b' ')));
            }
            // Chunked, column carried.
            let mut col = 0;
            let mut encoded = Vec::new();
            let mut pos = 0;
            while pos < data.len() {
                let take = (1 + rng.below(700)).min(data.len() - pos);
                let last = pos + take == data.len();
                encoded.extend(weaver_encode(
                    tier,
                    line_size,
                    &mut col,
                    &data[pos..pos + take],
                    last,
                ));
                pos += take;
            }
            assert_eq!(decode_body(&encoded), data, "{tier} case {case} chunked");
        }
    }
}

// Encode into exactly `encode_max_len` bytes followed by a guard region and
// check the guard is untouched and the count fits.
fn encode_with_canary(
    tier: EncoderTier,
    line_size: usize,
    column: &mut usize,
    src: &[u8],
    is_end: bool,
) -> usize {
    const GUARD: usize = 256;
    let budget = encode_max_len(src.len(), line_size);
    let mut buf = vec![0xa5u8; budget + GUARD];
    let dst = &mut buf[..budget];
    // SAFETY: the encoder writes only initialised bytes, so viewing
    // initialised `u8` as `MaybeUninit<u8>` for it is sound.
    let dst: &mut [MaybeUninit<u8>] = unsafe { &mut *(dst as *mut [u8] as *mut [MaybeUninit<u8>]) };
    let n = encode_body_with_tier(tier, line_size, column, src, dst, is_end).unwrap();
    assert!(n <= budget, "{tier}: wrote {n} > budget {budget}");
    assert!(
        buf[budget..].iter().all(|&b| b == 0xa5),
        "{tier}: wrote past encode_max_len (line_size {line_size}, len {})",
        src.len()
    );
    n
}

#[test]
fn max_len_is_never_exceeded() {
    for tier in EncoderTier::available() {
        // Worst case: every byte escaped, every line size, every column.
        for dist in [Dist::AllCritical, Dist::CriticalHeavy, Dist::EdgeHeavy] {
            let data = gen_input(0xca9a, 2000, dist);
            for line_size in (1..=40).chain([63, 64, 65, 127, 128, 129, 256, 1000]) {
                for column in exhaustive_columns(line_size) {
                    for len in (0..=80).chain([127, 128, 129, 255, 256, 1000, 2000]) {
                        for is_end in [false, true] {
                            let mut col = column;
                            encode_with_canary(tier, line_size, &mut col, &data[..len], is_end);
                        }
                    }
                }
            }
        }
        // Large all-critical bodies.
        let data = gen_input(0xb16, 300_000, Dist::AllCritical);
        for line_size in [1usize, 4, 6, 128, 129] {
            let mut col = 0;
            encode_with_canary(tier, line_size, &mut col, &data, true);
        }
        // Chunked, column carried.
        let mut rng = Rng::new(0xc0de);
        for _ in 0..200 {
            let line_size = [1usize, 3, 6, 12, 64, 128][rng.below(6)];
            let mut col = rng.below(line_size + 3);
            let chunk = gen_input(rng.next(), 1 + rng.below(3000), Dist::AllCritical);
            for piece in chunk.chunks(1 + rng.below(500)) {
                encode_with_canary(tier, line_size, &mut col, piece, rng.chance(50));
            }
        }
    }
}

#[test]
fn domain_is_checked() {
    let mut dst = vec![MaybeUninit::<u8>::uninit(); 1024];
    let mut col = 0;
    assert_eq!(
        encode_body(0, &mut col, b"abc", &mut dst, true),
        Err(EncodeError::LineSizeOutOfRange { line_size: 0 })
    );
    assert_eq!(
        encode_body(MAX_LINE_SIZE + 1, &mut col, b"abc", &mut dst, true),
        Err(EncodeError::LineSizeOutOfRange {
            line_size: MAX_LINE_SIZE + 1
        })
    );
    let mut col = MAX_COLUMN + 1;
    assert_eq!(
        encode_body(128, &mut col, b"abc", &mut dst, true),
        Err(EncodeError::ColumnOutOfRange {
            column: MAX_COLUMN + 1
        })
    );
    assert_eq!(col, MAX_COLUMN + 1);
    let mut col = 7;
    let needed = encode_max_len(3, 128);
    assert_eq!(
        encode_body(128, &mut col, b"abc", &mut dst[..needed - 1], true),
        Err(EncodeError::DestinationTooSmall {
            needed,
            available: needed - 1
        })
    );
    assert_eq!(col, 7, "a refused call leaves the column alone");
    // Empty input: nothing written, column unchanged, even with is_end.
    for tier in EncoderTier::available() {
        for column in [0usize, 5, 127, 128, 500] {
            let mut col = column;
            assert_eq!(
                encode_body_with_tier(tier, 128, &mut col, b"", &mut dst, true),
                Ok(0)
            );
            assert_eq!(col, column);
        }
    }
}

// A few hand-checked outputs, so the rules are visible in one place.
#[test]
fn hand_checked_rules() {
    let enc = |ls: usize, col: usize, src: &[u8], end: bool| {
        let mut col = col;
        let out = scalar(ls, &mut col, src, end);
        (String::from_utf8(out).unwrap(), col)
    };
    // 'A' - 42 encodes to 'A'.
    let a = b'A' - 42;
    let dot = b'.' - 42;
    let tab = b'\t'.wrapping_sub(42);
    let space = b' '.wrapping_sub(42);
    // '.' escaped at a line start, not in the middle.
    assert_eq!(enc(128, 0, &[dot, dot], true), ("=n.".to_string(), 2 + 1));
    // TAB at the start of a line and at the end of the article.
    assert_eq!(enc(128, 0, &[tab, a, tab], true), ("=IA=I".to_string(), 5));
    // ... but not in the middle, and not at the end of a non-final chunk.
    assert_eq!(
        enc(128, 0, &[a, tab, space], false),
        ("A\t ".to_string(), 3)
    );
    // Line breaks at 4 columns: 'AAAA' then CRLF.
    assert_eq!(
        enc(4, 0, &[a; 9], true),
        ("AAAA\r\nAAAA\r\nA".to_string(), 1)
    );
    // The last byte of a line escapes TAB/SPACE but not '.'.
    assert_eq!(
        enc(4, 0, &[a, a, a, space, a], true),
        ("AAA=`\r\nA".to_string(), 1)
    );
    assert_eq!(
        enc(4, 0, &[a, a, a, dot, a], true),
        ("AAA.\r\nA".to_string(), 1)
    );
    // A column at or past the line size breaks first.
    assert_eq!(enc(4, 9, &[a], true), ("\r\nA".to_string(), 1));
}
