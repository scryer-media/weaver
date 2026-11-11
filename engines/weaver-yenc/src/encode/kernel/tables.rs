// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Lookup tables for the scalar encoder, ported from rapidyenc
// src/encoder.cc:13-22 (`escapeLUT`, `escapedLUT`). Built at compile time.
//
// Input byte `n` encodes to `(n + 42) & 0xff`. The bytes it can become that
// matter:
//
//   always escaped:            NUL (n = 214), LF (224), CR (227), '=' (19)
//   escaped at a line edge:    TAB (223), SPACE (246)
//   escaped at a line start:   '.' (4)
//
// An escape is `=` followed by the encoded byte plus 64.

// Source bytes whose encoded form is always escaped, wherever it falls.
pub(super) const ALWAYS_CRITICAL: [u8; 4] = [214, 224, 227, 19];

// The source byte that encodes to '.', escaped only as a line's first byte.
pub(super) const DOT: u8 = b'.'.wrapping_sub(42);

// `ESCAPE[n]` is the encoded byte for `n` in the middle of a line, or 0 when
// `n` must be escaped there (NUL, LF, CR, '='). No byte that needs no escape
// encodes to 0, so 0 is a free marker. (rapidyenc `escapeLUT`.)
pub(super) static ESCAPE: [u8; 256] = build_escape();

// `ESCAPED[n]` is the two-byte escape for `n` when `n` would need escaping
// at the start of a line (NUL, LF, CR, '=', TAB, SPACE, '.'), or `[0, 0]`
// when it never needs one. A real escape always starts with '=', so
// `ESCAPED[n][0] != 0` is the "needs escaping at a line start" test.
// (rapidyenc `escapedLUT`, stored as bytes in output order.)
pub(super) static ESCAPED: [[u8; 2]; 256] = build_escaped();

const fn build_escape() -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut n = 0usize;
    while n < 256 {
        let b = n as u8;
        table[n] = if is_always_critical(b) {
            0
        } else {
            b.wrapping_add(42)
        };
        n += 1;
    }
    table
}

const fn build_escaped() -> [[u8; 2]; 256] {
    let mut table = [[0u8; 2]; 256];
    let mut n = 0usize;
    while n < 256 {
        let b = n as u8;
        let edge = b == b'\t'.wrapping_sub(42) || b == b' '.wrapping_sub(42) || b == DOT;
        if is_always_critical(b) || edge {
            table[n] = [b'=', b.wrapping_add(42 + 64)];
        }
        n += 1;
    }
    table
}

const fn is_always_critical(b: u8) -> bool {
    b == ALWAYS_CRITICAL[0]
        || b == ALWAYS_CRITICAL[1]
        || b == ALWAYS_CRITICAL[2]
        || b == ALWAYS_CRITICAL[3]
}
