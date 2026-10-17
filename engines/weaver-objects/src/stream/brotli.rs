// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// brotli: one stream whose first bits declare its window, with no magic and
// no check of its own.

use std::io::{self, Read};

use ::brotli::Decompressor;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Brotli;

// The largest window a large-window stream may declare.
const LARGEST_BITS: u32 = 30;
// How much compressed input the decoder pulls at once.
const BUFFER: usize = 64 << 10;

impl Decoder for Brotli {
    const KIND: Kind = Kind::Brotli;

    fn window(first: &[u8]) -> u64 {
        1 << first.first().map_or(LARGEST_BITS, |&byte| bits(byte))
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(Decompressor::new(input, BUFFER)))
    }
}

// WBITS, read from the stream's first bits, least significant first: a zero
// bit is 16; else three bits n give 17 + n; else three more bits m give
// 8 + m, 17 when m is zero, and a large window when m is one.
fn bits(byte: u8) -> u32 {
    if byte & 0x01 == 0 {
        return 16;
    }
    let n = u32::from((byte >> 1) & 0x07);
    if n != 0 {
        return 17 + n;
    }
    match u32::from((byte >> 4) & 0x07) {
        0 => 17,
        1 => LARGEST_BITS,
        m => 8 + m,
    }
}
