// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// bzip2: blocks of a size the header's level digit sets, each with its own
// CRC, concatenated streams read through as one.

use std::io::{self, Read};

use ::bzip2::read::MultiBzDecoder;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Bzip2;

// "BZh" and a level digit, 1 to 9: blocks of that many hundred thousand
// bytes.
const BLOCK: u64 = 100_000;

impl Decoder for Bzip2 {
    const KIND: Kind = Kind::Bzip2;

    fn window(first: &[u8], _budget: &weaver_graph::Budget) -> u64 {
        let level = match first {
            [b'B', b'Z', b'h', digit @ b'1'..=b'9', ..] => u64::from(digit - b'0'),
            _ => 9,
        };
        level * BLOCK
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(MultiBzDecoder::new(input)))
    }
}
