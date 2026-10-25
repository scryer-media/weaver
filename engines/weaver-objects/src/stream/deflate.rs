// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Raw deflate, with no header or check of its own: what a zip stores a
// deflated entry as.

use std::io::{self, Read};

use flate2::read::DeflateDecoder;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Deflate;

impl Decoder for Deflate {
    const KIND: Kind = Kind::Deflate;

    // Deflate's window is fixed.
    fn window(_first: &[u8]) -> u64 {
        32 << 10
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(DeflateDecoder::new(input)))
    }
}
