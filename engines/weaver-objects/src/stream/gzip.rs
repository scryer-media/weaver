// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// gzip: deflate in members, each with its own CRC, concatenated members read
// through as one.

use std::io::{self, Read};

use flate2::read::MultiGzDecoder;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Gzip;

impl Decoder for Gzip {
    const KIND: Kind = Kind::Gzip;

    // Deflate's window is fixed.
    fn window(_first: &[u8]) -> u64 {
        32 << 10
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(MultiGzDecoder::new(input)))
    }
}
