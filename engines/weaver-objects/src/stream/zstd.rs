// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// zstd: frames, concatenated frames read through as one. The decoder keeps
// its own default limit, and refuses a frame whose window is wider.

use std::io::{self, Read};

use ::zstd::stream::read::Decoder as FrameDecoder;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Zstd;

// The decoder's default window limit, 1 << ZSTD_WINDOWLOG_LIMIT_DEFAULT.
const WINDOW: u64 = 1 << 27;

impl Decoder for Zstd {
    const KIND: Kind = Kind::Zstd;

    fn window(_first: &[u8]) -> u64 {
        WINDOW
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(FrameDecoder::new(input)?))
    }
}
