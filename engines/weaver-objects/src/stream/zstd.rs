// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// zstd: frames, each declaring its window in its header, concatenated frames
// read through as one. The decoder is held to the first frame's window, so a
// later frame that asks for more fails instead of growing the reservation.

use std::io::{self, Read};

use ::zstd::stream::read::Decoder as FrameDecoder;
use weaver_graph::Kind;

use super::{Decoder, Front};

pub struct Zstd;

const MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
// The widest window the format allows.
const LARGEST_LOG: u32 = 31;
const SMALLEST_LOG: u32 = 10;

impl Decoder for Zstd {
    const KIND: Kind = Kind::Zstd;

    fn window(first: &[u8]) -> u64 {
        declared(first).unwrap_or(1 << LARGEST_LOG)
    }

    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        let mut decoder = FrameDecoder::new(input)?;
        let log = u64::BITS - window.saturating_sub(1).leading_zeros();
        decoder.window_log_max(log.clamp(SMALLEST_LOG, LARGEST_LOG))?;
        Ok(Box::new(decoder))
    }
}

// The frame header's window: the window descriptor, or, for a frame that
// is one segment, the content size it declares instead.
fn declared(first: &[u8]) -> Option<u64> {
    let descriptor = *first.get(4).filter(|_| first.starts_with(&MAGIC))?;
    if descriptor & 0x20 == 0 {
        let window = *first.get(5)?;
        let base = 1u64 << (10 + (window >> 3));
        return Some(base + (base / 8) * u64::from(window & 0x07));
    }
    let dictionary = [0, 1, 2, 4][usize::from(descriptor & 0x03)];
    let width = [1, 2, 4, 8][usize::from(descriptor >> 6)];
    let at = 5 + dictionary;
    let field = first.get(at..at + width)?;
    let mut size = [0u8; 8];
    size[..width].copy_from_slice(field);
    let size = u64::from_le_bytes(size);
    // A two-byte size is stored less 256.
    Some(if width == 2 { size + 256 } else { size })
}
