// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// zstd: frames, concatenated frames read through as one. The decoder is held
// to the window the run reserved, and refuses a frame whose window is wider.

use std::io::{self, Read};

use ::zstd::stream::read::Decoder as FrameDecoder;
use weaver_graph::{Budget, Kind};

use super::{Decoder, Front};

pub struct Zstd;

// The window a run granted none holds to: libzstd's own default,
// 1 << ZSTD_WINDOWLOG_LIMIT_DEFAULT. A frame no wider than this always has
// it, so a narrow first frame does not refuse a wider one after it.
const WINDOW_LOG: u32 = 27;
// The windows libzstd can be held to on a 64-bit build:
// ZSTD_WINDOWLOG_ABSOLUTEMIN and ZSTD_WINDOWLOG_MAX_64. 2 GiB is the widest
// zstd frame any grant admits.
const LOG_MIN: u32 = 10;
const LOG_MAX: u32 = 31;

const FRAME_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MASK: u32 = 0xFFFF_FFF0;

impl Decoder for Zstd {
    const KIND: Kind = Kind::Zstd;

    // The window the first frame declares, at least the decoder's own
    // default and no wider than the run was granted. A stream whose front
    // does not say reserves the decoder's own default, never the whole grant.
    fn window(first: &[u8], budget: &Budget) -> u64 {
        let most = log_floor(budget.window_or(1 << WINDOW_LOG));
        let least = most.min(WINDOW_LOG);
        let log = declared_log(first).map_or(least, |log| log.clamp(least, most));
        1 << log
    }

    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        let mut decoder = FrameDecoder::new(input)?;
        decoder.window_log_max(log_floor(window))?;
        Ok(Box::new(decoder))
    }
}

// The largest window log libzstd can be held to within `bytes`.
fn log_floor(bytes: u64) -> u32 {
    (u64::BITS - 1 - bytes.max(1).leading_zeros()).clamp(LOG_MIN, LOG_MAX)
}

// The window log a frame of `bytes` needs.
fn log_ceil(bytes: u64) -> u32 {
    match bytes {
        0 | 1 => 0,
        _ => u64::BITS - (bytes - 1).leading_zeros(),
    }
}

// The log of the window the first frame's header declares, past any
// skippable frames before it. A single-segment frame's window is its
// content.
fn declared_log(first: &[u8]) -> Option<u32> {
    let word = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(first.get(at..at.checked_add(4)?)?.try_into().ok()?))
    };
    let mut at = 0;
    while word(at)? & SKIPPABLE_MASK == SKIPPABLE_MAGIC {
        at = at.checked_add(8)?.checked_add(word(at + 4)? as usize)?;
    }
    if word(at)? != FRAME_MAGIC {
        return None;
    }
    let descriptor = *first.get(at + 4)?;
    if descriptor & 0x20 == 0 {
        // Window_Descriptor: 2^(10 + exponent), plus eighths of that.
        let window = *first.get(at + 5)?;
        let log = 10 + u32::from(window >> 3);
        return Some(log + u32::from(window & 7 != 0));
    }
    let id = [0, 1, 2, 4][usize::from(descriptor & 3)];
    let size = [1, 2, 4, 8][usize::from(descriptor >> 6)];
    let start = at + 5 + id;
    let mut content = [0; 8];
    content[..size].copy_from_slice(first.get(start..start + size)?);
    let offset = if size == 2 { 256 } else { 0 };
    Some(log_ceil(u64::from_le_bytes(content).saturating_add(offset)))
}

#[cfg(test)]
mod tests {
    use super::{declared_log, log_ceil, log_floor};

    #[test]
    fn logs_round_to_what_libzstd_can_hold() {
        assert_eq!(log_floor(1), 10);
        assert_eq!(log_floor(256 << 20), 28);
        assert_eq!(log_floor((256 << 20) + 1), 28);
        assert_eq!(log_floor(4 << 30), 31);
        assert_eq!(log_ceil(1 << 20), 20);
        assert_eq!(log_ceil((1 << 20) + 1), 21);
    }

    // Headers only: a window descriptor, eighths past a power of two, a
    // single-segment frame's content size, a skippable frame before it.
    #[test]
    fn the_first_frames_header_declares_its_window() {
        let frame = |descriptor: u8, rest: &[u8]| {
            let mut bytes = vec![0x28, 0xB5, 0x2F, 0xFD, descriptor];
            bytes.extend_from_slice(rest);
            bytes
        };
        assert_eq!(declared_log(&frame(0x00, &[20 << 3])), Some(30));
        assert_eq!(declared_log(&frame(0x00, &[(20 << 3) | 1])), Some(31));
        // Single segment, four content-size bytes: 3 MiB.
        assert_eq!(
            declared_log(&frame(0xA0, &(3u32 << 20).to_le_bytes())),
            Some(22)
        );
        // Single segment, two content-size bytes, offset by 256.
        assert_eq!(declared_log(&frame(0x60, &[0x00, 0x01])), Some(9));
        let mut skipped = vec![0x50, 0x2A, 0x4D, 0x18, 4, 0, 0, 0, 1, 2, 3, 4];
        skipped.extend(frame(0x00, &[17 << 3]));
        assert_eq!(declared_log(&skipped), Some(27));
        assert_eq!(declared_log(b"not zstd"), None);
        assert_eq!(declared_log(&[]), None);
    }
}
