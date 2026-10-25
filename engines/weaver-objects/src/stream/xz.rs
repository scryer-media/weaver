// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// xz: streams of blocks, each block checked as it decodes, concatenated
// streams read through as one. A block whose check fails names its stream,
// and the stream's own index, read back from the volume's tail, says where
// that stream lies, so the damage is called at that stream alone.

use std::io::{self, Read};

use lzma_turbo::Lzma2Reader;
use lzma_turbo::xz::block::header_size_from_first_byte;
use lzma_turbo::xz::{
    BlockHeader, StreamHeader, XzError, XzErrorKind, XzOptions, XzReader, stream_table,
};
use weaver_graph::{ByteRange, Kind};

use super::{Decoder, Front};

pub struct Xz;

const STREAM_HEADER: usize = 12;
// The most an xz decoder may hold: enough for the dictionary `xz -9`
// writes, and no tiny stream may ask for more.
const MEMORY_LIMIT: u64 = 128 << 20;

impl Decoder for Xz {
    const KIND: Kind = Kind::Xz;

    fn window(first: &[u8]) -> u64 {
        dictionary(first)
            .and_then(|property| Lzma2Reader::<io::Empty>::memory_required(property).ok())
            .map_or(MEMORY_LIMIT, |window| window.min(MEMORY_LIMIT))
    }

    // Held to the first block's window: a later block that asks for more
    // fails instead of growing the reservation.
    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        let options = XzOptions::default()
            .with_memory_limit(window.max(1))
            .with_concatenated(true);
        Ok(Box::new(XzReader::with_options(input, options)))
    }

    fn damaged(error: &io::Error, input: &mut Front) -> Option<ByteRange> {
        let failed = error.get_ref()?.downcast_ref::<XzError>()?;
        if failed.kind != XzErrorKind::CheckMismatch {
            return None;
        }
        let streams = stream_table(input, u64::MAX).ok()?;
        let stream = streams.get(usize::try_from(failed.stream).ok()?)?;
        Some(ByteRange::at(stream.stream_offset, stream.stream_size))
    }
}

// The dictionary property of the first block's LZMA2 filter.
fn dictionary(first: &[u8]) -> Option<u8> {
    let header: &[u8; STREAM_HEADER] = first.get(..STREAM_HEADER)?.try_into().ok()?;
    StreamHeader::parse(header).ok()?;
    let size = header_size_from_first_byte(*first.get(STREAM_HEADER)?)?;
    let block = first.get(STREAM_HEADER..STREAM_HEADER + size)?;
    Some(BlockHeader::parse(block).ok()?.chain.dict_prop)
}
