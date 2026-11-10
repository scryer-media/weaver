// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// xz: streams of blocks, each block checked as it decodes, concatenated
// streams read through as one. A block whose check fails fails the decode
// as any other error does: the check speaks of what the stream reads out.

use std::io::{self, Read, Seek, SeekFrom};

use lzma_turbo::xz::block::header_size_from_first_byte;
use lzma_turbo::xz::{
    BlockHeader, MAX_INDEX_SIZE, StreamHeader, XzIndex, XzOptions, XzParallelReader, XzReader,
    read_stream_index_ending_at,
};
use lzma_turbo::{Lzma2Reader, WindowBacking};
use weaver_graph::{Budget, Kind};

use super::{Decoder, Front};

pub struct Xz;

const STREAM_HEADER: usize = 12;
// The largest dictionary an xz decoder may hold when the run was granted no
// window: enough for the dictionary `xz -9` writes. The decoder holds its
// limit against a block's dictionary, so a grant of 1 GiB admits a 1 GiB
// dictionary.
const MEMORY_LIMIT: u64 = 128 << 20;

impl Decoder for Xz {
    const KIND: Kind = Kind::Xz;

    // What the first block's dictionary needs, no more than the window the
    // run was granted: a stream whose front has not said reserves the
    // decoder's own limit, never the whole grant.
    fn window(first: &[u8], budget: &Budget) -> u64 {
        let most = budget.window_or(MEMORY_LIMIT);
        dictionary(first)
            .and_then(|property| Lzma2Reader::<io::Empty>::memory_required(property).ok())
            .map_or(most.min(MEMORY_LIMIT), |window| window.min(most))
    }

    const LENDS: bool = true;

    // The first block's dictionary buffer, as the decoder sizes the window
    // it asks its lender for.
    fn lent(first: &[u8]) -> Option<u64> {
        let block = first_block(first)?;
        lent_window(block.chain.dict_prop, block.uncompressed_size)
    }

    // Held to the first block's window: a later block that asks for more
    // fails instead of growing the reservation.
    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(sequential(input, window, None)))
    }

    // One stream of several blocks, already closed, decodes its blocks side
    // by side on the run's threads: its index, at the tail, says where each
    // lies before a byte is decoded. Anything else, or anything the index
    // does not say plainly, is read from the front.
    //
    // A run handed a lender borrows a dictionary over its window instead of
    // refusing it. Side by side, each worker would hold a whole block, and a
    // block over the run's memory would be lent once per worker; a stream
    // with such a block is read from the front instead, so a run borrows one
    // dictionary at a time, never more than one window per block.
    fn open_with(
        mut input: Front,
        window: u64,
        budget: Budget,
        closed: bool,
    ) -> io::Result<Box<dyn Read + Send>> {
        let backing = crate::lent::xz(&budget, window);
        let memory = budget.window_or(MEMORY_LIMIT);
        if closed && budget.threads() > 1 {
            let kind = decoder_kind(&mut input);
            let fits = |largest: u64| backing.is_none() || largest <= memory;
            if let DecoderKind::Parallel { largest } = kind
                && fits(largest)
            {
                return parallel(input, memory, budget.threads())
                    .map(|reader| Box::new(reader) as Box<dyn Read + Send>);
            }
        }
        Ok(Box::new(sequential(input, window, backing)))
    }
}

// The sequential decoder, held to `window` in RAM, borrowing a dictionary
// over it from `backing` where the run lends.
fn sequential(input: Front, window: u64, backing: Option<WindowBacking>) -> XzReader<Front> {
    let options = XzOptions::default()
        .with_memory_limit(window.max(1))
        .with_concatenated(true);
    let options = match backing {
        Some(backing) => options.with_window(backing),
        None => options,
    };
    XzReader::with_options(input, options)
}

// The first block's header.
fn first_block(first: &[u8]) -> Option<BlockHeader> {
    let header: &[u8; STREAM_HEADER] = first.get(..STREAM_HEADER)?.try_into().ok()?;
    StreamHeader::parse(header).ok()?;
    let size = header_size_from_first_byte(*first.get(STREAM_HEADER)?)?;
    let block = first.get(STREAM_HEADER..STREAM_HEADER + size)?;
    BlockHeader::parse(block).ok()
}

// The dictionary property of the first block's LZMA2 filter.
fn dictionary(first: &[u8]) -> Option<u8> {
    Some(first_block(first)?.chain.dict_prop)
}

// The window lzma-turbo 0.12 asks its window allocator for, for a block of
// LZMA2 dictionary property `prop` that declares `uncompressed` bytes, if it
// declares any: the dictionary the property states, no larger than the
// block (but never under 4 KiB), rounded up as the SDK rounds a dictionary
// buffer (`LzmaProps::dic_buf_size`, C: `LzmaDec_Allocate`). The tests pin
// it to what the decoder asks.
fn lent_window(prop: u8, uncompressed: Option<u64>) -> Option<u64> {
    const DIC_MIN: u64 = 1 << 12;
    let declared = match prop {
        0..=39 => u64::from((2u32 | u32::from(prop & 1)) << (u32::from(prop) / 2 + 11)),
        40 => u64::from(u32::MAX),
        _ => return None,
    };
    let dict = uncompressed
        .map_or(declared, |size| declared.min(size.max(DIC_MIN)))
        .max(DIC_MIN);
    let mask: u64 = match dict {
        dict if dict >= 1 << 30 => (1 << 22) - 1,
        dict if dict >= 1 << 22 => (1 << 20) - 1,
        _ => (1 << 12) - 1,
    };
    Some((dict + mask) & !mask)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderKind {
    Sequential,
    // The largest block's uncompressed bytes, as the index states them.
    Parallel { largest: u64 },
}

// The block-parallel decoder for one stream, its index read first, so every
// block's offsets and sizes are known before a byte is decoded; it drops
// threads until their buffers fit `memory_limit`.
fn parallel<R: Read + Seek>(
    reader: R,
    memory_limit: u64,
    threads: usize,
) -> io::Result<XzParallelReader<R>> {
    let options = XzOptions::default()
        .with_memory_limit(memory_limit.max(1))
        .with_threads(threads.max(1))
        .with_concatenated(false);
    XzParallelReader::with_options(reader, options).map_err(io::Error::from)
}

// How a closed volume decodes, read from its structure without decoding:
// in parallel only as one stream of more than one block. Concatenated
// streams, padding, and anything malformed or unclear go to the sequential
// decoder, which checks the format whole. The reader is left where it was.
fn decoder_kind<R: Read + Seek>(reader: &mut R) -> DecoderKind {
    let position = reader.stream_position().ok();
    let index = single_stream_index(reader);
    if let Some(position) = position {
        let _ = reader.seek(SeekFrom::Start(position));
    }
    match index {
        Some(index) if index.block_count() > 1 => DecoderKind::Parallel {
            largest: index.largest_block(),
        },
        _ => DecoderKind::Sequential,
    }
}

// The index of the one stream the reader holds, starting at its front with
// nothing after it: as `single_stream_block_count` reads it.
fn single_stream_index<R: Read + Seek>(reader: &mut R) -> Option<XzIndex> {
    let len = reader.seek(SeekFrom::End(0)).ok()?;
    let found = read_stream_index_ending_at(reader, len, MAX_INDEX_SIZE).ok()?;
    (found.stream_offset == 0).then_some(found.index)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use lzma_turbo::{LzmaEncProps, XzWriter};

    use super::{DecoderKind, MEMORY_LIMIT, decoder_kind, parallel};

    fn compress(bytes: &[u8]) -> Vec<u8> {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }

    fn compress_multiblock(bytes: &[u8]) -> Vec<u8> {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
        writer.set_block_size(u64::from(props.dict_size()));
        writer.set_threads(2);
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }

    fn payload() -> Vec<u8> {
        (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect()
    }

    fn corrupt_index(input: &mut [u8]) {
        let footer = input.len() - 12;
        let backward =
            u32::from_le_bytes(input[footer + 4..footer + 8].try_into().unwrap()) as usize;
        let index = footer - (backward + 1) * 4;
        input[index - 4] ^= 0x80;
    }

    #[test]
    fn concatenated_streams_stay_on_the_sequential_decoder() {
        let mut input = compress(b"first ");
        input.extend(compress(b"second"));
        assert_eq!(
            decoder_kind(&mut Cursor::new(input)),
            DecoderKind::Sequential
        );
    }

    #[test]
    fn stream_padding_stays_on_the_sequential_decoder() {
        let mut input = compress_multiblock(&payload());
        input.extend([0; 4]);
        assert_eq!(
            decoder_kind(&mut Cursor::new(input)),
            DecoderKind::Sequential
        );
    }

    #[test]
    fn one_block_streams_stay_on_the_sequential_decoder() {
        assert_eq!(
            decoder_kind(&mut Cursor::new(compress(b"payload"))),
            DecoderKind::Sequential
        );
    }

    #[test]
    fn multiblock_single_stream_uses_the_parallel_decoder() {
        let payload = payload();
        let input = compress_multiblock(&payload);
        assert_eq!(
            decoder_kind(&mut Cursor::new(&input)),
            DecoderKind::Parallel { largest: 256 << 10 }
        );
        let mut output = Vec::new();
        parallel(Cursor::new(input), MEMORY_LIMIT, 2)
            .unwrap()
            .read_to_end(&mut output)
            .unwrap();
        assert_eq!(output, payload);
    }

    #[test]
    fn decoder_selection_restores_the_callers_position() {
        let mut reader = Cursor::new(compress_multiblock(&payload()));
        reader.set_position(7);
        assert!(matches!(
            decoder_kind(&mut reader),
            DecoderKind::Parallel { .. }
        ));
        assert_eq!(reader.position(), 7);
    }

    #[test]
    fn malformed_footer_stays_on_the_sequential_decoder() {
        let mut input = compress_multiblock(&payload());
        let footer = input.len() - 12;
        input[footer + 4..footer + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decoder_kind(&mut Cursor::new(input)),
            DecoderKind::Sequential
        );
    }

    #[test]
    fn parallel_decoder_clamps_zero_workers_and_enforces_the_memory_limit() {
        let payload = payload();
        let input = compress_multiblock(&payload);
        let mut output = Vec::new();
        parallel(Cursor::new(&input), MEMORY_LIMIT, 0)
            .unwrap()
            .read_to_end(&mut output)
            .unwrap();
        assert_eq!(output, payload);

        let result = parallel(Cursor::new(input), 1, 2).and_then(|mut decoder| {
            let mut output = Vec::new();
            decoder.read_to_end(&mut output)
        });
        assert!(result.is_err());
    }

    #[test]
    fn parallel_decoder_rejects_a_corrupted_multiblock_stream() {
        let mut input = compress_multiblock(&payload());
        corrupt_index(&mut input);
        let result = parallel(Cursor::new(input), MEMORY_LIMIT, 2).and_then(|mut decoder| {
            let mut output = Vec::new();
            decoder.read_to_end(&mut output)
        });
        assert!(result.is_err());
    }
}
