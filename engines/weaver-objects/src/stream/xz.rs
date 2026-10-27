// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// xz: streams of blocks, each block checked as it decodes, concatenated
// streams read through as one. A block whose check fails names its stream,
// and the stream's own index, read back from the volume's tail, says where
// that stream lies, so the damage is called at that stream alone.

use std::io::{self, Read, Seek, SeekFrom};

use lzma_turbo::Lzma2Reader;
use lzma_turbo::xz::block::header_size_from_first_byte;
use lzma_turbo::xz::{
    BlockHeader, StreamHeader, XzError, XzErrorKind, XzOptions, XzParallelReader, XzReader,
    single_stream_block_count, stream_table,
};
use weaver_graph::{Budget, ByteRange, Kind};

use super::{Decoder, Front};

pub struct Xz;

const STREAM_HEADER: usize = 12;
// The most an xz decoder may hold when the run was granted no memory:
// enough for the dictionary `xz -9` writes, and no tiny stream may ask for
// more.
const MEMORY_LIMIT: u64 = 128 << 20;

impl Decoder for Xz {
    const KIND: Kind = Kind::Xz;

    // No more than the run was granted, and never more than the most an xz
    // decoder may hold: a grant narrows the window and never widens it, so
    // a stream whose front has not said reserves no more than that.
    fn window(first: &[u8], budget: &Budget) -> u64 {
        let most = budget.memory_or(MEMORY_LIMIT).min(MEMORY_LIMIT);
        dictionary(first)
            .and_then(|property| Lzma2Reader::<io::Empty>::memory_required(property).ok())
            .map_or(most, |window| window.min(most))
    }

    // Held to the first block's window: a later block that asks for more
    // fails instead of growing the reservation.
    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        let options = XzOptions::default()
            .with_memory_limit(window.max(1))
            .with_concatenated(true);
        Ok(Box::new(XzReader::with_options(input, options)))
    }

    // One stream of several blocks, already closed, decodes its blocks side
    // by side on the run's threads: its index, at the tail, says where each
    // lies before a byte is decoded. Anything else, or anything the index
    // does not say plainly, is read from the front.
    fn open_with(
        mut input: Front,
        window: u64,
        budget: Budget,
        closed: bool,
    ) -> io::Result<Box<dyn Read + Send>> {
        if closed && budget.threads() > 1 && decoder_kind(&mut input) == DecoderKind::Parallel {
            return parallel(input, budget.memory_or(MEMORY_LIMIT), budget.threads())
                .map(|reader| Box::new(reader) as Box<dyn Read + Send>);
        }
        Self::open(input, window)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderKind {
    Sequential,
    Parallel,
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
    let blocks = single_stream_block_count(reader);
    if let Some(position) = position {
        let _ = reader.seek(SeekFrom::Start(position));
    }
    match blocks {
        Some(count) if count > 1 => DecoderKind::Parallel,
        _ => DecoderKind::Sequential,
    }
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
            DecoderKind::Parallel
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
        assert_eq!(decoder_kind(&mut reader), DecoderKind::Parallel);
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
