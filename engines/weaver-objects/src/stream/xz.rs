// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// xz: streams of blocks, each block checked as it decodes, concatenated
// streams read through as one. A block whose check fails fails the decode
// as any other error does: the check speaks of what the stream reads out.

use std::io::{self, Read, Seek, SeekFrom};

use lzma_turbo::xz::block::header_size_from_first_byte;
use lzma_turbo::xz::{
    BlockHeader, CheckType, MAX_INDEX_SIZE, StreamHeader, XzIndex, XzOptions, XzParallelReader,
    XzReader, read_stream_index_ending_at,
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
// The probability table each worker of the side-by-side decoder holds
// beside its block, as the decoder counts it.
const WORKER_TABLE: u64 = 32 << 10;

impl Decoder for Xz {
    const KIND: Kind = Kind::Xz;

    // What the first block's dictionary needs, or what decoding blocks like
    // the first side by side on the run's threads holds where that is more,
    // no more than the window the run was granted: a stream whose front has
    // not said reserves the decoder's own limit, never the whole grant.
    fn window(first: &[u8], length: Option<u64>, budget: &Budget) -> u64 {
        let most = budget.window_or(MEMORY_LIMIT);
        let first = first_block(first);
        let dictionary = first
            .as_ref()
            .and_then(|(_, block)| {
                Lzma2Reader::<io::Empty>::memory_required(block.chain.dict_prop).ok()
            })
            .map_or(most.min(MEMORY_LIMIT), |window| window.min(most));
        let side_by_side = first.map_or(0, |(check, block)| {
            side_by_side(check, &block, length, budget.threads(), most)
        });
        dictionary.max(side_by_side)
    }

    const LENDS: bool = true;

    // Held to the first block's window: a later block that asks for more
    // fails instead of growing the reservation.
    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(sequential(input, window, None)))
    }

    // One stream of several blocks, already closed, decodes its blocks side
    // by side on the run's threads, within the window the run reserved: its
    // index, at the tail, says where each lies and how much a worker holds
    // before a byte is decoded. A stream whose window pays for fewer than
    // two workers, anything else, and anything the index does not say
    // plainly, is read from the front.
    //
    // Side by side, every block is held whole within the window, so nothing
    // is lent. A run handed a lender that reads from the front borrows a
    // dictionary over its window instead of refusing it, one at a time.
    fn open_with(
        mut input: Front,
        window: u64,
        budget: Budget,
        closed: bool,
    ) -> io::Result<Box<dyn Read + Send>> {
        let threads = budget.threads();
        if closed
            && threads > 1
            && let DecoderKind::Parallel { worker, blocks } = decoder_kind(&mut input)
            && workers(window, worker, threads.min(blocks)) > 1
        {
            return parallel(input, window, threads)
                .map(|reader| Box::new(reader) as Box<dyn Read + Send>);
        }
        let backing = crate::lent::xz(&budget, window);
        Ok(Box::new(sequential(input, window, backing)))
    }
}

// What one worker of the side-by-side decoder holds: a block's decoded
// bytes, its packed bytes padded as the stream lays them, and a table.
fn worker(unpacked: u64, padded: u64) -> u64 {
    unpacked.saturating_add(padded).saturating_add(WORKER_TABLE)
}

// How many of `threads` workers, each holding `worker` bytes, `memory` pays
// for.
fn workers(memory: u64, worker: u64, threads: usize) -> usize {
    usize::try_from(memory / worker.max(1)).map_or(threads, |paid| paid.min(threads))
}

// What decoding blocks like the first side by side holds: a worker for each
// of `threads`, no more than `most`. An encoder that writes blocks on
// several threads writes them the same size, each header declaring both its
// sizes, though each packs to its own length: a worker is counted at the
// most an LZMA2 encoder packs a block that size to, so every such block
// fits. A volume of known `length` holds no more blocks than fit in it
// beside the stream's header, footer and smallest index; one of unknown
// length is counted at every thread, the most the decoder can hold. A
// header that leaves its sizes to the index, or a window or volume that pays
// for fewer than two workers like the first, holds nothing beside the
// dictionary.
fn side_by_side(
    check: CheckType,
    block: &BlockHeader,
    length: Option<u64>,
    threads: usize,
    most: u64,
) -> u64 {
    let (Some(packed), Some(unpacked)) = (block.compressed_size, block.uncompressed_size) else {
        return 0;
    };
    let unpadded = |packed: u64| {
        (block.header_size as u64)
            .saturating_add(packed)
            .saturating_add(check.size() as u64)
    };
    let padded = |packed: u64| unpadded(packed).saturating_add(3) & !3;
    let first = padded(packed);
    let threads = length.map_or(threads, |length| {
        let blocks = blocks_in(length, first, unpadded(packed), unpacked);
        usize::try_from(blocks).map_or(threads, |blocks| blocks.min(threads))
    });
    if workers(most, worker(unpacked, first), threads) < 2 {
        return 0;
    }
    let widest = worker(unpacked, padded(packed.max(packed_bound(unpacked))));
    widest.saturating_mul(threads as u64).min(most)
}

// The most blocks a volume of `length` bytes holds when every block but the
// last is `padded` bytes on disk, `unpadded` before its padding, holding
// `unpacked`: beside them the stream holds its header and footer, an
// index's indicator, count and check, and an index record per block.
fn blocks_in(length: u64, padded: u64, unpadded: u64, unpacked: u64) -> u64 {
    let varint = |value: u64| u64::from((64 - value.leading_zeros()).div_ceil(7).max(1));
    let record = varint(unpadded) + varint(unpacked);
    let framing = 2 * STREAM_HEADER as u64 + 6;
    length.saturating_sub(framing + record) / padded.saturating_add(record).max(1) + 1
}

// The most an LZMA2 encoder packs `unpacked` bytes to: where packing does
// not shrink a chunk it stores the chunk as it is, 64 KiB at most behind a
// three-byte header, and the data ends with one byte.
fn packed_bound(unpacked: u64) -> u64 {
    unpacked
        .saturating_add(unpacked.div_ceil(64 << 10).saturating_mul(3))
        .saturating_add(1)
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

// The check the stream's blocks carry, and the first block's header.
fn first_block(first: &[u8]) -> Option<(CheckType, BlockHeader)> {
    let header: &[u8; STREAM_HEADER] = first.get(..STREAM_HEADER)?.try_into().ok()?;
    let check = StreamHeader::parse(header).ok()?.flags.check;
    let size = header_size_from_first_byte(*first.get(STREAM_HEADER)?)?;
    let block = first.get(STREAM_HEADER..STREAM_HEADER + size)?;
    Some((check, BlockHeader::parse(block).ok()?))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderKind {
    Sequential,
    // What one worker holds for the largest blocks, and how many blocks
    // there are, as the index states them.
    Parallel { worker: u64, blocks: usize },
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
    let parallel = index
        .filter(|index| index.block_count() > 1)
        .and_then(|index| {
            let padded = index
                .records
                .iter()
                .try_fold(0, |most, record| Some(record.padded_size()?.max(most)))?;
            Some(DecoderKind::Parallel {
                worker: worker(index.largest_block(), padded),
                blocks: index.block_count(),
            })
        });
    parallel.unwrap_or(DecoderKind::Sequential)
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
    use std::io::{self, Cursor, Read, Write};

    use lzma_turbo::{Lzma2Reader, LzmaEncProps, XzWriter};
    use weaver_graph::Budget;

    use super::{
        Decoder, DecoderKind, MEMORY_LIMIT, Xz, decoder_kind, first_block, parallel, workers,
    };

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
        let kind = decoder_kind(&mut Cursor::new(&input));
        assert!(
            matches!(kind, DecoderKind::Parallel { worker, blocks: 4 } if worker > 256 << 10),
            "{kind:?}"
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

    fn granting(threads: usize) -> Budget {
        Budget {
            window_bytes: Some(MEMORY_LIMIT),
            threads,
            ..Budget::default()
        }
    }

    // Bytes no encoder can shrink, so every chunk is stored as it is.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 56) as u8
            })
            .collect()
    }

    // What a stream's front reserves holds the side-by-side decoder at as
    // many workers as the whole grant did, whether its blocks pack small or
    // are stored as they are, and the decoder holds no more than it.
    #[test]
    fn the_reservation_pays_for_every_worker_the_grant_paid_for() {
        for plain in [payload(), incompressible(1 << 20)] {
            let input = compress_multiblock(&plain);
            for threads in [2, 3, 4] {
                let reserved = Xz::window(&input[..64], None, &granting(threads));
                let granted = parallel(Cursor::new(&input), MEMORY_LIMIT, threads).unwrap();
                let held = parallel(Cursor::new(&input), reserved, threads).unwrap();
                assert_eq!(held.threads(), threads);
                assert_eq!(held.threads(), granted.threads());
                assert!(held.memory_estimate() <= reserved, "{threads} threads");
                let Some((_, first)) = first_block(&input) else {
                    panic!("no first block");
                };
                let dictionary =
                    Lzma2Reader::<io::Empty>::memory_required(first.chain.dict_prop).unwrap();
                assert!(reserved > dictionary, "{threads} threads");
            }
        }
    }

    // One thread, or a header that leaves its sizes to the index, reserves
    // the first block's dictionary alone.
    #[test]
    fn a_front_with_no_side_by_side_decode_reserves_the_dictionary() {
        let multiblock = compress_multiblock(&payload());
        let one = Xz::window(&multiblock[..64], None, &granting(1));
        let Some((_, first)) = first_block(&multiblock) else {
            panic!("no first block");
        };
        let dictionary = Lzma2Reader::<io::Empty>::memory_required(first.chain.dict_prop).unwrap();
        assert_eq!(one, dictionary);

        let streamed = compress(&payload());
        let Some((_, block)) = first_block(&streamed) else {
            panic!("no first block");
        };
        assert_eq!(block.uncompressed_size, None);
        let dictionary = Lzma2Reader::<io::Empty>::memory_required(block.chain.dict_prop).unwrap();
        assert_eq!(Xz::window(&streamed[..64], None, &granting(4)), dictionary);
    }

    // A stream whose first block is small and whose later blocks are wide.
    fn wider_after_the_first(plain: &[u8]) -> Vec<u8> {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
        writer.set_threads(2);
        writer.set_block_size(4 << 10);
        writer.write_all(&plain[..4 << 10]).unwrap();
        writer.set_block_size(256 << 10);
        writer.write_all(&plain[4 << 10..]).unwrap();
        writer.finish().unwrap()
    }

    // Later blocks wider than the first the front reserved for are not
    // decoded side by side past the reservation: the window pays for fewer
    // than two of their workers, where the whole grant paid the decoder for
    // far more than was reserved.
    #[test]
    fn later_blocks_wider_than_the_first_stay_within_the_reservation() {
        let plain = payload();
        let input = wider_after_the_first(&plain);
        let Some((_, first)) = first_block(&input) else {
            panic!("no first block");
        };
        assert_eq!(first.uncompressed_size, Some(4 << 10));
        let reserved = Xz::window(&input[..64], None, &granting(4));
        let DecoderKind::Parallel { worker, blocks } = decoder_kind(&mut Cursor::new(&input))
        else {
            panic!("not a side-by-side stream");
        };
        assert!(workers(reserved, worker, blocks.min(4)) < 2);
        let granted = parallel(Cursor::new(&input), MEMORY_LIMIT, 4).unwrap();
        assert!(granted.memory_estimate() > reserved);
    }

    // `plain` in 256 KiB blocks, written on two threads, so every block's
    // header declares its sizes.
    fn blocks_of_256k(plain: &[u8]) -> Vec<u8> {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
        writer.set_threads(2);
        writer.set_block_size(256 << 10);
        writer.write_all(plain).unwrap();
        writer.finish().unwrap()
    }

    // What the front of `input` reserves at eight threads, with the volume's
    // length known and not, and the workers the decoder runs within the
    // first.
    fn reserved_at_eight(input: &[u8]) -> (u64, u64, usize) {
        let budget = granting(8);
        let known = Xz::window(&input[..64], Some(input.len() as u64), &budget);
        let unknown = Xz::window(&input[..64], None, &budget);
        let mut reader = Cursor::new(input);
        let workers = match decoder_kind(&mut reader) {
            DecoderKind::Parallel { worker, blocks } => workers(known, worker, blocks.min(8)),
            DecoderKind::Sequential => 1,
        };
        (known, unknown, workers)
    }

    // A volume's length bounds the workers reserved to the blocks it can
    // hold: one block reserves its dictionary alone, as a stream read from
    // the front does; two blocks two workers; four blocks at four threads
    // what an unknown length reserves.
    #[test]
    fn the_volumes_length_bounds_the_workers_reserved() {
        let single = blocks_of_256k(&incompressible(200 << 10));
        let Some((_, first)) = first_block(&single) else {
            panic!("no first block");
        };
        assert_eq!(first.uncompressed_size, Some(200 << 10));
        let dictionary = Lzma2Reader::<io::Empty>::memory_required(first.chain.dict_prop).unwrap();
        let (known, unknown, workers) = reserved_at_eight(&single);
        assert_eq!(known, dictionary);
        assert_eq!(workers, 1);
        assert!(unknown > 4 * known, "{unknown} against {known}");

        let two = blocks_of_256k(&incompressible(512 << 10));
        let (known, unknown, workers) = reserved_at_eight(&two);
        assert_eq!(workers, 2);
        assert_eq!(unknown, 4 * known, "eight workers against two");
        let held = parallel(Cursor::new(&two), known, 8).unwrap();
        assert_eq!(held.threads(), 2);
        assert!(held.memory_estimate() <= known);

        let four = blocks_of_256k(&incompressible(1 << 20));
        let budget = granting(4);
        let known = Xz::window(&four[..64], Some(four.len() as u64), &budget);
        assert_eq!(known, Xz::window(&four[..64], None, &budget));
        assert_eq!(parallel(Cursor::new(&four), known, 4).unwrap().threads(), 4);
    }
}
