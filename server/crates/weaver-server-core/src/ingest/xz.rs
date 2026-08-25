use std::io::{self, Read, Seek, SeekFrom};

use liblzma::read::XzDecoder;
use liblzma::stream::{CONCATENATED, MtStreamBuilder, Stream};

/// Maximum memory liblzma may use while decoding an XZ input.
///
/// This covers the attacker-controlled LZMA2 dictionary as well as decoder
/// bookkeeping.  128 MiB accepts standard `xz -9` archives (64 MiB
/// dictionary) without allowing a tiny archive to request multi-gigabyte
/// allocations.
pub const XZ_DECODER_MEMORY_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

const XZ_STREAM_HEADER_SIZE: u64 = 12;
const XZ_STREAM_FOOTER_SIZE: u64 = 12;
const MAX_XZ_INDEX_SIZE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XzFilesystemDecoderKind {
    Sequential,
    Parallel,
}

/// Opens an integrity-checking, concatenated-stream XZ decoder with a hard
/// decoder-memory limit.
pub fn xz_multistream_decoder<R: Read>(
    reader: R,
    memory_limit_bytes: u64,
) -> io::Result<XzDecoder<R>> {
    let memory_limit_bytes = memory_limit_bytes.max(1);
    let stream =
        Stream::new_stream_decoder(memory_limit_bytes, CONCATENATED).map_err(io::Error::other)?;
    Ok(XzDecoder::new_stream(reader, stream))
}

/// Opens a bounded multithreaded decoder for one XZ stream.
///
/// Callers must first use [`xz_filesystem_decoder_kind`] to keep concatenated
/// streams on the sequential multistream decoder.
pub(crate) fn xz_parallel_decoder<R: Read>(
    reader: R,
    memory_limit_bytes: u64,
    worker_count: usize,
) -> io::Result<XzDecoder<R>> {
    let memory_limit_bytes = memory_limit_bytes.max(1);
    let worker_count = u32::try_from(worker_count).unwrap_or(u32::MAX).max(1);
    let mut builder = MtStreamBuilder::new();
    builder
        .threads(worker_count)
        .memlimit_threading(memory_limit_bytes)
        .memlimit_stop(memory_limit_bytes);
    let stream = builder.decoder().map_err(io::Error::other)?;
    Ok(XzDecoder::new_stream(reader, stream))
}

/// Selects the decoder for a completed XZ file without decompressing it.
///
/// The multithreaded decoder is used only for a structurally single stream
/// containing more than one block. Any malformed or ambiguous structure falls
/// back to the bounded sequential decoder, which remains responsible for full
/// format and integrity validation.
pub(crate) fn xz_filesystem_decoder_kind<R: Read + Seek>(
    reader: &mut R,
) -> XzFilesystemDecoderKind {
    let initial_position = reader.stream_position().ok();
    let block_count = xz_single_stream_block_count(reader);
    if let Some(position) = initial_position {
        let _ = reader.seek(SeekFrom::Start(position));
    }

    match block_count {
        Some(count) if count > 1 => XzFilesystemDecoderKind::Parallel,
        _ => XzFilesystemDecoderKind::Sequential,
    }
}

fn xz_single_stream_block_count<R: Read + Seek>(reader: &mut R) -> Option<usize> {
    let stream_len = reader.seek(SeekFrom::End(0)).ok()?;
    if stream_len < XZ_STREAM_HEADER_SIZE + XZ_STREAM_FOOTER_SIZE {
        return None;
    }

    let mut header_magic = [0_u8; 6];
    reader.seek(SeekFrom::Start(0)).ok()?;
    reader.read_exact(&mut header_magic).ok()?;
    if header_magic != [0xFD, b'7', b'z', b'X', b'Z', 0x00] {
        return None;
    }

    let footer_offset = stream_len.checked_sub(XZ_STREAM_FOOTER_SIZE)?;
    let mut footer = [0_u8; XZ_STREAM_FOOTER_SIZE as usize];
    reader.seek(SeekFrom::Start(footer_offset)).ok()?;
    reader.read_exact(&mut footer).ok()?;
    if footer[10..] != *b"YZ" {
        return None;
    }

    let backward_size = u32::from_le_bytes(footer[4..8].try_into().ok()?) as u64;
    let index_size = backward_size.checked_add(1)?.checked_mul(4)?;
    if index_size > MAX_XZ_INDEX_SIZE_BYTES {
        return None;
    }
    let index_offset = footer_offset.checked_sub(index_size)?;
    if index_offset < XZ_STREAM_HEADER_SIZE {
        return None;
    }

    let index_size = usize::try_from(index_size).ok()?;
    let mut index = vec![0_u8; index_size];
    reader.seek(SeekFrom::Start(index_offset)).ok()?;
    reader.read_exact(&mut index).ok()?;
    let index_body = index.get(..index.len().checked_sub(4)?)?;
    if index_body.first().copied()? != 0x00 {
        return None;
    }

    let mut offset = 1;
    let record_count = usize::try_from(read_xz_vli(index_body, &mut offset)?).ok()?;
    if record_count > index_body.len().saturating_sub(offset) / 2 {
        return None;
    }

    let mut padded_block_bytes = 0_u64;
    for _ in 0..record_count {
        let unpadded_size = read_xz_vli(index_body, &mut offset)?;
        if unpadded_size == 0 {
            return None;
        }
        let _uncompressed_size = read_xz_vli(index_body, &mut offset)?;
        let padded_size = unpadded_size.checked_add(3)? & !3;
        padded_block_bytes = padded_block_bytes.checked_add(padded_size)?;
    }
    if index_body.get(offset..)?.iter().any(|byte| *byte != 0) {
        return None;
    }

    let expected_index_offset = XZ_STREAM_HEADER_SIZE.checked_add(padded_block_bytes)?;
    (expected_index_offset == index_offset).then_some(record_count)
}

fn read_xz_vli(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    for byte_index in 0..9 {
        let byte = *bytes.get(*offset)?;
        *offset = offset.checked_add(1)?;
        let payload = u64::from(byte & 0x7F);
        value |= payload.checked_shl(byte_index * 7)?;
        if byte & 0x80 == 0 {
            if byte_index > 0 && payload == 0 {
                return None;
            }
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::num::NonZeroU64;

    use lzma_rust2::{XzOptions, XzWriter, XzWriterMt};

    use super::{
        XZ_DECODER_MEMORY_LIMIT_BYTES, XzFilesystemDecoderKind, xz_filesystem_decoder_kind,
        xz_multistream_decoder, xz_parallel_decoder,
    };

    fn xz_compress(bytes: &[u8]) -> Vec<u8> {
        let mut writer = XzWriter::new(Vec::new(), XzOptions::with_preset(0)).unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }

    fn xz_compress_multiblock(bytes: &[u8]) -> Vec<u8> {
        let mut options = XzOptions::with_preset(0);
        options.set_block_size(NonZeroU64::new(options.lzma_options.dict_size.into()));
        let mut writer = XzWriterMt::new(Vec::new(), options, 2).unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }

    fn corrupt_xz_index(input: &mut [u8]) {
        let footer_offset = input.len() - 12;
        let backward_size = u32::from_le_bytes(
            input[footer_offset + 4..footer_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let index_offset = footer_offset - (backward_size + 1) * 4;
        input[index_offset - 4] ^= 0x80;
    }

    #[test]
    fn decodes_concatenated_streams() {
        let mut input = xz_compress(b"first ");
        input.extend(xz_compress(b"second"));

        let mut decoder =
            xz_multistream_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES).unwrap();
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).unwrap();

        assert_eq!(output, b"first second");
    }

    #[test]
    fn concatenated_streams_stay_on_the_sequential_decoder() {
        let mut input = xz_compress(b"first ");
        input.extend(xz_compress(b"second"));

        assert_eq!(
            xz_filesystem_decoder_kind(&mut Cursor::new(input)),
            XzFilesystemDecoderKind::Sequential
        );
    }

    #[test]
    fn stream_padding_stays_on_the_sequential_decoder() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let mut input = xz_compress_multiblock(&payload);
        input.extend([0; 4]);

        assert_eq!(
            xz_filesystem_decoder_kind(&mut Cursor::new(&input)),
            XzFilesystemDecoderKind::Sequential
        );

        let mut decoder =
            xz_multistream_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES).unwrap();
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).unwrap();
        assert_eq!(output, payload);
    }

    #[test]
    fn one_block_streams_stay_on_the_sequential_decoder() {
        assert_eq!(
            xz_filesystem_decoder_kind(&mut Cursor::new(xz_compress(b"payload"))),
            XzFilesystemDecoderKind::Sequential
        );
    }

    #[test]
    fn multiblock_single_stream_uses_the_parallel_decoder() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let input = xz_compress_multiblock(&payload);

        assert_eq!(
            xz_filesystem_decoder_kind(&mut Cursor::new(&input)),
            XzFilesystemDecoderKind::Parallel
        );

        let mut decoder =
            xz_parallel_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES, 2).unwrap();
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).unwrap();

        assert_eq!(output, payload);
    }

    #[test]
    fn decoder_selection_restores_the_callers_position() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let mut reader = Cursor::new(xz_compress_multiblock(&payload));
        reader.set_position(7);

        assert_eq!(
            xz_filesystem_decoder_kind(&mut reader),
            XzFilesystemDecoderKind::Parallel
        );
        assert_eq!(reader.position(), 7);
    }

    #[test]
    fn malformed_footer_stays_on_the_sequential_decoder() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let mut input = xz_compress_multiblock(&payload);
        let footer_offset = input.len() - 12;
        input[footer_offset + 4..footer_offset + 8].copy_from_slice(&u32::MAX.to_le_bytes());

        assert_eq!(
            xz_filesystem_decoder_kind(&mut Cursor::new(input)),
            XzFilesystemDecoderKind::Sequential
        );
    }

    #[test]
    fn parallel_decoder_clamps_zero_workers_and_enforces_the_memory_limit() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let input = xz_compress_multiblock(&payload);

        let mut decoder =
            xz_parallel_decoder(Cursor::new(&input), XZ_DECODER_MEMORY_LIMIT_BYTES, 0).unwrap();
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).unwrap();
        assert_eq!(output, payload);

        let result = xz_parallel_decoder(Cursor::new(input), 1, 2).and_then(|mut decoder| {
            let mut output = Vec::new();
            decoder.read_to_end(&mut output)
        });
        assert!(result.is_err());
    }

    #[test]
    fn rejects_a_dictionary_over_the_configured_limit() {
        let input = xz_compress(b"payload");
        let mut decoder = xz_multistream_decoder(Cursor::new(input), 1).unwrap();
        let mut output = Vec::new();

        assert!(decoder.read_to_end(&mut output).is_err());
    }

    #[test]
    fn rejects_a_corrupted_block() {
        let mut input = xz_compress(b"payload");
        corrupt_xz_index(&mut input);

        let mut decoder =
            xz_multistream_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES).unwrap();
        let mut output = Vec::new();

        assert!(decoder.read_to_end(&mut output).is_err());
    }

    #[test]
    fn parallel_decoder_rejects_a_corrupted_multiblock_stream() {
        let payload: Vec<u8> = (0..(1024 * 1024))
            .map(|index| (index % 251) as u8)
            .collect();
        let mut input = xz_compress_multiblock(&payload);
        corrupt_xz_index(&mut input);

        let mut decoder =
            xz_parallel_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES, 2).unwrap();
        let mut output = Vec::new();

        assert!(decoder.read_to_end(&mut output).is_err());
    }
}
