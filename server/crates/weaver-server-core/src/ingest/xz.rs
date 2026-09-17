use std::io::{self, Read, Seek, SeekFrom};

use lzma_turbo::xz::{XzOptions, XzParallelReader, XzReader};

/// Maximum memory the xz decoder may use while decoding an XZ input.
///
/// This covers the attacker-controlled LZMA2 dictionary as well as decoder
/// bookkeeping.  128 MiB accepts standard `xz -9` archives (64 MiB
/// dictionary) without allowing a tiny archive to request multi-gigabyte
/// allocations.  The parallel decoder degrades its thread count to fit under
/// this figure rather than refusing the file.
pub const XZ_DECODER_MEMORY_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XzFilesystemDecoderKind {
    Sequential,
    Parallel,
}

/// Opens an integrity-checking, concatenated-stream XZ decoder with a hard
/// decoder-memory limit.
///
/// Single-threaded by construction: the input only has to be readable, so this
/// is the decoder for uploads, watch-folder intake and anything still arriving.
pub fn xz_multistream_decoder<R: Read>(
    reader: R,
    memory_limit_bytes: u64,
) -> io::Result<XzReader<R>> {
    let options = XzOptions::default()
        .with_memory_limit(memory_limit_bytes.max(1))
        .with_concatenated(true);
    Ok(XzReader::with_options(reader, options))
}

/// Opens a bounded block-parallel decoder for one XZ stream already on disk.
///
/// The file's index is read first, so every block's offsets and sizes are
/// known before a byte is decoded; the thread count is reduced until the
/// workers' buffers fit under `memory_limit_bytes`, and
/// [`XzParallelReader::memory_estimate`] then reports what the decode will
/// actually cost so the caller can reserve exactly that.
///
/// Callers must first use [`xz_filesystem_decoder_kind`] to keep concatenated
/// streams on the sequential multistream decoder.
pub(crate) fn xz_parallel_decoder<R: Read + Seek>(
    reader: R,
    memory_limit_bytes: u64,
    worker_count: usize,
) -> io::Result<XzParallelReader<R>> {
    let options = XzOptions::default()
        .with_memory_limit(memory_limit_bytes.max(1))
        .with_threads(worker_count.max(1))
        .with_concatenated(false);
    XzParallelReader::with_options(reader, options).map_err(io::Error::from)
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
    let block_count = lzma_turbo::xz::single_stream_block_count(reader);
    if let Some(position) = initial_position {
        let _ = reader.seek(SeekFrom::Start(position));
    }

    match block_count {
        Some(count) if count > 1 => XzFilesystemDecoderKind::Parallel,
        _ => XzFilesystemDecoderKind::Sequential,
    }
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

        let result = xz_parallel_decoder(Cursor::new(input), XZ_DECODER_MEMORY_LIMIT_BYTES, 2)
            .and_then(|mut decoder| {
                let mut output = Vec::new();
                decoder.read_to_end(&mut output)
            });

        assert!(result.is_err());
    }
}
