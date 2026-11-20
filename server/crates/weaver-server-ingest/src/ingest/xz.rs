// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{self, Read};

use lzma_turbo::xz::{XzOptions, XzReader};

// Maximum memory the xz decoder may use while decoding an XZ input.
//
// This covers the attacker-controlled LZMA2 dictionary as well as decoder
// bookkeeping.  128 MiB accepts standard `xz -9` archives (64 MiB
// dictionary) without allowing a tiny archive to request multi-gigabyte
// allocations.  The parallel decoder degrades its thread count to fit under
// this figure rather than refusing the file.
pub const XZ_DECODER_MEMORY_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

// Opens an integrity-checking, concatenated-stream XZ decoder with a hard
// decoder-memory limit.
//
// Single-threaded by construction: the input only has to be readable, so this
// is the decoder for uploads, watch-folder intake and anything still arriving.
pub fn xz_multistream_decoder<R: Read>(
    reader: R,
    memory_limit_bytes: u64,
) -> io::Result<XzReader<R>> {
    let options = XzOptions::default()
        .with_memory_limit(memory_limit_bytes.max(1))
        .with_concatenated(true);
    Ok(XzReader::with_options(reader, options))
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use lzma_turbo::{LzmaEncProps, XzWriter};

    use super::{XZ_DECODER_MEMORY_LIMIT_BYTES, xz_multistream_decoder};

    fn xz_compress(bytes: &[u8]) -> Vec<u8> {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
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
}
