// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An archive whose decode needs more memory than the profile's ceiling: one
// row per family, a RAR, a 7z and a stream, each the only thing posted.
//
// Under the efficient profile a RAR or 7z window over the profile's is
// lent from scratch; where the scratch has no room for it, or for a stream,
// the archive is withheld too large, nothing usable comes out, and the job
// fails with the sizes in its message. Under balanced the window is granted
// above the ceiling, up to what the machine can hold; past that the job
// fails naming the machine's memory. A job reprocessed with no memory limit
// is granted the window whatever its profile. Rows that may lend set the
// scratch room themselves, so none reads the machine's free disk.

pub fn body() -> Vec<u8> {
    (0..100u8).collect()
}

// A 7z of one LZMA2 folder whose coder names a dictionary of 4 GiB less a
// byte, property 40, over one uncompressed LZMA2 chunk. The header is
// plain, so nothing but the property says how wide the decoder must be.
pub fn wide_7z() -> Vec<u8> {
    let file = body();
    let mut packed = vec![0x01, 0x00, (file.len() - 1) as u8];
    packed.extend_from_slice(&file);
    packed.push(0x00);
    let name: Vec<u8> = "beacon.bin"
        .encode_utf16()
        .chain([0])
        .flat_map(u16::to_le_bytes)
        .collect();
    let mut header = vec![
        0x01, // Header
        0x04, // MainStreamsInfo
        0x06,
        0x00,
        0x01, // PackInfo: at 0, one stream
        0x09,
        packed.len() as u8, // its size
        0x00,
        0x07, // UnpackInfo
        0x0B,
        0x01,
        0x00, // one folder, inline
        0x01,
        0x21,
        0x21,
        0x01,
        40, // one coder: LZMA2, property 40
        0x0C,
        file.len() as u8, // its unpacked size
        0x0A,
        0x01, // every folder's CRC
    ];
    header.extend_from_slice(&par2_rs::checksum::crc32(&file).to_le_bytes());
    header.extend_from_slice(&[0x00, 0x00]); // end UnpackInfo, MainStreamsInfo
    header.extend_from_slice(&[0x05, 0x01, 0x11, (name.len() + 1) as u8, 0x00]);
    header.extend_from_slice(&name);
    header.extend_from_slice(&[0x00, 0x00]); // end FilesInfo, Header

    let mut start = Vec::new();
    start.extend_from_slice(&(packed.len() as u64).to_le_bytes());
    start.extend_from_slice(&(header.len() as u64).to_le_bytes());
    start.extend_from_slice(&par2_rs::checksum::crc32(&header).to_le_bytes());
    let mut bytes = vec![b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C, 0, 4];
    bytes.extend_from_slice(&par2_rs::checksum::crc32(&start).to_le_bytes());
    bytes.extend_from_slice(&start);
    bytes.extend_from_slice(&packed);
    bytes.extend_from_slice(&header);
    bytes
}

// A 7z of one file `name` holding `bytes`, through the coders `methods`.
pub fn sevenz(
    methods: Vec<sevenz_turbo::EncoderConfiguration>,
    name: &str,
    bytes: &[u8],
) -> Vec<u8> {
    use std::io::Cursor;
    let mut writer = sevenz_turbo::ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(methods);
    let mut entry = sevenz_turbo::ArchiveEntry::new_file(name);
    entry.size = bytes.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(bytes.to_vec())))
        .unwrap();
    writer.finish().unwrap().into_inner()
}
