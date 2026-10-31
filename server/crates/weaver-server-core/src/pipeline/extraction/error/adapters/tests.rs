// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each adapter against the rendering the pinned crate produces. Where the
// crate can be made to fail for real, it is; where the cause is a variant the
// crate renders through its own `Display`, that rendering is taken from the
// crate and never written out here.

use super::*;
use std::io::{Cursor, Read, Write};

fn read_all(mut reader: impl Read) -> io::Error {
    let mut sink = Vec::new();
    reader
        .read_to_end(&mut sink)
        .expect_err("the stream is damaged")
}

#[test]
fn std_short_read_exact_is_recoverable() {
    let error = Cursor::new([0u8; 3]).read_exact(&mut [0u8; 8]).unwrap_err();
    assert!(std_read_exact_eof(&error.to_string()));
    assert_eq!(
        untyped_io_class(&error),
        Some(ExtractionErrorClass::Recoverable)
    );
}

#[test]
fn zstd_0_13_checksum_mismatch_is_recoverable() {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    encoder.include_checksum(true).unwrap();
    encoder.write_all(&[7u8; 4096]).unwrap();
    let mut frame = encoder.finish().unwrap();
    // The content checksum is the frame's last four bytes.
    let last = frame.len() - 1;
    frame[last] ^= 0xff;
    let error = read_all(zstd::stream::read::Decoder::new(Cursor::new(frame)).unwrap());
    assert!(zstd_0_13_checksum_mismatch(&error.to_string()));
    assert_eq!(
        untyped_io_class(&error),
        Some(ExtractionErrorClass::Recoverable)
    );
}

#[test]
fn flate2_1_1_gzip_trailer_mismatch_is_recoverable() {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&[9u8; 4096]).unwrap();
    let mut member = encoder.finish().unwrap();
    // The trailer is the CRC-32 and then the length, four bytes each.
    let crc = member.len() - 8;
    member[crc] ^= 0xff;
    let error = read_all(flate2::read::MultiGzDecoder::new(Cursor::new(member)));
    assert!(flate2_1_1_gzip_checksum_mismatch(&error.to_string()));
    assert_eq!(
        untyped_io_class(&error),
        Some(ExtractionErrorClass::Recoverable)
    );
}

#[test]
fn zip_9_member_crc_mismatch_is_recoverable() {
    let payload = b"one stored member, read back with one byte changed";
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(
            "member.bin",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(payload).unwrap();
    let mut archive = writer.finish().unwrap().into_inner();
    let at = archive
        .windows(payload.len())
        .position(|window| window == payload)
        .unwrap();
    archive[at] ^= 0x01;
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).unwrap();
    let error = read_all(zip.by_index(0).unwrap());
    assert!(zip_9_crc32_mismatch(&error));
    assert_eq!(
        untyped_io_class(&error),
        Some(ExtractionErrorClass::Recoverable)
    );
}

// A one-member stored archive whose central entry carries an Info-ZIP
// Unicode Path field (0x7075) with a CRC-32 that does not match the name.
fn zip_with_bad_unicode_path_field() -> Vec<u8> {
    let name = b"member.bin";
    let mut extra = vec![0x75, 0x70];
    let field = [&[1u8][..], &0xdead_beef_u32.to_le_bytes(), b"member.bin"].concat();
    extra.extend_from_slice(&(field.len() as u16).to_le_bytes());
    extra.extend_from_slice(&field);

    let mut archive = Vec::new();
    archive.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&[20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&0u16.to_le_bytes());
    archive.extend_from_slice(name);

    let central_start = archive.len() as u32;
    archive.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&[20, 3, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&(extra.len() as u16).to_le_bytes());
    archive.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(&0u32.to_le_bytes());
    archive.extend_from_slice(name);
    archive.extend_from_slice(&extra);
    let central_len = archive.len() as u32 - central_start;

    archive.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&[0, 0, 0, 0, 1, 0, 1, 0]);
    archive.extend_from_slice(&central_len.to_le_bytes());
    archive.extend_from_slice(&central_start.to_le_bytes());
    archive.extend_from_slice(&0u16.to_le_bytes());
    archive
}

// zip 9.0 no longer checks the field's CRC-32 when it opens an archive, so
// no adapter classifies that failure: the member keeps its own name.
#[test]
fn zip_9_opens_past_a_unicode_path_field_mismatch() {
    let mut archive = zip::ZipArchive::new(Cursor::new(zip_with_bad_unicode_path_field()))
        .expect("the archive opens");
    let member = archive.by_index(0).expect("the member opens");
    assert_eq!(member.name().expect("the name decodes"), "member.bin");
}

#[test]
fn tar_0_4_header_checksum_mismatch_is_recoverable() {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_ustar();
    header.set_path("member.bin").unwrap();
    header.set_size(4);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append(&header, &b"data"[..]).unwrap();
    let mut archive = builder.into_inner().unwrap();
    // A byte of the name, after the checksum was computed over it.
    archive[0] ^= 0x01;
    let mut reader = tar::Archive::new(Cursor::new(archive));
    let error = reader
        .entries()
        .unwrap()
        .next()
        .expect("one entry")
        .err()
        .expect("the header fails its checksum");
    assert!(tar_0_4_header_checksum_mismatch(&error.to_string()));
    assert_eq!(
        untyped_io_class(&error),
        Some(ExtractionErrorClass::Recoverable)
    );
}
