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
fn zstd_0_13_frame_past_the_window_is_recognised() {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    encoder.window_log(24).unwrap();
    let data: Vec<u8> = (0u32..1 << 20)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    encoder.write_all(&data).unwrap();
    let frame = encoder.finish().unwrap();
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(frame)).unwrap();
    decoder.window_log_max(10).unwrap();
    let error = read_all(decoder);
    assert!(zstd_0_13_frame_exceeds_window(&error.to_string()));
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
fn zip_8_6_member_crc_mismatch_is_recoverable() {
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
    assert!(zip_8_6_crc32_mismatch(&error));
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

#[test]
fn zip_8_6_unicode_extra_field_mismatch_is_recognised() {
    let error = zip::ZipArchive::new(Cursor::new(zip_with_bad_unicode_path_field()))
        .err()
        .expect("the Unicode path field fails its CRC");
    let zip::result::ZipError::InvalidArchive(detail) = &error else {
        panic!("an invalid archive: {error:?}");
    };
    assert!(zip_8_6_unicode_extra_field_checksum(detail));
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

#[test]
fn unrar_0_11_provider_refusals_are_stale_topology() {
    let unavailable = unrar_rs::VolumeProviderError::Unavailable {
        volume: 3,
        reason: "the part is still downloading".into(),
    };
    assert_eq!(
        unrar_0_11_erased_class(&unavailable.to_string()),
        Some(ExtractionErrorClass::StaleTopology)
    );
    let provider = unrar_rs::StaticVolumeProvider::new(Default::default());
    let Err(refusal) = unrar_rs::VolumeProvider::get_volume(&provider, 1) else {
        panic!("an unregistered volume is refused");
    };
    assert_eq!(
        unrar_0_11_erased_class(&refusal.to_string()),
        Some(ExtractionErrorClass::StaleTopology)
    );
}

#[test]
fn unrar_0_11_erased_descriptor_exhaustion_is_capacity_pressure() {
    for code in [libc::EMFILE, libc::ENFILE] {
        let rendered = io::Error::from_raw_os_error(code).to_string();
        assert_eq!(
            unrar_0_11_erased_class(&rendered),
            Some(ExtractionErrorClass::CapacityPressure),
            "{code}"
        );
    }
    assert_eq!(
        unrar_0_11_erased_class(&io::Error::from_raw_os_error(libc::EIO).to_string()),
        None
    );
}

#[test]
fn unrar_0_11_erased_header_damage_is_recoverable() {
    use unrar_rs::RarError;
    let damaged = [
        RarError::InvalidSignature,
        RarError::TruncatedHeader { offset: 7 },
        RarError::HeaderCrcMismatch {
            expected: 1,
            actual: 2,
        },
        RarError::DataCrcMismatch {
            member: "member.bin".into(),
            expected: 1,
            actual: 2,
        },
        RarError::PackedDataCrcMismatch {
            member: "member.bin".into(),
            volume: 1,
            expected: 1,
            actual: 2,
        },
    ];
    for error in damaged {
        assert_eq!(
            unrar_0_11_erased_class(&error.to_string()),
            Some(ExtractionErrorClass::Recoverable),
            "{error:?}"
        );
    }
    assert_eq!(
        unrar_0_11_erased_class(&RarError::TruncatedData { offset: 7 }.to_string()),
        None
    );
}

// Member index and data offset of the first compressed file in a RAR5
// archive.
fn rar5_first_compressed_file_data(archive: &[u8]) -> (usize, usize) {
    fn vint(bytes: &[u8], at: &mut usize) -> u64 {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }
    let mut at = 8;
    let mut index = 0;
    loop {
        let mut cursor = at + 4;
        let header_size = vint(archive, &mut cursor) as usize;
        let body = cursor;
        let header_type = vint(archive, &mut cursor);
        let flags = vint(archive, &mut cursor);
        if flags & 0x01 != 0 {
            vint(archive, &mut cursor);
        }
        let data_size = if flags & 0x02 != 0 {
            vint(archive, &mut cursor) as usize
        } else {
            0
        };
        let data = body + header_size;
        if header_type == 2 && data_size > 0 {
            let file_flags = vint(archive, &mut cursor);
            vint(archive, &mut cursor);
            vint(archive, &mut cursor);
            if file_flags & 0x02 != 0 {
                cursor += 4;
            }
            if file_flags & 0x04 != 0 {
                cursor += 4;
            }
            let method = (vint(archive, &mut cursor) >> 7) & 0x07;
            if method != 0 {
                return (index, data);
            }
            index += 1;
        }
        at = data + data_size;
    }
}

#[test]
fn unrar_0_11_block_header_checksum_mismatch_is_recognised() {
    let mut archive = std::fs::read(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar5/rar5_multifile_lz.rar"),
    )
    .unwrap();
    let (index, data) = rar5_first_compressed_file_data(&archive);
    // A compressed block opens with its flags and then its header checksum.
    archive[data + 1] ^= 0x01;
    let mut rar = unrar_rs::RarArchive::open(Cursor::new(archive)).unwrap();
    let error = rar
        .by_index(index)
        .and_then(|entry| entry.copy_to(&mut std::io::sink()))
        .expect_err("the block header fails its checksum");
    let unrar_rs::RarError::CorruptArchive { detail } = &error else {
        panic!("a corrupt archive: {error:?}");
    };
    assert!(unrar_0_11_block_header_checksum_mismatch(detail));
}

#[test]
fn sevenz_0_27_block_errors_are_read_from_their_rendering() {
    use sevenz_turbo::BlockErrorKind;
    let block = |kind, message: String| sevenz_turbo::Error::BlockDecode {
        block_index: 0,
        packed_offset: 32,
        kind,
        message,
    };
    // The message a block carries is the reader error's own rendering.
    let read_failure = |error: io::Error| {
        block(
            BlockErrorKind::Io,
            sevenz_turbo::Error::Io(error, "".into()).to_string(),
        )
    };
    let class = |error: &sevenz_turbo::Error| {
        let sevenz_turbo::Error::BlockDecode { kind, message, .. } = error else {
            unreachable!()
        };
        sevenz_0_27_block_error_class(*kind, message, &error.to_string())
    };
    let recoverable = [
        read_failure(io::ErrorKind::UnexpectedEof.into()),
        read_failure(io::Error::new(
            io::ErrorKind::InvalidData,
            "kind: NotFound quoted in the message",
        )),
        read_failure(io::Error::new(io::ErrorKind::UnexpectedEof, "short read")),
    ];
    for error in &recoverable {
        assert_eq!(class(error), ExtractionErrorClass::Recoverable, "{error}");
    }
    let terminal = [
        block(BlockErrorKind::UnsupportedMethod, "detail".to_string()),
        block(BlockErrorKind::Password, "detail".to_string()),
        block(BlockErrorKind::Io, "detail".to_string()),
        read_failure(io::Error::from_raw_os_error(libc::EIO)),
        read_failure(io::ErrorKind::PermissionDenied.into()),
        read_failure(io::Error::new(
            io::ErrorKind::StorageFull,
            "kind: InvalidData quoted in the message",
        )),
    ];
    for error in &terminal {
        assert_eq!(class(error), ExtractionErrorClass::Terminal, "{error}");
    }
    assert_eq!(
        class(&block(
            BlockErrorKind::Password,
            format!("{:?}", sevenz_turbo::Error::PasswordRequired)
        )),
        ExtractionErrorClass::PasswordRequired
    );
}

// The one `Error::Other` text sevenz-turbo 0.27.0 raises that names a
// checksum. It is raised only when a decoder ends a verified stream without
// reporting its checksum, which no input built here can provoke, so its text
// is copied from the crate.
#[test]
fn sevenz_0_27_other_checksum_text_is_recognised() {
    assert!(sevenz_0_27_other_names_checksum(
        "the decoder reported no checksum for bytes that were read to their end"
    ));
    assert!(!sevenz_0_27_other_names_checksum("unsupported archive"));
}
