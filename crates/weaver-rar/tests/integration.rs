//! Integration tests for weaver-rar decompression.
//!
//! These tests construct RAR5 archives programmatically and verify
//! that the parser + decompressor produce correct output.

use std::io::Cursor;

/// Build a minimal RAR5 archive containing a single stored file.
///
/// This constructs the binary format by hand:
/// - RAR5 signature (8 bytes)
/// - Main archive header (type 1)
/// - File header (type 2) with stored data
/// - End of archive header (type 5)
fn build_stored_rar5_archive(filename: &str, content: &[u8]) -> Vec<u8> {
    let mut archive = Vec::new();

    // RAR5 signature
    archive.extend_from_slice(&[0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00]);

    // Main archive header (type 1, no flags)
    // Type-specific body needs archive_flags vint (0 = no flags)
    let main_type_body = encode_vint(0); // archive_flags = 0
    let main_header = build_header(1, 0, &main_type_body, &[]);
    archive.extend_from_slice(&main_header);

    // File header (type 2)
    let file_header = build_file_header(filename, content);
    archive.extend_from_slice(&file_header);

    // Data area (the actual file content, uncompressed)
    archive.extend_from_slice(content);

    // End of archive header (type 5, no more volumes)
    let end_type_body = encode_vint(0); // end_flags = 0 (no more volumes)
    let end_header = build_header(5, 0, &end_type_body, &[]);
    archive.extend_from_slice(&end_header);

    archive
}

/// Build a raw header with the given type, common flags, and type-specific body.
/// `extra` is appended after `type_body` inside the header and counted via the extra area flag.
fn build_header(header_type: u64, common_flags: u64, type_body: &[u8], extra: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(header_type));

    let mut flags = common_flags;
    if !extra.is_empty() {
        flags |= 0x0001; // EXTRA_AREA
    }

    body.extend_from_slice(&encode_vint(flags));

    if !extra.is_empty() {
        body.extend_from_slice(&encode_vint(extra.len() as u64));
    }

    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_vint(header_size);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header_size_bytes);
    hasher.update(&body);
    let crc = hasher.finalize();

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

/// Build a file header (type 2) for a stored file with data area.
fn build_file_header(filename: &str, content: &[u8]) -> Vec<u8> {
    // Compute CRC32 of content
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let data_crc = hasher.finalize();

    // File-specific flags: TIME_PRESENT (0x0002) + CRC32_PRESENT (0x0004)
    let file_flags: u64 = 0x0004; // just CRC32

    // Build type-specific body
    let mut type_body = Vec::new();
    // file_flags
    type_body.extend_from_slice(&encode_vint(file_flags));
    // unpacked_size
    type_body.extend_from_slice(&encode_vint(content.len() as u64));
    // attributes (Unix 0644)
    type_body.extend_from_slice(&encode_vint(0o644));
    // data_crc32 (since CRC32_PRESENT flag is set)
    type_body.extend_from_slice(&data_crc.to_le_bytes());
    // compression_info: version=0, solid=false, method=0(store), dict_code=0(128KB)
    type_body.extend_from_slice(&encode_vint(0));
    // host_os: 1 = Unix
    type_body.extend_from_slice(&encode_vint(1));
    // name_length
    type_body.extend_from_slice(&encode_vint(filename.len() as u64));
    // name
    type_body.extend_from_slice(filename.as_bytes());

    // Common flags: DATA_AREA present (0x0002)
    let common_flags: u64 = 0x0002;
    let data_size = content.len() as u64;

    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(2)); // header type = File
    body.extend_from_slice(&encode_vint(common_flags));
    body.extend_from_slice(&encode_vint(data_size)); // data area size
    body.extend_from_slice(&type_body);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_vint(header_size);

    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(&header_size_bytes);
    crc_hasher.update(&body);
    let crc = crc_hasher.finalize();

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

/// Encode a u64 as a RAR5 vint.
fn encode_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

/// RAR5 signature bytes.
const RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

/// Build a main archive header for a multi-volume archive.
/// `archive_flags`: 0x0001 = VOLUME, 0x0002 = VOLUME_NUMBER, 0x0004 = SOLID
fn build_main_archive_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_vint(archive_flags));
    if let Some(vn) = volume_number {
        type_body.extend_from_slice(&encode_vint(vn));
    }
    build_header(1, 0, &type_body, &[])
}

/// Build a file header with configurable split flags and data size.
///
/// `common_flags_extra`: additional common header flags (e.g. SPLIT_BEFORE=0x0008, SPLIT_AFTER=0x0010)
/// `data_size`: the size of the data area that follows this header
/// `unpacked_size`: the full unpacked size of the file
/// `data_crc`: CRC32 of the full unpacked content (set on the header that has the CRC)
/// `comp_info_raw`: raw compression info vint value
fn build_file_header_ex(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
) -> Vec<u8> {
    build_file_header_ex_with_extra(
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        comp_info_raw,
        &[],
    )
}

/// Build a file header with configurable split flags, data size, and extra area.
fn build_file_header_ex_with_extra(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
    extra_area: &[u8],
) -> Vec<u8> {
    let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };

    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_vint(file_flags));
    type_body.extend_from_slice(&encode_vint(unpacked_size));
    type_body.extend_from_slice(&encode_vint(0o644)); // attributes
    if let Some(crc) = data_crc {
        type_body.extend_from_slice(&crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_vint(comp_info_raw));
    type_body.extend_from_slice(&encode_vint(1)); // host_os = Unix
    type_body.extend_from_slice(&encode_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());

    // Common flags: DATA_AREA (0x0002) + EXTRA_AREA if extra present + any extra flags
    let mut common_flags: u64 = 0x0002 | common_flags_extra;
    if !extra_area.is_empty() {
        common_flags |= 0x0001; // EXTRA_AREA
    }

    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(2)); // header type = File
    body.extend_from_slice(&encode_vint(common_flags));
    if !extra_area.is_empty() {
        body.extend_from_slice(&encode_vint(extra_area.len() as u64));
    }
    body.extend_from_slice(&encode_vint(data_size));
    body.extend_from_slice(&type_body);
    body.extend_from_slice(extra_area);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_vint(header_size);

    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(&header_size_bytes);
    crc_hasher.update(&body);
    let crc = crc_hasher.finalize();

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

/// Build a single extra area record: size(vint) + type(vint) + body.
fn build_extra_record(record_type: u64, body: &[u8]) -> Vec<u8> {
    let type_bytes = encode_vint(record_type);
    let record_size = type_bytes.len() + body.len();
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(record_size as u64));
    data.extend_from_slice(&type_bytes);
    data.extend_from_slice(body);
    data
}

/// Build an end-of-archive header.
/// `more_volumes`: if true, sets the MORE_VOLUMES flag (0x0001).
fn build_end_header(more_volumes: bool) -> Vec<u8> {
    let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
    let type_body = encode_vint(end_flags);
    build_header(5, 0, &type_body, &[])
}

/// Build a 2-volume stored archive where a single file is split across volumes.
///
/// Returns (volume0_bytes, volume1_bytes, full_content).
fn build_two_volume_stored_archive(
    filename: &str,
    content: &[u8],
    split_at: usize,
) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let part1 = &content[..split_at];
    let part2 = &content[split_at..];

    // Compute CRC of the full unpacked content.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let full_crc = hasher.finalize();

    // --- Volume 0 ---
    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&RAR5_SIG);
    // Main archive header: VOLUME flag (0x0001), no volume number field (first volume)
    vol0.extend_from_slice(&build_main_archive_header(0x0001, None));
    // File header: SPLIT_AFTER (0x0010), data_size = part1.len()
    // CRC is on the final segment, not here.
    vol0.extend_from_slice(&build_file_header_ex(
        filename,
        0x0010, // SPLIT_AFTER
        part1.len() as u64,
        content.len() as u64,
        None, // CRC on last segment
        0,    // store, version 0, dict 128KB
    ));
    vol0.extend_from_slice(part1);
    // End of archive: more volumes follow
    vol0.extend_from_slice(&build_end_header(true));

    // --- Volume 1 ---
    let mut vol1 = Vec::new();
    vol1.extend_from_slice(&RAR5_SIG);
    // Main archive header: VOLUME (0x0001) + VOLUME_NUMBER (0x0002), volume_number=1
    vol1.extend_from_slice(&build_main_archive_header(0x0001 | 0x0002, Some(1)));
    // File header: SPLIT_BEFORE (0x0008), data_size = part2.len()
    vol1.extend_from_slice(&build_file_header_ex(
        filename,
        0x0008, // SPLIT_BEFORE
        part2.len() as u64,
        content.len() as u64,
        Some(full_crc),
        0, // store
    ));
    vol1.extend_from_slice(part2);
    // End of archive: no more volumes
    vol1.extend_from_slice(&build_end_header(false));

    (vol0, vol1, content.to_vec())
}

/// Build a solid archive with two stored files.
/// In a solid archive, the main header has the SOLID flag (0x0004),
/// and file headers after the first have the solid bit set in compression info.
///
/// For stored files, the solid flag doesn't affect decompression (there's no
/// LZ dictionary to carry over), but we still test that the code handles it.
fn build_solid_stored_archive(
    file1_name: &str,
    file1_content: &[u8],
    file2_name: &str,
    file2_content: &[u8],
) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&RAR5_SIG);

    // Main archive header: SOLID (0x0004)
    archive.extend_from_slice(&build_main_archive_header(0x0004, None));

    // File 1: not solid (first file in solid archive is not solid)
    let mut hasher1 = crc32fast::Hasher::new();
    hasher1.update(file1_content);
    let crc1 = hasher1.finalize();
    archive.extend_from_slice(&build_file_header_ex(
        file1_name,
        0, // no split flags
        file1_content.len() as u64,
        file1_content.len() as u64,
        Some(crc1),
        0, // store, not solid
    ));
    archive.extend_from_slice(file1_content);

    // File 2: solid flag set (bit 6 of compression info = 0x40)
    let mut hasher2 = crc32fast::Hasher::new();
    hasher2.update(file2_content);
    let crc2 = hasher2.finalize();
    archive.extend_from_slice(&build_file_header_ex(
        file2_name,
        0, // no split flags
        file2_content.len() as u64,
        file2_content.len() as u64,
        Some(crc2),
        0x40, // store + solid bit
    ));
    archive.extend_from_slice(file2_content);

    // End of archive
    archive.extend_from_slice(&build_end_header(false));

    archive
}

#[test]
fn test_open_and_list_stored_archive() {
    let content = b"Hello from a stored RAR5 archive!";
    let archive_bytes = build_stored_rar5_archive("hello.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt"]);

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    assert_eq!(meta.members[0].name, "hello.txt");
    assert_eq!(
        meta.members[0].compression.method,
        weaver_rar::CompressionMethod::Store
    );
    assert_eq!(meta.members[0].unpacked_size, Some(content.len() as u64));
}

#[test]
fn test_extract_stored_member() {
    let content = b"Extract me from RAR5!";
    let archive_bytes = build_stored_rar5_archive("test.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_rar::ExtractOptions::default(), None)
        .unwrap();

    assert_eq!(result, content);
}

#[test]
fn test_extract_stored_by_name() {
    let content = b"Named extraction test";
    let archive_bytes = build_stored_rar5_archive("named.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_by_name("named.txt", &weaver_rar::ExtractOptions::default(), None)
        .unwrap();

    assert_eq!(result, content);
}

#[test]
fn test_extract_stored_crc_verification() {
    let content = b"CRC verification test data";
    let archive_bytes = build_stored_rar5_archive("crc.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    // Should succeed with verification enabled (default)
    let result = archive
        .extract_member(
            0,
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_extract_stored_empty_file() {
    let content = b"";
    let archive_bytes = build_stored_rar5_archive("empty.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_rar::ExtractOptions::default(), None)
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_extract_stored_large_file() {
    // 256KB of pattern data
    let content: Vec<u8> = (0..=255u8).cycle().take(256 * 1024).collect();
    let archive_bytes = build_stored_rar5_archive("large.bin", &content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_rar::ExtractOptions::default(), None)
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_member_not_found() {
    let content = b"test";
    let archive_bytes = build_stored_rar5_archive("test.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let result = archive.extract_by_name(
        "nonexistent.txt",
        &weaver_rar::ExtractOptions::default(),
        None,
    );
    assert!(result.is_err());
}

// =============================================================================
// Multi-volume tests
// =============================================================================

#[test]
fn test_multivolume_open_volumes() {
    let content = b"Hello, this is data that spans two volumes!";
    let (vol0, vol1, _full) = build_two_volume_stored_archive("movie.mkv", content, 20);

    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_rar::RarArchive::open_volumes(readers).unwrap();

    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "movie.mkv");

    let result = archive
        .extract_by_name(
            "movie.mkv",
            &weaver_rar::ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_multivolume_extract_with_crc() {
    let content = b"CRC-verified multi-volume content spanning two volumes here";
    let (vol0, vol1, _full) = build_two_volume_stored_archive("data.bin", content, 30);

    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_rar::RarArchive::open_volumes(readers).unwrap();

    let result = archive
        .extract_by_name(
            "data.bin",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_multivolume_incremental_add() {
    let content = b"Incremental volume addition test data here!";
    let (vol0, vol1, _full) = build_two_volume_stored_archive("file.dat", content, 25);

    // Open first volume only.
    let mut archive = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();

    // Should not be extractable yet (missing volume 1).
    assert!(!archive.is_extractable("file.dat"));
    assert_eq!(archive.missing_volumes("file.dat"), vec![1]);

    // More volumes should be indicated.
    assert!(archive.more_volumes());

    // Add second volume.
    archive.add_volume(1, Box::new(Cursor::new(vol1))).unwrap();

    // Now should be extractable.
    assert!(archive.is_extractable("file.dat"));
    assert!(archive.missing_volumes("file.dat").is_empty());

    let result = archive
        .extract_by_name(
            "file.dat",
            &weaver_rar::ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_multivolume_is_extractable_missing() {
    let content = b"Test content for extractability check";
    let (vol0, _vol1, _full) = build_two_volume_stored_archive("split.bin", content, 15);

    let archive = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();

    // Only volume 0 present, file needs volume 1 too.
    assert!(!archive.is_extractable("split.bin"));

    // Non-existent member.
    assert!(!archive.is_extractable("nonexistent.dat"));
}

#[test]
fn test_multivolume_missing_volumes_query() {
    let content = b"Query missing volumes for this file";
    let (vol0, _vol1, _full) = build_two_volume_stored_archive("query.dat", content, 10);

    let archive = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();

    let missing = archive.missing_volumes("query.dat");
    assert_eq!(missing, vec![1]);

    // Non-existent member returns empty.
    let missing2 = archive.missing_volumes("nope.txt");
    assert!(missing2.is_empty());
}

#[test]
fn test_multivolume_volume_continuation_flags() {
    let content = b"Checking continuation flags in multi-volume archives";
    let (vol0, vol1, _) = build_two_volume_stored_archive("flags.dat", content, 25);

    // Parse volume 0 — it should indicate more volumes.
    let archive0 = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();
    assert!(archive0.more_volumes());

    // Parse volume 1 — it should NOT indicate more volumes.
    let archive1 = weaver_rar::RarArchive::open(Cursor::new(vol1)).unwrap();
    assert!(!archive1.more_volumes());
}

// =============================================================================
// Solid archive tests
// =============================================================================

#[test]
fn test_solid_archive_stored_files() {
    let file1_content = b"First file in solid archive";
    let file2_content = b"Second file in solid archive";

    let archive_bytes =
        build_solid_stored_archive("first.txt", file1_content, "second.txt", file2_content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_rar::RarArchive::open(cursor).unwrap();

    assert!(archive.is_solid());

    let names = archive.member_names();
    assert_eq!(names, vec!["first.txt", "second.txt"]);

    // Extract both in order.
    let result1 = archive
        .extract_by_name(
            "first.txt",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result1, file1_content);

    let result2 = archive
        .extract_by_name(
            "second.txt",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result2, file2_content);
}

#[test]
fn test_solid_archive_metadata() {
    let archive_bytes = build_solid_stored_archive("a.txt", b"AAA", "b.txt", b"BBB");

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert!(meta.is_solid);
    assert_eq!(meta.members.len(), 2);
    assert_eq!(meta.members[0].name, "a.txt");
    assert_eq!(meta.members[1].name, "b.txt");
}

// =============================================================================
// RAR4 multi-volume tests
// =============================================================================

/// RAR4 signature bytes.
const RAR4_SIG: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];

/// Build a RAR4 archive header.
/// `arch_flags`: 0x0001 = VOLUME, 0x0008 = SOLID, 0x0100 = FIRST_VOLUME, etc.
fn build_rar4_archive_header(arch_flags: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    let header_size: u16 = 7 + 6; // common 7 + reserved 6
    buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
    buf.push(0x73); // type: Archive
    buf.extend_from_slice(&arch_flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf.extend_from_slice(&[0u8; 6]); // reserved
    buf
}

/// Build a RAR4 file header with configurable split flags.
/// Returns the header bytes (no data area included — caller appends data).
fn build_rar4_file_header(
    filename: &str,
    packed_size: u32,
    unpacked_size: u32,
    crc32: u32,
    file_flags_extra: u16,
) -> Vec<u8> {
    let name_bytes = filename.as_bytes();
    let header_size: u16 = 7 + 25 + name_bytes.len() as u16;
    // HAS_DATA (0x8000) + any extra flags (SPLIT_BEFORE, SPLIT_AFTER, etc.)
    let file_flags: u16 = 0x8000 | file_flags_extra;

    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
    buf.push(0x74); // type: File
    buf.extend_from_slice(&file_flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf.extend_from_slice(&packed_size.to_le_bytes());
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(3); // Unix
    buf.extend_from_slice(&crc32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
    buf.push(29); // version
    buf.push(0x30); // method: Store
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // attrs
    buf.extend_from_slice(name_bytes);
    buf
}

/// Build a RAR4 end-of-archive header.
/// `more_volumes`: if true, sets flag 0x0001.
fn build_rar4_end_header(more_volumes: bool) -> Vec<u8> {
    let flags: u16 = if more_volumes { 0x0001 } else { 0x0000 };
    let header_size: u16 = 7;
    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
    buf.push(0x7B); // type: EndArchive
    buf.extend_from_slice(&flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf
}

/// Build a 2-volume RAR4 stored archive where a single file is split across volumes.
fn build_two_volume_rar4_stored_archive(
    filename: &str,
    content: &[u8],
    split_at: usize,
) -> (Vec<u8>, Vec<u8>) {
    let part1 = &content[..split_at];
    let part2 = &content[split_at..];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let full_crc = hasher.finalize();

    // --- Volume 0 ---
    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&RAR4_SIG);
    // Archive header: VOLUME (0x0001) + FIRST_VOLUME (0x0100)
    vol0.extend_from_slice(&build_rar4_archive_header(0x0001 | 0x0100));
    // File header: SPLIT_AFTER (0x0002)
    vol0.extend_from_slice(&build_rar4_file_header(
        filename,
        part1.len() as u32,
        content.len() as u32,
        full_crc,
        0x0002, // SPLIT_AFTER
    ));
    vol0.extend_from_slice(part1);
    vol0.extend_from_slice(&build_rar4_end_header(true));

    // --- Volume 1 ---
    let mut vol1 = Vec::new();
    vol1.extend_from_slice(&RAR4_SIG);
    // Archive header: VOLUME (0x0001), no FIRST_VOLUME
    vol1.extend_from_slice(&build_rar4_archive_header(0x0001));
    // File header: SPLIT_BEFORE (0x0001)
    vol1.extend_from_slice(&build_rar4_file_header(
        filename,
        part2.len() as u32,
        content.len() as u32,
        full_crc,
        0x0001, // SPLIT_BEFORE
    ));
    vol1.extend_from_slice(part2);
    vol1.extend_from_slice(&build_rar4_end_header(false));

    (vol0, vol1)
}

#[test]
fn test_rar4_multivolume_open_volumes() {
    let content = b"RAR4 multi-volume stored file content spanning two volumes!";
    let (vol0, vol1) = build_two_volume_rar4_stored_archive("movie.avi", content, 30);

    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_rar::RarArchive::open_volumes(readers).unwrap();

    assert_eq!(archive.format(), weaver_rar::ArchiveFormat::Rar4);

    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "movie.avi");

    let result = archive
        .extract_by_name(
            "movie.avi",
            &weaver_rar::ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_multivolume_incremental_add() {
    let content = b"Incremental RAR4 volume addition test data here!";
    let (vol0, vol1) = build_two_volume_rar4_stored_archive("file.dat", content, 25);

    // Open first volume only.
    let mut archive = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();
    assert_eq!(archive.format(), weaver_rar::ArchiveFormat::Rar4);

    // Should not be extractable yet.
    assert!(!archive.is_extractable("file.dat"));
    assert!(archive.more_volumes());

    // Add second volume.
    archive.add_volume(1, Box::new(Cursor::new(vol1))).unwrap();

    // Now extractable.
    assert!(archive.is_extractable("file.dat"));

    let result = archive
        .extract_by_name(
            "file.dat",
            &weaver_rar::ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_multivolume_crc_verification() {
    let content = b"CRC verified RAR4 multi-volume content here for testing";
    let (vol0, vol1) = build_two_volume_rar4_stored_archive("data.bin", content, 28);

    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_rar::RarArchive::open_volumes(readers).unwrap();

    let result = archive
        .extract_by_name(
            "data.bin",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_multivolume_missing_volumes_query() {
    let content = b"Query missing volumes for RAR4";
    let (vol0, _vol1) = build_two_volume_rar4_stored_archive("query.dat", content, 15);

    let archive = weaver_rar::RarArchive::open(Cursor::new(vol0)).unwrap();

    assert!(!archive.is_extractable("query.dat"));
    let missing = archive.missing_volumes("query.dat");
    assert_eq!(missing, vec![1]);
}

#[test]
fn test_rar4_single_volume_stored() {
    // Verify single-volume RAR4 still works (regression test).
    let content = b"Single volume RAR4 stored file";

    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0)); // no volume flag

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let crc = hasher.finalize();

    vol.extend_from_slice(&build_rar4_file_header(
        "single.txt",
        content.len() as u32,
        content.len() as u32,
        crc,
        0, // no split flags
    ));
    vol.extend_from_slice(content);
    vol.extend_from_slice(&build_rar4_end_header(false));

    let mut archive = weaver_rar::RarArchive::open(Cursor::new(vol)).unwrap();
    assert_eq!(archive.format(), weaver_rar::ArchiveFormat::Rar4);
    assert!(!archive.more_volumes());

    let result = archive
        .extract_by_name(
            "single.txt",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

/// Build a RAR4 file header with encryption (ENCRYPTED + SALT flags).
/// The salt is appended after the filename in the header.
fn build_rar4_encrypted_file_header(
    filename: &str,
    packed_size: u32,
    unpacked_size: u32,
    crc32: u32,
    salt: &[u8; 8],
    file_flags_extra: u16,
) -> Vec<u8> {
    let name_bytes = filename.as_bytes();
    // Header includes salt (8 bytes) after the filename.
    let header_size: u16 = 7 + 25 + name_bytes.len() as u16 + 8;
    // HAS_DATA (0x8000) + ENCRYPTED (0x0004) + SALT (0x0400) + extra flags
    let file_flags: u16 = 0x8000 | 0x0004 | 0x0400 | file_flags_extra;

    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
    buf.push(0x74); // type: File
    buf.extend_from_slice(&file_flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf.extend_from_slice(&packed_size.to_le_bytes());
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(3); // Unix
    buf.extend_from_slice(&crc32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
    buf.push(29); // version
    buf.push(0x30); // method: Store
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // attrs
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(salt);
    buf
}

#[test]
fn test_rar4_encrypted_stored_file() {
    use aes::Aes128;
    use cbc::cipher::{BlockEncryptMut, KeyIvInit};

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    let password = "secret123";
    let salt: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let content = b"RAR4 encrypted stored content!!"; // 31 bytes

    // Compute CRC of plaintext.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let crc = hasher.finalize();

    // Derive key/IV and encrypt.
    // We call the crypto module directly to build ciphertext.
    let (key, iv) = weaver_rar::crypto::rar4_derive_key(password, &salt);

    // Pad content to AES block boundary (16 bytes).
    let padded_len = (content.len() + 15) & !15; // round up to 32
    let mut plaintext_padded = vec![0u8; padded_len];
    plaintext_padded[..content.len()].copy_from_slice(content);

    let mut ciphertext = plaintext_padded.clone();
    let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
    encryptor
        .encrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, padded_len)
        .unwrap();

    // Build the archive.
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_encrypted_file_header(
        "secret.txt",
        ciphertext.len() as u32,
        content.len() as u32,
        crc,
        &salt,
        0, // no split flags
    ));
    vol.extend_from_slice(&ciphertext);
    vol.extend_from_slice(&build_rar4_end_header(false));

    // Open and extract with password.
    let mut archive =
        weaver_rar::RarArchive::open_with_password(Cursor::new(vol), password).unwrap();

    assert_eq!(archive.format(), weaver_rar::ArchiveFormat::Rar4);

    let meta = archive.metadata();
    assert!(meta.members[0].is_encrypted);

    let result = archive
        .extract_by_name(
            "secret.txt",
            &weaver_rar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    // The decrypted data is padded to block boundary; unpacked_size truncates it.
    assert_eq!(result, content);
}

#[test]
fn test_rar4_encrypted_no_password_fails() {
    use aes::Aes128;
    use cbc::cipher::{BlockEncryptMut, KeyIvInit};

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    let salt: [u8; 8] = [0xAA; 8];
    let content = b"needs a password";
    let (key, iv) = weaver_rar::crypto::rar4_derive_key("pass", &salt);

    let padded_len = (content.len() + 15) & !15;
    let mut ciphertext = vec![0u8; padded_len];
    ciphertext[..content.len()].copy_from_slice(content);
    let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
    encryptor
        .encrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, padded_len)
        .unwrap();

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let crc = hasher.finalize();

    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_encrypted_file_header(
        "locked.txt",
        ciphertext.len() as u32,
        content.len() as u32,
        crc,
        &salt,
        0,
    ));
    vol.extend_from_slice(&ciphertext);
    vol.extend_from_slice(&build_rar4_end_header(false));

    // Open without password — extraction should fail with EncryptedMember.
    let mut archive = weaver_rar::RarArchive::open(Cursor::new(vol)).unwrap();
    let result =
        archive.extract_by_name("locked.txt", &weaver_rar::ExtractOptions::default(), None);
    assert!(result.is_err());
}

// =============================================================================
// Extra record propagation tests
// =============================================================================

/// Build a stored archive with a single file that has extra area records.
fn build_stored_archive_with_extra(filename: &str, content: &[u8], extra_area: &[u8]) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&RAR5_SIG);

    // Main archive header
    archive.extend_from_slice(&build_main_archive_header(0, None));

    // Compute CRC
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let data_crc = hasher.finalize();

    // File header with extra area
    archive.extend_from_slice(&build_file_header_ex_with_extra(
        filename,
        0, // no split flags
        content.len() as u64,
        content.len() as u64,
        Some(data_crc),
        0, // store
        extra_area,
    ));
    archive.extend_from_slice(content);

    // End of archive
    archive.extend_from_slice(&build_end_header(false));
    archive
}

#[test]
fn test_file_encryption_propagated_to_member_info() {
    // Extra record type 0x01 = FILE_ENCRYPTION
    let extra = build_extra_record(0x01, &[0; 10]);
    let archive_bytes = build_stored_archive_with_extra("secret.txt", b"encrypted", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    assert!(
        meta.members[0].is_encrypted,
        "is_encrypted should be true when FILE_ENCRYPTION extra record is present"
    );
}

#[test]
fn test_blake2_hash_propagated_to_member_info() {
    // Extra record type 0x02 = FILE_HASH
    // Body: hash_type(vint=0 for BLAKE2sp) + 32-byte hash
    let mut hash_body = Vec::new();
    hash_body.extend_from_slice(&encode_vint(0)); // BLAKE2sp
    let expected_hash = [0xAB; 32];
    hash_body.extend_from_slice(&expected_hash);
    let extra = build_extra_record(0x02, &hash_body);

    let archive_bytes = build_stored_archive_with_extra("hashed.bin", b"data", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    match &meta.members[0].hash {
        Some(weaver_rar::FileHash::Blake2sp(hash)) => {
            assert_eq!(hash, &expected_hash, "BLAKE2sp hash should match");
        }
        other => panic!("expected Some(Blake2sp(...)), got {other:?}"),
    }
}

#[test]
fn test_symlink_propagated_to_member_info() {
    // Extra record type 0x05 = REDIRECTION
    // Body: redir_type(vint) + flags(vint) + name_length(vint) + name
    let target = "/usr/bin/target";
    let mut redir_body = Vec::new();
    redir_body.extend_from_slice(&encode_vint(1)); // UnixSymlink
    redir_body.extend_from_slice(&encode_vint(0)); // flags
    redir_body.extend_from_slice(&encode_vint(target.len() as u64));
    redir_body.extend_from_slice(target.as_bytes());
    let extra = build_extra_record(0x05, &redir_body);

    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    let member = &meta.members[0];
    assert!(
        member.is_symlink,
        "is_symlink should be true for UnixSymlink redirection"
    );
    assert!(
        !member.is_hardlink,
        "is_hardlink should be false for UnixSymlink"
    );
    assert_eq!(
        member.link_target.as_deref(),
        Some(target),
        "link_target should contain the symlink target path"
    );
}

#[test]
fn test_hardlink_propagated_to_member_info() {
    let target = "original.txt";
    let mut redir_body = Vec::new();
    redir_body.extend_from_slice(&encode_vint(4)); // Hardlink
    redir_body.extend_from_slice(&encode_vint(0)); // flags
    redir_body.extend_from_slice(&encode_vint(target.len() as u64));
    redir_body.extend_from_slice(target.as_bytes());
    let extra = build_extra_record(0x05, &redir_body);

    let archive_bytes = build_stored_archive_with_extra("hardlink", b"", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    let member = &meta.members[0];
    assert!(
        !member.is_symlink,
        "is_symlink should be false for Hardlink"
    );
    assert!(
        member.is_hardlink,
        "is_hardlink should be true for Hardlink redirection"
    );
    assert_eq!(member.link_target.as_deref(), Some(target));
}

#[test]
fn test_no_extra_records_defaults() {
    // A normal file without extra records should have default values.
    let content = b"plain file";
    let archive_bytes = build_stored_rar5_archive("plain.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_rar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    let member = &meta.members[0];
    assert!(!member.is_encrypted, "is_encrypted should default to false");
    assert!(member.hash.is_none(), "hash should default to None");
    assert!(!member.is_symlink, "is_symlink should default to false");
    assert!(!member.is_hardlink, "is_hardlink should default to false");
    assert!(
        member.link_target.is_none(),
        "link_target should default to None"
    );
}

// =============================================================================
// Real archive fixture tests (generated by rar 6.24 and 7.20)
//
// Encrypted tests are gated behind `slow-tests` because RAR uses kdf_count=15
// (1 billion PBKDF2 iterations, ~90s per key derivation in release mode).
//
//     cargo test -p weaver-rar --release --features slow-tests
//
// Unencrypted tests run normally.
// =============================================================================

const TEST_PASSWORD: &str = "testpass123";

fn fixture(dir: &str, name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir)
        .join(name)
}

fn original(name: &str) -> Vec<u8> {
    std::fs::read(fixture("originals", name)).unwrap()
}

fn open_single(dir: &str, filename: &str) -> weaver_rar::RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    weaver_rar::RarArchive::open(Cursor::new(data)).unwrap()
}

fn open_multi(dir: &str, filenames: &[&str]) -> weaver_rar::RarArchive {
    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> = filenames
        .iter()
        .map(|f| {
            let data = std::fs::read(fixture(dir, f)).unwrap();
            Box::new(Cursor::new(data)) as Box<dyn weaver_rar::ReadSeek>
        })
        .collect();
    weaver_rar::RarArchive::open_volumes(readers).unwrap()
}

// -- RAR5 unencrypted (fast) --------------------------------------------------

#[test]
fn test_rar5_fixture_store_batch() {
    let mut archive = open_single("rar5", "rar5_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
fn test_rar5_fixture_store_streaming() {
    let mut archive = open_single("rar5", "rar5_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let provider =
        weaver_rar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_store.rar")]);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("small.txt"));
}

#[test]
fn test_rar5_fixture_lz_batch() {
    let mut archive = open_single("rar5", "rar5_lz.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
fn test_rar5_fixture_lz_streaming() {
    let mut archive = open_single("rar5", "rar5_lz.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let provider =
        weaver_rar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_lz.rar")]);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("compressible.txt"));
}

#[test]
fn test_rar5_fixture_multivolume_store_batch() {
    let vols: Vec<&str> = (1..=5)
        .map(|i| match i {
            1 => "rar5_mv_store.part1.rar",
            2 => "rar5_mv_store.part2.rar",
            3 => "rar5_mv_store.part3.rar",
            4 => "rar5_mv_store.part4.rar",
            5 => "rar5_mv_store.part5.rar",
            _ => unreachable!(),
        })
        .collect();
    let mut archive = open_multi("rar5", &vols);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("binary.bin"));
}

#[test]
fn test_rar5_fixture_multivolume_store_streaming() {
    let vol_names = [
        "rar5_mv_store.part1.rar",
        "rar5_mv_store.part2.rar",
        "rar5_mv_store.part3.rar",
        "rar5_mv_store.part4.rar",
        "rar5_mv_store.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("binary.bin"));
}

// -- RAR4 unencrypted (fast) --------------------------------------------------

#[test]
fn test_rar4_fixture_store_batch() {
    let mut archive = open_single("rar4", "rar4_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
fn test_rar4_fixture_lz_batch() {
    let mut archive = open_single("rar4", "rar4_lz.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
fn test_rar4_fixture_multivolume_store_batch() {
    let vols: Vec<&str> = (1..=5)
        .map(|i| match i {
            1 => "rar4_mv_store.part1.rar",
            2 => "rar4_mv_store.part2.rar",
            3 => "rar4_mv_store.part3.rar",
            4 => "rar4_mv_store.part4.rar",
            5 => "rar4_mv_store.part5.rar",
            _ => unreachable!(),
        })
        .collect();
    let mut archive = open_multi("rar4", &vols);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("binary.bin"));
}

// -- Video multi-volume tests (MKV, ~1.1MB across 5 volumes) ------------------

#[test]
fn test_rar5_fixture_multivolume_video_batch() {
    let vols: Vec<&str> = (1..=5)
        .map(|i| match i {
            1 => "rar5_mv_video.part1.rar",
            2 => "rar5_mv_video.part2.rar",
            3 => "rar5_mv_video.part3.rar",
            4 => "rar5_mv_video.part4.rar",
            5 => "rar5_mv_video.part5.rar",
            _ => unreachable!(),
        })
        .collect();
    let mut archive = open_multi("rar5", &vols);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("test_clip.mkv"));
}

#[test]
fn test_rar5_fixture_multivolume_video_streaming() {
    let vol_names = [
        "rar5_mv_video.part1.rar",
        "rar5_mv_video.part2.rar",
        "rar5_mv_video.part3.rar",
        "rar5_mv_video.part4.rar",
        "rar5_mv_video.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("test_clip.mkv"));
}

#[test]
fn test_rar4_fixture_multivolume_video_batch() {
    let vols: Vec<&str> = (1..=5)
        .map(|i| match i {
            1 => "rar4_mv_video.part1.rar",
            2 => "rar4_mv_video.part2.rar",
            3 => "rar4_mv_video.part3.rar",
            4 => "rar4_mv_video.part4.rar",
            5 => "rar4_mv_video.part5.rar",
            _ => unreachable!(),
        })
        .collect();
    let mut archive = open_multi("rar4", &vols);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("test_clip.mkv"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_multivolume_video_batch() {
    let vol_names = [
        "rar5_enc_mv_video.part1.rar",
        "rar5_enc_mv_video.part2.rar",
        "rar5_enc_mv_video.part3.rar",
        "rar5_enc_mv_video.part4.rar",
        "rar5_enc_mv_video.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("test_clip.mkv"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_multivolume_video_streaming() {
    let vol_names = [
        "rar5_enc_mv_video.part1.rar",
        "rar5_enc_mv_video.part2.rar",
        "rar5_enc_mv_video.part3.rar",
        "rar5_enc_mv_video.part4.rar",
        "rar5_enc_mv_video.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("test_clip.mkv"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_multivolume_video_batch() {
    let vol_names = [
        "rar4_enc_mv_video.part1.rar",
        "rar4_enc_mv_video.part2.rar",
        "rar4_enc_mv_video.part3.rar",
        "rar4_enc_mv_video.part4.rar",
        "rar4_enc_mv_video.part5.rar",
    ];
    let mut archive = open_multi("rar4", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("test_clip.mkv"));
}

// -- RAR5 encrypted (slow — gated behind `slow-tests` feature) ----------------

#[test]
fn test_rar5_encrypted_no_password_fails() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None);
    assert!(
        result.is_err(),
        "no password should fail for encrypted archive"
    );
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_store_batch() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_store_streaming() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let provider =
        weaver_rar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_enc_store.rar")]);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("small.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_lz_batch() {
    let mut archive = open_single("rar5", "rar5_enc_lz.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_lz_streaming() {
    let mut archive = open_single("rar5", "rar5_enc_lz.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let provider =
        weaver_rar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_enc_lz.rar")]);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("compressible.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_multivolume_store_batch() {
    let vol_names = [
        "rar5_enc_mv_store.part1.rar",
        "rar5_enc_mv_store.part2.rar",
        "rar5_enc_mv_store.part3.rar",
        "rar5_enc_mv_store.part4.rar",
        "rar5_enc_mv_store.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("binary.bin"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_multivolume_store_streaming() {
    let vol_names = [
        "rar5_enc_mv_store.part1.rar",
        "rar5_enc_mv_store.part2.rar",
        "rar5_enc_mv_store.part3.rar",
        "rar5_enc_mv_store.part4.rar",
        "rar5_enc_mv_store.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("binary.bin"));
}

#[test]
fn test_cached_headers_resume_encrypted_out_of_order_multivolume_topology() {
    let first = std::fs::File::open(fixture("rar5", "rar5_enc_mv_store.part1.rar")).unwrap();
    let archive = weaver_rar::RarArchive::open_with_password(first, TEST_PASSWORD).unwrap();
    let headers = archive.serialize_headers();

    let mut restored = weaver_rar::RarArchive::deserialize_headers_with_password(
        &headers,
        Some(TEST_PASSWORD.to_string()),
    )
    .unwrap();

    restored
        .add_volume(
            2,
            Box::new(std::fs::File::open(fixture("rar5", "rar5_enc_mv_store.part3.rar")).unwrap()),
        )
        .unwrap();

    let pending = restored
        .topology_members()
        .into_iter()
        .find(|member| member.missing_start)
        .expect("out-of-order continuation should stay in cached topology");
    assert_eq!(pending.volumes.first_volume, 2);
    assert_eq!(pending.volumes.last_volume, 2);

    for (volume, name) in [
        (1usize, "rar5_enc_mv_store.part2.rar"),
        (4usize, "rar5_enc_mv_store.part5.rar"),
        (3usize, "rar5_enc_mv_store.part4.rar"),
    ] {
        restored
            .add_volume(
                volume,
                Box::new(std::fs::File::open(fixture("rar5", name)).unwrap()),
            )
            .unwrap();
    }

    let member = restored
        .topology_members()
        .into_iter()
        .find(|member| member.name == "binary.bin" && !member.missing_start)
        .expect("resolved member should be present after all volumes arrive");
    assert_eq!(member.volumes.first_volume, 0);
    assert_eq!(member.volumes.last_volume, 4);
    assert_eq!(restored.metadata().volume_count, Some(5));
}

#[test]
fn test_cached_headers_reverse_volume_arrival_merges_without_panicking() {
    let first = std::fs::File::open(fixture("rar5", "rar5_enc_mv_store.part1.rar")).unwrap();
    let archive = weaver_rar::RarArchive::open_with_password(first, TEST_PASSWORD).unwrap();
    let headers = archive.serialize_headers();

    let mut restored = weaver_rar::RarArchive::deserialize_headers_with_password(
        &headers,
        Some(TEST_PASSWORD.to_string()),
    )
    .unwrap();

    for (volume, name) in [
        (4usize, "rar5_enc_mv_store.part5.rar"),
        (3usize, "rar5_enc_mv_store.part4.rar"),
        (2usize, "rar5_enc_mv_store.part3.rar"),
        (1usize, "rar5_enc_mv_store.part2.rar"),
    ] {
        restored
            .add_volume(
                volume,
                Box::new(std::fs::File::open(fixture("rar5", name)).unwrap()),
            )
            .unwrap();
    }

    let member = restored
        .topology_members()
        .into_iter()
        .find(|member| member.name == "binary.bin" && !member.missing_start)
        .expect("resolved member should be present after reverse arrival");
    assert_eq!(member.volumes.first_volume, 0);
    assert_eq!(member.volumes.last_volume, 4);
    assert_eq!(restored.metadata().volume_count, Some(5));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_wrong_password_fails() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some("wrongpassword".into()),
    };
    let result = archive.extract_member(0, &opts, None);
    assert!(result.is_err(), "wrong password should fail");
}

// -- RAR4 encrypted (slow — gated behind `slow-tests` feature) ----------------

#[test]
fn test_rar4_encrypted_no_password_fails_fixture() {
    let mut archive = open_single("rar4", "rar4_enc_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None);
    assert!(
        result.is_err(),
        "no password should fail for encrypted archive"
    );
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_store_batch() {
    let mut archive = open_single("rar4", "rar4_enc_store.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_lz_batch() {
    let mut archive = open_single("rar4", "rar4_enc_lz.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_multivolume_store_batch() {
    let vol_names = [
        "rar4_enc_mv_store.part1.rar",
        "rar4_enc_mv_store.part2.rar",
        "rar4_enc_mv_store.part3.rar",
        "rar4_enc_mv_store.part4.rar",
        "rar4_enc_mv_store.part5.rar",
    ];
    let mut archive = open_multi("rar4", &vol_names);
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("binary.bin"));
}

// =============================================================================
// Edge-case fixtures — generated by generate_edge_cases.sh
// =============================================================================

// -- Multi-file archives (multiple members in one archive) --------------------

#[test]
fn test_rar5_multifile_store_list() {
    let archive = open_single("rar5", "rar5_multifile_store.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt", "second.txt", "random_512.bin"]);
}

#[test]
fn test_rar5_multifile_store_extract_all() {
    let mut archive = open_single("rar5", "rar5_multifile_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("hello.txt"));

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("second.txt"));

    let r2 = archive.extract_member(2, &opts, None).unwrap();
    assert_eq!(r2, original("random_512.bin"));
}

#[test]
fn test_rar5_multifile_store_extract_by_name() {
    let mut archive = open_single("rar5", "rar5_multifile_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_by_name("second.txt", &opts, None).unwrap();
    assert_eq!(result, original("second.txt"));
}

#[test]
fn test_rar4_multifile_store_list() {
    let archive = open_single("rar4", "rar4_multifile_store.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt", "second.txt", "random_512.bin"]);
}

#[test]
fn test_rar4_multifile_store_extract_all() {
    let mut archive = open_single("rar4", "rar4_multifile_store.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("hello.txt"));

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("second.txt"));

    let r2 = archive.extract_member(2, &opts, None).unwrap();
    assert_eq!(r2, original("random_512.bin"));
}

// -- Empty file members -------------------------------------------------------

#[test]
fn test_rar5_empty_member() {
    let mut archive = open_single("rar5", "rar5_empty_member.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let names = archive.member_names();
    assert_eq!(names, vec!["empty.txt", "hello.txt"]);

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert!(r0.is_empty(), "empty.txt should have 0 bytes");

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("hello.txt"));
}

#[test]
fn test_rar5_empty_member_info() {
    let archive = open_single("rar5", "rar5_empty_member.rar");
    let info = archive.member_info(0).unwrap();
    assert_eq!(info.unpacked_size, Some(0));
    assert!(!info.is_directory);
}

#[test]
fn test_rar4_empty_member() {
    let mut archive = open_single("rar4", "rar4_empty_member.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert!(r0.is_empty());

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("hello.txt"));
}

// -- Unicode filenames (RAR5 only — RAR4 Docker has locale issues) ------------

#[test]
fn test_rar5_unicode_filenames() {
    let archive = open_single("rar5", "rar5_unicode.rar");
    let names = archive.member_names();
    assert_eq!(names.len(), 2);
    // RAR5 stores filenames as UTF-8 internally
    assert!(
        names[0].contains("日本語"),
        "first name should contain Japanese: {}",
        names[0]
    );
    assert!(
        names[1].contains("café"),
        "second name should contain café: {}",
        names[1]
    );
}

#[test]
fn test_rar5_unicode_extract() {
    let mut archive = open_single("rar5", "rar5_unicode.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("日本語ファイル.txt"));

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("café-résumé.txt"));
}

// -- Directory entries --------------------------------------------------------

#[test]
fn test_rar5_dirs_list() {
    let archive = open_single("rar5", "rar5_dirs.rar");
    let names = archive.member_names();
    // Should contain files and a directory entry
    assert!(
        names.len() >= 2,
        "should have at least 2 file entries, got: {:?}",
        names
    );

    // Check for directory member
    let mut has_dir = false;
    for (i, _) in names.iter().enumerate() {
        let info = archive.member_info(i).unwrap();
        if info.is_directory {
            has_dir = true;
        }
    }
    assert!(has_dir, "should have at least one directory entry");
}

#[test]
fn test_rar5_dirs_extract_files() {
    let mut archive = open_single("rar5", "rar5_dirs.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    // Extract each non-directory member and verify content
    let names = archive
        .member_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    for (i, name) in names.iter().enumerate() {
        let info = archive.member_info(i).unwrap();
        if info.is_directory {
            continue;
        }
        let data = archive.extract_member(i, &opts, None).unwrap();
        if name.ends_with("a.txt") {
            assert_eq!(data, original("subdir_a.txt"));
        } else if name.ends_with("b.txt") {
            assert_eq!(data, original("subdir_nested_b.txt"));
        }
    }
}

#[test]
fn test_rar4_dirs_list() {
    let archive = open_single("rar4", "rar4_dirs.rar");
    let names = archive.member_names();
    assert!(
        names.len() >= 2,
        "should have at least 2 entries, got: {:?}",
        names
    );
}

// -- Solid archives -----------------------------------------------------------

#[test]
fn test_rar5_solid_metadata() {
    let archive = open_single("rar5", "rar5_solid.rar");
    assert!(archive.is_solid(), "archive should be marked solid");
    let names = archive.member_names();
    assert_eq!(names.len(), 4);
}

#[test]
fn test_rar5_solid_chunked_extraction_preserves_solid_continuation() {
    let fixture = fixture("rar5", "rar5_solid.rar");
    let options = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let mut expected_archive =
        weaver_rar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_first = expected_archive.extract_member(0, &options, None).unwrap();
    let expected_second = expected_archive.extract_member(1, &options, None).unwrap();

    let mut archive = weaver_rar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();

    let first_chunk_dir = temp_dir.path().join("first");
    let first_chunks = archive
        .extract_member_solid_chunked(0, &options, |volume_index| {
            let path = first_chunk_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_rar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_rar::RarError::Io)?;
            Ok(Box::new(file))
        })
        .unwrap();
    assert!(!first_chunks.is_empty());
    let mut actual_first = Vec::new();
    for (volume_index, bytes_written) in &first_chunks {
        let data = std::fs::read(first_chunk_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
        assert_eq!(data.len() as u64, *bytes_written);
        actual_first.extend_from_slice(&data);
    }
    assert_eq!(actual_first, expected_first);

    let second_chunk_dir = temp_dir.path().join("second");
    let second_chunks = archive
        .extract_member_solid_chunked(1, &options, |volume_index| {
            let path = second_chunk_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_rar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_rar::RarError::Io)?;
            Ok(Box::new(file))
        })
        .unwrap();
    assert!(!second_chunks.is_empty());
    let mut actual_second = Vec::new();
    for (volume_index, bytes_written) in &second_chunks {
        let data =
            std::fs::read(second_chunk_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
        assert_eq!(data.len() as u64, *bytes_written);
        actual_second.extend_from_slice(&data);
    }
    assert_eq!(actual_second, expected_second);
}

#[test]
fn test_rar4_solid_metadata() {
    let archive = open_single("rar4", "rar4_solid.rar");
    assert!(archive.is_solid(), "archive should be marked solid");
    let names = archive.member_names();
    assert_eq!(names.len(), 4);
}

// -- Recovery record archives -------------------------------------------------

#[test]
fn test_rar5_recovery_record_parses() {
    let archive = open_single("rar5", "rar5_recovery.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt", "second.txt"]);
}

#[test]
fn test_rar5_recovery_record_extract() {
    let mut archive = open_single("rar5", "rar5_recovery.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("hello.txt"));
    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("second.txt"));
}

#[test]
fn test_rar4_recovery_record_parses() {
    let archive = open_single("rar4", "rar4_recovery.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt", "second.txt"]);
}

#[test]
fn test_rar4_recovery_record_extract() {
    let mut archive = open_single("rar4", "rar4_recovery.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("hello.txt"));
    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("second.txt"));
}

// -- Comment archives ---------------------------------------------------------

#[test]
fn test_rar5_comment_archive_parses() {
    let archive = open_single("rar5", "rar5_comment.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt"]);
}

#[test]
fn test_rar5_comment_archive_extract() {
    let mut archive = open_single("rar5", "rar5_comment.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("hello.txt"));
}

#[test]
fn test_rar4_comment_archive_parses() {
    let archive = open_single("rar4", "rar4_comment.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt"]);
}

#[test]
fn test_rar4_comment_archive_extract() {
    let mut archive = open_single("rar4", "rar4_comment.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("hello.txt"));
}

// -- Tiny multi-volume (5 × 1KB volumes) --------------------------------------

#[test]
fn test_rar5_tiny_volumes_batch() {
    let vol_names = [
        "rar5_tiny_volumes.part1.rar",
        "rar5_tiny_volumes.part2.rar",
        "rar5_tiny_volumes.part3.rar",
        "rar5_tiny_volumes.part4.rar",
        "rar5_tiny_volumes.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("random_4k.bin"));
}

#[test]
fn test_rar5_tiny_volumes_streaming() {
    let vol_names = [
        "rar5_tiny_volumes.part1.rar",
        "rar5_tiny_volumes.part2.rar",
        "rar5_tiny_volumes.part3.rar",
        "rar5_tiny_volumes.part4.rar",
        "rar5_tiny_volumes.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("random_4k.bin"));
}

#[test]
fn test_rar4_tiny_volumes_batch() {
    let vol_names = [
        "rar4_tiny_volumes.part1.rar",
        "rar4_tiny_volumes.part2.rar",
        "rar4_tiny_volumes.part3.rar",
        "rar4_tiny_volumes.part4.rar",
        "rar4_tiny_volumes.part5.rar",
    ];
    let mut archive = open_multi("rar4", &vol_names);
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("random_4k.bin"));
}

// -- Long filenames (200 chars) -----------------------------------------------

#[test]
fn test_rar5_longname() {
    let archive = open_single("rar5", "rar5_longname.rar");
    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    let long_name = &names[0];
    assert!(
        long_name.len() >= 200,
        "filename should be >= 200 chars, got {}",
        long_name.len()
    );
    assert!(long_name.starts_with("aaa"), "should be all a's");
    assert!(long_name.ends_with(".txt"));
}

#[test]
fn test_rar5_longname_extract() {
    let mut archive = open_single("rar5", "rar5_longname.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"long filename test\n");
}

#[test]
fn test_rar4_longname() {
    let archive = open_single("rar4", "rar4_longname.rar");
    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    let long_name = &names[0];
    assert!(
        long_name.len() >= 200,
        "filename should be >= 200 chars, got {}",
        long_name.len()
    );
}

#[test]
fn test_rar4_longname_extract() {
    let mut archive = open_single("rar4", "rar4_longname.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"long filename test\n");
}

// -- Symlink archives (RAR5 only) ---------------------------------------------

#[test]
fn test_rar5_symlink_metadata() {
    let archive = open_single("rar5", "rar5_symlink.rar");
    let names = archive.member_names();
    assert_eq!(names.len(), 2);

    // Find the symlink member
    let mut found_symlink = false;
    for (i, _) in names.iter().enumerate() {
        let info = archive.member_info(i).unwrap();
        if info.is_symlink {
            found_symlink = true;
            assert!(
                info.link_target.is_some(),
                "symlink should have a link target"
            );
        }
    }
    assert!(found_symlink, "should have a symlink member");
}

// -- Best compression level ---------------------------------------------------

#[test]
fn test_rar5_best_compression_extract() {
    let mut archive = open_single("rar5", "rar5_best.rar");
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("zeros_64k.bin"));
}

// -- Multi-file LZ compression ------------------------------------------------

#[test]
fn test_rar5_multifile_lz_list() {
    let archive = open_single("rar5", "rar5_multifile_lz.rar");
    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt", "second.txt", "zeros_64k.bin"]);
}
