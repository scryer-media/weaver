//! Integration tests for weaver-unrar decompression.
//!
//! These tests construct RAR5 archives programmatically and verify
//! that the parser + decompressor produce correct output.

use std::io::Cursor;

#[cfg(unix)]
fn current_unix_owner_names() -> Option<(String, String)> {
    unsafe {
        let passwd = libc::getpwuid(libc::geteuid());
        let group = libc::getgrgid(libc::getegid());
        if passwd.is_null() || group.is_null() {
            return None;
        }

        let user_name = std::ffi::CStr::from_ptr((*passwd).pw_name)
            .to_string_lossy()
            .into_owned();
        let group_name = std::ffi::CStr::from_ptr((*group).gr_name)
            .to_string_lossy()
            .into_owned();
        Some((user_name, group_name))
    }
}

/// Build a minimal RAR5 archive containing a single stored file.
///
/// This constructs the binary format by hand:
/// - RAR5 signature (8 bytes)
/// - Main archive header (type 1)
/// - File header (type 2) with stored data
/// - End of archive header (type 5)
fn build_stored_rar5_archive(filename: &str, content: &[u8]) -> Vec<u8> {
    build_stored_rar5_archive_with_metadata(filename, content, 0o644, None, 1)
}

fn build_stored_rar5_archive_with_metadata(
    filename: &str,
    content: &[u8],
    attributes: u64,
    mtime: Option<u32>,
    host_os: u64,
) -> Vec<u8> {
    let mut archive = Vec::new();

    // RAR5 signature
    archive.extend_from_slice(&[0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00]);

    // Main archive header (type 1, no flags)
    // Type-specific body needs archive_flags vint (0 = no flags)
    let main_type_body = encode_vint(0); // archive_flags = 0
    let main_header = build_header(1, 0, &main_type_body, &[]);
    archive.extend_from_slice(&main_header);

    // File header (type 2)
    let file_header =
        build_file_header_with_metadata(filename, content, attributes, mtime, host_os);
    archive.extend_from_slice(&file_header);

    // Data area (the actual file content, uncompressed)
    archive.extend_from_slice(content);

    // End of archive header (type 5, no more volumes)
    let end_type_body = encode_vint(0); // end_flags = 0 (no more volumes)
    let end_header = build_header(5, 0, &end_type_body, &[]);
    archive.extend_from_slice(&end_header);

    archive
}

fn build_directory_rar5_archive(filename: &str, attributes: u64, mtime: Option<u32>) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&[0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00]);

    let main_type_body = encode_vint(0);
    archive.extend_from_slice(&build_header(1, 0, &main_type_body, &[]));

    let mut type_body = Vec::new();
    let mut file_flags: u64 = 0x0001; // directory
    if mtime.is_some() {
        file_flags |= 0x0002;
    }
    type_body.extend_from_slice(&encode_vint(file_flags));
    type_body.extend_from_slice(&encode_vint(0)); // unpacked_size
    type_body.extend_from_slice(&encode_vint(attributes));
    if let Some(ts) = mtime {
        type_body.extend_from_slice(&ts.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_vint(0)); // store, RAR5
    type_body.extend_from_slice(&encode_vint(1)); // Unix
    type_body.extend_from_slice(&encode_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());
    archive.extend_from_slice(&build_header(2, 0, &type_body, &[]));

    let end_type_body = encode_vint(0);
    archive.extend_from_slice(&build_header(5, 0, &end_type_body, &[]));
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
fn build_file_header_with_metadata(
    filename: &str,
    content: &[u8],
    attributes: u64,
    mtime: Option<u32>,
    host_os: u64,
) -> Vec<u8> {
    // Compute CRC32 of content
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let data_crc = hasher.finalize();

    // File-specific flags: optional TIME_PRESENT (0x0002) + CRC32_PRESENT (0x0004).
    let mut file_flags: u64 = 0x0004;
    if mtime.is_some() {
        file_flags |= 0x0002;
    }

    // Build type-specific body
    let mut type_body = Vec::new();
    // file_flags
    type_body.extend_from_slice(&encode_vint(file_flags));
    // unpacked_size
    type_body.extend_from_slice(&encode_vint(content.len() as u64));
    // attributes
    type_body.extend_from_slice(&encode_vint(attributes));
    // mtime, if present
    if let Some(ts) = mtime {
        type_body.extend_from_slice(&ts.to_le_bytes());
    }
    // data_crc32 (since CRC32_PRESENT flag is set)
    type_body.extend_from_slice(&data_crc.to_le_bytes());
    // compression_info: version=0, solid=false, method=0(store), dict_code=0(128KB)
    type_body.extend_from_slice(&encode_vint(0));
    // host_os: 1 = Unix
    type_body.extend_from_slice(&encode_vint(host_os));
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

fn build_single_file_rar5_archive(file_header: Vec<u8>, content: &[u8]) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&RAR5_SIG);
    archive.extend_from_slice(&build_main_archive_header(0, None));
    archive.extend_from_slice(&file_header);
    archive.extend_from_slice(content);
    archive.extend_from_slice(&build_end_header(false));
    archive
}

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
    build_file_header_ex_with_file_flags(
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        comp_info_raw,
        0,
        extra_area,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_file_header_ex_with_file_flags(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
    file_flags_extra: u64,
    extra_area: &[u8],
) -> Vec<u8> {
    build_file_header_ex_with_file_flags_and_attributes(
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        comp_info_raw,
        file_flags_extra,
        0o644,
        extra_area,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_file_header_ex_with_file_flags_and_attributes(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
    file_flags_extra: u64,
    attributes: u64,
    extra_area: &[u8],
) -> Vec<u8> {
    let mut file_flags: u64 = file_flags_extra;
    if data_crc.is_some() {
        file_flags |= 0x0004;
    }

    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_vint(file_flags));
    type_body.extend_from_slice(&encode_vint(unpacked_size));
    type_body.extend_from_slice(&encode_vint(attributes));
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

fn build_service_header_with_data(
    service_name: &str,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
    extra_area: &[u8],
) -> Vec<u8> {
    build_service_header_with_data_ex(
        service_name,
        0,
        data_size,
        unpacked_size,
        data_crc,
        comp_info_raw,
        extra_area,
    )
}

fn build_service_header_with_data_ex(
    service_name: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    comp_info_raw: u64,
    extra_area: &[u8],
) -> Vec<u8> {
    let mut file_flags: u64 = 0;
    if data_crc.is_some() {
        file_flags |= 0x0004;
    }

    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_vint(file_flags));
    type_body.extend_from_slice(&encode_vint(unpacked_size));
    type_body.extend_from_slice(&encode_vint(0)); // attributes
    if let Some(crc) = data_crc {
        type_body.extend_from_slice(&crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_vint(comp_info_raw));
    type_body.extend_from_slice(&encode_vint(1)); // host_os = Unix
    type_body.extend_from_slice(&encode_vint(service_name.len() as u64));
    type_body.extend_from_slice(service_name.as_bytes());

    let mut common_flags: u64 = 0x0002 | common_flags_extra; // DATA_AREA
    if !extra_area.is_empty() {
        common_flags |= 0x0001; // EXTRA_AREA
    }

    let mut body = Vec::new();
    body.extend_from_slice(&encode_vint(3)); // header type = Service
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
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let full_crc = hasher.finalize();
    let (vol0, vol1) =
        build_two_volume_stored_archive_with_final_crc(filename, content, split_at, full_crc);
    (vol0, vol1, content.to_vec())
}

fn build_two_volume_stored_archive_with_final_crc(
    filename: &str,
    content: &[u8],
    split_at: usize,
    final_crc: u32,
) -> (Vec<u8>, Vec<u8>) {
    build_two_volume_stored_archive_with_split_crc(filename, content, split_at, None, final_crc)
}

fn build_two_volume_stored_archive_with_split_crc(
    filename: &str,
    content: &[u8],
    split_at: usize,
    first_packed_crc: Option<u32>,
    final_crc: u32,
) -> (Vec<u8>, Vec<u8>) {
    let part1 = &content[..split_at];
    let part2 = &content[split_at..];

    // --- Volume 0 ---
    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&RAR5_SIG);
    // Main archive header: VOLUME flag (0x0001), no volume number field (first volume)
    vol0.extend_from_slice(&build_main_archive_header(0x0001, None));
    // File header: SPLIT_AFTER (0x0010), data_size = part1.len()
    // If present, this CRC is UnRAR's Pack-CRC32 for this split segment.
    vol0.extend_from_slice(&build_file_header_ex(
        filename,
        0x0010, // SPLIT_AFTER
        part1.len() as u64,
        content.len() as u64,
        first_packed_crc,
        0, // store, version 0, dict 128KB
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
        Some(final_crc),
        0, // store
    ));
    vol1.extend_from_slice(part2);
    // End of archive: no more volumes
    vol1.extend_from_slice(&build_end_header(false));

    (vol0, vol1)
}

fn write_two_temp_volumes(
    vol0: &[u8],
    vol1: &[u8],
) -> (tempfile::TempDir, Vec<std::path::PathBuf>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let paths = vec![
        temp_dir.path().join("part1.rar"),
        temp_dir.path().join("part2.rar"),
    ];
    std::fs::write(&paths[0], vol0).unwrap();
    std::fs::write(&paths[1], vol1).unwrap();
    (temp_dir, paths)
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
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let names = archive.member_names();
    assert_eq!(names, vec!["hello.txt"]);

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    assert_eq!(meta.members[0].name, "hello.txt");
    assert_eq!(
        meta.members[0].compression.method,
        weaver_unrar::CompressionMethod::Store
    );
    assert_eq!(meta.members[0].unpacked_size, Some(content.len() as u64));
}

#[test]
fn test_extract_stored_member() {
    let content = b"Extract me from RAR5!";
    let archive_bytes = build_stored_rar5_archive("test.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_unrar::ExtractOptions::default(), None)
        .unwrap();

    assert_eq!(result, content);
}

#[test]
fn test_extract_stored_by_name() {
    let content = b"Named extraction test";
    let archive_bytes = build_stored_rar5_archive("named.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_by_name("named.txt", &weaver_unrar::ExtractOptions::default(), None)
        .unwrap();

    assert_eq!(result, content);
}

#[test]
fn test_extract_stored_crc_verification() {
    let content = b"CRC verification test data";
    let archive_bytes = build_stored_rar5_archive("crc.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    // Should succeed with verification enabled (default)
    let result = archive
        .extract_member(
            0,
            &weaver_unrar::ExtractOptions {
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
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_unrar::ExtractOptions::default(), None)
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_extract_stored_large_file() {
    // 256KB of pattern data
    let content: Vec<u8> = (0..=255u8).cycle().take(256 * 1024).collect();
    let archive_bytes = build_stored_rar5_archive("large.bin", &content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let result = archive
        .extract_member(0, &weaver_unrar::ExtractOptions::default(), None)
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_archive_limits_reject_declared_unpacked_size() {
    let content = b"a";
    let file_header = build_file_header_ex("too-big.txt", 0, content.len() as u64, 5, None, 0);
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_unpacked_size: 4,
        ..Default::default()
    });

    let result = archive.extract_member(0, &weaver_unrar::ExtractOptions::default(), None);

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::ResourceLimit { .. })
    ));
}

#[test]
fn test_archive_limits_reject_packed_data_size() {
    let content = b"hello";
    let file_header =
        build_file_header_ex("packed-too-big.txt", 0, content.len() as u64, 5, None, 0);
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_data_segment: 4,
        ..Default::default()
    });

    let result = archive.extract_member(0, &weaver_unrar::ExtractOptions::default(), None);

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::ResourceLimit { .. })
    ));
}

#[test]
fn test_archive_limits_reject_compressed_dictionary_size() {
    let content = &[0u8];
    let normal_method_256k_dict = (3u64 << 7) | (1u64 << 10);
    let file_header = build_file_header_ex(
        "dict-too-big.bin",
        0,
        content.len() as u64,
        1,
        None,
        normal_method_256k_dict,
    );
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_dict_size: 128 * 1024,
        ..Default::default()
    });

    let result = archive.extract_member(0, &weaver_unrar::ExtractOptions::default(), None);

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::DictionaryTooLarge {
            size: 262_144,
            max: 131_072
        })
    ));
}

#[test]
fn test_unknown_unpacked_size_uses_configured_output_ceiling() {
    let content = b"hello";
    let file_header = build_file_header_ex_with_file_flags(
        "unknown-size.txt",
        0,
        content.len() as u64,
        0,
        None,
        0,
        0x0008,
        &[],
    );
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_unpacked_size: 4,
        ..Default::default()
    });

    let result = archive.extract_member(0, &weaver_unrar::ExtractOptions::default(), None);

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::ResourceLimit { .. })
    ));
}

#[test]
fn test_streaming_unknown_unpacked_size_uses_configured_output_ceiling() {
    let content = b"hello";
    let file_header = build_file_header_ex_with_file_flags(
        "unknown-streaming.txt",
        0,
        content.len() as u64,
        0,
        None,
        0,
        0x0008,
        &[],
    );
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("unknown-streaming.rar");
    std::fs::write(&archive_path, &archive_bytes).unwrap();

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_unpacked_size: 4,
        ..Default::default()
    });
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![archive_path]);
    let mut output = Vec::new();

    let result = archive.extract_member_streaming(
        0,
        &weaver_unrar::ExtractOptions::default(),
        &provider,
        &mut output,
    );

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::ResourceLimit { .. })
    ));
}

#[test]
fn test_streaming_chunked_unknown_unpacked_size_uses_configured_output_ceiling() {
    let content = b"hello";
    let file_header = build_file_header_ex_with_file_flags(
        "unknown-chunked.txt",
        0,
        content.len() as u64,
        0,
        None,
        0,
        0x0008,
        &[],
    );
    let archive_bytes = build_single_file_rar5_archive(file_header, content);
    let temp_dir = tempfile::tempdir().unwrap();
    let archive_path = temp_dir.path().join("unknown-chunked.rar");
    std::fs::write(&archive_path, &archive_bytes).unwrap();

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    archive.set_limits(weaver_unrar::Limits {
        max_unpacked_size: 4,
        ..Default::default()
    });
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![archive_path]);

    let result = archive.extract_member_streaming_chunked(
        0,
        &weaver_unrar::ExtractOptions::default(),
        &provider,
        |_| Ok(Box::new(std::io::sink())),
    );

    assert!(matches!(
        result,
        Err(weaver_unrar::RarError::ResourceLimit { .. })
    ));
}

#[test]
fn test_member_not_found() {
    let content = b"test";
    let archive_bytes = build_stored_rar5_archive("test.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let result = archive.extract_by_name(
        "nonexistent.txt",
        &weaver_unrar::ExtractOptions::default(),
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

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "movie.mkv");

    let result = archive
        .extract_by_name(
            "movie.mkv",
            &weaver_unrar::ExtractOptions {
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

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let result = archive
        .extract_by_name(
            "data.bin",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_multivolume_split_after_pack_crc_is_verified() {
    let content = b"first split segment carries an UnRAR Pack-CRC32 value";
    let split_at = 28;
    let first_packed_crc = crc32fast::hash(&content[..split_at]);
    let final_crc = crc32fast::hash(content);
    let (vol0, vol1) = build_two_volume_stored_archive_with_split_crc(
        "packed.bin",
        content,
        split_at,
        Some(first_packed_crc),
        final_crc,
    );

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let result = archive
        .extract_by_name(
            "packed.bin",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar5_volume_facts_expose_split_after_pack_crc() {
    let content = b"facts should label split-after CRC as Pack-CRC32";
    let split_at = 24;
    let first_packed_crc = crc32fast::hash(&content[..split_at]);
    let final_crc = crc32fast::hash(content);
    let (vol0, vol1) = build_two_volume_stored_archive_with_split_crc(
        "facts.bin",
        content,
        split_at,
        Some(first_packed_crc),
        final_crc,
    );

    let facts0 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol0), None).unwrap();
    assert_eq!(facts0.members.len(), 1);
    assert!(facts0.members[0].split_after);
    assert_eq!(facts0.members[0].data_crc32, Some(first_packed_crc));
    assert_eq!(facts0.members[0].packed_crc32, Some(first_packed_crc));
    assert!(facts0.members[0].packed_blake2_hash.is_none());
    assert!(!facts0.members[0].packed_hash_uses_mac);

    let facts1 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol1), None).unwrap();
    assert_eq!(facts1.members.len(), 1);
    assert!(!facts1.members[0].split_after);
    assert_eq!(facts1.members[0].data_crc32, Some(final_crc));
    assert_eq!(facts1.members[0].packed_crc32, None);
}

#[test]
fn test_rar5_volume_facts_expose_split_comment_pack_crc() {
    let comment = b"RAR5 service facts should label split CMT CRC as Pack-CRC32";
    let split_at = 31;
    let first_packed_crc = crc32fast::hash(&comment[..split_at]);
    let full_crc = crc32fast::hash(comment);
    let (vol0, vol1) = build_two_volume_rar5_comment_archive(comment, split_at);

    let facts0 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol0), None).unwrap();
    assert!(facts0.members.is_empty());
    assert_eq!(facts0.services.len(), 1);
    assert_eq!(facts0.services[0].name, "CMT");
    assert!(facts0.services[0].split_after);
    assert_eq!(facts0.services[0].data_crc32, Some(first_packed_crc));
    assert_eq!(facts0.services[0].packed_crc32, Some(first_packed_crc));
    assert!(!facts0.services[0].packed_hash_uses_mac);

    let facts1 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol1), None).unwrap();
    assert_eq!(facts1.services.len(), 1);
    assert_eq!(facts1.services[0].name, "CMT");
    assert!(!facts1.services[0].split_after);
    assert_eq!(facts1.services[0].data_crc32, Some(full_crc));
    assert_eq!(facts1.services[0].packed_crc32, None);
}

#[test]
fn test_multivolume_split_after_pack_crc_mismatch_fails_before_completion() {
    let content = b"final CRC is correct but the first packed segment CRC is wrong";
    let split_at = 27;
    let wrong_first_packed_crc = crc32fast::hash(&content[..split_at]) ^ 0x55AA_00FF;
    let final_crc = crc32fast::hash(content);
    let (vol0, vol1) = build_two_volume_stored_archive_with_split_crc(
        "bad-packed.bin",
        content,
        split_at,
        Some(wrong_first_packed_crc),
        final_crc,
    );

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let err = archive
        .extract_by_name(
            "bad-packed.bin",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        weaver_unrar::RarError::PackedDataCrcMismatch {
            member,
            volume: 0,
            expected,
            actual,
        } if member == "bad-packed.bin" && expected == wrong_first_packed_crc && actual != expected
    ));
}

#[test]
fn test_streaming_split_after_pack_crc_mismatch_fails() {
    let content = b"streaming catches the first split header Pack-CRC32 too";
    let split_at = 26;
    let wrong_first_packed_crc = crc32fast::hash(&content[..split_at]) ^ 0xCAFE_BABE;
    let final_crc = crc32fast::hash(content);
    let (vol0, vol1) = build_two_volume_stored_archive_with_split_crc(
        "stream-packed.bin",
        content,
        split_at,
        Some(wrong_first_packed_crc),
        final_crc,
    );
    let (_temp_dir, paths) = write_two_temp_volumes(&vol0, &vol1);
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();
    let mut out = Vec::new();

    let err = archive
        .extract_member_streaming(
            0,
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            &provider,
            &mut out,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        weaver_unrar::RarError::PackedDataCrcMismatch {
            member,
            volume: 0,
            expected,
            actual,
        } if member == "stream-packed.bin" && expected == wrong_first_packed_crc && actual != expected
    ));
}

#[test]
fn test_streaming_continuation_final_crc_is_verified_when_first_header_has_none() {
    let content = b"streaming continuation final crc should be authoritative";
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let wrong_final_crc = hasher.finalize() ^ 0xFFFF_FFFF;
    let (vol0, vol1) =
        build_two_volume_stored_archive_with_final_crc("stream.bin", content, 25, wrong_final_crc);
    let (_temp_dir, paths) = write_two_temp_volumes(&vol0, &vol1);
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();
    let mut out = Vec::new();

    let err = archive
        .extract_member_streaming(
            0,
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            &provider,
            &mut out,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        weaver_unrar::RarError::DataCrcMismatch {
            expected,
            actual,
            ..
        } if expected == wrong_final_crc && actual != wrong_final_crc
    ));
}

#[test]
fn test_streaming_chunked_continuation_final_crc_is_verified_when_first_header_has_none() {
    let content = b"chunked streaming continuation final crc should be authoritative";
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let wrong_final_crc = hasher.finalize() ^ 0xA5A5_A5A5;
    let (vol0, vol1) =
        build_two_volume_stored_archive_with_final_crc("chunk.bin", content, 30, wrong_final_crc);
    let (_temp_dir, paths) = write_two_temp_volumes(&vol0, &vol1);
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();
    let chunk_dir = tempfile::tempdir().unwrap();

    let err = archive
        .extract_member_streaming_chunked(
            0,
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            &provider,
            |volume_index| {
                let path = chunk_dir.path().join(format!("vol{volume_index}.chunk"));
                std::fs::File::create(path)
                    .map(|file| Box::new(file) as Box<dyn std::io::Write>)
                    .map_err(weaver_unrar::RarError::Io)
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        weaver_unrar::RarError::DataCrcMismatch {
            expected,
            actual,
            ..
        } if expected == wrong_final_crc && actual != wrong_final_crc
    ));
}

#[test]
fn test_multivolume_incremental_add() {
    let content = b"Incremental volume addition test data here!";
    let (vol0, vol1, _full) = build_two_volume_stored_archive("file.dat", content, 25);

    // Open first volume only.
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();

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
            &weaver_unrar::ExtractOptions {
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

    let archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();

    // Only volume 0 present, file needs volume 1 too.
    assert!(!archive.is_extractable("split.bin"));

    // Non-existent member.
    assert!(!archive.is_extractable("nonexistent.dat"));
}

#[test]
fn test_multivolume_missing_volumes_query() {
    let content = b"Query missing volumes for this file";
    let (vol0, _vol1, _full) = build_two_volume_stored_archive("query.dat", content, 10);

    let archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();

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
    let archive0 = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();
    assert!(archive0.more_volumes());

    // Parse volume 1 — it should NOT indicate more volumes.
    let archive1 = weaver_unrar::RarArchive::open(Cursor::new(vol1)).unwrap();
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
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    assert!(archive.is_solid());

    let names = archive.member_names();
    assert_eq!(names, vec!["first.txt", "second.txt"]);

    // Extract both in order.
    let result1 = archive
        .extract_by_name(
            "first.txt",
            &weaver_unrar::ExtractOptions {
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
            &weaver_unrar::ExtractOptions {
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
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

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

fn finalize_rar4_header_crc(mut header: Vec<u8>) -> Vec<u8> {
    let crc16 = (crc32fast::hash(&header[2..]) & 0xFFFF) as u16;
    header[0..2].copy_from_slice(&crc16.to_le_bytes());
    header
}

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
    finalize_rar4_header_crc(buf)
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
    finalize_rar4_header_crc(buf)
}

/// Build a RAR4 new-style service subheader (`HEAD3_SERVICE` / 0x7A).
/// It uses the same file-header body shape as regular RAR4 file headers.
fn build_rar4_service_header(
    service_name: &str,
    packed_size: u32,
    unpacked_size: u32,
    crc32: u32,
) -> Vec<u8> {
    build_rar4_service_header_ex(service_name, packed_size, unpacked_size, crc32, 0, 0)
}

fn build_rar4_service_header_ex(
    service_name: &str,
    packed_size: u32,
    unpacked_size: u32,
    crc32: u32,
    service_flags_extra: u16,
    sub_flags: u32,
) -> Vec<u8> {
    let name_bytes = service_name.as_bytes();
    let header_size: u16 = 7 + 25 + name_bytes.len() as u16;
    let flags: u16 = 0x8000 | 0x4000 | service_flags_extra; // HAS_DATA + SKIP_IF_UNKNOWN.

    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]);
    buf.push(0x7A); // type: NewSub / service
    buf.extend_from_slice(&flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf.extend_from_slice(&packed_size.to_le_bytes());
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(3); // Unix
    buf.extend_from_slice(&crc32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
    buf.push(29); // version
    buf.push(0x30); // method: Store
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&sub_flags.to_le_bytes());
    buf.extend_from_slice(name_bytes);
    finalize_rar4_header_crc(buf)
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
    finalize_rar4_header_crc(buf)
}

fn build_rar4_comment_service_archive(comment: &[u8], expected_crc: u32) -> Vec<u8> {
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0x0002)); // archive comment flag
    vol.extend_from_slice(&build_rar4_service_header(
        "CMT",
        comment.len() as u32,
        comment.len() as u32,
        expected_crc,
    ));
    vol.extend_from_slice(comment);
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_unicode_comment_service_archive(comment: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    for unit in comment.encode_utf16() {
        payload.extend_from_slice(&unit.to_le_bytes());
    }
    payload.extend_from_slice(&0u16.to_le_bytes());
    let crc = crc32fast::hash(&payload);

    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0x0002));
    vol.extend_from_slice(&build_rar4_service_header_ex(
        "CMT",
        payload.len() as u32,
        payload.len() as u32,
        crc,
        0,
        0x0001, // SUBHEAD_FLAGS_CMT_UNICODE
    ));
    vol.extend_from_slice(&payload);
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_uowner_service_header(owner: &str, group: &str) -> Vec<u8> {
    let name = b"UOW";
    let subdata = build_rar4_uowner_subdata(owner, group);

    let header_size: u16 = 7 + 25 + name.len() as u16 + subdata.len() as u16;
    let flags: u16 = 0x4000; // SKIP_IF_UNKNOWN.
    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]);
    buf.push(0x7A); // type: NewSub / service
    buf.extend_from_slice(&flags.to_le_bytes());
    buf.extend_from_slice(&header_size.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // packed size
    buf.extend_from_slice(&0u32.to_le_bytes()); // unpacked size
    buf.push(3); // Unix
    buf.extend_from_slice(&0u32.to_le_bytes()); // CRC
    buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
    buf.push(29); // version
    buf.push(0x30); // method: Store
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // sub flags
    buf.extend_from_slice(name);
    buf.extend_from_slice(&subdata);
    finalize_rar4_header_crc(buf)
}

fn build_rar4_uowner_subdata(owner: &str, group: &str) -> Vec<u8> {
    let mut subdata = Vec::new();
    subdata.extend_from_slice(owner.as_bytes());
    subdata.push(0);
    subdata.extend_from_slice(group.as_bytes());
    subdata
}

fn build_rar4_archive_with_uowner_service(
    filename: &str,
    content: &[u8],
    owner: &str,
    group: &str,
) -> Vec<u8> {
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_file_header(
        filename,
        content.len() as u32,
        content.len() as u32,
        crc32fast::hash(content),
        0,
    ));
    vol.extend_from_slice(content);
    vol.extend_from_slice(&build_rar4_uowner_service_header(owner, group));
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_archive_with_uowner_payload_service(
    filename: &str,
    content: &[u8],
    owner: &str,
    group: &str,
    crc_override: Option<u32>,
) -> Vec<u8> {
    let payload = build_rar4_uowner_subdata(owner, group);
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_file_header(
        filename,
        content.len() as u32,
        content.len() as u32,
        crc32fast::hash(content),
        0,
    ));
    vol.extend_from_slice(content);
    vol.extend_from_slice(&build_rar4_service_header(
        "UOW",
        payload.len() as u32,
        payload.len() as u32,
        crc_override.unwrap_or_else(|| crc32fast::hash(&payload)),
    ));
    vol.extend_from_slice(&payload);
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_old_comment_header(comment: &[u8], crc16: u16) -> Vec<u8> {
    build_rar4_old_comment_header_ex(comment, comment.len() as u16, 29, 0x30, crc16)
}

fn build_rar4_old_service_ntacl_header(
    packed_size: u32,
    unpacked_size: u32,
    unpack_version: u8,
    method: u8,
    crc32: u32,
) -> Vec<u8> {
    let header_size = 7usize + 4 + 2 + 1 + 4 + 1 + 1 + 4;
    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]);
    buf.push(0x77); // old service header
    buf.extend_from_slice(&0x8000u16.to_le_bytes()); // HAS_DATA
    buf.extend_from_slice(&(header_size as u16).to_le_bytes());
    buf.extend_from_slice(&packed_size.to_le_bytes());
    buf.extend_from_slice(&0x0104u16.to_le_bytes()); // NT ACL
    buf.push(0);
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(unpack_version);
    buf.push(method);
    buf.extend_from_slice(&crc32.to_le_bytes());
    finalize_rar4_header_crc(buf)
}

fn build_rar4_old_service_stream_header(
    stream_name: &[u8],
    packed_size: u32,
    unpacked_size: u32,
    unpack_version: u8,
    method: u8,
    crc32: u32,
) -> Vec<u8> {
    let header_size = 7usize + 4 + 2 + 1 + 4 + 1 + 1 + 4 + 2 + stream_name.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]);
    buf.push(0x77); // old service header
    buf.extend_from_slice(&0x8000u16.to_le_bytes()); // HAS_DATA
    buf.extend_from_slice(&(header_size as u16).to_le_bytes());
    buf.extend_from_slice(&packed_size.to_le_bytes());
    buf.extend_from_slice(&0x0105u16.to_le_bytes()); // NTFS stream
    buf.push(0);
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(unpack_version);
    buf.push(method);
    buf.extend_from_slice(&crc32.to_le_bytes());
    buf.extend_from_slice(&(stream_name.len() as u16).to_le_bytes());
    buf.extend_from_slice(stream_name);
    finalize_rar4_header_crc(buf)
}

fn build_rar4_old_comment_header_ex(
    packed_comment: &[u8],
    unpacked_size: u16,
    unpack_version: u8,
    method: u8,
    crc16: u16,
) -> Vec<u8> {
    let header_size = 13usize + packed_comment.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&[0x00, 0x00]);
    buf.push(0x75); // old comment header
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&(header_size as u16).to_le_bytes());
    buf.extend_from_slice(&unpacked_size.to_le_bytes());
    buf.push(unpack_version);
    buf.push(method);
    buf.extend_from_slice(&crc16.to_le_bytes());
    buf.extend_from_slice(packed_comment);
    finalize_rar4_header_crc(buf)
}

fn build_rar4_old_comment_archive(comment: &[u8], crc16: u16) -> Vec<u8> {
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0x0002));
    vol.extend_from_slice(&build_rar4_old_comment_header(comment, crc16));
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_archive_with_old_services() -> Vec<u8> {
    let file = b"host file for old services";
    let acl_payload = b"acl-data";
    let stream_payload = b"stream-data";
    let stream_name = b":metadata";

    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_file_header(
        "host.txt",
        file.len() as u32,
        file.len() as u32,
        crc32fast::hash(file),
        0,
    ));
    vol.extend_from_slice(file);
    vol.extend_from_slice(&build_rar4_old_service_ntacl_header(
        acl_payload.len() as u32,
        acl_payload.len() as u32,
        29,
        0x30,
        crc32fast::hash(acl_payload),
    ));
    vol.extend_from_slice(acl_payload);
    vol.extend_from_slice(&build_rar4_old_service_stream_header(
        stream_name,
        stream_payload.len() as u32,
        stream_payload.len() as u32,
        29,
        0x30,
        crc32fast::hash(stream_payload),
    ));
    vol.extend_from_slice(stream_payload);
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn build_rar4_old_compressed_comment_archive(
    packed_comment: &[u8],
    unpacked_size: u16,
    unpack_version: u8,
    method: u8,
    crc16: u16,
) -> Vec<u8> {
    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0x0002));
    vol.extend_from_slice(&build_rar4_old_comment_header_ex(
        packed_comment,
        unpacked_size,
        unpack_version,
        method,
        crc16,
    ));
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

fn first_rar4_file_payload(data: &[u8]) -> (Vec<u8>, u16, u8, u8) {
    assert!(data.starts_with(&RAR4_SIG));
    let mut pos = RAR4_SIG.len();

    loop {
        assert!(pos + 7 <= data.len(), "RAR4 header extends past fixture");
        let header_type = data[pos + 2];
        let flags = u16::from_le_bytes([data[pos + 3], data[pos + 4]]);
        let header_size = u16::from_le_bytes([data[pos + 5], data[pos + 6]]) as usize;
        assert!(header_size >= 7);
        assert!(pos + header_size <= data.len());

        let data_area_size = if flags & 0x8000 != 0 && pos + 11 <= data.len() {
            u32::from_le_bytes([data[pos + 7], data[pos + 8], data[pos + 9], data[pos + 10]])
                as usize
        } else {
            0
        };

        if header_type == 0x74 {
            let packed_size =
                u32::from_le_bytes([data[pos + 7], data[pos + 8], data[pos + 9], data[pos + 10]])
                    as usize;
            let unpacked_size = u32::from_le_bytes([
                data[pos + 11],
                data[pos + 12],
                data[pos + 13],
                data[pos + 14],
            ]);
            let unpack_version = data[pos + 24];
            let method = data[pos + 25];
            let data_offset = pos + header_size;
            assert_eq!(packed_size, data_area_size);
            assert!(unpacked_size <= u16::MAX as u32);
            assert!(data_offset + packed_size <= data.len());
            return (
                data[data_offset..data_offset + packed_size].to_vec(),
                unpacked_size as u16,
                unpack_version,
                method,
            );
        }

        pos += header_size + data_area_size;
    }
}

fn build_two_volume_rar4_comment_service_archive(
    comment: &[u8],
    split_at: usize,
) -> (Vec<u8>, Vec<u8>) {
    build_two_volume_rar4_comment_service_archive_ex(comment, split_at, None)
}

fn build_two_volume_rar4_comment_service_archive_ex(
    comment: &[u8],
    split_at: usize,
    first_packed_crc_override: Option<u32>,
) -> (Vec<u8>, Vec<u8>) {
    let part1 = &comment[..split_at];
    let part2 = &comment[split_at..];
    let full_crc = crc32fast::hash(comment);
    let first_packed_crc = first_packed_crc_override.unwrap_or_else(|| crc32fast::hash(part1));

    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&RAR4_SIG);
    vol0.extend_from_slice(&build_rar4_archive_header(0x0001 | 0x0002 | 0x0100));
    vol0.extend_from_slice(&build_rar4_service_header_ex(
        "CMT",
        part1.len() as u32,
        comment.len() as u32,
        first_packed_crc,
        0x0002, // SPLIT_AFTER
        0,
    ));
    vol0.extend_from_slice(part1);
    vol0.extend_from_slice(&build_rar4_end_header(true));

    let mut vol1 = Vec::new();
    vol1.extend_from_slice(&RAR4_SIG);
    vol1.extend_from_slice(&build_rar4_archive_header(0x0001 | 0x0002));
    vol1.extend_from_slice(&build_rar4_service_header_ex(
        "CMT",
        part2.len() as u32,
        comment.len() as u32,
        full_crc,
        0x0001, // SPLIT_BEFORE
        0,
    ));
    vol1.extend_from_slice(part2);
    vol1.extend_from_slice(&build_rar4_end_header(false));

    (vol0, vol1)
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
    let first_packed_crc = crc32fast::hash(part1);

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
        first_packed_crc,
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

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    assert_eq!(archive.format(), weaver_unrar::ArchiveFormat::Rar4);

    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "movie.avi");

    let result = archive
        .extract_by_name(
            "movie.avi",
            &weaver_unrar::ExtractOptions {
                verify: false,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_volume_facts_expose_split_after_pack_crc() {
    let content = b"RAR4 facts expose Pack-CRC32 on split-after headers";
    let split_at = 22;
    let first_packed_crc = crc32fast::hash(&content[..split_at]);
    let final_crc = crc32fast::hash(content);
    let (vol0, vol1) = build_two_volume_rar4_stored_archive("facts-rar4.bin", content, split_at);

    let facts0 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol0), None).unwrap();
    assert_eq!(facts0.members.len(), 1);
    assert!(facts0.members[0].split_after);
    assert_eq!(facts0.members[0].data_crc32, Some(first_packed_crc));
    assert_eq!(facts0.members[0].packed_crc32, Some(first_packed_crc));
    assert!(facts0.members[0].packed_blake2_hash.is_none());

    let facts1 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol1), None).unwrap();
    assert_eq!(facts1.members.len(), 1);
    assert!(!facts1.members[0].split_after);
    assert_eq!(facts1.members[0].data_crc32, Some(final_crc));
    assert_eq!(facts1.members[0].packed_crc32, None);
}

#[test]
fn test_rar4_volume_facts_expose_split_comment_pack_crc() {
    let comment = b"RAR4 CMT service facts should expose split-after Pack-CRC32";
    let split_at = 28;
    let first_packed_crc = crc32fast::hash(&comment[..split_at]);
    let full_crc = crc32fast::hash(comment);
    let (vol0, vol1) = build_two_volume_rar4_comment_service_archive(comment, split_at);

    let facts0 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol0), None).unwrap();
    assert!(facts0.members.is_empty());
    assert_eq!(facts0.services.len(), 1);
    assert_eq!(facts0.services[0].name, "CMT");
    assert!(facts0.services[0].split_after);
    assert_eq!(facts0.services[0].data_crc32, Some(first_packed_crc));
    assert_eq!(facts0.services[0].packed_crc32, Some(first_packed_crc));

    let facts1 = weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(vol1), None).unwrap();
    assert_eq!(facts1.services.len(), 1);
    assert_eq!(facts1.services[0].name, "CMT");
    assert!(!facts1.services[0].split_after);
    assert_eq!(facts1.services[0].data_crc32, Some(full_crc));
    assert_eq!(facts1.services[0].packed_crc32, None);
}

#[test]
fn test_rar4_volume_facts_expose_old_acl_and_stream_services() {
    let facts = weaver_unrar::RarArchive::parse_volume_facts(
        Cursor::new(build_rar4_archive_with_old_services()),
        None,
    )
    .unwrap();

    assert_eq!(facts.members.len(), 1);
    assert_eq!(facts.services.len(), 2);

    let acl = &facts.services[0];
    assert_eq!(acl.name, "ACL");
    assert_eq!(acl.subtype, Some(0x0104));
    assert_eq!(acl.level, Some(0));
    assert_eq!(acl.unpacked_size, Some(8));
    assert_eq!(acl.data_size, 8);
    assert_eq!(acl.data_crc32, Some(crc32fast::hash(b"acl-data")));
    assert_eq!(acl.compression_method, 0);
    assert_eq!(acl.compression_version, 29);
    assert_eq!(acl.dict_size, 0x10000);
    assert!(acl.stream_name.is_none());

    let stream = &facts.services[1];
    assert_eq!(stream.name, "STM");
    assert_eq!(stream.subtype, Some(0x0105));
    assert_eq!(stream.unpacked_size, Some(11));
    assert_eq!(stream.data_size, 11);
    assert_eq!(stream.data_crc32, Some(crc32fast::hash(b"stream-data")));
    assert_eq!(stream.stream_name.as_deref(), Some(":metadata"));
}

#[test]
fn test_rar4_multivolume_incremental_add() {
    let content = b"Incremental RAR4 volume addition test data here!";
    let (vol0, vol1) = build_two_volume_rar4_stored_archive("file.dat", content, 25);

    // Open first volume only.
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();
    assert_eq!(archive.format(), weaver_unrar::ArchiveFormat::Rar4);

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
            &weaver_unrar::ExtractOptions {
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

    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let result = archive
        .extract_by_name(
            "data.bin",
            &weaver_unrar::ExtractOptions {
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

    let archive = weaver_unrar::RarArchive::open(Cursor::new(vol0)).unwrap();

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

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol)).unwrap();
    assert_eq!(archive.format(), weaver_unrar::ArchiveFormat::Rar4);
    assert!(!archive.more_volumes());

    let result = archive
        .extract_by_name(
            "single.txt",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_uowner_service_attaches_owner_metadata_to_previous_member() {
    let archive_bytes =
        build_rar4_archive_with_uowner_service("owned.txt", b"owned content", "alice", "media");
    let archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();

    let info = archive.member_info(0).unwrap();
    let owner = info
        .owner
        .expect("RAR4 UOW service should set owner metadata");
    assert_eq!(owner.user_name.as_deref(), Some("alice"));
    assert_eq!(owner.group_name.as_deref(), Some("media"));
    assert_eq!(owner.uid, None);
    assert_eq!(owner.gid, None);
}

#[test]
fn test_rar4_uowner_payload_service_attaches_owner_metadata_to_previous_member() {
    let archive_bytes = build_rar4_archive_with_uowner_payload_service(
        "owned.txt",
        b"owned content",
        "alice",
        "media",
        None,
    );
    let archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();

    let info = archive.member_info(0).unwrap();
    let owner = info
        .owner
        .expect("RAR4 UOW payload service should set owner metadata");
    assert_eq!(owner.user_name.as_deref(), Some("alice"));
    assert_eq!(owner.group_name.as_deref(), Some("media"));
    assert_eq!(owner.uid, None);
    assert_eq!(owner.gid, None);
}

#[test]
fn test_rar4_uowner_payload_crc_error_is_optional_metadata() {
    let content = b"owned content";
    let archive_bytes = build_rar4_archive_with_uowner_payload_service(
        "owned.txt",
        content,
        "alice",
        "media",
        Some(0xdead_beef),
    );
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();

    assert!(archive.member_info(0).unwrap().owner.is_none());
    let result = archive
        .extract_by_name("owned.txt", &weaver_unrar::ExtractOptions::default(), None)
        .unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_rar4_uowner_service_ignores_empty_owner_or_group() {
    for (owner, group) in [("", "media"), ("alice", "")] {
        let archive_bytes =
            build_rar4_archive_with_uowner_service("owned.txt", b"owned content", owner, group);
        let archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();

        assert!(
            archive.member_info(0).unwrap().owner.is_none(),
            "RAR4 UOW with owner={owner:?} group={group:?} should not attach partial metadata"
        );
    }
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_restores_rar4_uowner_when_enabled() {
    use std::os::unix::fs::MetadataExt;

    let Some((user_name, group_name)) = current_unix_owner_names() else {
        eprintln!("skipping RAR4 UOW restore test: current uid/gid names are unavailable");
        return;
    };

    let uid = unsafe { libc::geteuid() } as u64;
    let gid = unsafe { libc::getegid() } as u64;
    let archive_bytes = build_rar4_archive_with_uowner_service(
        "owned.txt",
        b"owned content",
        &user_name,
        &group_name,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("owned.txt");

    let written = archive
        .extract_member_to_file(
            0,
            &weaver_unrar::ExtractOptions {
                restore_owners: true,
                ..Default::default()
            },
            None,
            &out_path,
        )
        .unwrap();

    assert_eq!(written, b"owned content".len() as u64);
    let metadata = std::fs::metadata(&out_path).unwrap();
    assert_eq!(u64::from(metadata.uid()), uid);
    assert_eq!(u64::from(metadata.gid()), gid);
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
    finalize_rar4_header_crc(buf)
}

fn encrypt_rar4_stored_payload(content: &[u8], key: &[u8; 16], iv: &[u8; 16]) -> Vec<u8> {
    use aes::Aes128;
    use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    let padded_len = (content.len() + 15) & !15;
    let mut ciphertext = vec![0u8; padded_len];
    ciphertext[..content.len()].copy_from_slice(content);
    let encryptor = Aes128CbcEnc::new(key.into(), iv.into());
    encryptor
        .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, padded_len)
        .unwrap();
    ciphertext
}

fn build_rar4_encrypted_stored_archive(
    filename: &str,
    content: &[u8],
    salt: &[u8; 8],
    key: &[u8; 16],
    iv: &[u8; 16],
) -> Vec<u8> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    let crc = hasher.finalize();
    let ciphertext = encrypt_rar4_stored_payload(content, key, iv);

    let mut vol = Vec::new();
    vol.extend_from_slice(&RAR4_SIG);
    vol.extend_from_slice(&build_rar4_archive_header(0));
    vol.extend_from_slice(&build_rar4_encrypted_file_header(
        filename,
        ciphertext.len() as u32,
        content.len() as u32,
        crc,
        salt,
        0,
    ));
    vol.extend_from_slice(&ciphertext);
    vol.extend_from_slice(&build_rar4_end_header(false));
    vol
}

#[test]
fn test_rar4_encrypted_stored_file() {
    let password = "secret123";
    let salt: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let content = b"RAR4 encrypted stored content!!"; // 31 bytes
    let (key, iv) = weaver_unrar::crypto::rar4_derive_key(password, Some(&salt));
    let vol = build_rar4_encrypted_stored_archive("secret.txt", content, &salt, &key, &iv);

    // Open and extract with password.
    let mut archive =
        weaver_unrar::RarArchive::open_with_password(Cursor::new(vol), password).unwrap();

    assert_eq!(archive.format(), weaver_unrar::ArchiveFormat::Rar4);

    let meta = archive.metadata();
    assert!(meta.members[0].is_encrypted);

    let result = archive
        .extract_by_name(
            "secret.txt",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    // The decrypted data is padded to block boundary; unpacked_size truncates it.
    assert_eq!(result, content);
}

const RAR4_LONG_PASSWORD: &str = "abcdefghijklmnopqrstuvwxyzabcdef";
const RAR4_LONG_PASSWORD_SALT: [u8; 8] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
const RAR4_LONG_PASSWORD_UNRAR_KEY: [u8; 16] = [
    0x64, 0x09, 0xb2, 0x06, 0xed, 0x97, 0x47, 0x88, 0xe3, 0xd4, 0x81, 0x9e, 0x4e, 0xdb, 0xa9, 0xb1,
];
const RAR4_LONG_PASSWORD_UNRAR_IV: [u8; 16] = [
    0x87, 0xbb, 0xc0, 0xbf, 0x98, 0xda, 0xa1, 0xaa, 0x13, 0xe0, 0x10, 0xcf, 0x14, 0xce, 0xd6, 0xce,
];
const RAR4_LONG_PASSWORD_CONTENT: &[u8] = b"RAR4 long-password fixture verified by local unrar";

fn build_rar4_long_password_unrar_fixture() -> Vec<u8> {
    build_rar4_encrypted_stored_archive(
        "long-password.txt",
        RAR4_LONG_PASSWORD_CONTENT,
        &RAR4_LONG_PASSWORD_SALT,
        &RAR4_LONG_PASSWORD_UNRAR_KEY,
        &RAR4_LONG_PASSWORD_UNRAR_IV,
    )
}

#[test]
fn test_rar4_long_password_unrar_vector_archive_extracts() {
    let vol = build_rar4_long_password_unrar_fixture();
    let mut archive =
        weaver_unrar::RarArchive::open_with_password(Cursor::new(vol), RAR4_LONG_PASSWORD).unwrap();

    let result = archive
        .extract_by_name(
            "long-password.txt",
            &weaver_unrar::ExtractOptions {
                verify: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    assert_eq!(result, RAR4_LONG_PASSWORD_CONTENT);
}

#[test]
fn test_rar4_long_password_fixture_is_accepted_by_local_unrar_when_configured() {
    let Ok(unrar_bin) = std::env::var("UNRAR_BIN") else {
        if std::env::var("WEAVER_REQUIRE_UNRAR_ORACLE").is_ok_and(|v| v == "1" || v == "true") {
            panic!("WEAVER_REQUIRE_UNRAR_ORACLE is set but UNRAR_BIN is missing");
        }
        eprintln!("skipping local unrar oracle check; set UNRAR_BIN to enable it");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let archive_path = dir.path().join("long-password.rar");
    std::fs::write(&archive_path, build_rar4_long_password_unrar_fixture()).unwrap();

    let output = std::process::Command::new(unrar_bin)
        .arg("t")
        .arg("-idq")
        .arg(format!("-p{RAR4_LONG_PASSWORD}"))
        .arg(&archive_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "local unrar rejected long-password fixture\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn first_regular_file_under(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let metadata = entry.metadata().ok()?;
            if metadata.is_file() {
                return Some(path);
            }
            if metadata.is_dir() {
                pending.push(path);
            }
        }
    }
    None
}

fn extract_old_rar_fixture_with_unrar(unrar_bin: &str, archive_path: &std::path::Path) -> Vec<u8> {
    let output_dir = tempfile::tempdir().unwrap();
    let destination = format!(
        "{}{}",
        output_dir.path().display(),
        std::path::MAIN_SEPARATOR
    );
    let output = std::process::Command::new(unrar_bin)
        .arg("x")
        .arg("-idq")
        .arg("-y")
        .arg("-p-")
        .arg(archive_path)
        .arg(destination)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {unrar_bin}: {err}"));

    assert!(
        output.status.success(),
        "unrar failed to extract {}\nstdout:\n{}\nstderr:\n{}",
        archive_path.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let output_file = first_regular_file_under(output_dir.path()).unwrap_or_else(|| {
        panic!(
            "unrar extracted {} without producing a regular file",
            archive_path.display()
        )
    });
    std::fs::read(&output_file)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", output_file.display()))
}

fn extract_first_regular_member_with_weaver(archive_path: &std::path::Path) -> Vec<u8> {
    let file = std::fs::File::open(archive_path)
        .unwrap_or_else(|err| panic!("failed to open {}: {err}", archive_path.display()));
    let mut archive = weaver_unrar::RarArchive::open(file)
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", archive_path.display()));
    let metadata = archive.metadata();
    let member_index = metadata
        .members
        .iter()
        .position(|member| !member.is_directory)
        .unwrap_or_else(|| panic!("{} has no regular members", archive_path.display()));

    archive
        .extract_member(member_index, &weaver_unrar::ExtractOptions::default(), None)
        .and_then(|member| member.into_bytes())
        .unwrap_or_else(|err| {
            panic!(
                "failed to extract {} with Weaver: {err}",
                archive_path.display()
            )
        })
}

#[test]
fn test_old_rar_lz_fixtures_extract_expected_bytes() {
    let expected = original("boat_modern_english.wav");
    let cases = [
        ("rar15_lz.rar", "BOATMO~1.WAV"),
        ("rar20_lz.rar", "BoatModernEnglish.wav"),
        ("rar20_audio_text.rar", "BoatModernEnglish.wav"),
    ];

    for (fixture_name, member_name) in cases {
        let mut archive = open_single("rar4", fixture_name);
        assert_eq!(archive.metadata().format, weaver_unrar::ArchiveFormat::Rar4);
        assert_eq!(archive.member_names()[0], member_name);
        assert_eq!(
            archive
                .extract_member(0, &weaver_unrar::ExtractOptions::default(), None)
                .and_then(|member| member.into_bytes())
                .unwrap(),
            expected,
            "old RAR LZ extraction mismatch for {fixture_name}"
        );
    }
}

#[test]
fn test_old_rar_oracle_fixtures_match_unrar_when_available() {
    let required = std::env::var("WEAVER_REQUIRE_UNRAR_ORACLE")
        .is_ok_and(|value| value == "1" || value == "true");
    let fixture_dir =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar4");
    let expected = ["rar15_lz.rar", "rar20_lz.rar", "rar20_audio_text.rar"];
    let missing = expected
        .iter()
        .copied()
        .filter(|name| !fixture_dir.join(name).exists())
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        if required {
            panic!(
                "WEAVER_REQUIRE_UNRAR_ORACLE is set but old RAR oracle fixtures are missing from {}: {}",
                fixture_dir.display(),
                missing.join(", ")
            );
        }

        eprintln!(
            "skipping old RAR oracle comparison; missing {} in {}",
            missing.join(", "),
            fixture_dir.display()
        );
        return;
    }

    let Ok(unrar_bin) = std::env::var("UNRAR_BIN") else {
        if required {
            panic!(
                "WEAVER_REQUIRE_UNRAR_ORACLE is set and old RAR oracle fixtures are present, \
                 but UNRAR_BIN is missing"
            );
        }
        eprintln!(
            "skipping old RAR oracle comparison; set UNRAR_BIN to compare {} against local unrar",
            expected.join(", ")
        );
        return;
    };

    for fixture in expected {
        let archive_path = fixture_dir.join(fixture);
        let unrar_bytes = extract_old_rar_fixture_with_unrar(&unrar_bin, &archive_path);
        let weaver_bytes = extract_first_regular_member_with_weaver(&archive_path);
        assert_eq!(
            weaver_bytes,
            unrar_bytes,
            "Weaver extraction diverged from local unrar for {}",
            archive_path.display()
        );
    }
}

#[test]
fn test_rar4_encrypted_no_password_fails() {
    use aes::Aes128;
    use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    let salt: [u8; 8] = [0xAA; 8];
    let content = b"needs a password";
    let (key, iv) = weaver_unrar::crypto::rar4_derive_key("pass", Some(&salt));

    let padded_len = (content.len() + 15) & !15;
    let mut ciphertext = vec![0u8; padded_len];
    ciphertext[..content.len()].copy_from_slice(content);
    let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
    encryptor
        .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, padded_len)
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
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(vol)).unwrap();
    let result =
        archive.extract_by_name("locked.txt", &weaver_unrar::ExtractOptions::default(), None);
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
    append_stored_member_with_extra(&mut archive, filename, content, extra_area);

    // End of archive
    archive.extend_from_slice(&build_end_header(false));
    archive
}

fn build_rar5_comment_archive(comment_payload: &[u8], expected_crc: Option<u32>) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&RAR5_SIG);
    archive.extend_from_slice(&build_main_archive_header(0, None));

    let data_crc = expected_crc.or_else(|| {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(comment_payload);
        Some(hasher.finalize())
    });
    archive.extend_from_slice(&build_service_header_with_data(
        "CMT",
        comment_payload.len() as u64,
        comment_payload.len() as u64,
        data_crc,
        0,
        &[],
    ));
    archive.extend_from_slice(comment_payload);
    archive.extend_from_slice(&build_end_header(false));
    archive
}

fn build_two_volume_rar5_comment_archive(comment: &[u8], split_at: usize) -> (Vec<u8>, Vec<u8>) {
    build_two_volume_rar5_comment_archive_ex(comment, split_at, None)
}

fn build_two_volume_rar5_comment_archive_ex(
    comment: &[u8],
    split_at: usize,
    first_packed_crc_override: Option<u32>,
) -> (Vec<u8>, Vec<u8>) {
    let part1 = &comment[..split_at];
    let part2 = &comment[split_at..];
    let first_packed_crc = first_packed_crc_override.unwrap_or_else(|| crc32fast::hash(part1));
    let full_crc = crc32fast::hash(comment);

    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&RAR5_SIG);
    vol0.extend_from_slice(&build_main_archive_header(0x0001, None));
    vol0.extend_from_slice(&build_service_header_with_data_ex(
        "CMT",
        0x0010, // SPLIT_AFTER
        part1.len() as u64,
        comment.len() as u64,
        Some(first_packed_crc),
        0,
        &[],
    ));
    vol0.extend_from_slice(part1);
    vol0.extend_from_slice(&build_end_header(true));

    let mut vol1 = Vec::new();
    vol1.extend_from_slice(&RAR5_SIG);
    vol1.extend_from_slice(&build_main_archive_header(0x0001 | 0x0002, Some(1)));
    vol1.extend_from_slice(&build_service_header_with_data_ex(
        "CMT",
        0x0008, // SPLIT_BEFORE
        part2.len() as u64,
        comment.len() as u64,
        Some(full_crc),
        0,
        &[],
    ));
    vol1.extend_from_slice(part2);
    vol1.extend_from_slice(&build_end_header(false));

    (vol0, vol1)
}

fn append_stored_member_with_extra(
    archive: &mut Vec<u8>,
    filename: &str,
    content: &[u8],
    extra_area: &[u8],
) {
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
}

fn build_two_stored_members_with_extra(
    first_name: &str,
    first_content: &[u8],
    first_extra: &[u8],
    second_name: &str,
    second_content: &[u8],
    second_extra: &[u8],
) -> Vec<u8> {
    let mut archive = Vec::new();
    archive.extend_from_slice(&RAR5_SIG);
    archive.extend_from_slice(&build_main_archive_header(0, None));
    append_stored_member_with_extra(&mut archive, first_name, first_content, first_extra);
    append_stored_member_with_extra(&mut archive, second_name, second_content, second_extra);
    // End of archive
    archive.extend_from_slice(&build_end_header(false));
    archive
}

#[test]
fn test_rar5_comment_service_is_read() {
    let archive_bytes = build_rar5_comment_archive("hello from CMT".as_bytes(), None);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("hello from CMT")
    );
}

#[test]
fn test_rar5_split_comment_service_is_read() {
    let comment = b"RAR5 split CMT service comment across two volumes";
    let (vol0, vol1) = build_two_volume_rar5_comment_archive(comment, 19);
    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    assert!(archive.member_names().is_empty());
    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("RAR5 split CMT service comment across two volumes")
    );
}

#[test]
fn test_rar5_split_comment_pack_crc_mismatch_fails() {
    let comment = b"RAR5 split CMT service should verify first packed CRC";
    let split_at = 22;
    let wrong_first_packed_crc = crc32fast::hash(&comment[..split_at]) ^ 0xA5A5_5A5A;
    let (vol0, vol1) =
        build_two_volume_rar5_comment_archive_ex(comment, split_at, Some(wrong_first_packed_crc));
    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let err = archive.comment().unwrap_err();
    assert!(matches!(
        err,
        weaver_unrar::RarError::PackedDataCrcMismatch {
            member,
            volume: 0,
            expected,
            actual,
        } if member == "CMT" && expected == wrong_first_packed_crc && actual != expected
    ));
}

#[test]
fn test_rar5_comment_defaults_to_none() {
    let archive_bytes = build_stored_rar5_archive("plain.txt", b"plain");
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    assert!(archive.comment().unwrap().is_none());
}

#[test]
fn test_rar5_comment_crc_mismatch_is_rejected() {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(b"right");
    let archive_bytes = build_rar5_comment_archive(b"wr0ng", Some(hasher.finalize()));
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let err = archive.comment().unwrap_err();
    assert!(matches!(
        err,
        weaver_unrar::RarError::DataCrcMismatch { .. }
    ));
}

#[test]
fn test_file_encryption_propagated_to_member_info() {
    // Extra record type 0x01 = FILE_ENCRYPTION
    let extra = build_extra_record(0x01, &[0; 10]);
    let archive_bytes = build_stored_archive_with_extra("secret.txt", b"encrypted", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

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
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    match &meta.members[0].hash {
        Some(weaver_unrar::FileHash::Blake2sp(hash)) => {
            assert_eq!(hash, &expected_hash, "BLAKE2sp hash should match");
        }
        other => panic!("expected Some(Blake2sp(...)), got {other:?}"),
    }
}

#[test]
fn test_file_version_propagated_to_member_info() {
    let mut version_body = Vec::new();
    version_body.extend_from_slice(&encode_vint(0)); // flags
    version_body.extend_from_slice(&encode_vint(7)); // file version
    let extra = build_extra_record(0x04, &version_body);

    let archive_bytes = build_stored_archive_with_extra("versioned.txt", b"data", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    let member = &meta.members[0];
    assert_eq!(member.version, Some(7));
    assert_eq!(member.name, "versioned.txt;7");
}

#[test]
fn test_high_precision_file_times_propagated_to_member_info() {
    let mtime = 1_700_000_100u32;
    let ctime = 1_700_000_200u32;
    let atime = 1_700_000_300u32;
    let mut time_body = Vec::new();
    time_body.extend_from_slice(&encode_vint(0x01 | 0x02 | 0x04 | 0x08)); // Unix mtime/ctime/atime.
    time_body.extend_from_slice(&mtime.to_le_bytes());
    time_body.extend_from_slice(&ctime.to_le_bytes());
    time_body.extend_from_slice(&atime.to_le_bytes());
    let extra = build_extra_record(0x03, &time_body);

    let archive_bytes = build_stored_archive_with_extra("times.txt", b"data", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    let member = &meta.members[0];
    assert_eq!(
        member
            .mtime
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        u64::from(mtime)
    );
    assert_eq!(
        member
            .ctime
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        u64::from(ctime)
    );
    assert_eq!(
        member
            .atime
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        u64::from(atime)
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_restores_rar5_numeric_owner_when_enabled() {
    use std::os::unix::fs::MetadataExt;

    let uid = unsafe { libc::geteuid() } as u64;
    let gid = unsafe { libc::getegid() } as u64;
    let mut owner_body = Vec::new();
    owner_body.extend_from_slice(&encode_vint(0x04 | 0x08)); // numeric uid + gid
    owner_body.extend_from_slice(&encode_vint(uid));
    owner_body.extend_from_slice(&encode_vint(gid));
    let extra = build_extra_record(0x06, &owner_body);
    let archive_bytes = build_stored_archive_with_extra("owned.txt", b"owned bytes", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("owned.txt");

    let written = archive
        .extract_member_to_file(
            0,
            &weaver_unrar::ExtractOptions {
                restore_owners: true,
                ..Default::default()
            },
            None,
            &out_path,
        )
        .unwrap();

    assert_eq!(written, b"owned bytes".len() as u64);
    let metadata = std::fs::metadata(&out_path).unwrap();
    assert_eq!(u64::from(metadata.uid()), uid);
    assert_eq!(u64::from(metadata.gid()), gid);
}

#[test]
fn test_symlink_propagated_to_member_info() {
    // Extra record type 0x05 = REDIRECTION
    // Body: redir_type(vint) + flags(vint) + name_length(vint) + name
    let target = "usr/bin/target";
    let mut redir_body = Vec::new();
    redir_body.extend_from_slice(&encode_vint(1)); // UnixSymlink
    redir_body.extend_from_slice(&encode_vint(0)); // flags
    redir_body.extend_from_slice(&encode_vint(target.len() as u64));
    redir_body.extend_from_slice(target.as_bytes());
    let extra = build_extra_record(0x05, &redir_body);

    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

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
    assert!(!member.is_file_copy);
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
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

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
    assert!(!member.is_file_copy);
    assert_eq!(member.link_target.as_deref(), Some(target));
}

#[test]
fn test_filecopy_propagated_to_member_info() {
    let target = "original.txt";
    let extra = redirection_extra(5, target);
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    assert_eq!(meta.members.len(), 1);
    let member = &meta.members[0];
    assert!(!member.is_symlink);
    assert!(!member.is_hardlink);
    assert!(member.is_file_copy);
    assert_eq!(member.link_target.as_deref(), Some(target));
}

fn redirection_extra(redir_type: u64, target: &str) -> Vec<u8> {
    let mut redir_body = Vec::new();
    redir_body.extend_from_slice(&encode_vint(redir_type));
    redir_body.extend_from_slice(&encode_vint(0)); // flags
    redir_body.extend_from_slice(&encode_vint(target.len() as u64));
    redir_body.extend_from_slice(target.as_bytes());
    build_extra_record(0x05, &redir_body)
}

fn assert_unsupported_link_without_file_target(err: weaver_unrar::RarError, member: &str) {
    match err {
        weaver_unrar::RarError::UnsupportedLinkType {
            member: actual,
            link_type,
        } => {
            assert_eq!(actual, member);
            assert!(
                link_type.contains("requires extract_member_to_file"),
                "unexpected link type detail: {link_type}"
            );
        }
        other => panic!("expected UnsupportedLinkType, got {other:?}"),
    }
}

#[test]
fn test_extract_member_rejects_rar5_link_without_file_target() {
    let extra = redirection_extra(1, "target.txt");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();

    let err = archive
        .extract_member(0, &weaver_unrar::ExtractOptions::default(), None)
        .unwrap_err();

    assert_unsupported_link_without_file_target(err, "link");
}

#[test]
fn test_extract_member_streaming_rejects_rar5_link_without_file_target() {
    let extra = redirection_extra(4, "target.txt");
    let archive_bytes = build_stored_archive_with_extra("hardlink", b"", &extra);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(vec![std::path::PathBuf::from(
            "/not-needed-before-link-guard.rar",
        )]);
    let mut out = Vec::new();

    let err = archive
        .extract_member_streaming(
            0,
            &weaver_unrar::ExtractOptions::default(),
            &provider,
            &mut out,
        )
        .unwrap_err();

    assert!(out.is_empty());
    assert_unsupported_link_without_file_target(err, "hardlink");
}

#[test]
fn test_extract_member_streaming_chunked_rejects_rar5_link_without_file_target() {
    let extra = redirection_extra(5, "target.txt");
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(vec![std::path::PathBuf::from(
            "/not-needed-before-link-guard.rar",
        )]);
    let mut factory_called = false;

    let err = archive
        .extract_member_streaming_chunked(
            0,
            &weaver_unrar::ExtractOptions::default(),
            &provider,
            |_| {
                factory_called = true;
                Ok(Box::new(Vec::<u8>::new()) as Box<dyn std::io::Write>)
            },
        )
        .unwrap_err();

    assert!(!factory_called);
    assert_unsupported_link_without_file_target(err, "copy.txt");
}

#[test]
fn test_extract_member_to_file_restores_rar5_mtime() {
    let expected_mtime = 1_700_000_123;
    let archive_bytes = build_stored_rar5_archive_with_metadata(
        "timed.txt",
        b"timed bytes",
        0o644,
        Some(expected_mtime),
        1,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("timed.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"timed bytes".len() as u64);
    let metadata = std::fs::metadata(&out_path).unwrap();
    let actual_mtime = filetime::FileTime::from_last_modification_time(&metadata).unix_seconds();
    assert_eq!(actual_mtime, i64::from(expected_mtime));
}

#[test]
fn test_extract_member_to_file_creates_missing_parent_dirs_for_regular_file() {
    let archive_bytes = build_stored_archive_with_extra("dir/file.txt", b"nested bytes", &[]);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("dir").join("file.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"nested bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"nested bytes");
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_converts_symlink_parent_to_directory_like_unrar() {
    let archive_bytes =
        build_stored_archive_with_extra("dir/link/poc.txt", b"protected bytes", &[]);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let dir = temp_dir.path().join("dir");
    let link_parent = dir.join("link");
    let outside = temp_dir.path().join("outside");
    let out_path = link_parent.join("poc.txt");

    std::fs::create_dir(&dir).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink("../outside", &link_parent).unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"protected bytes".len() as u64);
    assert!(std::fs::symlink_metadata(&link_parent).unwrap().is_dir());
    assert_eq!(std::fs::read(&out_path).unwrap(), b"protected bytes");
    assert!(!outside.join("poc.txt").exists());
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_replaces_existing_output_symlink_like_unrar() {
    let archive_bytes = build_stored_archive_with_extra("victim.txt", b"archive bytes", &[]);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let outside_target = temp_dir.path().join("outside.txt");
    let out_path = temp_dir.path().join("victim.txt");

    std::fs::write(&outside_target, b"outside sentinel").unwrap();
    std::os::unix::fs::symlink(&outside_target, &out_path).unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"archive bytes".len() as u64);
    assert!(
        !std::fs::symlink_metadata(&out_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&out_path).unwrap(), b"archive bytes");
    assert_eq!(std::fs::read(&outside_target).unwrap(), b"outside sentinel");
}

#[test]
fn test_extract_member_to_file_restores_rar5_atime_without_overwriting_mtime() {
    let expected_atime = 1_600_000_123u32;
    let mut time_body = Vec::new();
    time_body.extend_from_slice(&encode_vint(0x01 | 0x08)); // Unix atime only.
    time_body.extend_from_slice(&expected_atime.to_le_bytes());
    let extra = build_extra_record(0x03, &time_body);
    let archive_bytes = build_stored_archive_with_extra("atime.txt", b"timed bytes", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("atime.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"timed bytes".len() as u64);
    let metadata = std::fs::metadata(&out_path).unwrap();
    let actual_atime = filetime::FileTime::from_last_access_time(&metadata).unix_seconds();
    let actual_mtime = filetime::FileTime::from_last_modification_time(&metadata).unix_seconds();
    assert_eq!(actual_atime, i64::from(expected_atime));
    assert_ne!(
        actual_mtime,
        i64::from(expected_atime),
        "an atime-only extra record must not be applied as mtime"
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_restores_rar5_unix_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let archive_bytes =
        build_stored_rar5_archive_with_metadata("mode.txt", b"mode bytes", 0o600, None, 1);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("mode.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"mode bytes".len() as u64);
    assert_eq!(
        std::fs::metadata(&out_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_maps_rar5_windows_readonly_to_unix_mode() {
    use std::os::unix::fs::PermissionsExt;

    let archive_bytes =
        build_stored_rar5_archive_with_metadata("readonly.txt", b"readonly bytes", 0x01, None, 0);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("readonly.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"readonly bytes".len() as u64);
    let mode = std::fs::metadata(&out_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode & 0o222, 0, "Windows readonly should clear write bits");
    assert_ne!(mode & 0o444, 0, "readonly file should remain readable");
}

#[test]
fn test_extract_member_to_file_creates_rar5_directory_with_metadata() {
    let expected_mtime = 1_700_000_321;
    let archive_bytes = build_directory_rar5_archive("dir", 0o750, Some(expected_mtime));
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("dir");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    let metadata = std::fs::metadata(&out_path).unwrap();
    assert!(metadata.is_dir());
    let actual_mtime = filetime::FileTime::from_last_modification_time(&metadata).unix_seconds();
    assert_eq!(actual_mtime, i64::from(expected_mtime));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(metadata.permissions().mode() & 0o777, 0o750);
    }
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_creates_rar5_symlink() {
    let extra = redirection_extra(1, "target.txt");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert!(
        std::fs::symlink_metadata(&out_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("target.txt")
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_creates_missing_parent_dirs_for_rar5_symlink() {
    let extra = redirection_extra(1, "../target.txt");
    let archive_bytes = build_stored_archive_with_extra("dir/link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("dir").join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("../target.txt")
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_restores_rar5_symlink_times() {
    let expected_mtime = 1_650_000_111u32;
    let expected_atime = 1_650_000_222u32;
    let mut time_body = Vec::new();
    time_body.extend_from_slice(&encode_vint(0x01 | 0x02 | 0x08)); // Unix mtime/atime.
    time_body.extend_from_slice(&expected_mtime.to_le_bytes());
    time_body.extend_from_slice(&expected_atime.to_le_bytes());

    let mut extra = build_extra_record(0x03, &time_body);
    extra.extend_from_slice(&redirection_extra(1, "target.txt"));
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    let metadata = std::fs::symlink_metadata(&out_path).unwrap();
    assert!(metadata.file_type().is_symlink());
    let actual_mtime = filetime::FileTime::from_last_modification_time(&metadata).unix_seconds();
    let actual_atime = filetime::FileTime::from_last_access_time(&metadata).unix_seconds();
    assert_eq!(actual_mtime, i64::from(expected_mtime));
    assert_eq!(actual_atime, i64::from(expected_atime));
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_preserves_rar5_unix_symlink_backslashes() {
    let extra = redirection_extra(1, "dir\\target.txt");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("dir\\target.txt")
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_normalizes_rar5_windows_symlink_target() {
    let extra = redirection_extra(2, "dir\\target.txt");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("dir/target.txt")
    );
}

#[test]
fn test_extract_member_to_file_rejects_unsafe_rar5_link_target() {
    let extra = redirection_extra(1, "/etc/passwd");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let err = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap_err();

    assert!(matches!(
        err,
        weaver_unrar::RarError::UnsafeLinkTarget { .. }
    ));
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_creates_unrar_style_windows_device_rar5_symlink() {
    let extra = redirection_extra(2, "\\??\\C:\\Windows\\system32");
    let archive_bytes = build_stored_archive_with_extra("link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("link");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("C:/Windows/system32")
    );
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_converts_symlink_parent_before_link_safety_like_unrar() {
    let extra = redirection_extra(1, "../target.txt");
    let archive_bytes = build_stored_archive_with_extra("linkdir/link", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let real_parent = temp_dir.path().join("real-linkdir");
    let symlink_parent = temp_dir.path().join("linkdir");
    let out_path = symlink_parent.join("link");
    std::fs::create_dir(&real_parent).unwrap();
    std::os::unix::fs::symlink(&real_parent, &symlink_parent).unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert!(std::fs::symlink_metadata(&symlink_parent).unwrap().is_dir());
    assert!(
        std::fs::symlink_metadata(&out_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_link(&out_path).unwrap(),
        std::path::Path::new("../target.txt")
    );
    assert!(!real_parent.join("link").exists());
}

#[test]
fn test_extract_member_to_file_creates_rar5_hardlink() {
    let extra = redirection_extra(4, "original.txt");
    let archive_bytes = build_stored_archive_with_extra("hardlink", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("hardlink");
    std::fs::write(&original, b"linked bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"linked bytes");
}

#[test]
fn test_extract_member_to_file_resolves_nested_rar5_hardlink_target_from_root() {
    let extra = redirection_extra(4, "original.txt");
    let archive_bytes = build_stored_archive_with_extra("dir/hardlink", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("dir").join("hardlink");
    std::fs::write(&original, b"linked bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"linked bytes");
}

#[test]
#[cfg(unix)]
fn test_extract_member_to_file_applies_rar5_hardlink_attributes() {
    use std::os::unix::fs::PermissionsExt;

    let extra = redirection_extra(4, "original.txt");
    let mut archive_bytes = Vec::new();
    archive_bytes.extend_from_slice(&RAR5_SIG);
    archive_bytes.extend_from_slice(&build_main_archive_header(0, None));
    archive_bytes.extend_from_slice(&build_file_header_ex_with_file_flags_and_attributes(
        "hardlink", 0, 0, 0, None, 0, 0, 0o600, &extra,
    ));
    archive_bytes.extend_from_slice(&build_end_header(false));

    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("hardlink");
    std::fs::write(&original, b"linked bytes").unwrap();
    let mut original_permissions = std::fs::metadata(&original).unwrap().permissions();
    original_permissions.set_mode(0o644);
    std::fs::set_permissions(&original, original_permissions).unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"linked bytes");
    assert_eq!(
        std::fs::metadata(&out_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(&original).unwrap().permissions().mode() & 0o777,
        0o600,
        "hardlink attribute restore should affect the shared inode"
    );
}

#[test]
fn test_extract_member_to_file_normalizes_rar5_hardlink_target() {
    let extra = redirection_extra(4, "dir\\original.txt");
    let archive_bytes = build_stored_archive_with_extra("hardlink", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let dir = temp_dir.path().join("dir");
    std::fs::create_dir(&dir).unwrap();
    let original = dir.join("original.txt");
    let out_path = temp_dir.path().join("hardlink");
    std::fs::write(&original, b"linked bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, 0);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"linked bytes");
}

#[test]
fn test_extract_member_to_file_copies_existing_rar5_filecopy_source() {
    let extra = redirection_extra(5, "original.txt");
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("copy.txt");
    std::fs::write(&original, b"copied bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"copied bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"copied bytes");
}

#[test]
fn test_extract_member_to_file_resolves_nested_rar5_filecopy_target_from_root() {
    let extra = redirection_extra(5, "original.txt");
    let archive_bytes = build_stored_archive_with_extra("dir/copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("dir").join("copy.txt");
    std::fs::write(&original, b"copied bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"copied bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"copied bytes");
}

#[test]
fn test_extract_member_to_file_applies_rar5_filecopy_metadata() {
    let expected_mtime = 1_675_000_123u32;
    let mut time_body = Vec::new();
    time_body.extend_from_slice(&encode_vint(0x01 | 0x02)); // Unix mtime.
    time_body.extend_from_slice(&expected_mtime.to_le_bytes());

    let mut extra = build_extra_record(0x03, &time_body);
    extra.extend_from_slice(&redirection_extra(5, "original.txt"));
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("copy.txt");
    std::fs::write(&original, b"copied bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"copied bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"copied bytes");
    let actual_mtime =
        filetime::FileTime::from_last_modification_time(&std::fs::metadata(&out_path).unwrap())
            .unix_seconds();
    assert_eq!(actual_mtime, i64::from(expected_mtime));
}

#[test]
fn test_extract_member_to_file_normalizes_existing_rar5_filecopy_source() {
    let extra = redirection_extra(5, "dir\\original.txt");
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let dir = temp_dir.path().join("dir");
    std::fs::create_dir(&dir).unwrap();
    let original = dir.join("original.txt");
    let out_path = temp_dir.path().join("copy.txt");
    std::fs::write(&original, b"copied bytes").unwrap();

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"copied bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"copied bytes");
}

#[test]
fn test_extract_member_to_file_extracts_archive_filecopy_source() {
    let extra = redirection_extra(5, "original.txt");
    let archive_bytes = build_two_stored_members_with_extra(
        "original.txt",
        b"archive source",
        &[],
        "copy.txt",
        b"",
        &extra,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    let written = archive
        .extract_member_to_file(1, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"archive source".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"archive source");
}

#[test]
fn test_extract_member_to_file_uses_later_rar5_filecopy_source_like_unrar_reflist() {
    let extra = redirection_extra(5, "original.txt");
    let archive_bytes = build_two_stored_members_with_extra(
        "copy.txt",
        b"",
        &extra,
        "original.txt",
        b"future archive source",
        &[],
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    // UnRAR registers the file-copy reference in RefList, extracts a later
    // source member to a temporary file, then copies/moves it to the target.
    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"future archive source".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"future archive source");
}

#[test]
fn test_extract_member_to_file_copies_archive_hardlink_source_like_unrar() {
    let hardlink_extra = redirection_extra(4, "original.txt");
    let filecopy_extra = redirection_extra(5, "source-link");
    let archive_bytes = build_two_stored_members_with_extra(
        "source-link",
        b"",
        &hardlink_extra,
        "copy.txt",
        b"",
        &filecopy_extra,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let original = temp_dir.path().join("original.txt");
    let out_path = temp_dir.path().join("copy.txt");
    std::fs::write(&original, b"hardlink source bytes").unwrap();

    let written = archive
        .extract_member_to_file(1, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"hardlink source bytes".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"hardlink source bytes");
}

#[test]
fn test_extract_member_to_file_normalizes_archive_rar5_filecopy_source() {
    let extra = redirection_extra(5, "dir\\original.txt");
    let archive_bytes = build_two_stored_members_with_extra(
        "dir/original.txt",
        b"archive source",
        &[],
        "copy.txt",
        b"",
        &extra,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    let written = archive
        .extract_member_to_file(1, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"archive source".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"archive source");
}

#[test]
fn test_extract_member_to_file_sanitizes_absolute_rar5_filecopy_target_like_unrar() {
    let extra = redirection_extra(5, "/etc/passwd");
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let normalized_source = temp_dir.path().join("etc/passwd");
    std::fs::create_dir_all(normalized_source.parent().unwrap()).unwrap();
    std::fs::write(&normalized_source, b"safe normalized source").unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    let written = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap();

    assert_eq!(written, b"safe normalized source".len() as u64);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"safe normalized source");
}

#[test]
fn test_extract_member_to_file_rejects_missing_rar5_filecopy_source() {
    let extra = redirection_extra(5, "missing.txt");
    let archive_bytes = build_stored_archive_with_extra("copy.txt", b"", &extra);
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    let err = archive
        .extract_member_to_file(0, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap_err();

    assert!(
        matches!(err, weaver_unrar::RarError::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound)
    );
}

#[test]
fn test_extract_member_to_file_rejects_rar5_filecopy_cycle() {
    let first_extra = redirection_extra(5, "copy.txt");
    let second_extra = redirection_extra(5, "source.txt");
    let archive_bytes = build_two_stored_members_with_extra(
        "source.txt",
        b"",
        &first_extra,
        "copy.txt",
        b"",
        &second_extra,
    );
    let cursor = Cursor::new(archive_bytes);
    let mut archive = weaver_unrar::RarArchive::open(cursor).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("copy.txt");

    let err = archive
        .extract_member_to_file(1, &weaver_unrar::ExtractOptions::default(), None, &out_path)
        .unwrap_err();

    assert!(matches!(
        err,
        weaver_unrar::RarError::UnsupportedLinkType { .. }
    ));
}

#[test]
fn test_no_extra_records_defaults() {
    // A normal file without extra records should have default values.
    let content = b"plain file";
    let archive_bytes = build_stored_rar5_archive("plain.txt", content);

    let cursor = Cursor::new(archive_bytes);
    let archive = weaver_unrar::RarArchive::open(cursor).unwrap();

    let meta = archive.metadata();
    let member = &meta.members[0];
    assert!(!member.is_encrypted, "is_encrypted should default to false");
    assert!(member.hash.is_none(), "hash should default to None");
    assert!(!member.is_symlink, "is_symlink should default to false");
    assert!(!member.is_hardlink, "is_hardlink should default to false");
    assert!(member.ctime.is_none(), "ctime should default to None");
    assert!(member.atime.is_none(), "atime should default to None");
    assert!(member.version.is_none(), "version should default to None");
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
//     cargo test -p weaver-unrar --release --features slow-tests
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

fn open_single(dir: &str, filename: &str) -> weaver_unrar::RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    weaver_unrar::RarArchive::open(Cursor::new(data)).unwrap()
}

fn open_single_with_password(
    dir: &str,
    filename: &str,
    password: &str,
) -> weaver_unrar::RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    weaver_unrar::RarArchive::open_with_password(Cursor::new(data), password).unwrap()
}

fn open_single_file_with_password(
    dir: &str,
    filename: &str,
    password: &str,
) -> weaver_unrar::RarArchive {
    let file = std::fs::File::open(fixture(dir, filename)).unwrap();
    weaver_unrar::RarArchive::open_with_password(file, password).unwrap()
}

fn open_multi(dir: &str, filenames: &[&str]) -> weaver_unrar::RarArchive {
    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> = filenames
        .iter()
        .map(|f| {
            let data = std::fs::read(fixture(dir, f)).unwrap();
            Box::new(Cursor::new(data)) as Box<dyn weaver_unrar::ReadSeek>
        })
        .collect();
    weaver_unrar::RarArchive::open_volumes(readers).unwrap()
}

fn streaming_prefix_len_with_available_volumes(
    dir: &str,
    filenames: &[&str],
    available_volumes: usize,
    password: Option<&str>,
) -> usize {
    let mut archive = open_multi(dir, filenames);
    if let Some(password) = password {
        archive.set_password(password);
    }

    let paths: Vec<_> = filenames.iter().map(|name| fixture(dir, name)).collect();
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(paths[..available_volumes].to_vec());
    let options = weaver_unrar::ExtractOptions {
        verify: false,
        password: password.map(str::to_owned),
        restore_owners: false,
    };
    let mut out = Vec::new();
    let result = archive.extract_member_streaming(0, &options, &provider, &mut out);

    if available_volumes == filenames.len() {
        result.unwrap();
    } else {
        assert!(result.is_err());
    }

    out.len()
}

// -- RAR5 unencrypted (fast) --------------------------------------------------

#[test]
fn test_rar5_fixture_store_batch() {
    let mut archive = open_single("rar5", "rar5_store.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
fn test_rar5_fixture_store_streaming() {
    let mut archive = open_single("rar5", "rar5_store.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_store.rar")]);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("small.txt"));
}

#[test]
fn test_rar5_fixture_lz_batch() {
    let mut archive = open_single("rar5", "rar5_lz.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
fn test_rar5_fixture_lz_streaming() {
    let mut archive = open_single("rar5", "rar5_lz.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_lz.rar")]);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
fn test_rar4_fixture_lz_batch() {
    let mut archive = open_single("rar4", "rar4_lz.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("test_clip.mkv"));
}

#[test]
fn test_rar5_fixture_multivolume_video_chunked_matches_prefix_deltas() {
    let vol_names = [
        "rar5_mv_video.part1.rar",
        "rar5_mv_video.part2.rar",
        "rar5_mv_video.part3.rar",
        "rar5_mv_video.part4.rar",
        "rar5_mv_video.part5.rar",
    ];
    let mut archive = open_multi("rar5", &vol_names);
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|name| fixture("rar5", name)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths.clone());

    let chunk_dir = tempfile::tempdir().unwrap();
    let mut chunk_paths = std::collections::BTreeMap::new();
    let chunk_records = archive
        .extract_member_streaming_chunked(0, &options, &provider, |volume_index| {
            let path = chunk_dir.path().join(format!("vol{volume_index:03}.chunk"));
            chunk_paths.insert(volume_index, path.clone());
            std::fs::File::create(path)
                .map(|file| Box::new(file) as Box<dyn std::io::Write>)
                .map_err(weaver_unrar::RarError::Io)
        })
        .unwrap();

    let mut actual_sizes = vec![0u64; vol_names.len()];
    for (volume_index, bytes_written) in chunk_records {
        actual_sizes[volume_index] = bytes_written;
        let data = std::fs::read(chunk_paths.get(&volume_index).unwrap()).unwrap();
        assert_eq!(data.len() as u64, bytes_written);
    }

    let mut prefix_lengths = Vec::with_capacity(vol_names.len());
    for available_volumes in 1..=vol_names.len() {
        prefix_lengths.push(streaming_prefix_len_with_available_volumes(
            "rar5",
            &vol_names,
            available_volumes,
            None,
        ));
    }

    let mut expected_sizes = Vec::with_capacity(vol_names.len());
    let mut previous_prefix = 0usize;
    for prefix_len in prefix_lengths {
        expected_sizes.push((prefix_len - previous_prefix) as u64);
        previous_prefix = prefix_len;
    }

    assert_eq!(actual_sizes, expected_sizes);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("test_clip.mkv"));
}

// -- RAR5 encrypted (slow — gated behind `slow-tests` feature) ----------------

#[test]
fn test_rar5_encrypted_no_password_fails() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_store_streaming() {
    let mut archive = open_single("rar5", "rar5_enc_store.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture(
        "rar5",
        "rar5_enc_store.rar",
    )]);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("compressible.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar5_encrypted_lz_streaming() {
    let mut archive = open_single("rar5", "rar5_enc_lz.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let provider =
        weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture("rar5", "rar5_enc_lz.rar")]);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
    let mut out = Vec::new();
    archive
        .extract_member_streaming(0, &opts, &provider, &mut out)
        .unwrap();
    assert_eq!(out, original("binary.bin"));
}

#[test]
fn test_cached_headers_resume_encrypted_out_of_order_multivolume_topology() {
    let first = std::fs::File::open(fixture("rar5", "rar5_enc_mv_store.part1.rar")).unwrap();
    let archive = weaver_unrar::RarArchive::open_with_password(first, TEST_PASSWORD).unwrap();
    let headers = archive.serialize_headers();

    let mut restored = weaver_unrar::RarArchive::deserialize_headers_with_password(
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
    let archive = weaver_unrar::RarArchive::open_with_password(first, TEST_PASSWORD).unwrap();
    let headers = archive.serialize_headers();

    let mut restored = weaver_unrar::RarArchive::deserialize_headers_with_password(
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("wrongpassword".into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None);
    assert!(result.is_err(), "wrong password should fail");
}

// -- RAR4 encrypted (slow — gated behind `slow-tests` feature) ----------------

#[test]
fn test_rar4_encrypted_no_password_fails_fixture() {
    let mut archive = open_single("rar4", "rar4_enc_store.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None);
    assert!(
        result.is_err(),
        "no password should fail for encrypted archive"
    );
}

#[test]
fn test_rar4_hp_encrypted_store() {
    let mut archive = open_single_with_password("rar4", "rar4_hp_store.rar", "secretpass");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("secretpass".into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"This is a test file for RAR4 header encryption.\n");
}

#[test]
fn test_rar4_hp_encrypted_lz() {
    let mut archive = open_single_with_password("rar4", "rar4_hp_lz.rar", "secretpass");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("secretpass".into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"This is a test file for RAR4 header encryption.\n");
}

#[test]
fn test_rar4_hp_encrypted_large() {
    let path = fixture("rar4", "rar4_hp_large.rar");
    if !path.exists() {
        eprintln!("skipping: rar4_hp_large.rar not present");
        return;
    }
    let mut archive = open_single_with_password("rar4", "rar4_hp_large.rar", "e2e-test-password");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };
    archive.extract_member(0, &opts, None).unwrap();
}

#[test]
#[ignore = "requires rar4/rar4_hp_long_password.rar generated by tests/fixtures/generate_edge_cases.sh"]
fn test_rar4_hp_long_password_fixture_gate() {
    let path = fixture("rar4", "rar4_hp_long_password.rar");
    assert!(
        path.exists(),
        "missing real RAR4 long-password fixture; generate with engines/weaver-unrar/tests/fixtures/generate_edge_cases.sh"
    );

    let password = "abcdefghijklmnopqrstuvwxyzabcdef";
    let mut archive = open_single_with_password("rar4", "rar4_hp_long_password.rar", password);
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(password.into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"RAR4 long password KDF fixture\n");
}

// -- RAR5 header-encrypted (-hp) fixtures ---------------------------------

#[test]
fn test_rar5_hp_encrypted_store() {
    let mut archive = open_single_with_password("rar5", "rar5_hp_store.rar", "secretpass");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("secretpass".into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"This is a test file for RAR4 header encryption.\n");
}

#[test]
fn test_rar5_hp_encrypted_lz() {
    let mut archive = open_single_with_password("rar5", "rar5_hp_lz.rar", "secretpass");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("secretpass".into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, b"This is a test file for RAR4 header encryption.\n");
}

#[test]
fn test_rar5_hp_encrypted_large() {
    let path = fixture("rar5", "rar5_hp_large.rar");
    if !path.exists() {
        eprintln!("skipping: rar5_hp_large.rar not present");
        return;
    }
    let mut archive = open_single_with_password("rar5", "rar5_hp_large.rar", "e2e-test-password");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };
    archive.extract_member(0, &opts, None).unwrap();
}

#[test]
fn test_rar5_hp_encrypted_large_to_file() {
    let path = fixture("rar5", "rar5_hp_large.rar");
    if !path.exists() {
        eprintln!("skipping: rar5_hp_large.rar not present");
        return;
    }
    let mut archive = open_single_with_password("rar5", "rar5_hp_large.rar", "e2e-test-password");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("sample.mkv");
    archive
        .extract_member_to_file(0, &opts, None, &out_path)
        .unwrap();
}

#[test]
fn test_rar5_hp_encrypted_large_disk_open_to_file() {
    let path = fixture("rar5", "rar5_hp_large.rar");
    if !path.exists() {
        eprintln!("skipping: rar5_hp_large.rar not present");
        return;
    }
    let mut archive =
        open_single_file_with_password("rar5", "rar5_hp_large.rar", "e2e-test-password");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };
    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("sample.mkv");
    archive
        .extract_member_to_file(0, &opts, None, &out_path)
        .unwrap();
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_store_batch() {
    let mut archive = open_single("rar4", "rar4_enc_store.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("small.txt"));
}

#[test]
#[cfg(feature = "slow-tests")]
fn test_rar4_encrypted_lz_batch() {
    let mut archive = open_single("rar4", "rar4_enc_lz.rar");
    archive.set_password(TEST_PASSWORD);
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some(TEST_PASSWORD.into()),
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("日本語ファイル.txt"));

    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("café-résumé.txt"));
}

#[test]
fn test_rar5_unicode_cjk_filename() {
    let archive = open_single("rar5", "rar5_unicode_cjk.rar");
    let names = archive.member_names();
    assert_eq!(names.len(), 1);
    assert!(
        names[0].contains("映画テスト"),
        "got mangled name: {}",
        names[0]
    );
}

#[test]
fn test_rar5_unicode_cjk_extract() {
    let mut archive = open_single("rar5", "rar5_unicode_cjk.rar");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let index = archive
        .member_names()
        .iter()
        .position(|name| name.contains("映画テスト"))
        .expect("expected CJK filename in archive");
    let expected_size = archive.member_info(index).unwrap().unpacked_size.unwrap();
    let data = archive.extract_member(index, &opts, None).unwrap();
    assert_eq!(data.len() as u64, expected_size);
    assert!(
        !data.is_empty(),
        "CJK-named member should extract successfully"
    );
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    assert_eq!(names, vec!["sample.mkv", "file1.txt", "file2.txt"]);
}

#[test]
fn test_rar5_solid_chunked_extraction_preserves_solid_continuation() {
    let fixture = fixture("rar5", "rar5_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_first = expected_archive.extract_member(0, &options, None).unwrap();
    let expected_second = expected_archive.extract_member(1, &options, None).unwrap();

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();

    let first_chunk_dir = temp_dir.path().join("first");
    let first_chunks = archive
        .extract_member_solid_chunked(0, &options, |volume_index| {
            let path = first_chunk_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
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
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
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
fn test_rar5_solid_streaming_extracts_all_members_sequentially() {
    let fixture = fixture("rar5", "rar5_solid.rar");
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture.clone()]);
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_first = expected_archive.extract_member(0, &options, None).unwrap();
    let expected_second = expected_archive.extract_member(1, &options, None).unwrap();

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let mut first = Vec::new();
    archive
        .extract_member_streaming(0, &options, &provider, &mut first)
        .unwrap();
    assert_eq!(first, expected_first);

    let mut second = Vec::new();
    archive
        .extract_member_streaming(1, &options, &provider, &mut second)
        .unwrap();
    assert_eq!(second, expected_second);
}

#[test]
fn test_rar5_solid_streaming_chunked_preserves_solid_continuation() {
    let fixture = fixture("rar5", "rar5_solid.rar");
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture.clone()]);
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_first = expected_archive.extract_member(0, &options, None).unwrap();
    let expected_second = expected_archive.extract_member(1, &options, None).unwrap();

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();

    let first_dir = temp_dir.path().join("streaming-first");
    let first_chunks = archive
        .extract_member_streaming_chunked(0, &options, &provider, |volume_index| {
            let path = first_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
            Ok(Box::new(file))
        })
        .unwrap();
    let mut actual_first = Vec::new();
    for (volume_index, bytes_written) in &first_chunks {
        let data = std::fs::read(first_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
        assert_eq!(data.len() as u64, *bytes_written);
        actual_first.extend_from_slice(&data);
    }
    assert_eq!(actual_first, expected_first);

    let second_dir = temp_dir.path().join("streaming-second");
    let second_chunks = archive
        .extract_member_streaming_chunked(1, &options, &provider, |volume_index| {
            let path = second_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
            Ok(Box::new(file))
        })
        .unwrap();
    let mut actual_second = Vec::new();
    for (volume_index, bytes_written) in &second_chunks {
        let data = std::fs::read(second_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
        assert_eq!(data.len() as u64, *bytes_written);
        actual_second.extend_from_slice(&data);
    }
    assert_eq!(actual_second, expected_second);
}

#[test]
fn test_rar5_solid_reopen_archive_for_later_member() {
    let fixture = fixture("rar5", "rar5_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_second = expected_archive
        .extract_member(1, &options, None)
        .unwrap()
        .to_bytes()
        .unwrap();

    let mut reopened_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let actual_second = reopened_archive.extract_member(1, &options, None).unwrap();

    assert_eq!(actual_second, expected_second);
}

#[test]
fn test_rar5_solid_encrypted_extract_all_members_sequentially() {
    let fixture = fixture("rar5", "rar5_solid_encrypted.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };

    let mut archive = weaver_unrar::RarArchive::open_with_password(
        std::fs::File::open(&fixture).unwrap(),
        "e2e-test-password",
    )
    .unwrap();

    let names = archive.member_names();
    assert_eq!(names.len(), 2);

    let first = archive.extract_member(0, &options, None).unwrap();
    assert!(
        !first.is_empty(),
        "first encrypted solid member should extract"
    );

    let second = archive.extract_member(1, &options, None).unwrap();
    let second_info = archive.member_info(1).unwrap();
    assert_eq!(second.len() as u64, second_info.unpacked_size.unwrap());
    assert!(
        !second.is_empty(),
        "second encrypted solid member should extract"
    );
}

#[test]
fn test_rar5_solid_encrypted_chunked_preserves_solid_continuation() {
    let fixture = fixture("rar5", "rar5_solid_encrypted.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("e2e-test-password".into()),
        restore_owners: false,
    };

    let mut expected_archive = weaver_unrar::RarArchive::open_with_password(
        std::fs::File::open(&fixture).unwrap(),
        "e2e-test-password",
    )
    .unwrap();
    let expected_first = expected_archive.extract_member(0, &options, None).unwrap();
    let expected_second = expected_archive.extract_member(1, &options, None).unwrap();

    let mut archive = weaver_unrar::RarArchive::open_with_password(
        std::fs::File::open(&fixture).unwrap(),
        "e2e-test-password",
    )
    .unwrap();
    let temp_dir = tempfile::tempdir().unwrap();

    let first_chunk_dir = temp_dir.path().join("first");
    let first_chunks = archive
        .extract_member_solid_chunked(0, &options, |volume_index| {
            let path = first_chunk_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
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
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
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
    assert_eq!(names, vec!["sample.mkv", "file1.txt", "file2.txt"]);
}

#[test]
fn test_rar4_solid_reopen_archive_for_later_member() {
    let fixture = fixture("rar4", "rar4_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_second = expected_archive
        .extract_member(1, &options, None)
        .unwrap()
        .to_bytes()
        .unwrap();

    let mut reopened_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let actual_second = reopened_archive.extract_member(1, &options, None).unwrap();

    assert_eq!(actual_second, expected_second);
}

#[test]
fn test_rar4_solid_extracts_all_members_sequentially() {
    let fixture = fixture("rar4", "rar4_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let metadata = archive.metadata().clone();

    for member_index in 0..metadata.members.len() {
        let extracted = archive
            .extract_member(member_index, &options, None)
            .unwrap();
        let expected_size = metadata.members[member_index].unpacked_size.unwrap_or(0);
        assert_eq!(
            extracted.len() as u64,
            expected_size,
            "member index {member_index}"
        );
    }
}

#[test]
fn test_rar4_solid_chunked_extraction_preserves_solid_continuation() {
    let fixture = fixture("rar4", "rar4_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let metadata = expected_archive.metadata().clone();
    let expected_outputs = (0..metadata.members.len())
        .map(|member_index| {
            expected_archive
                .extract_member(member_index, &options, None)
                .unwrap()
                .to_bytes()
                .unwrap()
        })
        .collect::<Vec<_>>();

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let temp_dir = tempfile::tempdir().unwrap();

    for (member_index, expected) in expected_outputs.iter().enumerate() {
        let member_dir = temp_dir.path().join(format!("member-{member_index}"));
        let chunks = archive
            .extract_member_solid_chunked(member_index, &options, |volume_index| {
                let path = member_dir.join(format!("{volume_index:05}.chunk"));
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
                }
                let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
                Ok(Box::new(file))
            })
            .unwrap();
        assert!(!chunks.is_empty(), "member index {member_index}");

        let mut actual = Vec::new();
        for (volume_index, bytes_written) in &chunks {
            let data = std::fs::read(member_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
            assert_eq!(
                data.len() as u64,
                *bytes_written,
                "member index {member_index}"
            );
            actual.extend_from_slice(&data);
        }
        assert_eq!(actual, *expected, "member index {member_index}");
    }
}

#[test]
fn test_rar4_solid_streaming_extracts_all_members_sequentially() {
    let fixture = fixture("rar4", "rar4_solid.rar");
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![fixture.clone()]);
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let metadata = expected_archive.metadata().clone();
    let expected_outputs = (0..metadata.members.len())
        .map(|member_index| {
            expected_archive
                .extract_member(member_index, &options, None)
                .unwrap()
                .to_bytes()
                .unwrap()
        })
        .collect::<Vec<_>>();

    let mut archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    for (member_index, expected) in expected_outputs.iter().enumerate() {
        let mut actual = Vec::new();
        archive
            .extract_member_streaming(member_index, &options, &provider, &mut actual)
            .unwrap();
        assert_eq!(actual, *expected, "member index {member_index}");
    }
}

#[test]
fn test_rar4_solid_chunked_reopen_from_cached_headers_for_later_member() {
    let fixture = fixture("rar4", "rar4_solid.rar");
    let options = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let mut expected_archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let expected_second = expected_archive
        .extract_member(1, &options, None)
        .unwrap()
        .to_bytes()
        .unwrap();

    let archive = weaver_unrar::RarArchive::open(std::fs::File::open(&fixture).unwrap()).unwrap();
    let headers = archive.serialize_headers();
    let mut reopened =
        weaver_unrar::RarArchive::deserialize_headers_with_password(&headers, None::<String>)
            .unwrap();
    reopened.attach_volume_reader(0, Box::new(std::fs::File::open(&fixture).unwrap()));

    let temp_dir = tempfile::tempdir().unwrap();
    let member_dir = temp_dir.path().join("second");
    let chunks = reopened
        .extract_member_solid_chunked(1, &options, |volume_index| {
            let path = member_dir.join(format!("{volume_index:05}.chunk"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(weaver_unrar::RarError::Io)?;
            }
            let file = std::fs::File::create(&path).map_err(weaver_unrar::RarError::Io)?;
            Ok(Box::new(file))
        })
        .unwrap();

    let mut actual_second = Vec::new();
    for (volume_index, bytes_written) in &chunks {
        let data = std::fs::read(member_dir.join(format!("{volume_index:05}.chunk"))).unwrap();
        assert_eq!(data.len() as u64, *bytes_written);
        actual_second.extend_from_slice(&data);
    }
    assert_eq!(actual_second, expected_second);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let r0 = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(r0, original("hello.txt"));
    let r1 = archive.extract_member(1, &opts, None).unwrap();
    assert_eq!(r1, original("second.txt"));
}

#[test]
fn test_rar5_recovery_volumes_restore_missing_part() {
    let temp_dir = tempfile::tempdir().unwrap();
    let present = [
        "rar5_recovery_volumes.part01.rar",
        "rar5_recovery_volumes.part02.rar",
        "rar5_recovery_volumes.part03.rar",
        "rar5_recovery_volumes.part04.rar",
        "rar5_recovery_volumes.part06.rar",
        "rar5_recovery_volumes.part07.rar",
        "rar5_recovery_volumes.part08.rar",
        "rar5_recovery_volumes.part09.rar",
        "rar5_recovery_volumes.part10.rar",
        "rar5_recovery_volumes.part01.rev",
        "rar5_recovery_volumes.part02.rev",
    ];

    let mut paths = Vec::new();
    for name in present {
        let src = fixture("rar5", name);
        let dst = temp_dir.path().join(name);
        std::fs::copy(src, &dst).unwrap();
        paths.push(dst);
    }

    let expected_path = temp_dir.path().join("rar5_recovery_volumes.part05.rar");
    let options = weaver_unrar::RecoveryOptions {
        output_dir: Some(temp_dir.path().to_path_buf()),
        overwrite_existing: false,
        verify_restored: true,
    };
    let report = weaver_unrar::restore_volumes_from_paths(&paths, &options).unwrap();

    assert_eq!(report.format, weaver_unrar::ArchiveFormat::Rar5);
    assert_eq!(report.missing_volume_numbers, vec![4]);
    assert_eq!(report.restored_paths, vec![expected_path.clone()]);
    assert_eq!(report.used_recovery_paths.len(), 2);

    let expected = std::fs::read(fixture("rar5", "rar5_recovery_volumes.part05.rar")).unwrap();
    let restored = std::fs::read(expected_path).unwrap();
    assert_eq!(restored, expected);
}

#[test]
fn test_rar3_recovery_volumes_restore_missing_part() {
    let temp_dir = tempfile::tempdir().unwrap();
    let present = [
        "rar3_recovery_volumes.part1.rar",
        "rar3_recovery_volumes.part2.rar",
        "rar3_recovery_volumes.part4.rar",
        "rar3_recovery_volumes.part5.rar",
        "rar3_recovery_volumes.part1.rev",
        "rar3_recovery_volumes.part2.rev",
    ];

    let mut paths = Vec::new();
    for name in present {
        let src = fixture("rar4", name);
        let dst = temp_dir.path().join(name);
        std::fs::copy(src, &dst).unwrap();
        paths.push(dst);
    }

    let expected_path = temp_dir.path().join("rar3_recovery_volumes.part3.rar");
    let options = weaver_unrar::RecoveryOptions {
        output_dir: Some(temp_dir.path().to_path_buf()),
        overwrite_existing: false,
        verify_restored: true,
    };
    let report = weaver_unrar::restore_volumes_from_paths(&paths, &options).unwrap();

    assert_eq!(report.format, weaver_unrar::ArchiveFormat::Rar4);
    assert_eq!(report.missing_volume_numbers, vec![2]);
    assert_eq!(report.restored_paths, vec![expected_path.clone()]);
    assert_eq!(report.used_recovery_paths.len(), 2);

    let expected = std::fs::read(fixture("rar4", "rar3_recovery_volumes.part3.rar")).unwrap();
    let restored = std::fs::read(expected_path).unwrap();
    assert_eq!(restored, expected);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let result = archive.extract_member(0, &opts, None).unwrap();
    assert_eq!(result, original("hello.txt"));
}

#[test]
fn test_rar4_modern_comment_service_is_read() {
    let comment = b"RAR4 modern CMT service comment";
    let crc = crc32fast::hash(comment);
    let archive_bytes = build_rar4_comment_service_archive(comment, crc);

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    assert!(archive.member_names().is_empty());
    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("RAR4 modern CMT service comment")
    );
}

#[test]
fn test_rar4_unicode_comment_service_uses_raw_utf16le() {
    let archive_bytes = build_rar4_unicode_comment_service_archive("Résumé 日本語 comment");

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    assert!(archive.member_names().is_empty());
    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("Résumé 日本語 comment")
    );
}

#[test]
fn test_rar4_old_embedded_comment_is_read() {
    let comment = b"RAR4 old embedded archive comment";
    let crc16 = (crc32fast::hash(comment) & 0xffff) as u16;
    let archive_bytes = build_rar4_old_comment_archive(comment, crc16);

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    assert!(archive.member_names().is_empty());
    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("RAR4 old embedded archive comment")
    );
}

#[test]
fn test_rar4_old_compressed_comment_uses_rar20_decoder() {
    let rar20 = std::fs::read(fixture("rar4", "rar20_lz.rar")).unwrap();
    let (packed, unpacked_size, unpack_version, method) = first_rar4_file_payload(&rar20);
    assert_eq!(unpack_version, 20);
    assert_eq!(method, 0x35);

    let expected = original("boat_modern_english.wav");
    assert_eq!(expected.len(), unpacked_size as usize);
    let crc16 = (crc32fast::hash(&expected) & 0xffff) as u16;
    let archive_bytes = build_rar4_old_compressed_comment_archive(
        &packed,
        unpacked_size,
        unpack_version,
        method,
        crc16,
    );

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    let comment = archive.comment().unwrap().unwrap();
    assert!(
        comment.starts_with("RIFF"),
        "decoded WAV comment should begin with RIFF, got {comment:?}"
    );
}

#[test]
fn test_rar4_old_embedded_comment_crc_mismatch_is_rejected() {
    let comment = b"RAR4 old embedded comment with bad CRC";
    let archive_bytes = build_rar4_old_comment_archive(comment, 0xBEEF);

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    let err = archive.comment().unwrap_err();
    assert!(
        matches!(err, weaver_unrar::RarError::DataCrcMismatch { .. }),
        "expected DataCrcMismatch, got {err:?}"
    );
}

#[test]
fn test_rar4_split_modern_comment_service_is_read() {
    let comment = b"RAR4 split modern CMT service comment across volumes";
    let (vol0, vol1) = build_two_volume_rar4_comment_service_archive(comment, 22);
    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    assert!(archive.member_names().is_empty());
    assert_eq!(
        archive.comment().unwrap().as_deref(),
        Some("RAR4 split modern CMT service comment across volumes")
    );
}

#[test]
fn test_rar4_split_modern_comment_pack_crc_mismatch_fails() {
    let comment = b"RAR4 split modern CMT service should verify first packed CRC";
    let split_at = 25;
    let wrong_first_packed_crc = crc32fast::hash(&comment[..split_at]) ^ 0x1357_2468;
    let (vol0, vol1) = build_two_volume_rar4_comment_service_archive_ex(
        comment,
        split_at,
        Some(wrong_first_packed_crc),
    );
    let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> =
        vec![Box::new(Cursor::new(vol0)), Box::new(Cursor::new(vol1))];
    let mut archive = weaver_unrar::RarArchive::open_volumes(readers).unwrap();

    let err = archive.comment().unwrap_err();
    assert!(matches!(
        err,
        weaver_unrar::RarError::PackedDataCrcMismatch {
            member,
            volume: 0,
            expected,
            actual,
        } if member == "CMT" && expected == wrong_first_packed_crc && actual != expected
    ));
}

#[test]
fn test_rar4_modern_comment_service_crc_mismatch_is_rejected() {
    let comment = b"RAR4 comment with bad CRC";
    let archive_bytes = build_rar4_comment_service_archive(comment, 0xDEADBEEF);

    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(archive_bytes)).unwrap();
    let err = archive.comment().unwrap_err();
    assert!(
        matches!(err, weaver_unrar::RarError::DataCrcMismatch { .. }),
        "expected DataCrcMismatch, got {err:?}"
    );
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let paths: Vec<_> = vol_names.iter().map(|n| fixture("rar5", n)).collect();
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(paths);
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
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
