// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn started_member_names_excludes_orphan_split_continuations() {
    let payload = b"movie-payload-spanning-three-volumes";
    let crc = checksum::crc32(payload);
    let first = &payload[..12];
    let last = &payload[24..];

    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&TEST_RAR5_SIG);
    vol0.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    vol0.extend_from_slice(&build_test_rar_file_header(
        "MOVIE.m2ts",
        0x0010,
        first.len() as u64,
        payload.len() as u64,
        None,
    ));
    vol0.extend_from_slice(first);
    vol0.extend_from_slice(&build_test_rar_end_header(true));

    let mut vol2 = Vec::new();
    vol2.extend_from_slice(&TEST_RAR5_SIG);
    vol2.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
    vol2.extend_from_slice(&build_test_rar_file_header(
        "MOVIE.m2ts",
        0x0008,
        last.len() as u64,
        payload.len() as u64,
        Some(crc),
    ));
    vol2.extend_from_slice(last);
    vol2.extend_from_slice(&build_test_rar_end_header(false));

    // Volume 1 is missing: the member's continuation in volume 2 cannot be
    // linked to its base entry, so it surfaces as a second entry with the
    // same path. Path-collision checks must not treat that as a collision.
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(vol0)).unwrap();
    archive
        .add_volume(2, Box::new(std::io::Cursor::new(vol2)))
        .unwrap();

    let names = archive.member_names();
    assert_eq!(
        names.iter().filter(|name| **name == "MOVIE.m2ts").count(),
        2,
        "gapped volume set should surface the base entry and the orphan continuation"
    );
    assert_eq!(
        archive.started_member_names(),
        vec!["MOVIE.m2ts"],
        "collision checks must see only the started entry"
    );
}
