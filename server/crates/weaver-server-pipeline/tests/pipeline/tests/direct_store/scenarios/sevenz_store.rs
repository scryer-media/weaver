// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use sevenz_turbo::EncoderMethod;
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, payload,
};

// Encodes a fixture archive in memory.
fn build_7z(entries: &[Entry], method: EncoderMethod, password: Option<&str>) -> Vec<u8> {
    build_7z_shaped(entries, method, password, false)
}

const MEMBER: &str = "Silver.Horizon.S01E01.dat";
const SECOND_MEMBER: &str = "Silver.Horizon.S01E02.dat";

// The same container with every stored CRC32 taken out of its end header.
//
// 7z member checksums are optional and a writer that omits them produces a
// perfectly ordinary archive, but `sevenz_turbo`'s writer always records one,
// so the fixture has to be edited after the fact. All of a single-block
// archive's digests live in one place — the SubStreamsInfo's `kCRC` property —
// so removing it is a single, well-defined substitution, after which the end
// header's own two checksums are recomputed. Every checksum written here is
// **computed**, never a literal: a typed CRC is a fixture that agrees with
// itself and with nothing else.
fn strip_member_crcs(archive: &[u8], member: &[u8]) -> Vec<u8> {
    const K_END: u8 = 0x00;
    const K_HEADER: u8 = 0x01;
    const K_SUB_STREAMS_INFO: u8 = 0x08;
    const K_CRC: u8 = 0x0A;

    let crc32 =
        |bytes: &[u8]| crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32;

    let mut start = [0u8; 32];
    start.copy_from_slice(&archive[..32]);
    let next_header_offset = u64::from_le_bytes(start[12..20].try_into().unwrap());
    let next_header_size = u64::from_le_bytes(start[20..28].try_into().unwrap());
    let header_start = (32 + next_header_offset) as usize;
    let header = &archive[header_start..header_start + next_header_size as usize];
    assert_eq!(
        header.first().copied(),
        Some(K_HEADER),
        "the fixture's end header must be the plain form for this edit to apply"
    );

    // `kSubStreamsInfo, kCRC, all-defined, <crc32>, kEnd` -> an empty
    // `kSubStreamsInfo`. The sizes and stream counts a reader needs are the
    // block's own, which is exactly what an omitted property means.
    let mut wanted = vec![K_SUB_STREAMS_INFO, K_CRC, 1];
    wanted.extend_from_slice(&crc32(member).to_le_bytes());
    wanted.push(K_END);
    let at = header
        .windows(wanted.len())
        .position(|window| window == wanted.as_slice())
        .expect("the member's recorded digest is in the end header");

    let mut rewritten = Vec::with_capacity(header.len());
    rewritten.extend_from_slice(&header[..at]);
    rewritten.extend_from_slice(&[K_SUB_STREAMS_INFO, K_END]);
    rewritten.extend_from_slice(&header[at + wanted.len()..]);

    let mut out = archive[..header_start].to_vec();
    out.extend_from_slice(&rewritten);
    out[20..28].copy_from_slice(&(rewritten.len() as u64).to_le_bytes());
    out[28..32].copy_from_slice(&crc32(&rewritten).to_le_bytes());
    let start_crc = crc32(&out[12..32]);
    out[8..12].copy_from_slice(&start_crc.to_le_bytes());
    out
}

// The fixtures themselves, read back through the library at full size.
//
// A routing refusal and a fixture the library cannot read look identical from
// the outside, so this pins the difference: every archive these tests build is
// a valid 7z that parses from a complete image. What weaver then decides about
// it is the subject of the scenarios above.
#[test]
fn every_fixture_is_a_readable_archive() {
    let member = payload(29, 12_000);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "one member",
            build_7z(
                &[Entry::file(MEMBER, member.clone())],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "two members",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::file(SECOND_MEMBER, payload(31, 3_000)),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "an entry with no bytes",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::empty_file("Subs/Silver.Horizon.S01E01.idx"),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "an anti-item",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::anti_item("Silver.Horizon.S01E00.dat"),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "a redirection",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::symlink("Silver.Horizon.latest.dat", MEMBER),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "no member checksum",
            strip_member_crcs(
                &build_7z(
                    &[Entry::file(MEMBER, member.clone())],
                    EncoderMethod::COPY,
                    None,
                ),
                &member,
            ),
        ),
    ];
    for (label, archive) in cases {
        let read = sevenz_turbo::Archive::read(
            &mut std::io::Cursor::new(archive.clone()),
            &sevenz_turbo::Password::empty(),
        );
        assert!(read.is_ok(), "{label}: {:?}", read.err());
    }
}
