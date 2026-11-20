// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Direct routing of 7z containers whose members are stored with the Copy
// method.
//
// A 7z states its map at the *tail*, so these fixtures exercise the one thing
// a RAR set never does: a layout that cannot be known until the last volume's
// last article has landed, whatever order the articles arrive in.

use super::*;

pub(super) mod schedules;

use sevenz_turbo::encoder_options::AesEncoderOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

// One entry of a fixture archive.
pub(in crate::pipeline::tests) struct Entry {
    name: &'static str,
    // `None` for an entry the archive names but stores no bytes for. Such an
    // entry has no stream at all — a zero-length stream is a different thing,
    // and not one a 7z writer produces.
    bytes: Option<Vec<u8>>,
    // The deletion marker an incremental archive carries.
    anti: bool,
    // The entry's stored attribute word, when the fixture sets one.
    attributes: Option<u32>,
}

impl Entry {
    pub(in crate::pipeline::tests) fn file(name: &'static str, bytes: Vec<u8>) -> Self {
        Self {
            name,
            bytes: Some(bytes),
            anti: false,
            attributes: None,
        }
    }

    fn empty_file(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: false,
            attributes: None,
        }
    }

    // An entry the header marks for deletion rather than for writing.
    fn anti_item(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: true,
            attributes: None,
        }
    }

    // An entry whose attributes say the name is a redirection, not a file.
    fn symlink(name: &'static str, target: &str) -> Self {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        Self {
            name,
            bytes: Some(target.as_bytes().to_vec()),
            anti: false,
            attributes: Some(FILE_ATTRIBUTE_REPARSE_POINT),
        }
    }
}

// Deterministic member payload: compressible enough that a non-Copy fixture
// really does shrink, and varied enough that a misrouted byte shows up.
pub(super) fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let index = index as u64;
            (seed as u64)
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(index.wrapping_mul(31))
                .rotate_left((index % 13) as u32) as u8
        })
        .collect()
}

// Encodes a fixture archive in memory.
fn build_7z(entries: &[Entry], method: EncoderMethod, password: Option<&str>) -> Vec<u8> {
    build_7z_shaped(entries, method, password, false)
}

pub(in crate::pipeline::tests) fn build_7z_shaped(
    entries: &[Entry],
    method: EncoderMethod,
    password: Option<&str>,
    encrypt_header: bool,
) -> Vec<u8> {
    let mut writer =
        ArchiveWriter::new(std::io::Cursor::new(Vec::new())).expect("create the 7z writer");
    let mut methods = vec![EncoderConfiguration::new(method)];
    if let Some(password) = password {
        // Encryption is applied to already-coded bytes, so it sits at the
        // output end of the chain.
        methods.insert(
            0,
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(password))),
        );
    }
    writer.set_content_methods(methods);
    writer.set_encrypt_header(encrypt_header);
    for entry in entries {
        let mut written = ArchiveEntry::new_file(entry.name);
        written.size = entry.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
        written.is_anti_item = entry.anti;
        if let Some(attributes) = entry.attributes {
            written.has_windows_attributes = true;
            written.windows_attributes = attributes;
        }
        match &entry.bytes {
            Some(bytes) => writer
                .push_archive_entry(written, Some(std::io::Cursor::new(bytes.clone())))
                .expect("write a file entry"),
            None => writer
                .push_archive_entry::<std::io::Cursor<Vec<u8>>>(written, None)
                .expect("write an entry with no stream"),
        };
    }

    writer.finish().expect("finish the archive").into_inner()
}

// Cuts one container into `count` posted volumes, the way a 7z set is made:
// a pure byte split at a fixed size, with no per-volume header. The last
// volume is whatever is left, so an `archive` that does not divide evenly
// produces the short tail real sets have.
pub(in crate::pipeline::tests) fn split_volumes(
    archive: &[u8],
    count: usize,
) -> Vec<(String, Vec<u8>)> {
    assert!(count >= 1);
    if count == 1 {
        return vec![("silver.horizon.7z".to_string(), archive.to_vec())];
    }
    let chunk = archive.len().div_ceil(count);
    (0..count)
        .map(|index| {
            let start = index * chunk;
            let end = ((index + 1) * chunk).min(archive.len());
            (
                format!("silver.horizon.7z.{:03}", index + 1),
                archive[start..end].to_vec(),
            )
        })
        .collect()
}

// [`direct_store_job_spec`] with the volumes' lengths stated **exactly**.
//
// The shared helper pads each segment's `bytes=` with yEnc overhead, which is
// what an NZB carries; a 7z set instead needs the decoded length its articles'
// yEnc headers state, because the volumes are a byte split of one container
// and their lengths are the only way a container offset becomes a (volume,
// offset) pair. The test harness drives the yEnc layout from the assembly's
// total, so the spec is where the true length has to be put.
pub(super) fn sevenz_job_spec(volumes: &[(String, Vec<u8>)], articles: usize) -> JobSpec {
    sevenz_job_spec_stating(volumes, articles, |decoded| decoded)
}

// [`sevenz_job_spec`] with each segment's `bytes=` passed through `stated`,
// for a test that needs the NZB to say something other than the exact decoded
// length — an encoded size, the way a real NZB states it.
fn sevenz_job_spec_stating(
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    stated: impl Fn(u32) -> u32,
) -> JobSpec {
    let files: Vec<FileSpec> = volumes
        .iter()
        .enumerate()
        .map(|(index, (filename, bytes))| FileSpec {
            filename: filename.clone(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..articles as u32)
                .map(|segment_number| {
                    let (start, end) = article_extent(bytes.len(), segment_number, articles);
                    segment_spec! {
                        number: segment_number,
                        bytes: stated((end - start) as u32),
                        message_id: format!("sevenz-{index}-{segment_number}@example.com"),
                    }
                })
                .collect(),
        })
        .collect();
    JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        // Ingest's rule: a job's total is its files' declared segment bytes.
        total_bytes: files
            .iter()
            .flat_map(|file| &file.segments)
            .map(|segment| u64::from(segment.bytes))
            .sum(),
        category: None,
        metadata: vec![],
        files,
    }
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
