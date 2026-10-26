// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Direct-store routing.
//
// The spine is differential: the identical job gate is run with routing on and
// off, and the outputs must be byte-identical. With routing on, no source
// volume may ever appear on disk.

use super::*;

use std::path::{Path, PathBuf};

pub(in crate::pipeline::tests) mod scenarios;

// A real NZB's `<segment bytes=…>` is the yEnc-**encoded** article size, about
// 3% larger than the decoded payload. Every fixture here declares inflated
// sizes so no routing arithmetic can accidentally be right by reading an NZB
// total: all of it goes through `file_offset`/`decoded_size`.
fn yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

// One stored member split across `volume_count` volumes, RAR5, unencrypted.
//
// Non-final parts carry the packed CRC32 of *their* bytes (the RAR5 spec's
// rule for split files); the final part carries the whole-member CRC32. That
// is exactly what the two integrity layers read.
pub(in crate::pipeline::tests) fn single_member_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 1);
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);

    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }
            let data_crc = if is_last {
                member_crc
            } else {
                checksum::crc32(part)
            };

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            bytes.extend_from_slice(&build_test_rar_file_header(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(data_crc),
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// The RAR5 extra record that points a main header at a `QO` block: a LOCATOR
// record carrying the quick-open offset **relative to the main header itself**.
fn build_test_rar_locator_extra(qopen_offset_from_main: u64) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&encode_test_rar_vint(0x01)); // LOCATOR.
    record.extend_from_slice(&encode_test_rar_vint(0x01)); // LOCATOR_QLIST.
    record.extend_from_slice(&encode_test_rar_vint(qopen_offset_from_main));
    let mut out = encode_test_rar_vint(record.len() as u64);
    out.extend_from_slice(&record);
    out
}

// One cached header inside a `QO` block: `crc32(size || body) || size || body`,
// where the body is the header's offset *back* from the `QO` header, its
// length, and the header bytes themselves.
fn build_test_rar_qopen_record(
    qopen_header_offset: u64,
    original_header_offset: u64,
    cached_header: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(0)); // flags, unused here.
    body.extend_from_slice(&encode_test_rar_vint(
        qopen_header_offset - original_header_offset,
    ));
    body.extend_from_slice(&encode_test_rar_vint(cached_header.len() as u64));
    body.extend_from_slice(cached_header);

    let size = encode_test_rar_vint(body.len() as u64);
    let crc = checksum::crc32(&[size.as_slice(), body.as_slice()].concat());

    let mut out = Vec::new();
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&size);
    out.extend_from_slice(&body);
    out
}

// Where the `QO` block starts in the last volume. Fixed rather than derived,
// because the locator that names it lives in the main header *before* it and
// its own vint width would otherwise depend on the answer.
const QOPEN_OFFSET: u64 = 512;

// A two-volume stored set whose **last** volume carries a locator and a `QO`
// cache, past its end-of-archive record where a real archiver puts one.
//
// `forged_name` is the whole point. `None` builds an honest cache that echoes
// exactly the header the physical walk finds. `Some(name)` appends a second
// cached file header that no physical header describes — the shape the RAR
// spec warns can be crafted, and the one direct-store forbids routing a byte
// on.
fn quick_open_store_set(
    member_name: &str,
    payload: &[u8],
    forged_name: Option<&str>,
) -> Vec<(String, Vec<u8>)> {
    quick_open_store_set_shaped(member_name, payload, forged_name, true)
}

// The same fixture, with control over whether the `QO` block is closed by a
// cached **end-of-archive** record.
//
// `cached_end_record` is what decides whether the cache is used at all:
// unrar's reader adopts a Quick Open list only once it has seen the end record
// that proves the list is complete, and drops the whole cache otherwise. Real
// archivers do not write one — a `QO` block caches file headers, and the end
// header comes after it — so `false` is the shape found in the wild, where the
// locator is present, the cache is read and rejected, and the physical walk
// supplies the members. `true` is the shape the cross-check tests need, and
// the only one in which a forged cache entry can reach anything at all.
fn quick_open_store_set_shaped(
    member_name: &str,
    payload: &[u8],
    forged_name: Option<&str>,
    cached_end_record: bool,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let split = payload.len() / 2;

    let mut first = Vec::new();
    first.extend_from_slice(&TEST_RAR5_SIG);
    first.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    first.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0010,
        split as u64,
        payload.len() as u64,
        Some(checksum::crc32(&payload[..split])),
    ));
    first.extend_from_slice(&payload[..split]);
    first.extend_from_slice(&build_test_rar_end_header(true));

    let main = {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(0x0001 | 0x0002));
        type_body.extend_from_slice(&encode_test_rar_vint(1));
        build_test_rar_header(
            1,
            0,
            &type_body,
            &build_test_rar_locator_extra(QOPEN_OFFSET - TEST_RAR5_SIG.len() as u64),
        )
    };
    let real_header = build_test_rar_file_header(
        member_name,
        0x0008,
        (payload.len() - split) as u64,
        payload.len() as u64,
        Some(member_crc),
    );
    let real_header_offset = (TEST_RAR5_SIG.len() + main.len()) as u64;

    let mut second = Vec::new();
    second.extend_from_slice(&TEST_RAR5_SIG);
    second.extend_from_slice(&main);
    second.extend_from_slice(&real_header);
    second.extend_from_slice(&payload[split..]);
    second.extend_from_slice(&build_test_rar_end_header(false));
    assert!(
        second.len() as u64 <= QOPEN_OFFSET,
        "the physical headers must end before the QO block"
    );
    second.resize(QOPEN_OFFSET as usize, 0);

    let mut records = build_test_rar_qopen_record(QOPEN_OFFSET, real_header_offset, &real_header);
    if let Some(forged_name) = forged_name {
        // Cached at an offset inside the padding, so nothing physical sits
        // there: the only thing that says this member exists is the cache.
        records.extend_from_slice(&build_test_rar_qopen_record(
            QOPEN_OFFSET,
            QOPEN_OFFSET - 64,
            &build_test_rar_file_header(forged_name, 0, 16, 16, Some(0)),
        ));
    }
    if cached_end_record {
        records.extend_from_slice(&build_test_rar_qopen_record(
            QOPEN_OFFSET,
            QOPEN_OFFSET - 32,
            &build_test_rar_end_header(false),
        ));
    }
    second.extend_from_slice(&build_test_rar_service_header("QO", records.len() as u64));
    second.extend_from_slice(&records);

    vec![
        ("silver.horizon.part01.rar".to_string(), first),
        ("silver.horizon.part02.rar".to_string(), second),
    ]
}

// The decoded extent of one article, for a volume cut into `articles` equal
// pieces. At `articles == 2` this is the head/tail split every fixture uses.
fn article_extent(volume_len: usize, segment_number: u32, articles: usize) -> (usize, usize) {
    let chunk = volume_len.div_ceil(articles);
    let start = (segment_number as usize * chunk).min(volume_len);
    let end = ((segment_number as usize + 1) * chunk).min(volume_len);
    (start, end)
}

// Two articles per volume, so a volume's payload arrives after its header and
// routing has to split at least one article across destinations.
pub(super) fn direct_store_job_spec(name: &str, volumes: &[(String, Vec<u8>)]) -> JobSpec {
    direct_store_job_spec_with_articles(name, volumes, 2)
}

fn direct_store_job_spec_with_articles(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    articles: usize,
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
                        bytes: yenc_declared_bytes((end - start) as u32),
                        message_id: format!("direct-{index}-{segment_number}@example.com"),
                    }
                })
                .collect(),
        })
        .collect();
    JobSpec {
        name: name.to_string(),
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

// Article arrival plan: `(file index, segment number)` in submission order.
fn in_order_arrivals(volume_count: usize) -> Vec<(u32, u32)> {
    (0..volume_count as u32)
        .flat_map(|file_index| [(file_index, 0), (file_index, 1)])
        .collect()
}

async fn submit_volume_article_of(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
    articles: usize,
) {
    submit_volume_article_indexed_of(
        pipeline,
        job_id,
        volumes,
        file_index,
        file_index,
        segment_number,
        articles,
    )
    .await;
}

// [`submit_volume_article`] for a set whose volumes are **not** NZB files
// `0..n-1`: `ordinal` picks the bytes out of `volumes`, `file_index` is what
// the job knows the file as. The two are the same number only when nothing
// precedes the set in the NZB.
async fn submit_volume_article_indexed_of(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    ordinal: u32,
    file_index: u32,
    segment_number: u32,
    articles: usize,
) {
    let (filename, bytes) = &volumes[ordinal as usize];
    let (start, end) = article_extent(bytes.len(), segment_number, articles);
    // The article states the volume's real length, as a posted `=ybegin`
    // does; the spec's `bytes` may be the larger encoded size.
    submit_decoded_segment_declaring(
        pipeline,
        NzbFileId { job_id, file_index },
        segment_number,
        start as u64,
        &bytes[start..end],
        filename,
        None,
        true,
        None,
        bytes.len() as u64,
    )
    .await;
}

// Slice size for the PAR2 fixtures. Small enough that every volume carries
// several slices — so a single damaged article shows up as damaged *slices*
// rather than as one whole-file verdict — and large enough that the sets stay
// quick to build.
const PAR2_SLICE_BYTES: u64 = 256;

// Appends the index as one more downloadable file and returns its NZB index.
//
// Split out of [`par2_bearing_job_spec`] so a spec built with a different
// article count — the restart harness's — can carry one too.
fn append_par2_index(spec: &mut JobSpec, par2_bytes: &[u8]) -> u32 {
    let index_filename = "silver.horizon.par2".to_string();
    let file_index = spec.files.len() as u32;
    spec.total_bytes += u64::from(yenc_declared_bytes(par2_bytes.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&index_filename),
        filename: index_filename,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(par2_bytes.len() as u32),
            message_id: "direct-par2-index@example.com".to_string(),
        }],
    });
    file_index
}

// The same volume bytes under hex names that classify to nothing — the shape
// of a fully obfuscated posting. The real names survive only inside the PAR2
// descriptions the caller builds from the un-obfuscated list.
pub(in crate::pipeline::tests) fn obfuscate_volumes(
    volumes: &[(String, Vec<u8>)],
) -> Vec<(String, Vec<u8>)> {
    volumes
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                bytes.clone(),
            )
        })
        .collect()
}

// [`single_member_rar4_store_set`] with **numbered** end-of-archive records
// — the shape WinRAR's new-numbering era actually writes. The unnumbered
// variant is kept for the paths that must tolerate it; a renamed
// conventional set needs the numbers, because every unnumbered volume's
// parsed facts claim position zero and the fact-driven topology collides.
pub(in crate::pipeline::tests) fn single_member_rar4_store_set_numbered(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    rar4_store_set_with_records(member_name, payload, volume_count, |_| Vec::new())
}

// [`single_member_rar4_store_set_numbered`] with a RAR3 recovery record of
// `slots` parity sectors in each volume over every byte before its header,
// as `rar a -rr` writes one.
pub(in crate::pipeline::tests) fn protected_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    slots: u32,
) -> Vec<(String, Vec<u8>)> {
    rar4_store_set_with_records(member_name, payload, volume_count, |protected| {
        let mut record =
            weaver_objects::rar_record::encode::rar3_header(protected.len() as u64, slots);
        record.extend(weaver_objects::rar_record::encode::rar3(protected, slots));
        record
    })
}

// [`protected_rar4_store_set`] with every header past the main one
// enciphered under `password`, as `rar a -hp -rr` writes it, the member's
// data stored in the clear. Each volume's record is over its bytes before
// its own header's salt.
pub(in crate::pipeline::tests) fn protected_sealed_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    slots: u32,
    password: &str,
) -> Vec<(String, Vec<u8>)> {
    protected_rar4_store_set(member_name, payload, volume_count, slots)
        .into_iter()
        .map(|(name, plain)| (name, seal_rar4_volume(&plain, slots, password)))
        .collect()
}

// `plain` with the main header's ENCRYPTED_HEADERS flag set and every
// header's CRC made whole, every one past the main as its salt and its
// cipher; data areas are
// copied, and the record is made again over what precedes it.
fn seal_rar4_volume(plain: &[u8], slots: u32, password: &str) -> Vec<u8> {
    use weaver_objects::rar_record::encode;
    let size_at = |at: usize| usize::from(u16::from_le_bytes([plain[at + 5], plain[at + 6]]));
    let main_end = TEST_RAR4_SIG.len() + size_at(TEST_RAR4_SIG.len());
    let mut sealed = plain[..main_end].to_vec();
    // The main header's flags, past its CRC and type: ENCRYPTED_HEADERS,
    // and its CRC made whole over them.
    let main = TEST_RAR4_SIG.len();
    sealed[main + 3] |= 0x80;
    let crc = (checksum::crc32(&sealed[main + 2..main_end]) & 0xffff) as u16;
    sealed[main..main + 2].copy_from_slice(&crc.to_le_bytes());
    let mut at = main_end;
    let mut salt = 0u8;
    while at < plain.len() {
        let kind = plain[at + 2];
        let flags = u16::from_le_bytes([plain[at + 3], plain[at + 4]]);
        let header_end = at + size_at(at);
        let data = match flags & 0x8000 {
            0 => 0,
            _ => u32::from_le_bytes(plain[at + 7..at + 11].try_into().unwrap()) as usize,
        };
        let (header, area) = match kind {
            0x7a => (
                encode::rar3_header(sealed.len() as u64, slots),
                encode::rar3(&sealed, slots),
            ),
            _ => (
                plain[at..header_end].to_vec(),
                plain[header_end..header_end + data].to_vec(),
            ),
        };
        let mut header = header;
        let crc = (checksum::crc32(&header[2..]) & 0xffff) as u16;
        header[..2].copy_from_slice(&crc.to_le_bytes());
        salt += 1;
        sealed.extend(encode::seal_rar3(&header, password, [salt; 8]));
        sealed.extend(area);
        at = header_end + data;
    }
    sealed
}

// A numbered RAR4 store set whose volumes each carry what `record` makes of
// the bytes before it, between the member's part and the end header.
fn rar4_store_set_with_records(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    record: impl Fn(&[u8]) -> Vec<u8>,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);
    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;
            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_main_header(is_first));
            bytes.extend_from_slice(&build_test_rar4_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
            ));
            bytes.extend_from_slice(part);
            let record = record(&bytes);
            bytes.extend(record);
            bytes.extend_from_slice(&build_test_rar4_end_header_numbered(
                !is_last,
                volume as u16,
            ));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

fn blake2_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    digest: [u8; 32],
    packed_blake2: bool,
) -> Vec<(String, Vec<u8>)> {
    let chunk = payload.len().div_ceil(volume_count);
    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            let extra = if is_last {
                build_test_rar_blake2_extra(digest)
            } else if packed_blake2 {
                build_test_rar_blake2_extra(unrar_rs::crypto::blake2sp_hash(part))
            } else {
                Vec::new()
            };
            bytes.extend_from_slice(&build_test_rar_file_header_with_extra(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                (!is_last && !packed_blake2).then(|| checksum::crc32(part)),
                &extra,
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// The RAR4 twin of [`single_member_store_set`].
//
// RAR4 states the whole-member CRC32 in the *last* part's header and each
// earlier part's own packed CRC32 in its own — the same two integrity layers,
// in a completely different container.
fn single_member_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);

    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_main_header(is_first));
            bytes.extend_from_slice(&build_test_rar4_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// A store set carrying several members, split across `volume_count` volumes.
//
// The members are laid end to end and the concatenation is cut into equal
// volume payloads, so member boundaries and volume boundaries deliberately do
// **not** line up: members start mid-volume, at least one is split across
// volumes, and one volume carries the tail of one member and the head of the
// next. That is the shape a season pack posts as, and the shape the first
// shape demoted on sight.
pub(in crate::pipeline::tests) fn multi_member_store_set(
    members: &[(&str, Vec<u8>)],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 1);
    let total: usize = members.iter().map(|(_, bytes)| bytes.len()).sum();
    let chunk = total.div_ceil(volume_count);

    // Each member's span in the concatenated payload space.
    let mut spans = Vec::with_capacity(members.len());
    let mut cursor = 0usize;
    for (name, bytes) in members {
        spans.push((*name, bytes.as_slice(), cursor, cursor + bytes.len()));
        cursor += bytes.len();
    }

    (0..volume_count)
        .map(|volume| {
            let window_start = (volume * chunk).min(total);
            let window_end = ((volume + 1) * chunk).min(total);
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));

            for (name, payload, start, end) in &spans {
                let part_start = (*start).max(window_start);
                let part_end = (*end).min(window_end);
                if part_start >= part_end {
                    continue;
                }
                let part = &payload[part_start - start..part_end - start];
                let split_before = part_start > *start;
                let split_after = part_end < *end;

                let mut split_flags = 0u64;
                if split_before {
                    split_flags |= 0x0008;
                }
                if split_after {
                    split_flags |= 0x0010;
                }
                // The RAR5 rule the per-part layer reads: a non-final part
                // states the CRC32 of *its own* packed bytes, the final part
                // the whole member's.
                let data_crc = if split_after {
                    checksum::crc32(part)
                } else {
                    checksum::crc32(payload)
                };
                bytes.extend_from_slice(&build_test_rar_file_header(
                    name,
                    split_flags,
                    part.len() as u64,
                    payload.len() as u64,
                    Some(data_crc),
                ));
                bytes.extend_from_slice(part);
            }

            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// A store set whose volumes each carry a RAR5 recovery record of `parity`
// units over every byte before its header, as `rar a -rr` writes one.
pub(in crate::pipeline::tests) fn protected_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    parity: u16,
) -> Vec<(String, Vec<u8>)> {
    store_set_with_records(member_name, payload, volume_count, |protected, _| {
        weaver_objects::rar_record::encode::rar5(protected, parity)
    })
}

// [`protected_store_set`] with a second member, `lead`, stored whole in the
// first volume ahead of the split one.
pub(in crate::pipeline::tests) fn protected_store_set_led_by(
    lead: (&str, &[u8]),
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    parity: u16,
) -> Vec<(String, Vec<u8>)> {
    led_store_set_with_records(
        Some(lead),
        member_name,
        payload,
        volume_count,
        |protected, _| weaver_objects::rar_record::encode::rar5(protected, parity),
    )
}

// A store set whose volumes each end in an `RR` service header, the record
// `record` makes of the bytes before it and the volume's index, and the end
// header.
fn store_set_with_records(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    record: impl Fn(&[u8], usize) -> Vec<u8>,
) -> Vec<(String, Vec<u8>)> {
    led_store_set_with_records(None, member_name, payload, volume_count, record)
}

// [`store_set_with_records`], `lead` stored whole in the first volume ahead
// of the split member when there is one.
fn led_store_set_with_records(
    lead: Option<(&str, &[u8])>,
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    record: impl Fn(&[u8], usize) -> Vec<u8>,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);

    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            if let Some((name, body)) = lead.filter(|_| is_first) {
                bytes.extend_from_slice(&build_test_rar_file_header(
                    name,
                    0,
                    body.len() as u64,
                    body.len() as u64,
                    Some(checksum::crc32(body)),
                ));
                bytes.extend_from_slice(body);
            }
            bytes.extend_from_slice(&build_test_rar_file_header(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                }),
            ));
            bytes.extend_from_slice(part);
            let record = record(&bytes, volume);
            bytes.extend_from_slice(&build_test_rar_service_header("RR", record.len() as u64));
            bytes.extend(record);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// [`queued_segments`] without draining: restart tests need to *read* the queue
// and then keep feeding the pipeline, and a drained queue makes the completion
// gate conclude the download is exhausted and fail the job.
fn peek_queued_segments(pipeline: &mut Pipeline, job_id: JobId) -> Vec<(u32, u32)> {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let work = state.download_queue.drain_all();
    let mut out: Vec<(u32, u32)> = work
        .iter()
        .map(|item| {
            (
                item.segment_id.file_id.file_index,
                item.segment_id.segment_number,
            )
        })
        .collect();
    for item in work {
        state.download_queue.push(item);
    }
    out.sort_unstable();
    out
}

// A PAR2 index over the set's decoded volume bytes that also carries
// **recovery blocks**, so the damage it describes can actually be repaired.
//
// `build_test_par2_index_for_files` stops at descriptions and slice checksums,
// which is why every damaged-set test before repair landed could only assert a
// verdict: with no recovery stream neither gate can repair, so "repairs while
// direct" had nothing to compare against. The blocks are computed over the
// global input-slice ordering PAR2 defines — files in main-packet order,
// slices in order within each file, each padded to `slice_size` — which is the
// same ordering `plan_repair` reconstructs from the parsed set.
fn build_test_par2_with_recovery(
    files: &[(&str, &[u8])],
    slice_size: u64,
    recovery_block_count: usize,
) -> Vec<u8> {
    let mut stream = build_test_par2_index_for_files(files, slice_size);
    if recovery_block_count == 0 {
        return stream;
    }
    // Recomputed rather than parsed back: the recovery-set id is the MD5 of the
    // main packet body, and every packet's own hash covers it, so the two
    // builders have to agree on the derivation or nothing merges.
    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for (filename, data) in files {
        let hash_16k = checksum::md5(&data[..data.len().min(16 * 1024)]);
        let mut file_id_input = Vec::new();
        file_id_input.extend_from_slice(&hash_16k);
        file_id_input.extend_from_slice(&(data.len() as u64).to_le_bytes());
        file_id_input.extend_from_slice(filename.as_bytes());
        main_body.extend_from_slice(&checksum::md5(&file_id_input));
    }
    let recovery_set_id = checksum::md5(&main_body);

    let slice_size_bytes = slice_size as usize;
    let word_count = slice_size_bytes / 2;
    // Every input slice of every file, padded, concatenated in PAR2's global
    // ordering.
    let mut padded: Vec<u8> = Vec::new();
    for (_, data) in files {
        let slices = (data.len() as u64).div_ceil(slice_size) as usize;
        let mut block = data.to_vec();
        block.resize(slices * slice_size_bytes, 0);
        padded.extend_from_slice(&block);
    }
    let slice_count = padded.len() / slice_size_bytes;
    let constants = par2_rs::input_slice_constants(slice_count);

    for exponent in 0..recovery_block_count as u32 {
        let mut recovery = vec![0u8; slice_size_bytes];
        for (input_index, &constant) in constants.iter().enumerate() {
            let factor = par2_rs::gf_pow(constant, exponent);
            for word_index in 0..word_count {
                let at = input_index * slice_size_bytes + word_index * 2;
                let input_word = u16::from_le_bytes([padded[at], padded[at + 1]]);
                let contribution = par2_rs::gf_mul(input_word, factor);
                let current =
                    u16::from_le_bytes([recovery[word_index * 2], recovery[word_index * 2 + 1]]);
                let updated = par2_rs::gf_add(current, contribution).to_le_bytes();
                recovery[word_index * 2] = updated[0];
                recovery[word_index * 2 + 1] = updated[1];
            }
        }
        let mut body = Vec::with_capacity(4 + slice_size_bytes);
        body.extend_from_slice(&exponent.to_le_bytes());
        body.extend_from_slice(&recovery);
        stream.extend_from_slice(&build_test_par2_packet(
            par2_rs::packet::header::TYPE_RECOVERY,
            &body,
            recovery_set_id,
        ));
    }
    stream
}

// The same recovery set laid out as posters lay it out: an index carrying the
// descriptions alone, then volumes of exponentially growing block counts,
// each carrying the descriptions again and its own run of recovery packets,
// named `<base>.vol<first>+<count>.par2`. The last volume takes whatever the
// ladder has left. The index comes first.
fn build_test_par2_ladder(
    files: &[(&str, &[u8])],
    slice_size: u64,
    recovery_block_count: usize,
    base: &str,
) -> Vec<(String, Vec<u8>)> {
    let index = build_test_par2_index_for_files(files, slice_size);
    let whole = build_test_par2_with_recovery(files, slice_size, recovery_block_count);
    // Every recovery packet is a 64-byte header, the exponent and one slice.
    let packet = 64 + 4 + slice_size as usize;
    let packets = &whole[index.len()..];
    assert_eq!(packets.len(), recovery_block_count * packet);
    let digits = |value: usize| value.max(1).ilog10() as usize + 1;
    let first_width = digits(recovery_block_count).max(3);
    let count_width = digits(recovery_block_count).max(2);
    let mut carriers = vec![(format!("{base}.par2"), index.clone())];
    let (mut first, mut count) = (0, 1);
    while first < recovery_block_count {
        let count_here = count.min(recovery_block_count - first);
        let mut bytes = index.clone();
        bytes.extend_from_slice(&packets[first * packet..(first + count_here) * packet]);
        carriers.push((
            format!("{base}.vol{first:0first_width$}+{count_here:0count_width$}.par2"),
            bytes,
        ));
        first += count_here;
        count *= 2;
    }
    carriers
}

// PAR3 carriers authored over `volumes`, named the way a poster's would be:
// the index and the recovery volumes of one set, `(filename, bytes)`.
//
// Authored rather than checked in because every direct-store fixture is built
// in the test that uses it, and a carrier over bytes that are not the ones
// posted protects nothing.
fn par3_carriers_over(
    volumes: &[(String, Vec<u8>)],
    block_size: u64,
    recovery_blocks: usize,
) -> Vec<(String, Vec<u8>)> {
    let scratch = tempfile::tempdir().unwrap();
    let protected = scratch.path().join("protected");
    std::fs::create_dir_all(&protected).unwrap();
    let mut names = Vec::with_capacity(volumes.len());
    for (filename, bytes) in volumes {
        std::fs::write(protected.join(filename), bytes).unwrap();
        names.push(PathBuf::from(filename));
    }
    let report = par3_rs::create(
        &par3_rs::InputSpec::new(&protected, &names),
        &scratch.path().join("silver.horizon"),
        &par3_rs::CreateOptions::default()
            .with_block_size(block_size)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(recovery_blocks as u64)),
    )
    .expect("a PAR3 set over the fixture volumes");
    report
        .files_written
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

// Appends `files` to the spec as single-article NZB files, each stated at its
// yEnc-encoded size, and returns their file indices in order.
fn append_single_article_files(spec: &mut JobSpec, files: &[(String, Vec<u8>)]) -> Vec<u32> {
    files
        .iter()
        .map(|(filename, bytes)| {
            let file_index = spec.files.len() as u32;
            let declared = yenc_declared_bytes(bytes.len() as u32);
            spec.total_bytes += u64::from(declared);
            spec.files.push(FileSpec {
                role: FileRole::from_filename(filename),
                filename: filename.clone(),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: declared,
                    message_id: format!("direct-recovery-{file_index}@example.com"),
                }],
            });
            file_index
        })
        .collect()
}

// The KDF tuple every encrypted fixture here shares. `lg2 = 4` is 16 PBKDF2
// rounds: real archives use 2^15 and up, and paying that per fixture would put
// seconds into the suite for a number nothing under test reads.
const TEST_CRYPT_SALT: [u8; 16] = [0x5A; 16];

const TEST_CRYPT_IV: [u8; 16] = [0xA5; 16];

const TEST_CRYPT_KDF_LG2: u8 = 4;

// A RAR4 end-of-archive header that **states its volume number**, which
// [`build_test_rar4_end_header`] does not.
//
// RAR4 has no per-volume number anywhere else — RAR5 carries one in the main
// header, RAR4 carries it in `ENDARC` behind the `VOLUME_NUMBER` flag — and
// `unrar-rs` reads `RarVolumeFacts::volume_number` from exactly there. Both
// halves of a differential key volumes by the layout rather than by that
// parsed number (an old-numbering set states none at all), but a numbered end
// record is what modern RAR4 writers emit for `.partNN` sets, and it is the
// cross-check the conventional path (`persist_rar_volume_facts`) holds the
// layout against — so the realistic fixture states it.
fn build_test_rar4_end_header_numbered(more_volumes: bool, volume: u16) -> Vec<u8> {
    let mut flags: u16 = 0x0004; // VOLUME_NUMBER
    if more_volumes {
        flags |= 0x0001; // NEXT_VOLUME
    }
    build_test_rar4_block(0x7b, flags, &volume.to_le_bytes())
}
