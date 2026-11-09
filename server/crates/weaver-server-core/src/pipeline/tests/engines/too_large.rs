// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An archive whose decode needs more memory than the profile's ceiling: one
// row per family, a RAR, a 7z and a stream, each the only thing posted.
//
// Under the efficient profile the archive is withheld too large, nothing
// usable comes out, and the job fails with the sizes in its message. Under
// balanced the window is granted above the ceiling, up to what the machine
// can hold; past that the job fails naming the machine's memory. A job
// reprocessed with no memory limit is granted the window whatever its
// profile.

use weaver_graph::Why;

use super::harness::{Outcome, Post, Scenario, run};
use crate::history::attributes::NO_MEMORY_LIMIT_ATTRIBUTE_KEY;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::{
    TEST_RAR5_SIG, build_test_rar_compressed_file_header, build_test_rar_end_header,
    build_test_rar_main_header, test_rar_compression_info,
};
use crate::runtime::HardwareProfile;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

fn body() -> Vec<u8> {
    (0..100u8).collect()
}

// Nothing was withheld for memory and the job did not fail for it.
fn granted(outcome: &Outcome) {
    assert!(
        !outcome
            .graph()
            .delivery
            .withheld
            .iter()
            .any(|(_, why)| matches!(why, Why::TooLarge { .. } | Why::OverMachine { .. })),
        "{:?}",
        outcome.graph().delivery
    );
    if let Some(JobStatus::Failed { error }) = &outcome.status {
        assert!(!error.contains("decode memory"), "{error}");
    }
}

// The job completed with the one member every byte as written.
fn decoded(outcome: &Outcome) {
    granted(outcome);
    assert_eq!(outcome.status, Some(JobStatus::Complete), "{outcome:?}");
    assert_eq!(
        outcome.files.get("lantern beacon/beacon.bin"),
        Some(&body()),
        "{:?}",
        outcome.files.keys()
    );
}

fn failed(outcome: &Outcome, name: &str, why: Why, error: &str) {
    assert_eq!(
        outcome.graph().delivery.withheld,
        [(name.to_string(), why)],
        "{:?}",
        outcome.graph().delivery
    );
    assert!(outcome.graph().delivery.whole.is_empty());
    assert_eq!(
        outcome.status,
        Some(JobStatus::Failed {
            error: error.to_string()
        })
    );
}

// One member whose header names RAR5 method 1 and a 4 GiB dictionary, the
// most the format can name: more than the profile's decode window.
fn wide_rar() -> Vec<u8> {
    let file = body();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEST_RAR5_SIG);
    bytes.extend_from_slice(&build_test_rar_main_header(0, None));
    bytes.extend_from_slice(&build_test_rar_compressed_file_header(
        "beacon.bin",
        0,
        file.len() as u64,
        file.len() as u64,
        Some(par2_rs::checksum::crc32(&file)),
        test_rar_compression_info(1, false) | (15 << 10),
    ));
    bytes.extend_from_slice(&file);
    bytes.extend_from_slice(&build_test_rar_end_header(false));
    bytes
}

#[tokio::test]
async fn a_rar_whose_dictionary_is_wider_than_the_efficient_window_fails_with_the_sizes() {
    let outcome = run(&Scenario::new(vec![Post::plain("silver.rar", wide_rar())])
        .profile(HardwareProfile::Efficient))
    .await;
    failed(
        &outcome,
        "silver.rar",
        Why::TooLarge {
            needed: Some(4 * GIB),
            allowed: Some(256 * MIB),
        },
        "silver.rar needs 4.0 GiB of decode memory, 256.0 MiB allowed",
    );
}

// Balanced grants the 4 GiB dictionary above its 1 GiB ceiling: the decoder
// opens the member, and nothing says it was too large.
#[tokio::test]
async fn a_rar_whose_dictionary_is_wider_than_the_balanced_window_is_granted_it() {
    let outcome = run(&Scenario::new(vec![Post::plain("silver.rar", wide_rar())])
        .profile(HardwareProfile::Balanced))
    .await;
    granted(&outcome);
}

// A 7z of one LZMA2 folder whose coder names a dictionary of 4 GiB less a
// byte, property 40, over one uncompressed LZMA2 chunk. The header is
// plain, so nothing but the property says how wide the decoder must be.
fn wide_7z() -> Vec<u8> {
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

#[tokio::test]
async fn a_7z_whose_coder_needs_more_than_the_efficient_memory_fails_with_the_sizes() {
    let outcome = run(&Scenario::new(vec![Post::plain("lantern.7z", wide_7z())])
        .profile(HardwareProfile::Efficient))
    .await;
    failed(
        &outcome,
        "lantern.7z",
        // What the decoder estimates its coder needs: the dictionary and
        // the decoder's own buffers beside it.
        Why::TooLarge {
            needed: Some(4_296_081_408),
            allowed: Some(512 * MIB),
        },
        "lantern.7z needs 4.0 GiB of decode memory, 512.0 MiB allowed",
    );
}

// Balanced grants the coder's 4 GiB above its 1 GiB ceiling, within the
// 8 GiB machine, and the member comes out whole.
#[tokio::test]
async fn a_7z_whose_coder_needs_more_than_the_balanced_memory_decodes() {
    let outcome = run(&Scenario::new(vec![Post::plain("lantern.7z", wide_7z())])
        .profile(HardwareProfile::Balanced))
    .await;
    decoded(&outcome);
}

// The same 7z under efficient, reprocessed with no memory limit: the job
// carries the flag, and the window is granted for it alone.
#[tokio::test]
async fn a_7z_reprocessed_with_no_memory_limit_decodes_under_efficient() {
    let outcome = run(&Scenario::new(vec![Post::plain("lantern.7z", wide_7z())])
        .profile(HardwareProfile::Efficient)
        .metadata(NO_MEMORY_LIMIT_ATTRIBUTE_KEY, "1"))
    .await;
    decoded(&outcome);
}

// A zstd frame whose header names a window of 2 TiB, Window_Descriptor
// exponent 31: wider than libzstd holds any decoder to. One raw byte.
fn wide_zstd() -> Vec<u8> {
    vec![
        0x28,
        0xB5,
        0x2F,
        0xFD,
        0x00,
        31 << 3,
        0x09,
        0x00,
        0x00,
        b'z',
    ]
}

#[tokio::test]
async fn a_zstd_frame_wider_than_the_efficient_window_fails_with_the_sizes() {
    let outcome = run(
        &Scenario::new(vec![Post::plain("lantern.mkv.zst", wide_zstd())])
            .profile(HardwareProfile::Efficient),
    )
    .await;
    // The run reserves no more than the widest window the profile grants,
    // which the decode pool always fits, so the decoder opens and refuses
    // the frame with the window its header names.
    failed(
        &outcome,
        "lantern.mkv.zst",
        Why::TooLarge {
            needed: Some(1 << 41),
            allowed: Some(256 * MIB),
        },
        "lantern.mkv.zst needs 2.0 TiB of decode memory, 256.0 MiB allowed",
    );
}

// Balanced would grant the window above its ceiling, but 2 TiB is past
// what the 8 GiB machine can hold: the job fails naming the machine.
#[tokio::test]
async fn a_zstd_frame_wider_than_the_machine_fails_naming_the_machine() {
    let outcome = run(
        &Scenario::new(vec![Post::plain("lantern.mkv.zst", wide_zstd())])
            .profile(HardwareProfile::Balanced),
    )
    .await;
    failed(
        &outcome,
        "lantern.mkv.zst",
        Why::OverMachine {
            needed: 1 << 41,
            memory: 8 * GIB,
        },
        "lantern.mkv.zst needs 2.0 TiB of decode memory, this machine has 8.0 GiB",
    );
}
