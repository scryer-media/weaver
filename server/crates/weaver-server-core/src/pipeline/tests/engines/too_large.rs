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

// `posts` as a job `more` sets, where no scratch has room for a window.
// Where windows are mapped the data folder is given no room, so none reads
// the machine's free disk, and none is mapped. Where none can be mapped,
// nothing is lent: the decoder is held to its grant and refuses.
async fn without_room(posts: Vec<Post>, more: impl FnOnce(Scenario) -> Scenario) -> Outcome {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let (outcome, lent) = super::lent_windows::with_room(posts, 0, more).await;
        assert!(!lent);
        outcome
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        run(&more(Scenario::new(posts))).await
    }
}

// Under efficient a dictionary over the window is lent from scratch. With
// no scratch room anywhere the scratch refuses it, and where no window can
// be mapped the decoder refuses it: either way the job fails with the
// dictionary's size and the window, offers a retry with no memory limit,
// and nothing is mapped. The member's data is not a real compressed
// stream, so only the refusal is read off it; real RAR dictionaries lent,
// and refused, are `lent_windows`'s.
#[tokio::test]
async fn a_rar_whose_dictionary_is_wider_than_the_efficient_window_and_its_scratch_fails_with_the_sizes()
 {
    let outcome = without_room(vec![Post::plain("silver.rar", wide_rar())], |s| {
        s.profile(HardwareProfile::Efficient)
    })
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
    assert!(offers_retry(&outcome));
}

// Balanced grants the 4 GiB dictionary above its 1 GiB ceiling once the
// scratch has no room for it, or from the start where no window can be
// mapped: the decoder opens the member in RAM, and nothing says it was too
// large.
#[tokio::test]
async fn a_rar_whose_dictionary_is_wider_than_the_balanced_window_is_granted_it() {
    let outcome = without_room(vec![Post::plain("silver.rar", wide_rar())], |s| {
        s.profile(HardwareProfile::Balanced)
    })
    .await;
    granted(&outcome);
}

// A 7z of one LZMA2 folder whose coder names a dictionary of 4 GiB less a
// byte, property 40, over one uncompressed LZMA2 chunk. The header is
// plain, so nothing but the property says how wide the decoder must be.
pub(super) fn wide_7z() -> Vec<u8> {
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

// The decoder holds a dictionary to its coder's output, so the 4 GiB this
// coder names over 100 bytes fits in RAM under efficient: the member comes
// out whole, and nothing is lent.
#[tokio::test]
async fn a_7z_whose_coder_names_more_than_the_efficient_memory_over_a_few_bytes_decodes() {
    let outcome = without_room(vec![Post::plain("lantern.7z", wide_7z())], |s| {
        s.profile(HardwareProfile::Efficient)
    })
    .await;
    decoded(&outcome);
}

// A 7z of one file `name` holding `bytes`, through the coders `methods`.
pub(super) fn sevenz(
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

// A 7z of one PPMd folder over `body()` whose model is 520 MiB: a model is
// the size its coder declares, whatever the folder holds, so it is over
// what the efficient profile's 512 MiB keeps in RAM.
fn wide_ppmd_7z() -> Vec<u8> {
    use sevenz_turbo::encoder_options::PpmdOptions;
    let model = PpmdOptions::from_order_memory_size(6, 520 << 20);
    sevenz(vec![model.into()], "beacon.bin", &body())
}

// What a decode of `archive` holds in RAM on one thread, nothing lent.
fn in_ram(archive: &[u8]) -> u64 {
    let parsed = sevenz_turbo::Archive::read(
        &mut std::io::Cursor::new(archive),
        &sevenz_turbo::Password::empty(),
    )
    .unwrap();
    parsed
        .decoder_memory(&sevenz_turbo::ArchiveLimits::default(), 1)
        .unwrap()
        .resident_bytes
}

// Under efficient a model over the memory is lent from scratch where the
// scratch has room for it, and the member comes out whole.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn a_7z_model_over_the_efficient_memory_is_lent_where_the_scratch_has_room() {
    let post = vec![Post::plain("lantern.7z", wide_ppmd_7z())];
    let (outcome, lent) =
        super::lent_windows::with_room(post, 521 * MIB, |s| s.profile(HardwareProfile::Efficient))
            .await;
    assert!(lent);
    decoded(&outcome);
}

// With no scratch room anywhere the scratch refuses the model, and where no
// window can be mapped the decoder refuses it: either way the job fails
// with what the decode holds in RAM and the memory it was allowed there,
// and offers a retry with no memory limit, which decodes it in RAM.
#[tokio::test]
async fn a_7z_model_over_the_efficient_memory_without_scratch_fails_with_the_sizes() {
    let archive = wide_ppmd_7z();
    let post = || vec![Post::plain("lantern.7z", archive.clone())];
    let efficient = |s: Scenario| s.profile(HardwareProfile::Efficient);

    let outcome = without_room(post(), efficient).await;
    let needed = in_ram(&archive);
    assert!(needed > 520 * MIB, "{needed}");
    let error = outcome_error(&outcome);
    assert!(error.starts_with("lantern.7z needs "), "{error}");
    assert!(
        error.ends_with(" of decode memory, 512.0 MiB allowed"),
        "{error}"
    );
    failed(
        &outcome,
        "lantern.7z",
        Why::TooLarge {
            needed: Some(needed),
            allowed: Some(512 * MIB),
        },
        &error,
    );
    assert!(offers_retry(&outcome));

    let outcome = without_room(post(), |s| {
        efficient(s).metadata(NO_MEMORY_LIMIT_ATTRIBUTE_KEY, "1")
    })
    .await;
    decoded(&outcome);
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
    // More than the machine leaves a run: a retry with no limit would fail
    // again, so none is offered.
    assert!(!offers_retry(&outcome));
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
    assert!(!offers_retry(&outcome));
}

// A zstd frame of one raw byte whose header names a window of 1 << `log`.
fn zstd_frame(log: u8) -> Vec<u8> {
    vec![
        0x28,
        0xB5,
        0x2F,
        0xFD,
        0x00,
        (log - 10) << 3,
        0x09,
        0x00,
        0x00,
        b'z',
    ]
}

// Two frames, a narrow one first, so the front declares only it, and one a
// window of 1 << `log` after it.
fn later_wide_zstd(log: u8) -> Vec<u8> {
    [zstd_frame(20), zstd_frame(log)].concat()
}

fn decoded_zstd(outcome: &Outcome) {
    granted(outcome);
    assert_eq!(outcome.status, Some(JobStatus::Complete), "{outcome:?}");
    assert_eq!(
        outcome.files.get("lantern beacon/lantern.mkv"),
        Some(&b"zz".to_vec()),
        "{:?}",
        outcome.files.keys()
    );
}

// A later frame wider than the decoder's own 128 MiB, within the window the
// profile grants a decoder: libzstd does not say how wide, and the run
// starts again at the profile's window, which holds it.
#[tokio::test]
async fn a_later_zstd_frame_within_the_profiles_window_decodes() {
    for profile in [HardwareProfile::Efficient, HardwareProfile::Balanced] {
        let outcome = run(&Scenario::new(vec![Post::plain(
            "lantern.mkv.zst",
            later_wide_zstd(28),
        )])
        .profile(profile))
        .await;
        decoded_zstd(&outcome);
    }
}

// A later 2 GiB frame, above Balanced's 1 GiB ceiling: the run starts again
// at the ceiling, is refused again, and starts once more at the widest
// window libzstd has, in RAM above the ceiling.
#[tokio::test]
async fn a_later_zstd_frame_above_the_balanced_ceiling_decodes() {
    let outcome = run(
        &Scenario::new(vec![Post::plain("lantern.mkv.zst", later_wide_zstd(31))])
            .profile(HardwareProfile::Balanced),
    )
    .await;
    decoded_zstd(&outcome);
}

// The same under efficient fails, held to its window, and the job is
// marked as failed for memory: a retry with no limit can help.
#[tokio::test]
async fn a_later_zstd_frame_above_the_efficient_window_fails_and_is_lifted_by_a_retry() {
    let outcome = run(
        &Scenario::new(vec![Post::plain("lantern.mkv.zst", later_wide_zstd(31))])
            .profile(HardwareProfile::Efficient),
    )
    .await;
    failed(
        &outcome,
        "lantern.mkv.zst",
        Why::TooLarge {
            needed: None,
            allowed: Some(256 * MIB),
        },
        &outcome_error(&outcome),
    );
    assert!(offers_retry(&outcome));
    let outcome = run(
        &Scenario::new(vec![Post::plain("lantern.mkv.zst", later_wide_zstd(31))])
            .profile(HardwareProfile::Efficient)
            .metadata(NO_MEMORY_LIMIT_ATTRIBUTE_KEY, "1"),
    )
    .await;
    decoded_zstd(&outcome);
}

// Whether the job's history row says it failed for memory, offering a
// retry with no limit.
fn offers_retry(outcome: &Outcome) -> bool {
    crate::history::attributes::failed_for_memory(&crate::parse_history_metadata(
        outcome.history_metadata.as_deref(),
    ))
}

fn outcome_error(outcome: &Outcome) -> String {
    match &outcome.status {
        Some(JobStatus::Failed { error }) => error.clone(),
        other => panic!("{other:?}"),
    }
}
