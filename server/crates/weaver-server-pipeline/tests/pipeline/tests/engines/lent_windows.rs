// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Real archives whose dictionary is wider than the run's window, decoded
// by a whole job through the pipeline's own lender: windows are real files
// mapped under the data folder, on a disk whose free space the test sets.
//
// The job's own folder is held at exactly the reserve, so it refuses every
// window, and the data folder has `room` bytes over the reserve: a window
// that fits is lent there, under `.weaver-windows`. Only a mapping makes
// that folder, so it standing afterwards says a window was mapped, and it
// standing empty says every window file went. A fresh lender on the same
// ledger lends exactly `room` once the job is gone, so nothing is still
// charged against the disk.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options, PpmdOptions};
use sevenz_turbo::{EncoderConfiguration, Password};
use weaver_graph::Why;

use super::harness::{Outcome, Post, Scenario, admit, payload};
use crate::history::attributes::NO_MEMORY_LIMIT_ATTRIBUTE_KEY;
use crate::jobs::model::JobStatus;
use weaver_server_pipeline::Pipeline;
use weaver_server_pipeline::runtime::{HardwareProfile, OverLimit};

use weaver_server_pipeline::operations::StorageRoot;
use weaver_server_pipeline::pipeline::tests::engines::disk_archive::{finish_disk_job, hears};
use weaver_server_pipeline::pipeline::tests::engines::disk_reserve::{dispatch_reason, status};
use weaver_server_pipeline::pipeline::tests::engines::large_window_fixtures::{
    sevenz, wide_7z as wide_7z_fixture,
};
use weaver_server_pipeline::pipeline::tests::engines::lent_windows::{
    Lab, Left, RESERVE, RUN_OVERHEAD, Record, activate_window_job, changing_room,
    drive_until_window_held, held_for_room, nothing_left, park_window_job, run_lab, run_lab_with,
    spare, window_held, window_needs,
};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const PASSWORD: &str = "lantern-window-secret";

fn crc(bytes: &[u8]) -> u32 {
    par2_rs::checksum::crc32(bytes)
}

// The job completed and delivered `name`, `len` bytes under `crc32`.
fn delivered(outcome: &Outcome, name: &str, len: usize, crc32: u32) {
    assert_eq!(outcome.status, Some(JobStatus::Complete), "{outcome:?}");
    let path = format!("lantern beacon/{name}");
    let bytes = outcome
        .files
        .get(&path)
        .unwrap_or_else(|| panic!("{path} not in {:?}", outcome.files.keys()));
    assert_eq!(bytes.len(), len, "{path}");
    assert_eq!(crc(bytes), crc32, "{path}");
}

// A window was mapped under the data folder, and every one went: nothing
// is left in its folder, on its ledger, or in the decode pool.
fn lent_and_given_back(left: &Left) {
    assert!(left.spare_made, "no window was mapped: {left:?}");
    nothing_left(left);
}

fn never_lent(left: &Left) {
    assert!(!left.spare_made, "a window was mapped: {left:?}");
    nothing_left(left);
}

// The run started again in RAM: it reserved the widest window the scratch
// refused it, whole, with its overhead, within the pool or above it.
fn held_in_ram(record: &Record, name: &str) {
    let needed = record
        .refused
        .iter()
        .filter_map(|(_, why)| match why {
            Why::TooLarge { needed, .. } => *needed,
            _ => None,
        })
        .max()
        .unwrap_or_else(|| panic!("{name}: no refusal said its size: {record:?}"));
    assert!(
        record.reserved.max(record.above) >= needed + RUN_OVERHEAD,
        "{name}: {record:?}"
    );
}

// Runs per node that returned, nodes in order.
fn runs(record: &Record) -> Vec<u32> {
    record.ended.values().copied().collect()
}

// ---- archives ----

// Text a dictionary can reach back into, `len` bytes of it.
fn text(len: usize, seed: u8) -> Vec<u8> {
    let line = payload(4 * KIB as usize, seed);
    line.iter().copied().cycle().take(len).collect()
}

fn xz_at(bytes: &[u8], dictionary: u32) -> Vec<u8> {
    let properties = lzma_turbo::LzmaEncProps::new().with_dict_size(dictionary);
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &properties).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn lzma2(dictionary: u32) -> EncoderConfiguration {
    let mut options = Lzma2Options::from_level(1);
    options.set_dictionary_size(dictionary);
    options.into()
}

fn ppmd(memory: u32) -> EncoderConfiguration {
    PpmdOptions::from_order_memory_size(6, memory).into()
}

fn locked(method: EncoderConfiguration) -> Vec<EncoderConfiguration> {
    vec![
        AesEncoderOptions::new(Password::new(PASSWORD)).into(),
        method,
    ]
}

fn fixture(path: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/../weaver-server-core/tests/fixtures/{path}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

// RAR5, LZ, a 256 KiB dictionary: compressible.txt.
const RAR5_LZ: (&str, &str, usize, u32) = (
    "extraction_profiles/rar5_lz.rar",
    "compressible.txt",
    226_943,
    0x06B1_9020,
);
// RAR 2.9, solid PPMd, a 1 MiB dictionary and a 1 MiB model: three
// members.
const RAR3_PPMD: &str = "extraction_profiles/rar4_solid_ppmd_plain.rar";
const RAR3_PPMD_MEMBERS: [(&str, usize, u32); 3] = [
    ("part0.mkv", 8_193, 0x99B2_7D05),
    ("part1.mkv", 8_193, 0xC9DA_9BB2),
    ("part2.mkv", 8_193, 0xD021_FDDD),
];
// RAR5 stored, made by rar 7.20: one 7z, itself LZMA2 with an 8 MiB
// dictionary over 24 MiB of one repeated line.
const NESTED: &str = "lent_windows/rar5_stored_7z_lzma2_8m.rar";
// The same 7z in a RAR5 compressed with method 3, its dictionary 128 KiB.
const NESTED_COMPRESSED: &str = "lent_windows/rar5_lz_7z_lzma2_8m.rar";
const NESTED_PAYLOAD: (&str, usize, u32) = ("payload.txt", 25_165_824, 0xDED2_32C8);

// One family of archive, its dictionary over the lab's window.
struct Wide {
    posted: Vec<(&'static str, Vec<u8>)>,
    // Delivered name, length and CRC32 of each member.
    members: Vec<(String, usize, u32)>,
    password: bool,
}

// The grant every wide archive is decoded under: room for a 7z folder's
// coders, a cipher's too, and a window narrower than every dictionary
// here, RAR's 256 KiB the narrowest.
const MEMORY: u64 = 3 * MIB;
const WINDOW: u64 = 128 * KIB;

fn wide_xz() -> Wide {
    let bytes = text(12 * MIB as usize, 3);
    Wide {
        members: vec![("lantern.bin".into(), bytes.len(), crc(&bytes))],
        posted: vec![("lantern.bin.xz", xz_at(&bytes, 4 << 20))],
        password: false,
    }
}

fn wide_7z(method: EncoderConfiguration, password: bool) -> Wide {
    let bytes = text(24 * MIB as usize, 5);
    let methods = if password {
        locked(method)
    } else {
        vec![method]
    };
    Wide {
        members: vec![("big.bin".into(), bytes.len(), crc(&bytes))],
        posted: vec![("lantern.7z", sevenz(methods, "big.bin", &bytes))],
        password,
    }
}

fn wide_rar5() -> Wide {
    let (path, name, len, crc32) = RAR5_LZ;
    Wide {
        posted: vec![("lantern.rar", fixture(path))],
        members: vec![(name.into(), len, crc32)],
        password: false,
    }
}

fn wide_rar3_ppmd() -> Wide {
    Wide {
        posted: vec![("lantern.rar", fixture(RAR3_PPMD))],
        members: RAR3_PPMD_MEMBERS
            .iter()
            .map(|(name, len, crc32)| (name.to_string(), *len, *crc32))
            .collect(),
        password: false,
    }
}

impl Wide {
    // The runs of its node that read it once: an encrypted archive's run
    // starts again once the password opens it.
    fn once(&self) -> u32 {
        1 + u32::from(self.password)
    }

    fn posts(&self) -> Vec<Post> {
        self.posted
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect()
    }
}

// One test per family of `$case(wide, name)`, in a module named for it.
macro_rules! per_family {
    ($case:ident) => {
        mod $case {
            use super::*;

            #[tokio::test]
            async fn xz() {
                super::$case(wide_xz(), "xz").await;
            }

            #[tokio::test]
            async fn rar5_lz() {
                super::$case(wide_rar5(), "rar5 lz").await;
            }

            #[tokio::test]
            async fn rar3_ppmd() {
                super::$case(wide_rar3_ppmd(), "rar3 ppmd").await;
            }

            #[tokio::test]
            async fn sevenz_lzma2() {
                super::$case(wide_7z(lzma2(8 << 20), false), "7z lzma2").await;
            }

            #[tokio::test]
            async fn sevenz_ppmd() {
                super::$case(wide_7z(ppmd(8 << 20), false), "7z ppmd").await;
            }

            #[tokio::test]
            async fn sevenz_lzma2_encrypted() {
                super::$case(wide_7z(lzma2(8 << 20), true), "7z lzma2 encrypted").await;
            }
        }
    };
}

fn with_password(wide: &Wide) -> impl FnOnce(Scenario) -> Scenario + use<> {
    let password = wide.password;
    move |scenario| {
        if password {
            scenario.password(PASSWORD)
        } else {
            scenario
        }
    }
}

fn every_member(outcome: &Outcome, wide: &Wide, name: &str) {
    for (member, len, crc32) in &wide.members {
        delivered(outcome, member, *len, *crc32);
    }
    assert_eq!(
        outcome.files.len(),
        wide.members.len(),
        "{name}: {:?}",
        outcome.files.keys()
    );
}

// ---- 1: lent from a mapped scratch file, byte for byte ----

// Every family, its dictionary over a 128 KiB window under scratch-then-
// fail: the dictionary is lent from a file under the data folder, the
// members come out whole, and every window file, every charge on the disk
// and every reservation is gone once the job is. One run each, never above
// the ceiling.
per_family!(lent_from_a_mapped_scratch_window);

async fn lent_from_a_mapped_scratch_window(wide: Wide, name: &str) {
    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 64 * MIB);
    let (outcome, left, record) = run_lab_with(wide.posts(), lab, with_password(&wide)).await;
    every_member(&outcome, &wide, name);
    lent_and_given_back(&left);
    assert!(record.refused.is_empty(), "{name}: {record:?}");
    assert!(
        runs(&record).iter().all(|runs| *runs == wide.once()),
        "{name}: {record:?}"
    );
    assert_eq!(record.above, 0, "{name}: {record:?}");
    assert!(
        record.reserved <= MEMORY + RUN_OVERHEAD,
        "{name}: {record:?}"
    );
}

// ---- 2 and 8: scratch-then-fail with no room ----

// The same archives with no room on any disk under scratch-then-fail: the
// run is refused, nothing is mapped, and the job is held for a full disk,
// not failed, waiting for a window wider than the one it was held to and
// the free space its disk keeps. A run with no memory limit is lifted to
// scratch-then-RAM: the scratch still refuses, the run starts again in RAM,
// and the members come out whole.
per_family!(full_scratch_holds_the_job_and_a_lifted_run_decodes);

async fn full_scratch_holds_the_job_and_a_lifted_run_decodes(wide: Wide, name: &str) {
    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 0);
    let (needs, mapped) = held_for_room(wide.posts(), lab, with_password(&wide))
        .await
        .unwrap_or_else(|| panic!("{name}: the job is held"));
    assert!(!mapped, "{name}");
    assert!(needs > RESERVE + WINDOW, "{name}: {needs}");

    let password = with_password(&wide);
    let (outcome, left, record) = run_lab_with(wide.posts(), lab, |scenario| {
        password(scenario).metadata(NO_MEMORY_LIMIT_ATTRIBUTE_KEY, "1")
    })
    .await;
    every_member(&outcome, &wide, name);
    never_lent(&left);
    assert!(
        runs(&record).contains(&(wide.once() + 1)),
        "{name}: {record:?}"
    );
    held_in_ram(&record, name);
}

// A dictionary wider than the window under scratch-then-fail, with a
// quarter of the room it takes free in both folders: the job is held for a
// full disk, not failed, and waits for the window and the free space its
// disk keeps. Three quarters of the room keeps it held, and so does a byte
// short of it. Once the job's folder shows the whole window over the
// reserve, the job runs again from its checkpoint, the window is lent, and
// the member comes out whole.
#[tokio::test]
async fn a_window_with_no_room_holds_the_job_until_its_folder_shows_the_room() {
    let wide = wide_xz();
    // The dictionary the window is asked for.
    let window = 4 * MIB;
    let folder = Arc::new(AtomicU64::new(window / 4));
    let room = Arc::new(AtomicU64::new(window / 4));
    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 0);
    let scenario = changing_room(wide.posts(), lab, &folder, &room);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let held = |pipeline: &Pipeline| {
        let status = status(pipeline);
        assert!(
            !matches!(status, Some(JobStatus::Failed { .. })),
            "{status:?}"
        );
        assert_eq!(
            dispatch_reason(pipeline).as_deref(),
            Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
        );
        window_held(pipeline)
    };
    let heard = |pipeline: &mut Pipeline, value: u64| {
        hears(pipeline, &folder, StorageRoot::Intermediate, value);
    };

    drive_until_window_held(&mut pipeline, &scenario).await;
    assert!(held(&pipeline), "the job is held for the window");
    assert_eq!(window_needs(&pipeline), Some(window + RESERVE));
    assert!(!spare(&pipeline).is_dir(), "nothing was mapped");
    // The job parks, and what its old graph had out is heard.
    park_window_job(&mut pipeline).await;

    heard(&mut pipeline, 3 * window / 4);
    assert!(held(&pipeline), "three quarters of the room is not enough");
    heard(&mut pipeline, window - 1);
    assert!(held(&pipeline), "a byte short is not enough");

    heard(&mut pipeline, window);
    assert!(!window_held(&pipeline), "the job was let go");
    activate_window_job(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    let mut files = BTreeMap::new();
    super::harness::collect(&complete_dir, &complete_dir, &mut files);
    let (name, len, crc32) = &wide.members[0];
    let bytes = files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(name.as_str()))
        .map(|(_, bytes)| bytes)
        .unwrap_or_else(|| panic!("{name} is delivered: {:?}", files.keys()));
    assert_eq!((bytes.len(), crc(bytes)), (*len, *crc32));
}

// ---- 3: scratch-then-RAM with no room ----

// Under scratch-then-RAM with no room anywhere, each archive's run is
// refused by the scratch and starts once more, in RAM above the ceiling,
// and the members come out whole. Nothing is mapped.
per_family!(full_scratch_reruns_once_in_ram);

async fn full_scratch_reruns_once_in_ram(wide: Wide, name: &str) {
    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenRam, 0);
    let (outcome, left, record) = run_lab_with(wide.posts(), lab, with_password(&wide)).await;
    every_member(&outcome, &wide, name);
    never_lent(&left);
    assert!(
        record
            .refused
            .iter()
            .all(|(_, why)| matches!(why, Why::TooLarge { .. })),
        "{name}: {record:?}"
    );
    // The archive's own node ran twice, the refused lent run and the run
    // in RAM; nothing else ran more than once.
    let mut counts = runs(&record);
    counts.sort_unstable();
    let rerun = wide.once() + 1;
    assert_eq!(counts.last(), Some(&rerun), "{name}: {record:?}");
    assert!(
        counts.iter().filter(|runs| **runs == rerun).count() == 1,
        "{name}: {record:?}"
    );
    held_in_ram(&record, name);
}

// ---- 4: refused after a window was lent ----

// RAR3 PPMd asks for two windows, its 1 MiB dictionary and its 1 MiB
// model; the data folder has room for one. The first is lent, the second
// refused. Under scratch-then-fail the job is held for a full disk, waiting
// for the window refused; under scratch-then-RAM the run starts once more
// in RAM and every member comes out whole.
#[tokio::test]
async fn a_second_window_refused_after_the_first_was_lent_holds_the_job_or_reruns_in_ram() {
    let wide = wide_rar3_ppmd();
    let room = MIB + MIB / 2;

    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, room);
    let (needs, mapped) = held_for_room(wide.posts(), lab, |scenario| scenario)
        .await
        .expect("the job is held");
    assert!(mapped, "the first window was lent");
    assert_eq!(needs, RESERVE + MIB, "it waits for the window refused");

    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenRam, room);
    let (outcome, left, record) = run_lab(wide.posts(), lab).await;
    every_member(&outcome, &wide, "rar3 ppmd");
    lent_and_given_back(&left);
    assert_eq!(runs(&record), [2], "{record:?}");
    held_in_ram(&record, "rar3 ppmd");
}

// ---- 5: a 7z inside a RAR, and two 7z at once ----

// A RAR holding a 7z whose 8 MiB dictionary is over the window, the RAR
// stored or compressed: the 7z's dictionary is lent and the payload comes
// out whole. A compressed RAR runs too, beside the 7z. No run reserves more
// than its grant and its overhead, and nothing goes above the ceiling. A
// stored RAR's member is read where it lies, so only the 7z runs.
#[tokio::test]
async fn a_7z_nested_in_a_rar_is_lent_its_dictionary_and_decodes() {
    for (fixture_path, nodes) in [(NESTED, 1), (NESTED_COMPRESSED, 2)] {
        let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 64 * MIB);
        let (outcome, left, record) =
            run_lab(vec![Post::plain("lantern.rar", fixture(fixture_path))], lab).await;
        let (name, len, crc32) = NESTED_PAYLOAD;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{fixture_path}: {outcome:?}"
        );
        let bytes = outcome
            .files
            .iter()
            .find(|(path, _)| path.ends_with(name))
            .map(|(_, bytes)| bytes)
            .unwrap_or_else(|| panic!("{fixture_path}: {:?}", outcome.files.keys()));
        assert_eq!((bytes.len(), crc(bytes)), (len, crc32), "{fixture_path}");
        lent_and_given_back(&left);
        assert!(record.refused.is_empty(), "{fixture_path}: {record:?}");
        assert_eq!(runs(&record), vec![1; nodes], "{fixture_path}: {record:?}");
        assert_eq!(record.above, 0, "{fixture_path}: {record:?}");
        assert!(
            record.reserved <= nodes as u64 * (MEMORY + RUN_OVERHEAD),
            "{fixture_path}: {record:?}"
        );
    }
}

// Two 7z archives in one job, each over the window, with room for two runs
// at once and for both dictionaries: both come out whole, both lent, and
// the pool never held more than two runs' grants.
#[tokio::test]
async fn two_7z_runs_at_once_are_each_lent_and_both_decode() {
    let first = text(12 * MIB as usize, 7);
    let second = text(12 * MIB as usize, 8);
    let posts = vec![
        Post::plain(
            "first.7z",
            sevenz(vec![lzma2(8 << 20)], "first.bin", &first),
        ),
        Post::plain(
            "second.7z",
            sevenz(vec![lzma2(8 << 20)], "second.bin", &second),
        ),
    ];
    let lab = Lab {
        extractions: Some(2),
        ..Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 64 * MIB)
    };
    let (outcome, left, record) = run_lab(posts, lab).await;
    delivered(&outcome, "first.bin", first.len(), crc(&first));
    delivered(&outcome, "second.bin", second.len(), crc(&second));
    lent_and_given_back(&left);
    assert!(record.refused.is_empty(), "{record:?}");
    assert_eq!(runs(&record), [1, 1], "{record:?}");
    assert_eq!(record.above, 0, "{record:?}");
    assert!(record.reserved <= 2 * (MEMORY + RUN_OVERHEAD), "{record:?}");
}

// ---- 6: the common case borrows nothing ----

// Small dictionaries, far inside every stock profile's window: no window
// is asked for on any profile, every run runs once, and nothing is mapped.
#[tokio::test]
async fn small_dictionaries_borrow_nothing_on_every_profile() {
    let bytes = text(3 * MIB as usize, 11);
    let families = [
        (
            "xz",
            ("lantern.bin.xz", xz_at(&bytes, 1 << 20)),
            vec![("lantern.bin".to_string(), bytes.len(), crc(&bytes))],
        ),
        (
            "rar5 lz",
            ("lantern.rar", fixture(RAR5_LZ.0)),
            vec![(RAR5_LZ.1.to_string(), RAR5_LZ.2, RAR5_LZ.3)],
        ),
        (
            "7z lzma2",
            (
                "lantern.7z",
                sevenz(vec![lzma2(1 << 20)], "big.bin", &bytes),
            ),
            vec![("big.bin".to_string(), bytes.len(), crc(&bytes))],
        ),
    ];
    for profile in [
        HardwareProfile::Efficient,
        HardwareProfile::Balanced,
        HardwareProfile::Performance,
    ] {
        for (name, (posted, bytes), members) in &families {
            let post = Post::plain(posted, bytes.clone());
            let (outcome, left, record) = run_lab_with(vec![post], Lab::stock(), |scenario| {
                scenario.profile(profile)
            })
            .await;
            for (member, len, crc32) in members {
                delivered(&outcome, member, *len, *crc32);
            }
            never_lent(&left);
            assert!(record.refused.is_empty(), "{profile:?} {name}: {record:?}");
            assert!(
                runs(&record).iter().all(|runs| *runs == 1),
                "{profile:?} {name}: {record:?}"
            );
            assert_eq!(record.above, 0, "{profile:?} {name}: {record:?}");
        }
    }
}

// A lone RAR decoded in RAM books the window its headers state, its
// member's unpacked size under its dictionary, beside the run's overhead,
// on every stock profile: a lone archive is no volume, yet its window is
// known. Nothing is lent and it runs once.
#[tokio::test]
async fn a_lone_rar_in_ram_books_its_window() {
    let (path, name, len, crc32) = RAR5_LZ;
    for profile in [
        HardwareProfile::Efficient,
        HardwareProfile::Balanced,
        HardwareProfile::Performance,
    ] {
        let post = Post::plain("lantern.rar", fixture(path));
        let (outcome, left, record) = run_lab_with(vec![post], Lab::stock(), |scenario| {
            scenario.profile(profile)
        })
        .await;
        delivered(&outcome, name, len, crc32);
        never_lent(&left);
        assert_eq!(runs(&record), [1], "{profile:?}: {record:?}");
        assert_eq!(
            record.reserved,
            len as u64 + RUN_OVERHEAD,
            "{profile:?}: {record:?}"
        );
    }
}

// ---- 7: a header that lies ----

// An xz whose block header names a 4 GiB dictionary over a few bytes, and
// the 7z of too_large whose coder names one: on Efficient with 64 MiB of
// scratch room, each is either decoded from a window the disk held or
// refused with the sizes. Nothing is placed in RAM above the ceiling and
// nothing is left behind.
#[tokio::test]
async fn a_header_naming_a_4_gib_dictionary_on_efficient_is_bounded() {
    let body = text(100, 13);
    let families = [
        ("xz", Post::plain("lantern.bin.xz", lying_xz(&body, 40))),
        ("7z", Post::plain("lantern.7z", wide_7z_fixture())),
    ];
    for (name, post) in families {
        let lab = Lab {
            room: 64 * MIB,
            ..Lab::stock()
        };
        let (outcome, left, record) = run_lab_with(vec![post], lab, |scenario| {
            scenario.profile(HardwareProfile::Efficient)
        })
        .await;
        match &outcome.status {
            Some(JobStatus::Complete) => {
                assert_eq!(outcome.files.len(), 1, "{name}: {:?}", outcome.files.keys());
                let bytes = outcome.files.values().next().unwrap();
                assert_eq!(bytes.len(), body.len(), "{name}");
            }
            Some(JobStatus::Failed { error }) => {
                assert!(error.contains("of decode memory"), "{name}: {error}");
                assert!(outcome.files.is_empty(), "{name}");
            }
            other => panic!("{name}: {other:?}"),
        }
        nothing_left(&left);
        assert_eq!(record.above, 0, "{name}: {record:?}");
        assert!(record.lent_seen <= 64 * MIB, "{name}: {record:?}");
    }
}

// An xz of `bytes` whose LZMA2 filter names dictionary property `property`
// (40 names 4 GiB less a byte): the block header's property byte rewritten
// and its CRC32 made good again. Nothing else in the stream names the
// dictionary.
fn lying_xz(bytes: &[u8], property: u8) -> Vec<u8> {
    let mut xz = xz_at(bytes, 64 << 10);
    // Stream header is 12 bytes; the block header follows, its size byte
    // first, in units of four bytes less one.
    let start = 12;
    let size = (usize::from(xz[start]) + 1) * 4;
    let header = start..start + size - 4;
    let at = xz[header.clone()]
        .windows(2)
        .position(|pair| pair == [0x21, 0x01])
        .expect("an LZMA2 filter")
        + start
        + 2;
    xz[at] = property;
    let check = crc(&xz[header.clone()]).to_le_bytes();
    xz[header.end..header.end + 4].copy_from_slice(&check);
    xz
}
