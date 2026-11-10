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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options, PpmdOptions};
use sevenz_turbo::{EncoderConfiguration, Password};
use weaver_graph::holds::HoldBudgets;
use weaver_graph::{Emit, NodeId, Why};

use super::harness::{Outcome, Post, Scenario, payload, run_then};
use super::too_large::sevenz;
use crate::history::attributes::NO_MEMORY_LIMIT_ATTRIBUTE_KEY;
use crate::jobs::model::JobStatus;
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use crate::pipeline::Pipeline;
use crate::pipeline::graph::engine::{DecodePool, Heard};
use crate::pipeline::graph::window::{SPARE_DIR, Windows};
use crate::runtime::{HardwareProfile, OverLimit};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
// The free space every disk keeps, far above any real one, so no test
// depends on what the machine has free.
const RESERVE: u64 = 1 << 40;
const NEVER: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);
const PASSWORD: &str = "lantern-window-secret";

// What a run is granted and where its windows may go.
#[derive(Clone, Copy, Debug)]
struct Lab {
    // The run's memory and window, and what follows a scratch refusal. None
    // keeps the profile's.
    grant: Option<(u64, u64, OverLimit)>,
    // Bytes over the reserve the data folder has free.
    room: u64,
    // Runs a job may hold at once.
    extractions: Option<usize>,
}

impl Lab {
    fn granting(memory: u64, window: u64, over_limit: OverLimit, room: u64) -> Self {
        Self {
            grant: Some((memory, window, over_limit)),
            room,
            extractions: None,
        }
    }

    // The profile's own grant, the data folder with 16 MiB to spare.
    fn stock() -> Self {
        Self {
            grant: None,
            room: 16 * MIB,
            extractions: None,
        }
    }
}

// What the job's runs said, as the run loop heard it, and the decode pool
// and data folder as they stood at each word.
#[derive(Debug, Default)]
struct Record {
    // Runs that returned, per node.
    ended: BTreeMap<NodeId, u32>,
    // Every refusal a run said.
    refused: Vec<(NodeId, Why)>,
    // The most the decode pool held for runs, within it and above the
    // ceiling, at any word heard.
    reserved: u64,
    above: u64,
    // The largest window file seen under the data folder at any word.
    lent_seen: u64,
}

#[derive(Default)]
struct Watch {
    pool: Option<Arc<DecodePool>>,
    spare: Option<PathBuf>,
    record: Record,
}

type Shared = Arc<Mutex<Watch>>;

fn lock(watch: &Shared) -> std::sync::MutexGuard<'_, Watch> {
    watch.lock().unwrap_or_else(PoisonError::into_inner)
}

// The run grant `lab` sets, in force from here, as a profile change sets
// it: the tuner, the hardware every run reads, and the decode pool.
fn tune(pipeline: &mut Pipeline, lab: Lab) {
    let mut tuning = pipeline.tuner.profile_tuning();
    if let Some((memory, window, over_limit)) = lab.grant {
        tuning.sevenz_decode_memory_bytes = memory;
        tuning.decode_window_bytes = window;
        tuning.over_limit = over_limit;
    }
    if let Some(extractions) = lab.extractions {
        tuning.max_concurrent_extractions = extractions;
    }
    pipeline.tuner.set_profile_tuning(tuning);
    pipeline.graph_hardware.follow(&pipeline.tuner);
    pipeline
        .graph_decode
        .follow(&pipeline.graph_hardware.grant().run);
}

// The disks as `room` says: the job's folder at the reserve, the data
// folder `room` over it, the complete folder with room for anything.
fn disks(pipeline: &mut Pipeline, room: u64) {
    let data = pipeline.script_data_dir.clone();
    let intermediate = pipeline.intermediate_dir.clone();
    let complete = pipeline.complete_dir.clone();
    let probe = {
        let (intermediate, complete) = (intermediate.clone(), complete.clone());
        Arc::new(move |path: &Path| {
            let available_bytes = if path == intermediate {
                RESERVE
            } else if path == complete {
                RESERVE * 4
            } else {
                RESERVE + room
            };
            Ok(DiskSpace {
                total_bytes: RESERVE * 8,
                available_bytes,
            })
        })
    };
    let capacity = StorageCapacity::with_probe([data, intermediate, complete], NEVER, probe);
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    pipeline.storage_capacity = Arc::new(capacity);
}

fn spare(pipeline: &Pipeline) -> PathBuf {
    pipeline.script_data_dir.join(SPARE_DIR)
}

// The window files under `dir`, by length.
fn window_files(dir: &Path) -> Vec<u64> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| entry.metadata().ok())
                .map(|meta| meta.len())
                .collect()
        })
        .unwrap_or_default()
}

// `posts` as a job run under `lab`, every word its runs say recorded.
fn scenario(posts: Vec<Post>, lab: Lab, watch: &Shared) -> Scenario {
    let setup = Arc::clone(watch);
    let heard = Arc::clone(watch);
    Scenario::new(posts)
        .framed(64 * KIB as usize)
        .holds(HoldBudgets {
            resident_limit: u64::MAX,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: RESERVE,
        })
        .setup(move |pipeline| {
            tune(pipeline, lab);
            disks(pipeline, lab.room);
            let mut watch = lock(&setup);
            watch.pool = Some(Arc::clone(&pipeline.graph_decode));
            watch.spare = Some(spare(pipeline));
        })
        .heard(move |said| {
            let mut watch = lock(&heard);
            let room = watch.pool.as_ref().map(|pool| pool.room());
            let seen = watch.spare.as_deref().map(window_files).unwrap_or_default();
            let record = &mut watch.record;
            if let Some(room) = room {
                record.reserved = record.reserved.max(room.reserved);
                record.above = record.above.max(room.above);
            }
            record.lent_seen = record.lent_seen.max(seen.into_iter().max().unwrap_or(0));
            match &said.heard {
                Heard::Ended => *record.ended.entry(said.node).or_insert(0) += 1,
                Heard::Emit(Emit::Refuse(why)) => record.refused.push((said.node, *why)),
                _ => {}
            }
        })
}

// What a job left behind on the lender, read once it finished.
#[derive(Debug)]
struct Left {
    // The data folder's window subfolder: whether it was made, and the
    // files in it now.
    spare_made: bool,
    spare_files: usize,
    // Whether a fresh lender on the same disk and ledger lends exactly the
    // room, and refuses a byte more.
    room_whole: bool,
    // What the decode pool still holds.
    reserved: u64,
    above: u64,
}

async fn run_lab(posts: Vec<Post>, lab: Lab) -> (Outcome, Left, Record) {
    run_lab_with(posts, lab, |scenario| scenario).await
}

async fn run_lab_with(
    posts: Vec<Post>,
    lab: Lab,
    more: impl FnOnce(Scenario) -> Scenario,
) -> (Outcome, Left, Record) {
    let watch = Shared::default();
    let left = Arc::new(Mutex::new(None));
    let outcome = {
        let left = Arc::clone(&left);
        run_then(
            &more(scenario(posts, lab, &watch)),
            async move |pipeline: &mut Pipeline, _: &Path| {
                *left.lock().unwrap() = Some(leftover(pipeline, lab.room));
            },
        )
        .await
    };
    let left = left.lock().unwrap().take().expect("the job finished");
    let record = std::mem::take(&mut lock(&watch).record);
    (outcome, left, record)
}

// `posts` as a job on the stock profile `more` sets, the data folder with
// `room` bytes over the reserve and every other folder with none: whether a
// window was mapped. Nothing is left of any window or reservation.
pub(super) async fn with_room(
    posts: Vec<Post>,
    room: u64,
    more: impl FnOnce(Scenario) -> Scenario,
) -> (Outcome, bool) {
    let lab = Lab {
        room,
        ..Lab::stock()
    };
    let (outcome, left, _) = run_lab_with(posts, lab, more).await;
    nothing_left(&left);
    (outcome, left.spare_made)
}

fn leftover(pipeline: &Pipeline, room: u64) -> Left {
    let dir = spare(pipeline);
    let spare_made = dir.is_dir();
    let spare_files = std::fs::read_dir(&dir).map_or(0, |entries| entries.count());
    let check = tempfile::tempdir().unwrap();
    let fresh = Arc::new(Windows::new(
        check.path().to_path_buf(),
        pipeline.storage_capacity.reader(StorageRoot::Data),
        pipeline.window_ledgers.data.clone(),
        Arc::clone(&pipeline.graph_holds),
        64 * GIB,
    ));
    let over = fresh.allocate(NodeId(900), (room + 1) as usize).is_err();
    // An empty window is never lent: with no room there is nothing to take.
    let whole = room == 0 || fresh.allocate(NodeId(901), room as usize).is_ok();
    let room_left = pipeline.graph_decode.room();
    Left {
        spare_made,
        spare_files,
        room_whole: over && whole && fresh.live() == (0, 0),
        reserved: room_left.reserved,
        above: room_left.above,
    }
}

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

// Nothing is left of any window, nor of any reservation.
fn nothing_left(left: &Left) {
    assert_eq!(left.spare_files, 0, "{left:?}");
    assert!(left.room_whole, "the disk is still charged: {left:?}");
    assert_eq!((left.reserved, left.above), (0, 0), "{left:?}");
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
        record.reserved.max(record.above) >= needed + crate::pipeline::graph::engine::RUN_OVERHEAD,
        "{name}: {record:?}"
    );
}

// Runs per node that returned, nodes in order.
fn runs(record: &Record) -> Vec<u32> {
    record.ended.values().copied().collect()
}

// Whether the job's history row offers a retry with no memory limit.
fn offers_retry(outcome: &Outcome) -> bool {
    crate::history::attributes::failed_for_memory(&crate::parse_history_metadata(
        outcome.history_metadata.as_deref(),
    ))
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
        "{}/tests/fixtures/{path}",
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
        record.reserved <= MEMORY + crate::pipeline::graph::engine::RUN_OVERHEAD,
        "{name}: {record:?}"
    );
}

// ---- 2 and 8: scratch-then-fail with no room ----

// The same archives with no room on any disk under scratch-then-fail: the
// run is refused, nothing is mapped, the job fails naming what the decoder
// asked for and the window it was held to, nothing is placed in RAM above
// the ceiling, and the job offers a retry with no memory limit. That retry
// is lifted to scratch-then-RAM: the scratch still refuses, the run starts
// again in RAM, and the members come out whole.
per_family!(full_scratch_fails_with_the_sizes_and_a_lifted_retry_decodes);

async fn full_scratch_fails_with_the_sizes_and_a_lifted_retry_decodes(wide: Wide, name: &str) {
    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, 0);
    let (outcome, left, record) = run_lab_with(wide.posts(), lab, with_password(&wide)).await;
    let Some(JobStatus::Failed { error }) = &outcome.status else {
        panic!("{name}: {outcome:?}");
    };
    // A 7z decode was allowed the run's memory, and kept in RAM what fit
    // there; a RAR decode was held to the window.
    let (held, said) = if name.starts_with("7z") {
        (MEMORY, "of decode memory, 3.0 MiB allowed")
    } else {
        (WINDOW, "of decode memory, 128.0 KiB allowed")
    };
    assert!(error.contains(said), "{name}: {error}");
    let withheld = &outcome.graph().delivery.withheld;
    assert_eq!(withheld.len(), 1, "{name}: {withheld:?}");
    let Why::TooLarge { needed, allowed } = withheld[0].1 else {
        panic!("{name}: {withheld:?}");
    };
    assert_eq!(allowed, Some(held), "{name}");
    assert!(
        needed.is_some_and(|needed| needed > held),
        "{name}: {needed:?}"
    );
    assert!(
        outcome.files.is_empty(),
        "{name}: {:?}",
        outcome.files.keys()
    );
    assert!(offers_retry(&outcome), "{name}");
    never_lent(&left);
    assert!(
        runs(&record).iter().all(|runs| *runs == wide.once()),
        "{name}: {record:?}"
    );
    assert_eq!(record.above, 0, "{name}: {record:?}");

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
// refused. Under scratch-then-fail the job fails with the sizes and the
// lent window is given back; under scratch-then-RAM the run starts once
// more in RAM and every member comes out whole.
#[tokio::test]
async fn a_second_window_refused_after_the_first_was_lent_fails_or_reruns_in_ram() {
    let wide = wide_rar3_ppmd();
    let room = MIB + MIB / 2;

    let lab = Lab::granting(MEMORY, WINDOW, OverLimit::ScratchThenFail, room);
    let (outcome, left, record) = run_lab(wide.posts(), lab).await;
    let Some(JobStatus::Failed { error }) = &outcome.status else {
        panic!("{outcome:?}");
    };
    assert!(
        error.contains("1.0 MiB of decode memory, 128.0 KiB allowed"),
        "{error}"
    );
    assert!(outcome.files.is_empty(), "{:?}", outcome.files.keys());
    lent_and_given_back(&left);
    assert_eq!(runs(&record), [1], "{record:?}");
    assert_eq!(record.above, 0, "{record:?}");

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
            record.reserved
                <= nodes as u64 * (MEMORY + crate::pipeline::graph::engine::RUN_OVERHEAD),
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
    assert!(
        record.reserved <= 2 * (MEMORY + crate::pipeline::graph::engine::RUN_OVERHEAD),
        "{record:?}"
    );
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
            len as u64 + crate::pipeline::graph::engine::RUN_OVERHEAD,
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
        ("7z", Post::plain("lantern.7z", super::too_large::wide_7z())),
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
