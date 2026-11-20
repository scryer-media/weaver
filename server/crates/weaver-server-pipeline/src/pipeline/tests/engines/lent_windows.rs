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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::holds::HoldBudgets;
use weaver_graph::{Emit, NodeId, Why};

use super::actor_drive::drive;
use super::harness::{JOB, Outcome, Post, Scenario, admit, run_then};
use crate::jobs::model::JobStatus;
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use crate::pipeline::Pipeline;
use crate::pipeline::graph::engine::{DecodePool, Heard};
use crate::runtime::OverLimit;
use weaver_server_disk::scratch::{SPARE_DIR, Scratch};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
// The free space every disk keeps, far above any real one, so no test
// depends on what the machine has free.
pub const RESERVE: u64 = 1 << 40;
const NEVER: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

// What a run is granted and where its windows may go.
#[derive(Clone, Copy, Debug)]
pub struct Lab {
    // The run's memory and window, and what follows a scratch refusal. None
    // keeps the profile's.
    pub grant: Option<(u64, u64, OverLimit)>,
    // Bytes over the reserve the data folder has free.
    pub room: u64,
    // Runs a job may hold at once.
    pub extractions: Option<usize>,
}

impl Lab {
    pub fn granting(memory: u64, window: u64, over_limit: OverLimit, room: u64) -> Self {
        Self {
            grant: Some((memory, window, over_limit)),
            room,
            extractions: None,
        }
    }

    // The profile's own grant, the data folder with 16 MiB to spare.
    pub fn stock() -> Self {
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
pub struct Record {
    // Runs that returned, per node.
    pub ended: BTreeMap<NodeId, u32>,
    // Every refusal a run said.
    pub refused: Vec<(NodeId, Why)>,
    // The most the decode pool held for runs, within it and above the
    // ceiling, at any word heard.
    pub reserved: u64,
    pub above: u64,
    // The largest window file seen under the data folder at any word.
    pub lent_seen: u64,
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
    disks_reading(
        pipeline,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(room)),
    );
}

// The same, the job's folder reading `folder` bytes over the reserve and
// the data folder `room`, each as it stands when its disk is looked at.
fn disks_reading(pipeline: &mut Pipeline, folder: Arc<AtomicU64>, room: Arc<AtomicU64>) {
    let data = pipeline.script_data_dir.clone();
    let intermediate = pipeline.intermediate_dir.clone();
    let complete = pipeline.complete_dir.clone();
    let probe = {
        let (intermediate, complete) = (intermediate.clone(), complete.clone());
        Arc::new(move |path: &Path| {
            let available_bytes = if path == intermediate {
                RESERVE + folder.load(Ordering::Acquire)
            } else if path == complete {
                RESERVE * 4
            } else {
                RESERVE + room.load(Ordering::Acquire)
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

pub fn spare(pipeline: &Pipeline) -> PathBuf {
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
pub struct Left {
    // The data folder's window subfolder: whether it was made, and the
    // files in it now.
    pub spare_made: bool,
    pub spare_files: usize,
    // Whether a fresh lender on the same disk and ledger lends exactly the
    // room, and refuses a byte more.
    pub room_whole: bool,
    // What the decode pool still holds.
    pub reserved: u64,
    pub above: u64,
}

pub async fn run_lab(posts: Vec<Post>, lab: Lab) -> (Outcome, Left, Record) {
    run_lab_with(posts, lab, |scenario| scenario).await
}

pub async fn run_lab_with(
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
pub async fn with_room(
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
    let fresh = Arc::new(Scratch::new(
        check.path().to_path_buf(),
        pipeline.storage_capacity.reader(StorageRoot::Data),
        pipeline.window_ledgers.data.clone(),
        Arc::clone(&pipeline.graph_holds),
        64 * GIB,
    ));
    let over = fresh.allocate(900, (room + 1) as usize).is_err();
    // An empty window is never lent: with no room there is nothing to take.
    let whole = room == 0 || fresh.allocate(901, room as usize).is_ok();
    let room_left = pipeline.graph_decode.room();
    Left {
        spare_made,
        spare_files,
        room_whole: over && whole && fresh.live() == (0, 0),
        reserved: room_left.reserved,
        above: room_left.above,
    }
}

// Nothing is left of any window, nor of any reservation.
pub fn nothing_left(left: &Left) {
    assert_eq!(left.spare_files, 0, "{left:?}");
    assert!(left.room_whole, "the disk is still charged: {left:?}");
    assert_eq!((left.reserved, left.above), (0, 0), "{left:?}");
}

// `posts` as a job under `lab`, served until it is held for a full disk or
// ends. Held, it is not failed and says it waits for disk space: what it
// waits for its disk to show, and whether a window was ever mapped. `None`
// when it was not held.
pub async fn held_for_room(
    posts: Vec<Post>,
    lab: Lab,
    more: impl FnOnce(Scenario) -> Scenario,
) -> Option<(u64, bool)> {
    let watch = Shared::default();
    let scenario = more(scenario(posts, lab, &watch));
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    if !pipeline.held_for_full_disk(JOB) {
        return None;
    }
    let status = crate::pipeline::tests::job_status_for_assert(&pipeline, JOB);
    assert!(
        !matches!(status, Some(JobStatus::Failed { .. })),
        "{status:?}"
    );
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    let needs = pipeline.full_disk_needs(JOB).expect("the window's size");
    Some((needs, spare(&pipeline).is_dir()))
}

// The same on the stock profile `more` sets, with no room in any folder:
// what the job waits for its disk to show, when it is held. Nothing was
// mapped.
pub async fn held_without_room(
    posts: Vec<Post>,
    more: impl FnOnce(Scenario) -> Scenario,
) -> Option<u64> {
    let lab = Lab {
        room: 0,
        ..Lab::stock()
    };
    let (needs, mapped) = held_for_room(posts, lab, more).await?;
    assert!(!mapped);
    Some(needs)
}

// The free space every disk here keeps.
pub const KEPT_FREE: u64 = RESERVE;

pub const RUN_OVERHEAD: u64 = crate::pipeline::graph::engine::RUN_OVERHEAD;

pub fn changing_room(
    posts: Vec<Post>,
    lab: Lab,
    folder: &Arc<AtomicU64>,
    room: &Arc<AtomicU64>,
) -> Scenario {
    let watch = Shared::default();
    let (folder, room) = (Arc::clone(folder), Arc::clone(room));
    scenario(posts, lab, &watch).setup(move |pipeline| {
        tune(pipeline, lab);
        disks_reading(pipeline, Arc::clone(&folder), Arc::clone(&room));
    })
}

pub fn window_held(pipeline: &Pipeline) -> bool {
    pipeline.held_for_full_disk(JOB)
}

pub fn window_needs(pipeline: &Pipeline) -> Option<u64> {
    pipeline.full_disk_needs(JOB)
}

pub async fn drive_until_window_held(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, window_held).await;
}

pub async fn park_window_job(pipeline: &mut Pipeline) {
    pipeline.park_drained_jobs();
    while pipeline.retiring_graph_jobs.contains_key(&JOB) {
        let said = pipeline.graph_runs.recv().await.expect("the runs answer");
        pipeline.graph_run_said(said);
    }
}

pub async fn activate_window_job(pipeline: &mut Pipeline) {
    let done = pipeline
        .job_activated_rx
        .recv()
        .await
        .expect("the activation channel stays open");
    pipeline.handle_job_activated(done);
}
