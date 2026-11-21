// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::disk_archive::{NEVER, RESERVE};
use super::harness::{JOB, Scenario, admit_with};
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use crate::pipeline::Pipeline;
use crate::pipeline::completion::finalize::{
    CLAIM_FULL, COPY_ACROSS, DESTINATION_ROOM, FIXTURE_BOOKED_HOOKS, MOVING_ROW_REFUSED,
    MOVING_ROWS_WRITTEN, PARTIAL_STAYS,
};
use crate::pipeline::full_disk::Stage;
use crate::post_processing::model::{PipelineOutcome, PostProcessingFacts};

pub fn destination_room(room: Option<u32>) {
    *DESTINATION_ROOM.lock().unwrap() = room;
}

pub fn claim_full(full: bool) {
    CLAIM_FULL.store(full, Ordering::Release);
}

pub fn keep_partial_copy(keep: bool) {
    PARTIAL_STAYS.store(keep, Ordering::Release);
}

pub fn refuse_moving_row(error: crate::StateError) {
    MOVING_ROW_REFUSED.lock().unwrap().push_back(error);
}

pub fn moving_rows_written() -> u64 {
    MOVING_ROWS_WRITTEN.load(Ordering::Relaxed)
}

pub fn move_inflight(pipeline: &Pipeline) -> bool {
    pipeline.inflight_moves.contains(&JOB)
}

pub fn move_destination_reserved(pipeline: &Pipeline) -> bool {
    pipeline.reserved_complete_destinations.contains_key(&JOB)
}

pub async fn resume_full_disk_moves(pipeline: &mut Pipeline) {
    pipeline.resume_full_disk_moves().await;
}

pub async fn recheck_move_completion(pipeline: &mut Pipeline) {
    pipeline.check_job_completion(JOB).await;
}

pub fn begin_let_go_move(pipeline: &mut Pipeline) -> bool {
    pipeline.begin_let_go_move(JOB)
}

pub fn full_disk_needs(pipeline: &Pipeline) -> Option<u64> {
    pipeline.full_disk_needs(JOB)
}

pub fn cut_short_copies(pipeline: &Pipeline) -> Vec<PathBuf> {
    pipeline.full_disk_cut_short(JOB)
}

pub fn intermediate_directory(pipeline: &Pipeline) -> &Path {
    &pipeline.intermediate_dir
}

pub fn database_directory(pipeline: &Pipeline) -> PathBuf {
    pipeline.database_dir()
}

pub fn data_directory(pipeline: &Pipeline) -> &Path {
    &pipeline.script_data_dir
}

pub fn retarget_intermediate(pipeline: &mut Pipeline, path: PathBuf) {
    pipeline
        .storage_capacity
        .retarget(StorageRoot::Intermediate, path.clone());
    pipeline.intermediate_dir = path;
}

pub fn hold_restored_move(pipeline: &mut Pipeline, path: &Path) {
    pipeline.hold_move_for_full_disk(JOB, Stage::Restored, path, "no room", None);
}

pub fn delivery_source(pipeline: &Pipeline) -> Option<&Path> {
    pipeline.jobs[&JOB].delivery_source.as_deref()
}

pub fn successful_move_facts(pipeline: &Pipeline) -> PostProcessingFacts {
    pipeline.fresh_post_processing_facts(JOB, PipelineOutcome::Succeeded)
}

pub async fn resume_recorded_move(
    pipeline: &mut Pipeline,
    destination: PathBuf,
    source: Option<PathBuf>,
) {
    pipeline.reserved_complete_destinations.remove(&JOB);
    let state = pipeline.jobs.get_mut(&JOB).unwrap();
    state.working_dir = destination;
    state.delivery_source = source;
    pipeline.resume_restored_move(JOB).await;
}

pub fn move_spool_filled(pipeline: &mut Pipeline) {
    let file_id = weaver_server_base::jobs::ids::NzbFileId {
        job_id: JOB,
        file_index: 0,
    };
    pipeline.fail_job_for_disk_write(
        crate::pipeline::decode::SegmentWriteError::new(
            file_id,
            std::io::Error::from(std::io::ErrorKind::StorageFull),
        ),
        "UU spool write failed",
    );
}

pub async fn admit_library_move(
    scenario: &Scenario,
    probe_room: Arc<AtomicU64>,
    probe_library: Arc<AtomicU64>,
) -> (tempfile::TempDir, Pipeline, PathBuf) {
    let mut admitted = admit_with(scenario, move |pipeline, temp| {
        let library = temp.join("library-disk");
        std::fs::create_dir_all(&library).unwrap();
        weaver_server_disk::runtime::fs::test_devices::assign(&pipeline.intermediate_dir, 1);
        weaver_server_disk::runtime::fs::test_devices::assign(&pipeline.complete_dir, 2);
        weaver_server_disk::runtime::fs::test_devices::assign(&library, 3);
        pipeline.config.try_write().unwrap().categories =
            vec![weaver_server_settings::categories::CategoryConfig {
                id: 0,
                name: "library".to_string(),
                dest_dir: Some(library.display().to_string()),
                aliases: String::new(),
                upload_cap: Default::default(),
                seeding: None,
                torrent_route: false,
                unpack: None,
            }];
        let roots = [
            pipeline.script_data_dir.clone(),
            pipeline.intermediate_dir.clone(),
            pipeline.complete_dir.clone(),
        ];
        let probe = Arc::new(move |path: &Path| {
            let room = if path.starts_with(&library) {
                &probe_library
            } else {
                &probe_room
            };
            Ok(DiskSpace {
                total_bytes: RESERVE * 8,
                available_bytes: RESERVE + room.load(Ordering::Acquire),
            })
        });
        let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        pipeline.storage_capacity = Arc::new(capacity);
    })
    .await;
    let (_, pipeline, _) = &mut admitted;
    pipeline.jobs.get_mut(&JOB).unwrap().spec.category = Some("library".to_string());
    admitted
}

// The complete folder on a disk of its own, which reads `complete_room`
// over the reserve; the other folders on another, which reads `room` over
// it. A rename between the two cannot be made and is copied.
pub async fn admit_split_move(
    scenario: &Scenario,
    room: Arc<AtomicU64>,
    complete_room: Arc<AtomicU64>,
) -> (tempfile::TempDir, Pipeline, PathBuf) {
    COPY_ACROSS.store(true, Ordering::Release);
    admit_with(scenario, move |pipeline, _| {
        use weaver_server_disk::runtime::fs::test_devices::assign;
        assign(&pipeline.script_data_dir, 1);
        assign(&pipeline.intermediate_dir, 1);
        assign(&pipeline.complete_dir, 2);
        let roots = [
            pipeline.script_data_dir.clone(),
            pipeline.intermediate_dir.clone(),
            pipeline.complete_dir.clone(),
        ];
        let complete_dir = pipeline.complete_dir.clone();
        let probe = Arc::new(move |path: &Path| {
            let room = if path.starts_with(&complete_dir) {
                &complete_room
            } else {
                &room
            };
            Ok(DiskSpace {
                total_bytes: RESERVE * 8,
                available_bytes: RESERVE + room.load(Ordering::Acquire),
            })
        });
        let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        pipeline.storage_capacity = Arc::new(capacity);
    })
    .await
}

// A rename between a test's devices that differ is copied.
pub fn copy_across(across: bool) {
    COPY_ACROSS.store(across, Ordering::Release);
}

// Once the job's move has booked its copy on the complete folder's disk,
// and before it moves an entry, `then` is told what that disk's ledger
// holds promised, on a thread of its own the move waits for.
pub fn when_booked(pipeline: &Pipeline, then: impl FnOnce(u64) + Send + 'static) {
    let capacity = Arc::clone(&pipeline.storage_capacity);
    FIXTURE_BOOKED_HOOKS.lock().unwrap().insert(
        pipeline.complete_dir.clone(),
        Box::new(move |_| then(capacity.ledger(StorageRoot::Complete).promised_total())),
    );
}

// Once the job's moving row is written, and before its move books or moves
// anything, the complete folder's disk is read afresh showing `value` over
// the reserve. The loop has not heard that reading.
pub fn complete_reads_before_move(pipeline: &Pipeline, complete_room: &Arc<AtomicU64>, value: u64) {
    let capacity = Arc::clone(&pipeline.storage_capacity);
    let complete_room = Arc::clone(complete_room);
    crate::pipeline::completion::finalize::FIXTURE_MOVE_HOOKS
        .lock()
        .unwrap()
        .insert(
            pipeline.complete_dir.clone(),
            Box::new(move |_| {
                complete_room.store(value, Ordering::Release);
                let begun = capacity.probes_begun(StorageRoot::Complete);
                capacity.request_refresh(StorageRoot::Complete);
                capacity.wait_for_reading_begun_after(StorageRoot::Complete, begun);
            }),
        );
}

// What the waits' register holds promised on the complete folder's disk.
pub fn register_on_complete(pipeline: &Pipeline) -> u64 {
    use weaver_server_disk::operations::room_wait::Disk;
    let capacity = &pipeline.storage_capacity;
    let disk = Disk::Root(StorageRoot::Complete);
    capacity.promises().on(&disk, disk.device(capacity))
}

// What the complete folder's disk has free to spend by its ledger: its
// last reading less what landed since and what is promised there.
pub fn complete_room_now(pipeline: &Pipeline) -> Option<u64> {
    let capacity = &pipeline.storage_capacity;
    capacity.ledger(StorageRoot::Complete).start_room_now(
        &capacity.reader(StorageRoot::Complete),
        0,
        0,
        true,
    )
}

// What the complete folder's own ledger holds promised; on a disk of its
// own, everything promised there.
pub fn complete_promised(pipeline: &Pipeline) -> u64 {
    pipeline
        .storage_capacity
        .ledger(StorageRoot::Complete)
        .promised()
}

pub async fn admit_database_move(
    scenario: &Scenario,
    probe_room: Arc<AtomicU64>,
    probe_database: Arc<AtomicU64>,
) -> (tempfile::TempDir, Pipeline, PathBuf) {
    admit_with(scenario, move |pipeline, temp| {
        let database = temp.to_path_buf();
        let roots = [
            pipeline.script_data_dir.clone(),
            pipeline.intermediate_dir.clone(),
            pipeline.complete_dir.clone(),
        ];
        let probe = Arc::new(move |path: &Path| {
            let room = if path == database {
                &probe_database
            } else {
                &probe_room
            };
            Ok(DiskSpace {
                total_bytes: RESERVE * 8,
                available_bytes: RESERVE + room.load(Ordering::Acquire),
            })
        });
        let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        pipeline.storage_capacity = Arc::new(capacity);
    })
    .await
}
