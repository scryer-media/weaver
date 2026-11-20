// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use weaver_server_composition::{JobId, JobStatus};
use weaver_server_db::persistence::executor::blocking_call_probe;
use weaver_server_pipeline::Pipeline;
use weaver_server_pipeline::jobs::persistence::JobsPersistenceQueries;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::jobs::working_dir::{sanitize_dirname, stamp_working_dir};
use weaver_server_pipeline::pipeline::tests::{
    control_fixture::{CompletedMove, move_in_flight, start_move_to_complete},
    delivery_fixture::*,
    history_fixture::{database, finished_jobs},
    insert_active_job, new_direct_pipeline, new_direct_pipeline_at_roots,
    queue_fixture::report_all,
    queue_scale_fixture::enqueue,
    retire_pipeline_database,
    script_fixture::settle_terminal_scripts,
    terminal_fixture::{any_move_in_flight, minimal_spec, settle_moves},
    window_fixture::{intermediate_dir as intermediate_dir_of, rows},
};

// The moving row, naming the destination, commits before the first file
// leaves the working directory: a restart at any point of the move finds the
// delivery instead of a job still downloading.
#[tokio::test]
async fn moving_row_commits_before_the_first_file_moves() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let id = JobId(10301);
    let spec = minimal_spec(id, "durable delivery");
    let working_dir = insert_active_job(&mut pipeline, id, spec).await;
    tokio::fs::write(working_dir.join("payload.bin"), b"payload")
        .await
        .unwrap();
    report_all(&pipeline);

    let (seen_tx, seen_rx) = std::sync::mpsc::channel();
    let db = database(&pipeline).clone();
    let payload = working_dir.join("payload.bin");
    before_first_move(&pipeline, move |job_id| {
        let row = db.load_active_job(job_id).unwrap().unwrap();
        let _ = seen_tx.send((row.status, row.output_dir, payload.exists()));
    });

    start_move_to_complete(&mut pipeline, id).await.unwrap();
    let done = CompletedMove::receive(&mut pipeline).await;
    let destination = complete_dir.join("durable delivery");
    let (status, output_dir, payload_unmoved) = seen_rx
        .try_recv()
        .expect("the move started without its moving row committed");
    assert_eq!(status, "moving");
    assert_eq!(output_dir, destination);
    assert!(payload_unmoved);
    done.deliver(&mut pipeline).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        b"payload"
    );
    database(&pipeline).flush_write_queue().await.unwrap();
}

async fn finished_once(restored: &mut Pipeline, job_id: JobId, complete_dir: &std::path::Path) {
    let finished = finished_jobs(restored)
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .expect("the job finishes");
    assert_eq!(finished.status, JobStatus::Complete);
    // One delivery, never a second `<name>.#<id>` claim.
    assert_eq!(std::fs::read_dir(complete_dir).unwrap().count(), 1);
}

// The move finished and the restart came before the terminal pass wrote
// anything: the job comes back in its terminal pass and completes on what
// was delivered, fetching nothing.
#[tokio::test]
async fn restart_after_the_final_move_completes_the_delivered_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10302);
    let job_name = "Slate Harbour Delivered";
    let payload = b"delivered payload".to_vec();
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(done.succeeded());
    assert!(!working_dir.exists());
    let destination = complete_dir.join(sanitize_dirname(job_name));
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(!any_move_in_flight(&restored));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
}

// A restart part way through the move: what is left in the working
// directory moves into the same destination, and the job completes.
#[tokio::test]
async fn restart_mid_move_finishes_into_the_same_destination() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10303);
    let job_name = "Slate Harbour Midway";
    let payload = b"delivered payload".to_vec();
    let destination = complete_dir.join(sanitize_dirname(job_name));
    // The first file moves, and the move goes no further: the destination
    // now holds something, so the move refuses it and stops as the restart
    // would have stopped it.
    move_payload_first(&pipeline, &intermediate_dir, &destination);
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(!done.succeeded());
    assert!(working_dir.join("sidecar.nfo").exists());
    assert!(destination.join("payload.bin").exists());
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(move_in_flight(&restored, job_id));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(destination.join("sidecar.nfo"))
            .await
            .unwrap(),
        b"sidecar"
    );
}

// A restart part way through copying one file: the destination holds a
// truncated copy while the whole source is still in the working directory.
// The resumed move takes the source over the half copy.
#[tokio::test]
async fn restart_mid_copy_replaces_the_truncated_entry_with_its_source() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10304);
    let job_name = "Slate Harbour Truncated";
    let payload = b"delivered payload, whole".to_vec();
    let destination = complete_dir.join(sanitize_dirname(job_name));
    // The copy of the first file stops part way, its source untouched,
    // and the move goes no further, as the restart would have stopped it.
    let half_copy = destination.join("payload.bin");
    let truncated = payload[..9].to_vec();
    before_first_move(&pipeline, move |_| {
        std::fs::write(&half_copy, &truncated).unwrap();
    });
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(!done.succeeded());
    assert_eq!(
        std::fs::read(working_dir.join("payload.bin")).unwrap(),
        payload
    );
    assert_eq!(
        std::fs::read(destination.join("payload.bin")).unwrap(),
        &payload[..9]
    );
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(move_in_flight(&restored, job_id));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(destination.join("sidecar.nfo"))
            .await
            .unwrap(),
        b"sidecar"
    );
}

// A pipeline whose complete directory lies inside its intermediate one, as
// an operator may configure it.
async fn new_nested_pipeline(temp: &tempfile::TempDir) -> (Pipeline, PathBuf, PathBuf) {
    let intermediate_dir = temp.path().join("intermediate");
    new_direct_pipeline_at_roots(
        temp.path().join("data"),
        intermediate_dir.clone(),
        intermediate_dir.join("complete"),
        temp.path().join("weaver.db"),
        16 * 1024 * 1024,
        0,
        None,
    )
    .await
}

// The complete directory lies inside the intermediate one, so the moving
// row's destination does too. A restart part way through the move resumes
// into that one destination: the job is not split across it and a second,
// suffixed folder beside it.
#[tokio::test]
async fn restart_mid_move_into_a_nested_complete_dir_keeps_one_destination() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_nested_pipeline(&temp).await;
    assert!(complete_dir.starts_with(&intermediate_dir));
    let job_id = JobId(10305);
    let job_name = "Slate Harbour Nested";
    let payload = b"delivered payload".to_vec();
    let destination = complete_dir.join(sanitize_dirname(job_name));
    move_payload_first(&pipeline, &intermediate_dir, &destination);
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(!done.succeeded());
    assert!(working_dir.join("sidecar.nfo").exists());
    assert!(destination.join("payload.bin").exists());
    database(&pipeline).flush_write_queue().await.unwrap();
    let row = database(&pipeline)
        .load_active_job(job_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.output_dir, destination);
    retire_pipeline_database(pipeline).await;

    let (restored, _, _) = new_nested_pipeline(&temp).await;
    let mut restored = restore_into(restored, &temp, &intermediate_dir, job_id).await;
    assert!(move_in_flight(&restored, job_id));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(destination.join("sidecar.nfo"))
            .await
            .unwrap(),
        b"sidecar"
    );
}

// A moving row that names the job's own working directory, not a
// destination, as one written before the destination was recorded would:
// the resumed move claims a destination and delivers everything into it.
#[tokio::test]
async fn restart_with_a_row_naming_the_working_dir_claims_the_destination() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10306);
    let job_name = "Slate Harbour Unclaimed";
    let payload = b"delivered payload".to_vec();
    let destination = complete_dir.join(sanitize_dirname(job_name));
    move_payload_first(&pipeline, &intermediate_dir, &destination);
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(!done.succeeded());
    // Nothing was delivered and no destination recorded: the first file is
    // back in the working directory, and the row names that directory.
    std::fs::rename(
        destination.join("payload.bin"),
        working_dir.join("payload.bin"),
    )
    .unwrap();
    std::fs::remove_dir(&destination).unwrap();
    database(&pipeline).flush_write_queue().await.unwrap();
    let db = database(&pipeline).clone();
    let named = working_dir.clone();
    tokio::task::spawn_blocking(move || db.update_active_job_output_dir(job_id, &named))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        database(&pipeline)
            .load_active_job(job_id)
            .unwrap()
            .unwrap()
            .output_dir,
        working_dir
    );
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(move_in_flight(&restored, job_id));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert!(!working_dir.exists());
    // The destination claimed now, the one folder under the complete
    // directory, holds every file.
    let claimed = std::fs::read_dir(&complete_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(
        tokio::fs::read(claimed.join("payload.bin")).await.unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(claimed.join("sidecar.nfo")).await.unwrap(),
        b"sidecar"
    );
}

// The turn that hears a final move finish reads nothing from the database,
// however long the queue: the script settings that decide the terminal pass
// are read off the actor and come back as an event.
#[tokio::test]
async fn move_done_turn_reads_no_database_with_a_long_queue() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    for id in 1..=2_000u64 {
        enqueue(&mut pipeline, id).await;
    }
    let job_id = JobId(10304);
    let (_, done) = move_without_hearing_it(&mut pipeline, job_id, "long queue", b"payload").await;
    assert!(done.succeeded());
    assert_eq!(rows(&pipeline), 2_001);

    blocking_call_probe::arm();
    done.deliver_without_settling(&mut pipeline).await;
    let calls = blocking_call_probe::take();
    assert!(
        calls.is_empty(),
        "the move-done turn read the database at {calls:?}"
    );

    settle_terminal_scripts(&mut pipeline).await;
    let finished = finished_jobs(&mut pipeline)
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .expect("the job finishes");
    assert_eq!(finished.status, JobStatus::Complete);
    assert!(complete_dir.join("long queue").join("payload.bin").exists());
}

// A consumed input whose removal failed before the move stays in the working
// directory. A restart part way through that move resumes it, and the resumed
// move leaves the same input: it is no part of the delivery, before the
// restart or after it.
#[tokio::test]
async fn a_resumed_move_leaves_the_consumed_input_it_was_told_to_leave() {
    use weaver_server_pipeline::pipeline::tests::engines::harness::{
        Post, par2_set, payload, spec,
    };
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10311);
    let job_name = "Slate Harbour Held Input";
    let data = payload(4096, 53);
    let mut posts = vec![Post::plain("payload.bin", data.clone())];
    posts.extend(par2_set("payload", &[("payload.bin", &data)], 2));
    // One article a file.
    let working_dir =
        insert_active_job(&mut pipeline, job_id, spec(job_name, &posts, 1 << 20)).await;
    stamp_working_dir(intermediate_dir_of(&pipeline), &working_dir, job_id)
        .await
        .unwrap();
    let works = drain_delivery_work(&mut pipeline, job_id);
    assert_eq!(works.len(), posts.len());
    deliver_posts(&mut pipeline, job_id, &works, &posts).await;

    // A consumed input the delivery cannot remove: a directory with an entry
    // in it, which no file removal takes away on any host or under any user.
    let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
    before_spent_removal(&pipeline, move |spent: &[PathBuf]| {
        let pinned = spent
            .iter()
            .find(|path| path.is_file())
            .expect("the recovery set is spent before delivery")
            .clone();
        std::fs::remove_file(&pinned).unwrap();
        std::fs::create_dir(&pinned).unwrap();
        std::fs::write(pinned.join("held"), b"held").unwrap();
        let _ = pinned_tx.send(pinned.file_name().unwrap().to_owned());
    });
    // The payload moves, and the move goes no further, as a restart would
    // have stopped it.
    let destination = complete_dir.join(sanitize_dirname(job_name));
    move_payload_first(&pipeline, &intermediate_dir, &destination);
    let (working_dir, done) = until_the_move(&mut pipeline, job_id, working_dir).await;
    let pinned = pinned_rx
        .try_recv()
        .expect("the delivery removed its consumed inputs");
    assert!(!done.succeeded());
    assert!(working_dir.join(&pinned).join("held").exists());
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(move_in_flight(&restored, job_id));
    settle_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        data
    );
    assert!(
        !destination.join(&pinned).exists(),
        "the resumed move delivered the consumed input"
    );
    assert!(
        working_dir.join(&pinned).join("held").exists(),
        "the consumed input left the working directory"
    );
}
