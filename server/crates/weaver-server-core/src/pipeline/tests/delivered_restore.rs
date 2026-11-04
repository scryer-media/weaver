// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The moving row, naming the destination, commits before the first file
// leaves the working directory: a restart at any point of the move finds the
// delivery instead of a job still downloading.
#[tokio::test]
async fn moving_row_commits_before_the_first_file_moves() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let id = JobId(10301);
    let spec = minimal_job_state(id, "durable delivery", PathBuf::new()).spec;
    let working_dir = insert_active_job(&mut pipeline, id, spec).await;
    tokio::fs::write(working_dir.join("payload.bin"), b"payload")
        .await
        .unwrap();
    pipeline.report_all_for_test();

    let (seen_tx, seen_rx) = std::sync::mpsc::channel();
    let db = pipeline.db.clone();
    let payload = working_dir.join("payload.bin");
    *crate::pipeline::completion::finalize::BEFORE_FIRST_MOVE
        .lock()
        .unwrap() = Some(Box::new(move |job_id| {
        let row = db.load_active_job(job_id).unwrap().unwrap();
        let _ = seen_tx.send((row.status, row.output_dir, payload.exists()));
    }));

    pipeline.start_move_to_complete(id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let destination = complete_dir.join("durable delivery");
    let (status, output_dir, payload_unmoved) = seen_rx
        .try_recv()
        .expect("the move started without its moving row committed");
    assert_eq!(status, "moving");
    assert_eq!(output_dir, destination);
    assert!(payload_unmoved);
    pipeline.handle_move_to_complete_done(done).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        b"payload"
    );
    pipeline.db.flush_write_queue().await.unwrap();
}
