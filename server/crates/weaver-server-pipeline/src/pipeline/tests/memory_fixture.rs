// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// When the pipeline gives memory back: once it settles with nothing it can
// do, whether no job waits or jobs hold slots no server can serve, and after
// a turn that did something while it stays settled; never on an idle tick.

use super::*;

const QUEUED: u64 = 40;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Settled NZB {id}"), "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("settled-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// A queue with two jobs holding slots, its ring's reads not yet heard.
pub async fn queue_with_reads_out(temp: &tempfile::TempDir) -> (Pipeline, SchedulerHandle) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    (pipeline, handle)
}

pub async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

pub fn server_count(pipeline: &Pipeline) -> usize {
    pipeline.nntp.pool().server_count()
}
pub fn admitted_count(pipeline: &Pipeline) -> usize {
    pipeline.nzb_admitted.len()
}
pub fn reading(pipeline: &Pipeline) -> bool {
    pipeline.next_up.reading()
}
pub fn settled(pipeline: &Pipeline, driving: bool) -> bool {
    pipeline.memory_settled(driving)
}
pub fn release_due(was: bool, settled: bool) -> bool {
    Pipeline::memory_release_due(was, settled)
}
pub fn set_client(pipeline: &mut Pipeline, client: NntpClient) {
    pipeline.nntp = Arc::new(client);
}
