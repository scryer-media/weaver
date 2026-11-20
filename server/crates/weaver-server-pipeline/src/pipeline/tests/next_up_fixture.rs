// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::queue_scale_fixture::settle_next_up;
use super::window_fixture::settle as settle_window;
use super::*;
use crate::pipeline::db_reply::DbReply;

const QUEUED: u64 = 200;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Ring NZB {id}"), "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("ring-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Turns as `settle_window` does, but every read ahead that comes back is
// held back unheard and returned, so the test chooses when the ring hears
// it: the pipeline hears only the replies the test hands it.
pub async fn settle_window_holding_reads(pipeline: &mut Pipeline) -> HeldReads {
    let mut held = Vec::new();
    loop {
        pipeline.run_queue_intents().await;
        let mut walk_again = false;
        while pipeline.db_replies_out() {
            let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
            if matches!(reply, DbReply::NextUp { .. }) {
                held.push(reply);
                continue;
            }
            walk_again |= matches!(
                reply,
                DbReply::Slot {
                    committed: true,
                    ..
                }
            );
            let installs = matches!(reply, DbReply::Installs(_));
            pipeline.handle_db_reply(reply).await;
            walk_again |= installs && !pipeline.restores_waiting;
        }
        if !walk_again && !pipeline.restores_waiting {
            return HeldReads(held);
        }
    }
}

// Wait, holding them back unheard, until the ring's read numbered `read` is
// among the reads held.
pub async fn hold_reads_through(pipeline: &mut Pipeline, held: &mut HeldReads, read: u64) {
    let back = |held: &[DbReply]| {
        held.iter()
            .any(|reply| matches!(reply, DbReply::NextUp { read: number, .. } if *number == read))
    };
    while !back(&held.0) {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        if matches!(reply, DbReply::NextUp { .. }) {
            held.0.push(reply);
        } else {
            pipeline.handle_db_reply(reply).await;
        }
    }
}

// Hand the ring the reads held back numbered `reads`, in that order.
pub async fn hear(pipeline: &mut Pipeline, held: &mut HeldReads, reads: &[u64]) {
    for read in reads {
        let index = held
            .0
            .iter()
            .position(
                |reply| matches!(reply, DbReply::NextUp { read: number, .. } if number == read),
            )
            .expect("the read is held");
        let reply = held.0.remove(index);
        pipeline.handle_db_reply(reply).await;
    }
}

pub async fn ring(slots: u32) -> (tempfile::TempDir, Pipeline, SchedulerHandle) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    settle_window(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    (temp, pipeline, handle)
}

pub async fn turn(pipeline: &mut Pipeline) {
    settle_window(pipeline).await;
    settle_next_up(pipeline).await;
}

pub struct HeldReads(Vec<DbReply>);

impl HeldReads {
    pub fn extend(&mut self, more: Self) {
        self.0.extend(more.0);
    }

    pub async fn deliver_all(&mut self, pipeline: &mut Pipeline) {
        for reply in self.0.drain(..) {
            pipeline.handle_db_reply(reply).await;
        }
    }
}

pub fn next_up_ids(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.next_up.ids()
}

pub fn last_read(pipeline: &Pipeline) -> u64 {
    pipeline.next_up.last_read()
}

pub fn move_offset(pipeline: &Pipeline, from: JobId, to: JobId) -> i64 {
    pipeline.shared_state.queue.read(|inner| {
        let from = inner.job_position(from).unwrap() as i64;
        let to = inner.job_position(to).unwrap() as i64;
        to - from
    })
}

pub struct PackedLists(crate::pipeline::next_up::HeldLists);

impl PackedLists {
    pub fn pack(lists: &[Vec<SegmentSpec>]) -> Option<Self> {
        crate::pipeline::next_up::HeldLists::pack(lists).map(Self)
    }

    pub fn unpack(&self) -> Vec<Vec<SegmentSpec>> {
        self.0.unpack()
    }

    pub fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}
