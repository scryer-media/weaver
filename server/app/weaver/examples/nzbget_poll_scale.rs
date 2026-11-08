// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Times one NZBGet `status`, `listgroups` and in-memory `history` poll over
//! a large queue, the way each read the queue before and the way it reads it
//! now, and the most heap each held at once.
//!
//! `cargo run --release --locked -p weaver --example nzbget_poll_scale -- 10000`
//!
//! A group is stood in by its queue item's JSON; the facade's own group
//! object is built from the same item and is about the same size.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use weaver_server_api::{
    QueueItemState, QueueStateFacts, queue_item_from_job, queue_item_state,
    queue_item_state_for_download_accounting,
    queue_item_state_from_job_info_for_download_accounting, queue_table_item_from_job,
};
use weaver_server_core::operations::metrics::PipelineMetrics;
use weaver_server_core::{
    DownloadState, JobId, JobInfo, JobStatus, PostState, RunState, SharedPipelineState,
};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(live, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const CHUNK: usize = 256;

fn job(id: u64) -> JobInfo {
    let downloading = id.is_multiple_of(10);
    let status = if downloading {
        JobStatus::Downloading
    } else {
        JobStatus::Queued
    };
    JobInfo {
        job_id: JobId(id),
        job_hash: None,
        name: format!("Silver.Horizon.S05E{id:05}.1080p.BluRay.DD5.1.x264-WVR"),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        status,
        download_state: if downloading {
            DownloadState::Downloading
        } else {
            DownloadState::Queued
        },
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: PostState::Idle,
        run_state: RunState::Active,
        progress: 0.25,
        total_bytes: 4 << 30,
        downloaded_bytes: 1 << 30,
        optional_recovery_bytes: 200 << 20,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 60,
        completed_files: 15,
        remaining_par_files: 8,
        // A job's per-file remaining bytes: one entry per file.
        file_remaining_bytes: vec![70 << 20; 60],
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
        password: None,
        category: Some("tv".to_string()),
        metadata: vec![
            ("drone".to_string(), format!("{id:032x}")),
            ("priority".to_string(), "NORMAL".to_string()),
            ("nzbget.dupe_key".to_string(), format!("tvdb-{id}")),
        ],
        output_dir: Some(format!("/data/complete/tv/Silver.Horizon.S05E{id:05}")),
        created_at_epoch_ms: 1_900_000_000_000.0,
        server_attribution: Vec::new(),
    }
}

fn measure<T>(label: &str, run: impl FnOnce() -> T) -> T {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let started = Instant::now();
    let out = run();
    let elapsed = started.elapsed();
    let peak = PEAK.load(Ordering::Relaxed) - base;
    println!(
        "{label:<28} {:>8.2} ms  peak {:>8.2} MiB",
        elapsed.as_secs_f64() * 1e3,
        peak as f64 / (1 << 20) as f64
    );
    out
}

fn terminal(state: QueueItemState) -> bool {
    matches!(state, QueueItemState::Completed | QueueItemState::Failed)
}

fn main() {
    let count: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(10_000);
    let state = SharedPipelineState::new(PipelineMetrics::new(), (1..=count).map(job).collect());
    println!("{count} queued jobs");

    measure("status before", || {
        let jobs = state.list_jobs();
        jobs.iter()
            .map(queue_item_state_from_job_info_for_download_accounting)
            .filter(|state| !terminal(*state))
            .count()
    });
    measure("status now", || {
        state.read_queue(|inner| {
            inner
                .jobs
                .iter()
                .filter(|row| {
                    let view = row.view(&inner.globals);
                    !terminal(queue_item_state_for_download_accounting(
                        QueueStateFacts::of_view(&view),
                    ))
                })
                .count()
        })
    });

    let before = measure("listgroups before", || {
        let jobs = state.list_jobs();
        let groups = jobs
            .iter()
            .map(queue_item_from_job)
            .filter(|item| !terminal(item.state))
            .map(|item| serde_json::to_value(&item).unwrap())
            .collect::<Vec<_>>();
        serde_json::to_vec(&serde_json::json!({ "version": "1.1", "id": 1, "result": groups }))
            .unwrap()
            .len()
    });
    let now = measure("listgroups now", || {
        let ids: Vec<JobId> = state.read_queue(|inner| {
            inner
                .jobs
                .iter()
                .filter(|row| {
                    let view = row.view(&inner.globals);
                    !terminal(queue_item_state(QueueStateFacts::of_view(&view)))
                })
                .map(|row| row.id)
                .collect()
        });
        let mut sent = 0usize;
        let mut buf = Vec::with_capacity(CHUNK * 1024);
        for chunk in ids.chunks(CHUNK) {
            let jobs = state.read_queue(|inner| {
                chunk
                    .iter()
                    .filter_map(|id| inner.row(*id))
                    .map(|row| row.view(&inner.globals).list_info())
                    .collect::<Vec<_>>()
            });
            for job in &jobs {
                let item = queue_table_item_from_job(job);
                serde_json::to_writer(&mut buf, &serde_json::to_value(&item).unwrap()).unwrap();
                buf.push(b',');
            }
            // The chunk goes to the client here.
            sent += buf.len();
            buf.clear();
        }
        sent
    });
    println!("listgroups bytes            before {before}  now {now}");

    measure("history (in memory) before", || {
        let jobs = state.list_jobs();
        jobs.iter()
            .map(queue_item_from_job)
            .filter(|item| terminal(item.state))
            .count()
    });
    measure("history (in memory) now", || {
        let finished = state.read_queue(|inner| {
            inner
                .jobs
                .iter()
                .filter_map(|row| {
                    let view = row.view(&inner.globals);
                    terminal(queue_item_state(QueueStateFacts::of_view(&view)))
                        .then(|| view.list_info())
                })
                .collect::<Vec<_>>()
        });
        let history = state.history_rows();
        finished
            .iter()
            .chain(history.iter())
            .map(queue_table_item_from_job)
            .filter(|item| terminal(item.state))
            .count()
    });
}
