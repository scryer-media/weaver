// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub fn assert_admissions(pipeline: &Pipeline) {
    pipeline.shared_state.queue.read(|inner| {
        assert_eq!(inner.globals.nntp_generation, pipeline.pool_generation);
        for row in &inner.jobs {
            if pipeline.nzb_admitted.contains(&row.id) {
                assert!(
                    row.report.nzb_admitted,
                    "admission was not published for {:?}",
                    row.id
                );
                assert!(!row.report.retiring && !row.report.restoring && row.admission.is_none());
                assert_ne!(row.report.job.run_state, crate::RunState::Paused);
            }
        }
    });
}

pub async fn command_while_reserved(
    pipeline: &mut Pipeline,
    observed: &mut tokio::sync::mpsc::UnboundedReceiver<(usize, String)>,
    format: &str,
) -> (usize, String) {
    loop {
        return tokio::select! {
            command = observed.recv() => command.unwrap(),
            event = pipeline.owned_download_lane_event_rx.recv() => {
                match event.unwrap() {
                    event @ OwnedDownloadLaneEvent::AcquireFailed { .. } => {
                        // Health-lock contention returns a reservation to
                        // the actor. Drive that event exactly as runtime
                        // does, rather than assuming every dial wins a
                        // concurrently held selection lock on its first try.
                        let mut results = VecDeque::new();
                        pipeline.handle_owned_download_lane_event(event, &mut results);
                        assert!(results.is_empty(), "local admission must not become article failure");
                        pipeline.dispatch_downloads();
                        continue;
                    },
                    OwnedDownloadLaneEvent::LeaseObsolete { lease } => panic!("{format}: obsolete lease for {:?} generation {}", lease.job_id, lease.runtime_generation),
                    OwnedDownloadLaneEvent::BatchComplete { .. } => panic!("{format}: premature completion"),
                }
            }
        };
    }
}

pub fn hold_admission(pipeline: &Pipeline) {
    pipeline.shared_state.queue.write(|inner| {
        inner
            .globals
            .holds
            .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
        crate::pipeline::queue::Change::changed(())
    });
}

pub async fn await_parked(pipeline: &mut Pipeline, count: usize) {
    let mut parked = 0;
    while parked < count {
        tokio::select! {
            Some(request) = pipeline.download_refill_rx.recv() => {
                assert!(request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                }).is_ok());
            }
            Some(event) = pipeline.owned_download_lane_event_rx.recv() => {
                let OwnedDownloadLaneEvent::BatchComplete { ack, .. } = event else {
                    panic!("only issued leases remain during fixture teardown");
                };
                if let Some(ack) = ack { ack.send(()).unwrap(); }
            }
            Some(_) = pipeline.download_lane_parked_rx.recv() => parked += 1,
        }
    }
}

pub async fn command_while_refilling(
    pipeline: &mut Pipeline,
    observed: &mut tokio::sync::mpsc::UnboundedReceiver<(usize, String)>,
) -> (usize, String) {
    loop {
        tokio::select! {
            command = observed.recv() => break command.unwrap(),
            event = pipeline.owned_download_lane_event_rx.recv() => {
                let event = event.unwrap();
                let mut results = VecDeque::new();
                pipeline.handle_owned_download_lane_event(event, &mut results);
                pipeline.dispatch_downloads();
            }
            Some(request) = pipeline.download_refill_rx.recv() => {
                let _ = request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                });
            }
        }
    }
}

pub fn set_providers(pipeline: &mut Pipeline, client: weaver_nntp::NntpClient, lanes: usize) {
    pipeline.nntp = Arc::new(client);
    pipeline.tuner.set_connection_limit(lanes);
    pipeline.owned_download_lane_pool.resize(lanes);
}
pub fn dispatch(pipeline: &mut Pipeline) {
    pipeline.dispatch_downloads();
}
pub fn owner_count(pipeline: &Pipeline) -> usize {
    pipeline.download_lane_owners.len()
}
pub fn queue_count(pipeline: &Pipeline) -> usize {
    pipeline.shared_state.queue.read(|inner| inner.jobs.len())
}
pub fn reset_empty(pipeline: &mut Pipeline) -> bool {
    pipeline.owned_download_lane_pool.reset().leases.is_empty()
}
pub async fn stop_client(pipeline: &Pipeline) {
    pipeline.nntp.shutdown().await;
}
