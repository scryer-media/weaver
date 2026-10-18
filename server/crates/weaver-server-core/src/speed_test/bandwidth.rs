// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;
use weaver_nntp::transfer::{BodyTransferPermit, ServerTransferControl};

use super::adaptive::{Controller, Decision, TICK, Tick};
use super::failure::SpeedTestFailure;
use super::http::{HttpLeg, HttpOptions, RoutedClient};
use super::progress::{LegRecorder, Progress};
use super::runner::Stop;

// One leg that is up, as a bandwidth run opens connections on it.
pub(crate) struct BandwidthLeg {
    pub http: HttpLeg,
    pub recorder: LegRecorder,
    // The live egress control the leg's bytes are booked against.
    pub egress_control: Option<Arc<ServerTransferControl>>,
}

pub(crate) struct BandwidthPlan {
    // By position; `None` for a leg that is down.
    pub legs: Vec<Option<Arc<BandwidthLeg>>>,
    pub weights: Vec<u8>,
    pub url: reqwest::Url,
    // What one GET of `url` moves; `None` when only the response says.
    pub size: Option<u64>,
    pub options: HttpOptions,
    // Ignore the controller's stop and measure until cancelled or until
    // every connection has ended; for tests that end a run on a condition.
    pub until_stopped: bool,
}

// Open connections as the controller asks, each repeating GETs of the URL,
// until the controller stops the run, it is cancelled, or every connection
// has ended.
pub(crate) async fn run(plan: BandwidthPlan, progress: Arc<Progress>, mut stop: Stop) {
    let plan = Arc::new(plan);
    let (halt, halted) = watch::channel(false);
    let worker_stop = stop.with_halt(halted);
    let mut controller = Controller::new(
        plan.weights.clone(),
        plan.legs.iter().map(Option::is_some).collect(),
    );
    let mut tasks = tokio::task::JoinSet::new();
    // The leg of every connection still open, and whether it moved a byte.
    let mut open: HashMap<tokio::task::Id, (usize, Arc<AtomicBool>)> = HashMap::new();
    let spawn =
        |tasks: &mut tokio::task::JoinSet<Ended>, open: &mut HashMap<_, _>, position: usize| {
            let Some(Some(leg)) = plan.legs.get(position) else {
                return;
            };
            let moved = Arc::new(AtomicBool::new(false));
            let handle = tasks.spawn(connection(
                leg.clone(),
                plan.clone(),
                progress.clone(),
                worker_stop.clone(),
                moved.clone(),
            ));
            open.insert(handle.id(), (position, moved));
        };
    for position in controller.initial() {
        spawn(&mut tasks, &mut open, position);
    }
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    while !tasks.is_empty() {
        tokio::select! {
            biased;
            _ = stop.stopped() => break,
            joined = tasks.join_next_with_id() => {
                let (id, ended) = match joined {
                    Some(Ok((id, ended))) => (id, Some(ended)),
                    Some(Err(error)) => (error.id(), None),
                    None => break,
                };
                if let Some((position, moved)) = open.remove(&id) {
                    // Only a connection that failed after reaching the URL
                    // is worth another try.
                    let replaceable =
                        ended == Some(Ended::Failed) && moved.load(Ordering::Relaxed);
                    if controller.ended(position, replaceable) {
                        spawn(&mut tasks, &mut open, position);
                    }
                }
            }
            _ = ticker.tick() => {
                let waiting = open
                    .values()
                    .filter(|(_, moved)| !moved.load(Ordering::Relaxed))
                    .count();
                let decision = controller.tick(Tick {
                    at: progress.elapsed(),
                    bytes: progress.bytes(),
                    waiting,
                });
                if let Some((at, bytes)) = controller.warm_up() {
                    progress.mark_warm_up(at, bytes);
                }
                match decision {
                    Decision::Wait => {}
                    Decision::Open(position) => spawn(&mut tasks, &mut open, position),
                    Decision::Stop if plan.until_stopped => {}
                    Decision::Stop => break,
                }
            }
        }
    }
    progress.finish();
    halt.send_replace(true);
    while tasks.join_next().await.is_some() {}
}

// The most of its egress's quota a connection holds at a time. 16 MiB is a
// few reservations a second for a connection on a gigabit line, small next
// to what a 30 s run moves, and the most a connection reads past the quota.
pub(super) const QUOTA_SLICE: u64 = 16 * 1024 * 1024;

// One GET's hold on the egress quota: a slice at a time, each booked with
// the bytes read under it and given back when the next is taken.
struct QuotaSlice<'a> {
    leg: &'a BandwidthLeg,
    permit: Option<BodyTransferPermit>,
    // Bytes the current slice still has room for.
    room: u64,
}

impl<'a> QuotaSlice<'a> {
    fn new(leg: &'a BandwidthLeg) -> Self {
        Self {
            leg,
            permit: None,
            room: 0,
        }
    }

    // Hold a slice with room before more of the GET is read. `remaining` is
    // what is left of the GET when its length is known; a slice is never
    // larger than that, and none is taken once it is all in.
    fn ready(&mut self, remaining: Option<u64>) -> Result<(), SpeedTestFailure> {
        let Some(control) = &self.leg.egress_control else {
            return Ok(());
        };
        if self.permit.is_some() && (self.room > 0 || remaining == Some(0)) {
            return Ok(());
        }
        // Dropping the used slice books what was read under it.
        self.permit = None;
        let size = remaining.map_or(QUOTA_SLICE, |remaining| remaining.min(QUOTA_SLICE));
        let permit = control
            .try_reserve(size)
            .map_err(|_| SpeedTestFailure::EgressQuotaUsed {
                egress: self.leg.recorder.egress,
            })?;
        self.permit = Some(permit);
        self.room = size;
        Ok(())
    }

    fn book(&mut self, bytes: usize) {
        if let Some(permit) = &mut self.permit {
            permit.record_unpaced(bytes);
        }
        self.room = self.room.saturating_sub(bytes as u64);
    }
}

// Why a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    // The run stopped it.
    Stopped,
    // A dial, a GET or a body failed, or a body was empty.
    Failed,
    // Its egress has used its download quota.
    Quota,
}

// One connection: GET the URL again each time a body ends, until stopped
// or until a GET fails.
async fn connection(
    leg: Arc<BandwidthLeg>,
    plan: Arc<BandwidthPlan>,
    progress: Arc<Progress>,
    mut stop: Stop,
    moved: Arc<AtomicBool>,
) -> Ended {
    let recorder = leg.recorder.clone();
    let connect =
        RoutedClient::connect(&leg.http, &plan.url, &plan.options, Some(recorder.clone()));
    let client = tokio::select! {
        biased;
        _ = stop.stopped() => return Ended::Stopped,
        connected = connect => match connected {
            Ok(client) => client,
            Err((provider, error)) => {
                match provider {
                    Some(provider) => provider.fail(error),
                    None => {
                        recorder.error_row(error);
                    }
                }
                progress.counted();
                return Ended::Failed;
            }
        },
    };
    let ended = 'gets: loop {
        if stop.is_stopped() {
            break Ended::Stopped;
        }
        let mut quota = QuotaSlice::new(&leg);
        // A known size takes its first slice before the GET, so an egress
        // out of quota sends nothing; any other waits for the headers.
        if let Some(size) = plan.size
            && let Err(error) = quota.ready(Some(size))
        {
            client.provider.fail(error);
            break Ended::Quota;
        }
        let response = tokio::select! {
            biased;
            _ = stop.stopped() => break Ended::Stopped,
            response = client.get(&plan.url) => response,
        };
        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                client.provider.fail(error);
                break Ended::Failed;
            }
        };
        let length = response.content_length().or(plan.size);
        let mut received = 0u64;
        // Each chunk is counted and dropped: nothing of the body is kept,
        // buffered across chunks or written anywhere.
        loop {
            // Past a declared length that proved short, the rest is unknown.
            let remaining = length.and_then(|length| length.checked_sub(received));
            if let Err(error) = quota.ready(remaining) {
                client.provider.fail(error);
                break 'gets Ended::Quota;
            }
            let chunk = tokio::select! {
                biased;
                _ = stop.stopped() => break 'gets Ended::Stopped,
                chunk = response.chunk() => chunk,
            };
            match chunk {
                Ok(Some(chunk)) => {
                    let n = chunk.len();
                    drop(chunk);
                    received += n as u64;
                    moved.store(true, Ordering::Relaxed);
                    progress.add_leg_bytes(recorder.leg, n as u64);
                    client.provider.add_bytes(n as u64);
                    quota.book(n);
                }
                Ok(None) => break,
                Err(error) => {
                    client
                        .provider
                        .fail(format!("reading the body failed: {error}"));
                    break 'gets Ended::Failed;
                }
            }
        }
        drop(quota);
        progress.counted();
        if received == 0 {
            client.provider.fail("the response has no body");
            break Ended::Failed;
        }
    };
    client.close().await;
    progress.counted();
    ended
}
