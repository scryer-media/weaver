// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use weaver_nntp::blocking::BlockingBodyLane;
use weaver_nntp::client::{
    BlockingBodyLaneAcquireError, DecodedBody, DecodedBodyError, NntpClient, NntpClientConfig,
};
use weaver_nntp::route_dialer::RouteDialer;
use weaver_nntp::transfer::{ServerTransferControl, ServerTransferRegistry, TransferScope};
use weaver_nntp::{NntpError, ServerSet};
use weaver_tunnel::pipe::Dialer;

use super::http::TestArticle;
use super::progress::{LegRecorder, Progress, RecordingDialer};
use super::runner::Stop;
use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::download::acquire_owned_lane_through_contention;

// One leg of a Usenet run.
pub(crate) struct UsenetLeg {
    pub position: usize,
    pub connections: u16,
    pub stage: Arc<dyn Dialer>,
    pub egress: u32,
    pub rung_map: Arc<[usize]>,
    // The live egress control the leg's bytes are booked against.
    pub egress_control: Option<Arc<ServerTransferControl>>,
}

pub(crate) struct UsenetPlan {
    pub server: crate::servers::ServerConfig,
    pub legs: Vec<UsenetLeg>,
    pub articles: Vec<TestArticle>,
    // The live server control the run's bytes are booked against.
    pub server_control: Option<Arc<ServerTransferControl>>,
    pub buffer_profile: weaver_nntp::connection::NntpBufferProfile,
}

// The server's NNTP settings exactly as the live pool builds them, dialed
// through one leg of the draft.
fn server_config(
    server: &crate::servers::ServerConfig,
    dialer: Arc<RouteDialer>,
    buffer_profile: weaver_nntp::connection::NntpBufferProfile,
) -> weaver_nntp::ServerConfig {
    weaver_nntp::ServerConfig {
        host: server.host.clone(),
        port: server.port,
        tls: server.tls,
        username: server.username.clone(),
        password: server.password.clone(),
        tls_ca_cert: server.tls_ca_cert.clone(),
        tls_name_mismatch_certificate_der: server.tls_name_mismatch_certificate_der.clone(),
        buffer_profile,
        pipelining: weaver_nntp::PipeliningCapability::Known(server.supports_pipelining),
        pipelining_depth: server.pipelining_depth,
        ..weaver_nntp::ServerConfig::new(dialer)
    }
}

struct Booking {
    progress: Arc<Progress>,
    metrics: Arc<PipelineMetrics>,
    leg: usize,
}

impl Booking {
    // Book an article's raw size exactly as the download worker does on
    // completion, plus the leg's own counter.
    fn book(&self, raw_size: u32, permits: &mut [weaver_nntp::transfer::BodyTransferPermit]) {
        for permit in permits.iter_mut() {
            permit.record_unpaced(raw_size as usize);
        }
        self.metrics
            .bytes_downloaded
            .fetch_add(u64::from(raw_size), Ordering::Relaxed);
        self.progress.add_leg_bytes(self.leg, u64::from(raw_size));
    }
}

// Each connection is a lane of its own on a worker thread, dialled and
// driven as the download lanes are: the run measures the path downloads take.
pub(crate) async fn run(
    plan: UsenetPlan,
    progress: Arc<Progress>,
    metrics: Arc<PipelineMetrics>,
    mut stop: Stop,
) {
    let queue = Arc::new(Mutex::new(VecDeque::from(plan.articles)));
    let server = Arc::new(plan.server);
    let runtime = tokio::runtime::Handle::current();
    let mut clients = Vec::new();
    // Every worker holds a sender; the channel closes once the last one has
    // dropped its connection.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    for leg in plan.legs {
        if leg.connections == 0 {
            continue;
        }
        let recording = Arc::new(RecordingDialer {
            inner: leg.stage.clone(),
            recorder: LegRecorder {
                progress: progress.clone(),
                leg: leg.position,
                egress: leg.egress,
                rung_map: leg.rung_map.clone(),
            },
        });
        // A fresh egress registry: the speed limits do not apply to a run.
        let dialer = Arc::new(RouteDialer {
            inner: recording,
            plan: None,
            egress_controls: Arc::new(ServerTransferRegistry::with_scope(TransferScope::Egress)),
            runtime: runtime.clone(),
            server: server.id,
        });
        let client = Arc::new(NntpClient::new(NntpClientConfig::single(
            server_config(&server, dialer, plan.buffer_profile),
            usize::from(leg.connections),
        )));
        clients.push(client.clone());
        for connection in 0..leg.connections {
            let worker = Worker {
                client: client.clone(),
                queue: queue.clone(),
                booking: Booking {
                    progress: progress.clone(),
                    metrics: metrics.clone(),
                    leg: leg.position,
                },
                egress: leg.egress,
                server_control: plan.server_control.clone(),
                egress_control: leg.egress_control.clone(),
                server_name: server.host.clone(),
                stop: stop.clone(),
                runtime: runtime.clone(),
                _done: done_tx.clone(),
            };
            let spawned = std::thread::Builder::new()
                .name(format!("weaver-speed-test-{}-{connection}", leg.position))
                .spawn(move || {
                    crate::runtime::affinity::pin_current_thread_for_hot_download_path();
                    crate::runtime::thread_release::start_lane_thread();
                    worker.run();
                });
            if let Err(error) = spawned {
                progress.end_leg_connection(
                    leg.position,
                    leg.egress,
                    format!("could not start a connection thread: {error}"),
                );
            }
        }
    }
    drop(done_tx);
    tokio::select! {
        biased;
        _ = done_rx.recv() => {}
        _ = stop.stopped() => {
            // The clock stops at the stop. An article still on the wire is
            // read to its end off the clock and not booked; waiting for it
            // keeps the run's sockets inside the connections it was lent.
            progress.finish();
            done_rx.recv().await;
        }
    }
    // The last connection closes here; the clock stops after it.
    for client in clients {
        client.shutdown().await;
    }
}

struct Worker {
    client: Arc<NntpClient>,
    queue: Arc<Mutex<VecDeque<TestArticle>>>,
    booking: Booking,
    egress: u32,
    server_control: Option<Arc<ServerTransferControl>>,
    egress_control: Option<Arc<ServerTransferControl>>,
    server_name: String,
    stop: Stop,
    runtime: tokio::runtime::Handle,
    _done: tokio::sync::mpsc::Sender<()>,
}

impl Worker {
    // Runs on the worker's own thread, never on the async runtime.
    fn run(mut self) {
        let progress = self.booking.progress.clone();
        let leg = self.booking.leg;
        let mut lane = None;
        loop {
            if self.stop.is_stopped() {
                return;
            }
            let Some(article) = self.queue.lock().expect("article queue").pop_front() else {
                return;
            };
            let declared = u64::from(article.bytes);
            let mut permits = Vec::new();
            for (control, holder) in [
                (&self.server_control, format!("server {}", self.server_name)),
                (&self.egress_control, format!("egress {}", self.egress)),
            ] {
                let Some(control) = control else { continue };
                match control.try_reserve(declared) {
                    Ok(permit) => permits.push(permit),
                    Err(_) => {
                        progress.end_leg_connection(
                            leg,
                            self.egress,
                            format!("{holder} has used its download quota"),
                        );
                        return;
                    }
                }
            }
            let fetched = self.fetch(&mut lane, &article);
            // A stop during the fetch wins over its answer.
            if self.stop.is_stopped() {
                return;
            }
            progress.usenet.requested.fetch_add(1, Ordering::Relaxed);
            progress
                .usenet
                .declared
                .fetch_add(declared, Ordering::Relaxed);
            match fetched {
                Ok(body) => {
                    self.booking.book(body.raw_size, &mut permits);
                    progress.usenet.delivered.fetch_add(1, Ordering::Relaxed);
                    progress.add_decoded(leg, body.io.decoded_bytes_written);
                }
                Err(DecodedBodyError::Decode { raw_size, .. }) => {
                    // A decode error still books its raw size.
                    self.booking.book(raw_size, &mut permits);
                    progress.usenet.failed.fetch_add(1, Ordering::Relaxed);
                }
                Err(DecodedBodyError::Nntp(error)) if error.is_article_not_found() => {
                    progress.usenet.missing.fetch_add(1, Ordering::Relaxed);
                }
                Err(DecodedBodyError::Nntp(error)) => {
                    progress.usenet.failed.fetch_add(1, Ordering::Relaxed);
                    progress.end_leg_connection(leg, self.egress, error.to_string());
                    progress.counted();
                    return;
                }
            }
            progress.counted();
            if crate::e2e_clock::hold_speed_test_after_article() {
                self.runtime.block_on(self.stop.stopped());
                return;
            }
        }
    }

    // One article, with a second try on a fresh connection when the first
    // ended without the server's answer to it: the socket was lost, or a 412
    // taught that the server needs a group the next connection now selects.
    fn fetch(
        &self,
        lane: &mut Option<BlockingBodyLane>,
        article: &TestArticle,
    ) -> Result<DecodedBody, DecodedBodyError> {
        let first = self.attempt(lane, article);
        let unanswered = lane.is_none()
            && matches!(&first, Err(DecodedBodyError::Nntp(error))
                if !error.is_article_not_found() && !error.is_credentials_refusal());
        if unanswered && !self.stop.is_stopped() {
            return self.attempt(lane, article);
        }
        first
    }

    // One BODY on the worker's connection, dialled first when it has none.
    // A connection that cannot carry another request is let go.
    fn attempt(
        &self,
        lane: &mut Option<BlockingBodyLane>,
        article: &TestArticle,
    ) -> Result<DecodedBody, DecodedBodyError> {
        if let Some(open) = lane.as_mut()
            && open.adopt_groups(&article.groups).is_err()
        {
            *lane = None;
        }
        let open = match lane {
            Some(open) => open,
            None => match self.dial(&article.groups) {
                Ok(dialled) => lane.insert(dialled),
                Err(error) => return Err(DecodedBodyError::Nntp(dial_error(error))),
            },
        };
        let trace = open.fetch_decoded_sequential(&article.message_id);
        self.client.record_blocking_attempts(&trace.attempts);
        #[cfg(test)]
        lane_fetches::count(self.client.as_ref());
        if !open.is_healthy() {
            *lane = None;
        }
        trace.result
    }

    // Dial a connection for `groups`, waiting out a refusal the download
    // lanes would hand back to the scheduler, as their scheduler does: a
    // lost selection race is asked again at once; a seat or the server's
    // one recovery probe held elsewhere is asked again when the server's
    // health changes, or at the instant its hold lapses by itself, the two
    // wakes the pipeline's dispatch waits on. All of it within `DIAL_WAIT`
    // of the first ask, one deadline, and never past the run's stop; the
    // async acquire it replaces waited as long.
    fn dial(&self, groups: &[String]) -> Result<BlockingBodyLane, BlockingBodyLaneAcquireError> {
        let pool = self.client.pool();
        let mut changes = pool.server_health_changes();
        let give_up = std::time::Instant::now() + DIAL_WAIT;
        loop {
            // Read before the ask as well as after it: a hold that lapses
            // between the refusal and the read after it leaves the read
            // empty, and the wait would hear nothing.
            let held_until = pool.next_connection_hold_deadline();
            let dialled = acquire_owned_lane_through_contention(
                || !self.stop.is_stopped(),
                || {
                    self.client
                        .try_acquire_blocking_body_lane(groups, ServerSet::EMPTY)
                },
            );
            let error = match dialled {
                Err(error)
                    if error.should_requeue_owned_work()
                        && !self.stop.is_stopped()
                        && std::time::Instant::now() < give_up =>
                {
                    error
                }
                outcome => return outcome,
            };
            if matches!(error, BlockingBodyLaneAcquireError::SelectionContended) {
                continue;
            }
            let wake = held_until
                .into_iter()
                .chain(pool.next_connection_hold_deadline())
                .min()
                .map_or(give_up, |at| at.min(give_up));
            let mut stop = self.stop.clone();
            self.runtime.block_on(async {
                tokio::select! {
                    _ = changes.changed() => {}
                    _ = stop.stopped() => {}
                    _ = tokio::time::sleep_until(wake.into()) => {}
                }
            });
        }
    }
}

// How long a dial refused for capacity keeps asking.
const DIAL_WAIT: std::time::Duration = std::time::Duration::from_secs(15);

// What a failed dial reports for the article it was for. A refused login is
// told as one, whichever code the server refused it with.
fn dial_error(error: BlockingBodyLaneAcquireError) -> NntpError {
    match error {
        BlockingBodyLaneAcquireError::CredentialsRefused { .. } => NntpError::AuthenticationFailed,
        BlockingBodyLaneAcquireError::ProviderCapacity(error)
        | BlockingBodyLaneAcquireError::Other(error) => error,
        BlockingBodyLaneAcquireError::ServerRecovering => NntpError::ServerRecovering,
        BlockingBodyLaneAcquireError::LocalCapacity
        | BlockingBodyLaneAcquireError::NoEligibleServer
        | BlockingBodyLaneAcquireError::SelectionContended => NntpError::PoolExhausted,
    }
}

// BODY requests the run's connections made, by server port: what the tests
// read to see the run fetched on the download lanes' connections.
#[cfg(test)]
pub(super) mod lane_fetches {
    use std::collections::HashMap;
    use std::sync::Mutex;

    static FETCHES: Mutex<Option<HashMap<u16, usize>>> = Mutex::new(None);

    pub(super) fn count(client: &weaver_nntp::NntpClient) {
        let Some(port) = client
            .pool()
            .server_configs()
            .first()
            .map(|config| config.port)
        else {
            return;
        };
        *FETCHES
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .entry(port)
            .or_default() += 1;
    }

    pub(in crate::speed_test) fn on(port: u16) -> usize {
        FETCHES
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|fetches| fetches.get(&port).copied())
            .unwrap_or(0)
    }
}
