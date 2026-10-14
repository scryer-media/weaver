use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use weaver_nntp::client::{DecodedBodyError, NntpClient, NntpClientConfig};
use weaver_nntp::route_dialer::RouteDialer;
use weaver_nntp::transfer::{ServerTransferControl, ServerTransferRegistry, TransferScope};
use weaver_tunnel::pipe::Dialer;

use super::http::TestArticle;
use super::progress::{LegRecorder, Progress, RecordingDialer};
use super::runner::Stop;
use crate::operations::metrics::PipelineMetrics;

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

pub(crate) async fn run(
    plan: UsenetPlan,
    progress: Arc<Progress>,
    metrics: Arc<PipelineMetrics>,
    stop: Stop,
) {
    let queue = Arc::new(Mutex::new(VecDeque::from(plan.articles)));
    let server = Arc::new(plan.server);
    let mut clients = Vec::new();
    let mut workers = tokio::task::JoinSet::new();
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
            egress_controls: Arc::new(ServerTransferRegistry::with_scope(TransferScope::Egress)),
            runtime: tokio::runtime::Handle::current(),
            server: server.id,
        });
        let client = Arc::new(NntpClient::new(NntpClientConfig::single(
            server_config(&server, dialer, plan.buffer_profile),
            usize::from(leg.connections),
        )));
        clients.push(client.clone());
        for _ in 0..leg.connections {
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
            };
            workers.spawn(worker.run());
        }
    }
    while workers.join_next().await.is_some() {}
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
}

impl Worker {
    async fn run(mut self) {
        let progress = self.booking.progress.clone();
        let leg = self.booking.leg;
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
            let fetched = tokio::select! {
                biased;
                _ = self.stop.stopped() => return,
                fetched = self
                    .client
                    .fetch_body_decoded_with_groups(&article.message_id, &article.groups) => fetched,
            };
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
        }
    }
}
