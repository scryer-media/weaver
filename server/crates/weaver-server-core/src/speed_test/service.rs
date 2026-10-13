use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{info, warn};
use weaver_nntp::socket_budget::ConnectionLoan;
use weaver_nntp::transfer::{ServerTransferControl, StableServerId};

use super::bandwidth::{self, BandwidthStream};
use super::http::{HttpLeg, HttpOptions, fetch_test_nzb, read_capped, routed_get};
use super::model::*;
use super::overrides::{TestedLeg, TestedRoute};
use super::progress::{LegRecorder, Progress, UsenetFacts};
use super::providers::{self, ProviderServer};
use super::runner::Stop;
use super::usenet::{self, UsenetLeg, UsenetPlan};
use crate::proxies::{
    Consumer, DraftNetworkRoute, LegPath, NetworkRefusal, ProxyPool, ProxyProfile, ProxyRuntime,
    RoutingPolicy, Rung, SpeedTestQuotaHolder,
};
use crate::settings::SharedConfig;
use crate::{Database, SchedulerHandle};

// Finished runs kept beside the running one.
const FINISHED_RUNS_KEPT: usize = 10;
// How long a list provider's server list is held for the UI.
const SERVER_LIST_TTL: Duration = Duration::from_secs(10 * 60);
// How long a draft dial may take before the leg's attempt gives up.
const DRAFT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpeedTestError {
    // Refused with a stable code.
    Refused(NetworkRefusal),
    // The request cannot be run as given.
    Invalid(String),
}

impl std::fmt::Display for SpeedTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => refusal.fmt(f),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl From<NetworkRefusal> for SpeedTestError {
    fn from(refusal: NetworkRefusal) -> Self {
        Self::Refused(refusal)
    }
}

// Runs speed tests one at a time and keeps the recent ones in memory.
#[derive(Clone)]
pub struct SpeedTestService {
    inner: Arc<ServiceInner>,
}

pub struct ServiceInner {
    handle: SchedulerHandle,
    config: SharedConfig,
    db: Database,
    allow_private_network: bool,
    state: Mutex<State>,
    revision: watch::Sender<u64>,
    server_lists: tokio::sync::Mutex<HashMap<String, (Instant, Vec<ProviderServer>)>>,
    roots: Mutex<Vec<Vec<u8>>>,
}

#[derive(Default)]
struct State {
    next_id: u32,
    // Newest last.
    runs: VecDeque<Arc<RunCell>>,
    active: Option<Arc<RunCell>>,
}

struct RunCell {
    id: u32,
    consumer: Consumer,
    consumer_name: String,
    request: SpeedTestRequest,
    started_at_ms: i64,
    tested: TestedRouteSnapshot,
    progress: Arc<Progress>,
    cancel: watch::Sender<bool>,
    life: Mutex<Life>,
}

struct Life {
    status: SpeedTestStatus,
    phase: Option<SpeedTestPhase>,
    error: Option<String>,
    finished_at_ms: Option<i64>,
    result: Option<SpeedTestResult>,
    bandwidth: Option<BandwidthResult>,
}

impl RunCell {
    fn usenet(&self) -> bool {
        matches!(self.request.kind, SpeedTestKind::Usenet { .. })
    }
}

impl SpeedTestService {
    pub fn new(
        handle: SchedulerHandle,
        config: SharedConfig,
        db: Database,
        allow_private_network: bool,
    ) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                handle,
                config,
                db,
                allow_private_network,
                state: Mutex::new(State {
                    next_id: 1,
                    ..State::default()
                }),
                revision: watch::channel(0).0,
                server_lists: tokio::sync::Mutex::new(HashMap::new()),
                roots: Mutex::new(Vec::new()),
            }),
        }
    }

    // The scheduler's one service, created on first use. A handle runs one
    // speed test at a time however many schemas are built over it.
    pub fn for_handle(
        handle: &SchedulerHandle,
        config: SharedConfig,
        db: Database,
        allow_private_network: bool,
    ) -> Self {
        handle.speed_test_service_or_init(|| {
            Self::new(handle.clone(), config, db, allow_private_network)
        })
    }

    pub(crate) fn from_inner(inner: Arc<ServiceInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn downgrade(&self) -> Weak<ServiceInner> {
        Arc::downgrade(&self.inner)
    }

    // Trust only these roots for HTTPS fetches; for loopback fixtures.
    #[cfg(any(test, feature = "test-support"))]
    pub fn trust_roots_for_tests(&self, roots: Vec<Vec<u8>>) {
        *self.inner.roots.lock().expect("speed test roots") = roots;
    }

    // Bumped on every change of a run's status or phase, and whenever an
    // article or a stream is counted.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.revision.subscribe()
    }

    pub fn providers(&self) -> &'static [providers::ProviderInfo] {
        providers::PROVIDERS
    }

    // The running run and the finished ones kept, newest first.
    pub fn runs(&self) -> Vec<SpeedTestRun> {
        let runs: Vec<Arc<RunCell>> = self
            .inner
            .state
            .lock()
            .expect("speed test state")
            .runs
            .iter()
            .rev()
            .cloned()
            .collect();
        runs.iter().map(|cell| self.inner.snapshot(cell)).collect()
    }

    pub fn run(&self, id: u32) -> Option<SpeedTestRun> {
        let cell = self
            .inner
            .state
            .lock()
            .expect("speed test state")
            .runs
            .iter()
            .find(|cell| cell.id == id)
            .cloned()?;
        Some(self.inner.snapshot(&cell))
    }

    // Cancel a running run. False when no run with this id is running.
    pub fn cancel(&self, id: u32) -> bool {
        let state = self.inner.state.lock().expect("speed test state");
        match &state.active {
            Some(cell) if cell.id == id => {
                cell.cancel.send_replace(true);
                true
            }
            _ => false,
        }
    }

    // The server list of a list provider, fetched through the first leg of
    // the consumer's saved route and held for ten minutes.
    pub async fn provider_servers(
        &self,
        provider_id: &str,
        consumer: Consumer,
    ) -> Result<Vec<ProviderServer>, SpeedTestError> {
        let provider = providers::provider(provider_id).ok_or_else(|| {
            SpeedTestError::Invalid(format!("no bandwidth provider is called {provider_id}"))
        })?;
        if !provider.needs_server {
            return Ok(Vec::new());
        }
        let runtime = self.inner.runtime()?;
        let policy = runtime.policy(consumer);
        let (_, profiles, pools) = runtime.network.configuration_snapshot();
        let tested = PathOverride::default()
            .apply(&policy.route(), &pool_map(pools))
            .map_err(SpeedTestError::Invalid)?;
        let draft = runtime
            .network
            .draft_route(consumer, policy, DRAFT_TIMEOUT)
            .map_err(SpeedTestError::Invalid)?;
        let leg = first_leg_up(&tested).ok_or(NetworkRefusal::SpeedTestLegDownAll)?;
        let http = HttpLeg {
            stage: draft.leg_stage(leg).map_err(SpeedTestError::Invalid)?,
            dns_servers: leg_dns_servers(
                &tested.legs[leg],
                &profile_map(profiles),
                &pool_map_of(&runtime),
                &[],
            ),
        };
        let servers = self
            .inner
            .list_servers(provider_id, &http)
            .await
            .map_err(SpeedTestError::Invalid);
        draft.revoke().await;
        servers
    }

    // Start a run. It outlives the request: the run comes back RUNNING and
    // `run(id)` follows it.
    pub async fn start(&self, request: SpeedTestRequest) -> Result<SpeedTestRun, SpeedTestError> {
        let inner = &self.inner;
        validate_request(&request)?;
        inner.check_idle()?;
        let runtime = inner.runtime()?;
        let consumer = request.consumer;
        let servers = inner.config.read().await.servers.clone();
        let server = match consumer {
            Consumer::Server(id) => Some(
                servers
                    .iter()
                    .find(|server| server.id == id)
                    .cloned()
                    .ok_or_else(|| {
                        SpeedTestError::Invalid(format!("server {id} does not exist"))
                    })?,
            ),
            Consumer::Rss(_) => None,
        };
        let consumer_name = runtime
            .network
            .consumer_snapshot()
            .into_iter()
            .find(|(c, ..)| *c == consumer)
            .map(|(_, name, ..)| name)
            .or_else(|| server.as_ref().map(|server| server.host.clone()))
            .unwrap_or_else(|| match consumer {
                Consumer::Server(id) => format!("server {id}"),
                Consumer::Rss(id) => format!("feed {id}"),
            });

        let policy = match &request.route {
            Some(route) => RoutingPolicy {
                legs: route.legs.clone(),
                failover: route.failover,
                ..Default::default()
            },
            None => runtime.policy(consumer),
        };
        runtime.network.validate_policy(consumer, &policy)?;
        let route = policy.route();
        let (egresses, profiles, pools) = runtime.network.configuration_snapshot();
        let egress_names: HashMap<u32, String> =
            egresses.iter().map(|e| (e.id, e.name.clone())).collect();
        let profiles = profile_map(profiles);
        let pools = pool_map(pools);
        let tested = request
            .overrides
            .apply(&route, &pools)
            .map_err(SpeedTestError::Invalid)?;
        if !tested.any_up() {
            return Err(NetworkRefusal::SpeedTestLegDownAll.into());
        }

        // Targets per leg: connections for Usenet, streams for bandwidth.
        let targets = match (&request.kind, &server) {
            (SpeedTestKind::Usenet { connections, .. }, Some(server)) => {
                let connections = connections.unwrap_or(server.connections);
                if connections == 0 || connections > server.connections {
                    return Err(SpeedTestError::Invalid(format!(
                        "a Usenet run uses 1 to {} connections on this server",
                        server.connections
                    )));
                }
                let targets = tested
                    .connection_targets(connections)
                    .map_err(SpeedTestError::Invalid)?;
                if targets.iter().all(|target| *target == 0) {
                    return Err(SpeedTestError::Invalid(
                        "no leg that is up gets a connection; raise the connection count".into(),
                    ));
                }
                targets
            }
            (SpeedTestKind::Usenet { .. }, None) => {
                return Err(NetworkRefusal::SpeedTestUsenetNeedsServer.into());
            }
            (SpeedTestKind::Bandwidth { streams, .. }, _) => tested.stream_targets(*streams),
        };

        // Quotas: a run books its bytes against every quota it crosses.
        let blocked_egresses = runtime.network.quota_blocked_egresses();
        for (leg, target) in tested.original.legs.iter().zip(&targets) {
            if *target == 0 {
                continue;
            }
            let control = runtime
                .network
                .egress_controls
                .get(StableServerId(leg.egress_id));
            if blocked_egresses.contains(&leg.egress_id)
                || control.is_some_and(|c| quota_blocked(&c))
            {
                return Err(NetworkRefusal::SpeedTestQuotaBlocked {
                    holder: SpeedTestQuotaHolder::Egress,
                    id: leg.egress_id,
                    name: egress_names
                        .get(&leg.egress_id)
                        .cloned()
                        .unwrap_or_else(|| format!("egress {}", leg.egress_id)),
                }
                .into());
            }
        }
        let server_control = server
            .as_ref()
            .and_then(|server| inner.server_control(server.id));
        if let (Some(server), true) = (&server, request.kind.label() == SpeedTestKindLabel::Usenet)
        {
            if server_control.as_ref().is_some_and(quota_blocked) {
                return Err(NetworkRefusal::SpeedTestQuotaBlocked {
                    holder: SpeedTestQuotaHolder::Server,
                    id: server.id,
                    name: consumer_name.clone(),
                }
                .into());
            }
            if let Some(reason) = inner.lease_refusal(server.id).await {
                return Err(NetworkRefusal::SpeedTestLeaseRefused {
                    server: server.id,
                    name: consumer_name.clone(),
                    reason,
                }
                .into());
            }
        }

        // What the run pulls from.
        let bandwidth_url = match &request.kind {
            SpeedTestKind::Bandwidth {
                provider: BandwidthProvider::Custom { url },
                ..
            } => Some(url.clone()),
            SpeedTestKind::Bandwidth {
                provider: BandwidthProvider::Builtin { id, .. },
                bytes,
                ..
            } => {
                let provider = providers::provider(id).ok_or_else(|| {
                    SpeedTestError::Invalid(format!("no bandwidth provider is called {id}"))
                })?;
                if provider.needs_server {
                    None
                } else {
                    Some(
                        providers::provider_url(provider, *bytes)
                            .map_err(SpeedTestError::Invalid)?,
                    )
                }
            }
            SpeedTestKind::Usenet { .. } => None,
        };
        let nzb_url = match &request.kind {
            SpeedTestKind::Usenet { nzb, .. } => Some(nzb.url().map_err(SpeedTestError::Invalid)?),
            SpeedTestKind::Bandwidth { .. } => None,
        };

        // The draft: the tested route with its downed pool members left out.
        let drafted = tested.drafted();
        let draft = runtime
            .network
            .draft_route_without_members(
                consumer,
                RoutingPolicy {
                    legs: drafted.legs,
                    failover: drafted.failover,
                    ..Default::default()
                },
                DRAFT_TIMEOUT,
                &request.overrides.down_proxies,
            )
            .map_err(SpeedTestError::Invalid)?;

        let legs: Vec<PlannedLeg> = tested
            .legs
            .iter()
            .enumerate()
            .filter_map(|(position, leg)| match leg {
                TestedLeg::Up { rung_map, .. } if targets[position] > 0 => Some(PlannedLeg {
                    position,
                    target: targets[position],
                    egress: tested.original.legs[position].egress_id,
                    rung_map: rung_map.clone().into(),
                    dns_servers: leg_dns_servers(
                        leg,
                        &profiles,
                        &pools,
                        &request.overrides.down_proxies,
                    ),
                }),
                _ => None,
            })
            .collect();
        let snapshot = tested_snapshot(
            &tested,
            &targets,
            &request.overrides,
            &egress_names,
            &profiles,
            &pools,
        );
        let plan = RunPlan {
            draft,
            legs,
            server,
            server_control,
            nzb_url,
            bandwidth_url,
        };

        let cell = {
            let mut state = inner.state.lock().expect("speed test state");
            if let Some(active) = &state.active {
                return Err(inner.busy_refusal(active).into());
            }
            let id = state.next_id;
            state.next_id += 1;
            let cell = Arc::new(RunCell {
                id,
                consumer,
                consumer_name,
                request: request.clone(),
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                tested: snapshot,
                progress: {
                    let progress = Progress::new(tested.legs.len());
                    progress.notify_with(inner.revision.clone());
                    progress
                },
                cancel: watch::channel(false).0,
                life: Mutex::new(Life {
                    status: SpeedTestStatus::Running,
                    phase: Some(SpeedTestPhase::Preparing),
                    error: None,
                    finished_at_ms: None,
                    result: None,
                    bandwidth: None,
                }),
            });
            state.active = Some(cell.clone());
            state.runs.push_back(cell.clone());
            cell
        };
        info!(
            run = cell.id,
            consumer = %consumer.key(),
            kind = request.kind.label().as_str(),
            "speed test started"
        );
        inner.bump();
        let snapshot = inner.snapshot(&cell);
        tokio::spawn(drive(inner.clone(), cell, plan));
        Ok(snapshot)
    }
}

fn validate_request(request: &SpeedTestRequest) -> Result<(), SpeedTestError> {
    if request.max_duration.is_zero() || request.max_duration > MAX_DURATION {
        return Err(SpeedTestError::Invalid(format!(
            "a run measures for 1 to {} seconds",
            MAX_DURATION.as_secs()
        )));
    }
    match (&request.kind, request.consumer) {
        (SpeedTestKind::Usenet { .. }, Consumer::Rss(_)) => {
            Err(NetworkRefusal::SpeedTestUsenetNeedsServer.into())
        }
        (
            SpeedTestKind::Bandwidth {
                streams,
                bytes,
                provider,
            },
            _,
        ) => {
            if *streams == 0 || *streams > MAX_STREAMS {
                return Err(SpeedTestError::Invalid(format!(
                    "a bandwidth run opens 1 to {MAX_STREAMS} streams"
                )));
            }
            if *bytes == 0 {
                return Err(SpeedTestError::Invalid(
                    "a bandwidth run must ask for at least one byte".into(),
                ));
            }
            match provider {
                BandwidthProvider::Custom { url } if url.scheme() != "https" => Err(
                    SpeedTestError::Invalid("a custom bandwidth URL must use https".into()),
                ),
                BandwidthProvider::Builtin { id, server } => {
                    let info = providers::provider(id).ok_or_else(|| {
                        SpeedTestError::Invalid(format!("no bandwidth provider is called {id}"))
                    })?;
                    if info.needs_server && server.is_none() {
                        return Err(SpeedTestError::Invalid(format!(
                            "{} needs a server from its list",
                            info.name
                        )));
                    }
                    Ok(())
                }
                BandwidthProvider::Custom { .. } => Ok(()),
            }
        }
        (SpeedTestKind::Usenet { nzb, .. }, Consumer::Server(_)) => match nzb {
            TestNzbSource::Url(url) if !matches!(url.scheme(), "http" | "https") => Err(
                SpeedTestError::Invalid("a test NZB URL must use http or https".into()),
            ),
            _ => Ok(()),
        },
    }
}

fn quota_blocked(control: &Arc<ServerTransferControl>) -> bool {
    control.snapshot().quota_blocked || control.quota_rejection_for(1).is_some()
}

fn profile_map(profiles: Vec<ProxyProfile>) -> HashMap<u32, ProxyProfile> {
    profiles.into_iter().map(|p| (p.id, p)).collect()
}

fn pool_map(pools: Vec<ProxyPool>) -> HashMap<u32, ProxyPool> {
    pools.into_iter().map(|p| (p.id, p)).collect()
}

fn pool_map_of(runtime: &ProxyRuntime) -> HashMap<u32, ProxyPool> {
    pool_map(runtime.network.configuration_snapshot().2)
}

fn first_leg_up(tested: &TestedRoute) -> Option<usize> {
    tested.legs.iter().position(TestedLeg::is_up)
}

// The DNS servers of the proxies a leg exits through, for resolving a host
// through a ladder.
fn leg_dns_servers(
    leg: &TestedLeg,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
    down_proxies: &[u32],
) -> Vec<IpAddr> {
    let TestedLeg::Up {
        leg:
            crate::proxies::RouteLeg {
                path: LegPath::Ladder { rungs, .. },
                ..
            },
        ..
    } = leg
    else {
        return Vec::new();
    };
    let mut exits = Vec::new();
    for rung in rungs {
        match rung {
            Rung::Proxy { id } => exits.push(*id),
            Rung::Chain { ids } => exits.extend(ids.last()),
            Rung::Pool { id } => exits.extend(
                pools
                    .get(id)
                    .into_iter()
                    .flat_map(|pool| pool.member_ids.iter())
                    .filter(|member| !down_proxies.contains(member)),
            ),
        }
    }
    let mut servers = Vec::new();
    for exit in exits {
        for server in profiles
            .get(&exit)
            .map(|p| p.dns_servers.as_slice())
            .unwrap_or(&[])
        {
            if !servers.contains(server) {
                servers.push(*server);
            }
        }
    }
    servers
}

fn tested_snapshot(
    tested: &TestedRoute,
    targets: &[u16],
    overrides: &PathOverride,
    egress_names: &HashMap<u32, String>,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> TestedRouteSnapshot {
    let mut egresses: Vec<u32> = overrides.down_egresses.clone();
    let mut proxies: Vec<u32> = overrides.down_proxies.clone();
    let mut pool_ids = Vec::new();
    for leg in &tested.original.legs {
        egresses.push(leg.egress_id);
        if let LegPath::Ladder { rungs, .. } = &leg.path {
            for rung in rungs {
                match rung {
                    Rung::Proxy { id } => proxies.push(*id),
                    Rung::Chain { ids } => proxies.extend(ids),
                    Rung::Pool { id } => {
                        pool_ids.push(*id);
                        if let Some(pool) = pools.get(id) {
                            proxies.extend(&pool.member_ids);
                        }
                    }
                }
            }
        }
    }
    let mut names = Vec::new();
    let mut push =
        |kind: &'static str, ids: &mut Vec<u32>, name: &dyn Fn(u32) -> Option<String>| {
            ids.sort_unstable();
            ids.dedup();
            for id in ids.iter() {
                if let Some(found) = name(*id) {
                    names.push(NamedId {
                        kind,
                        id: *id,
                        name: found,
                    });
                }
            }
        };
    push("egress", &mut egresses, &|id| {
        egress_names.get(&id).cloned()
    });
    push("proxy", &mut proxies, &|id| {
        profiles.get(&id).map(|p| p.name.clone())
    });
    push("pool", &mut pool_ids, &|id| {
        pools.get(&id).map(|p| p.name.clone())
    });
    TestedRouteSnapshot {
        legs: tested
            .original
            .legs
            .iter()
            .zip(&tested.legs)
            .enumerate()
            .map(|(position, (leg, state))| TestedLegSnapshot {
                position,
                egress_id: leg.egress_id,
                weight: leg.weight,
                down: !state.is_up(),
                down_reason: match state {
                    TestedLeg::Down { reason } => Some(reason.clone()),
                    TestedLeg::Up { .. } => None,
                },
                path: leg.path.clone(),
                target: targets.get(position).copied().unwrap_or(0),
            })
            .collect(),
        failover: tested.original.failover,
        overrides: overrides.clone(),
        names,
    }
}

struct PlannedLeg {
    position: usize,
    target: u16,
    egress: u32,
    rung_map: Arc<[usize]>,
    dns_servers: Vec<IpAddr>,
}

struct RunPlan {
    draft: Arc<DraftNetworkRoute>,
    legs: Vec<PlannedLeg>,
    server: Option<crate::servers::ServerConfig>,
    server_control: Option<Arc<ServerTransferControl>>,
    nzb_url: Option<reqwest::Url>,
    // `None` for a list provider until its server is looked up.
    bandwidth_url: Option<reqwest::Url>,
}

enum Outcome {
    Completed,
    Cancelled,
    Failed(String),
}

impl ServiceInner {
    fn runtime(&self) -> Result<Arc<ProxyRuntime>, SpeedTestError> {
        self.handle
            .proxy_runtime()
            .ok_or_else(|| SpeedTestError::Invalid("proxy runtime unavailable".into()))
    }

    fn bump(&self) {
        self.revision.send_modify(|revision| *revision += 1);
    }

    fn busy_refusal(&self, active: &RunCell) -> NetworkRefusal {
        if active.life.lock().expect("speed test run").phase == Some(SpeedTestPhase::Releasing) {
            NetworkRefusal::SpeedTestQueueBusy
        } else {
            NetworkRefusal::SpeedTestAlreadyRunning { run: active.id }
        }
    }

    fn check_idle(&self) -> Result<(), SpeedTestError> {
        match &self.state.lock().expect("speed test state").active {
            Some(active) => Err(self.busy_refusal(active).into()),
            None => Ok(()),
        }
    }

    fn server_control(&self, server: u32) -> Option<Arc<ServerTransferControl>> {
        self.handle
            .server_transfer_policy()?
            .transfer_registry()
            .get(StableServerId(server))
    }

    // Why the live pool cannot lend this server's connections, if it cannot.
    async fn lease_refusal(&self, server: u32) -> Option<String> {
        let pool = self.handle.nntp_pool()?;
        let index = pool.server_for_stable_id(StableServerId(server))?;
        let available = pool.health().lock().await.is_available(index.0);
        (!available).then(|| "its health tracker has it out of service".to_string())
    }

    fn options(&self) -> HttpOptions {
        HttpOptions {
            allow_private_network: self.allow_private_network,
            roots: self.roots.lock().expect("speed test roots").clone(),
        }
    }

    async fn list_servers(
        &self,
        provider_id: &str,
        leg: &HttpLeg,
    ) -> Result<Vec<ProviderServer>, String> {
        let mut lists = self.server_lists.lock().await;
        if let Some((fetched, servers)) = lists.get(provider_id)
            && fetched.elapsed() < SERVER_LIST_TTL
        {
            return Ok(servers.clone());
        }
        let url =
            reqwest::Url::parse(providers::LIBRESPEED_SERVER_LIST).map_err(|e| e.to_string())?;
        let routed = routed_get(leg, &url, &self.options(), None)
            .await
            .map_err(|(_, error)| format!("fetching the server list failed: {error}"))?;
        let body = read_capped(routed, 4 * 1024 * 1024).await?;
        let servers = providers::parse_librespeed_servers(&body)?;
        lists.insert(provider_id.to_owned(), (Instant::now(), servers.clone()));
        Ok(servers)
    }

    fn snapshot(&self, cell: &RunCell) -> SpeedTestRun {
        let life = cell.life.lock().expect("speed test run");
        let result = match life.status {
            SpeedTestStatus::Running => cell.progress.result(cell.usenet(), life.bandwidth.clone()),
            _ => life.result.clone(),
        };
        let draining = (life.phase == Some(SpeedTestPhase::Draining)).then(|| {
            self.handle
                .pipeline_metrics()
                .active_downloads
                .load(Ordering::Acquire)
        });
        SpeedTestRun {
            id: cell.id,
            consumer: cell.consumer,
            consumer_name: cell.consumer_name.clone(),
            kind: cell.request.kind.label(),
            request: cell.request.clone(),
            status: life.status,
            phase: life.phase,
            draining,
            started_at_ms: cell.started_at_ms,
            finished_at_ms: life.finished_at_ms,
            error: life.error.clone(),
            tested: cell.tested.clone(),
            result,
        }
    }

    fn set_phase(&self, cell: &RunCell, phase: SpeedTestPhase) {
        cell.life.lock().expect("speed test run").phase = Some(phase);
        self.bump();
    }
}

// What a run holds while it has the line, given back on every exit path.
struct Gate {
    db: Database,
    // Whether this run paused the queue, and so must resume it.
    paused: AtomicBool,
    loan: Mutex<Option<Arc<ConnectionLoan>>>,
    draft: Mutex<Option<Arc<DraftNetworkRoute>>>,
}

impl Gate {
    // Restore the budget, resume the queue if this run paused it, revoke
    // the draft.
    async fn release(&self, handle: &SchedulerHandle) {
        let loan = self.loan.lock().expect("speed test loan").take();
        drop(loan);
        if self.paused.load(Ordering::Acquire) {
            if let Err(error) = handle.resume_all().await {
                warn!(%error, "speed test could not resume the queue; clearing the stored pause");
                let db = self.db.clone();
                let _ =
                    tokio::task::spawn_blocking(move || db.set_setting("global_paused", "false"))
                        .await;
            }
            self.paused.store(false, Ordering::Release);
        }
        let draft = self.draft.lock().expect("speed test draft").take();
        if let Some(draft) = draft {
            draft.revoke().await;
        }
    }
}

impl Drop for Gate {
    // Weaver is shutting down mid-run: the scheduler is gone, so the pause
    // this run made must not outlive the process. The loan and the draft
    // give themselves back as they drop.
    fn drop(&mut self) {
        if self.paused.load(Ordering::Acquire) {
            let _ = self.db.set_setting("global_paused", "false");
        }
    }
}

async fn drive(inner: Arc<ServiceInner>, cell: Arc<RunCell>, plan: RunPlan) {
    let gate = Arc::new(Gate {
        db: inner.db.clone(),
        paused: AtomicBool::new(false),
        loan: Mutex::new(None),
        draft: Mutex::new(Some(plan.draft.clone())),
    });
    // The run works in its own task so a panic still reaches the release.
    let work = tokio::spawn(execute(inner.clone(), cell.clone(), plan, gate.clone()));
    let outcome = match work.await {
        Ok(outcome) => outcome,
        Err(error) if error.is_panic() => {
            Outcome::Failed("the speed test stopped unexpectedly".into())
        }
        Err(_) => Outcome::Failed("the speed test was stopped".into()),
    };
    cell.progress.finish();
    inner.set_phase(&cell, SpeedTestPhase::Releasing);
    gate.release(&inner.handle).await;

    let (status, error) = match outcome {
        Outcome::Completed => (SpeedTestStatus::Completed, None),
        Outcome::Cancelled => (SpeedTestStatus::Cancelled, None),
        Outcome::Failed(error) => (SpeedTestStatus::Failed, Some(error)),
    };
    let result = {
        let mut life = cell.life.lock().expect("speed test run");
        let result = cell.progress.result(cell.usenet(), life.bandwidth.clone());
        life.status = status;
        life.phase = None;
        life.error = error.clone();
        life.finished_at_ms = Some(chrono::Utc::now().timestamp_millis());
        life.result = result.clone();
        result
    };
    if let Some(result) = &result {
        for leg in &result.legs {
            info!(
                run = cell.id,
                leg = leg.position,
                bytes = leg.bytes,
                average_bps = leg.average_bps,
                peak_bps = leg.peak_bps,
                "speed test leg finished"
            );
        }
    }
    info!(
        run = cell.id,
        consumer = %cell.consumer.key(),
        kind = cell.request.kind.label().as_str(),
        status = status.as_str(),
        bytes = result.as_ref().map_or(0, |r| r.bytes),
        average_bps = result.as_ref().map_or(0.0, |r| r.average_bps),
        peak_bps = result.as_ref().map_or(0.0, |r| r.peak_bps),
        error = error.as_deref().unwrap_or(""),
        "speed test finished"
    );
    record_metrics(&cell, status, result.as_ref());
    {
        let mut state = inner.state.lock().expect("speed test state");
        state.active = None;
        while state.runs.len() > FINISHED_RUNS_KEPT {
            state.runs.pop_front();
        }
    }
    inner.bump();
}

async fn execute(
    inner: Arc<ServiceInner>,
    cell: Arc<RunCell>,
    plan: RunPlan,
    gate: Arc<Gate>,
) -> Outcome {
    let mut stop = Stop::new(cell.cancel.subscribe());
    let options = inner.options();
    let http_leg = |leg: &PlannedLeg| -> Result<HttpLeg, String> {
        Ok(HttpLeg {
            stage: plan.draft.leg_stage(leg.position)?,
            dns_servers: leg.dns_servers.clone(),
        })
    };
    let Some(first) = plan.legs.first() else {
        return Outcome::Failed("no leg is up".into());
    };

    // Preparing: everything that can fail before the queue is touched.
    let articles = match &plan.nzb_url {
        Some(url) => {
            let leg = match http_leg(first) {
                Ok(leg) => leg,
                Err(error) => return Outcome::Failed(error),
            };
            tokio::select! {
                biased;
                _ = stop.stopped() => return Outcome::Cancelled,
                fetched = fetch_test_nzb(&leg, url, &options) => match fetched {
                    Ok(articles) => Some(articles),
                    Err(error) => return Outcome::Failed(error),
                },
            }
        }
        None => None,
    };
    let bandwidth = if let SpeedTestKind::Bandwidth {
        provider, bytes, ..
    } = &cell.request.kind
    {
        let (provider_id, server) = match provider {
            BandwidthProvider::Builtin { id, server } => (Some(id.clone()), server.clone()),
            BandwidthProvider::Custom { .. } => (None, None),
        };
        let url = match (&plan.bandwidth_url, &provider_id, &server) {
            (Some(url), ..) => url.clone(),
            (None, Some(provider_id), Some(server)) => {
                let leg = match http_leg(first) {
                    Ok(leg) => leg,
                    Err(error) => return Outcome::Failed(error),
                };
                let listed = tokio::select! {
                    biased;
                    _ = stop.stopped() => return Outcome::Cancelled,
                    listed = inner.list_servers(provider_id, &leg) => listed,
                };
                let found = match listed {
                    Ok(servers) => servers.into_iter().find(|s| &s.id == server),
                    Err(error) => return Outcome::Failed(error),
                };
                let Some(found) = found else {
                    return Outcome::Failed(format!("{provider_id} lists no server {server}"));
                };
                match providers::list_server_url(&found, *bytes) {
                    Ok(url) => url,
                    Err(error) => return Outcome::Failed(error),
                }
            }
            _ => return Outcome::Failed("the bandwidth run has no URL".into()),
        };
        let result = BandwidthResult {
            provider_id,
            server,
            url: url.to_string(),
            streams: plan.legs.iter().map(|leg| u32::from(leg.target)).sum(),
        };
        cell.life.lock().expect("speed test run").bandwidth = Some(result);
        Some((url, *bytes))
    } else {
        None
    };

    // Pause, unless the queue already is.
    inner.set_phase(&cell, SpeedTestPhase::Pausing);
    if !inner.handle.is_globally_paused() {
        gate.paused.store(true, Ordering::Release);
        if let Err(error) = inner.handle.pause_all().await {
            return Outcome::Failed(format!("could not pause downloads: {error}"));
        }
    }

    // Drain: the in-flight gauge is the condition.
    inner.set_phase(&cell, SpeedTestPhase::Draining);
    let metrics = inner.handle.pipeline_metrics();
    tokio::select! {
        biased;
        _ = stop.stopped() => return Outcome::Cancelled,
        _ = metrics.wait_downloads_drained() => {}
    }

    let measure_stop =
        |stop: &Stop| stop.with_deadline(tokio::time::Instant::now() + cell.request.max_duration);
    match (articles, &plan.server, bandwidth) {
        (Some(articles), Some(server), _) => {
            // Lease: the live pool gives the server's connections back.
            inner.set_phase(&cell, SpeedTestPhase::Leasing);
            let connections: u16 = plan.legs.iter().map(|leg| leg.target).sum();
            if let Some(pool) = inner.handle.nntp_pool()
                && let Some(index) = pool.server_for_stable_id(StableServerId(server.id))
            {
                if let Some(reason) = inner.lease_refusal(server.id).await {
                    return Outcome::Failed(
                        NetworkRefusal::SpeedTestLeaseRefused {
                            server: server.id,
                            name: cell.consumer_name.clone(),
                            reason,
                        }
                        .to_string(),
                    );
                }
                let configured = pool.configured_connections(index).unwrap_or(0);
                let lent = usize::from(connections).min(configured);
                let Some(loan) = pool.lend_connections(index, lent) else {
                    return Outcome::Failed("the server left the live pool".into());
                };
                let loan = Arc::new(loan);
                *gate.loan.lock().expect("speed test loan") = Some(loan.clone());
                tokio::select! {
                    biased;
                    _ = stop.stopped() => return Outcome::Cancelled,
                    _ = loan.wait_open_at_most(configured - lent) => {}
                }
            }
            let tls_cipher_suite = if server.tls {
                let db = inner.db.clone();
                let id = server.id;
                tokio::task::spawn_blocking(move || db.server_tls_diagnostics(id))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten()
                    .map(|diagnostics| diagnostics.cipher_suite)
            } else {
                None
            };
            cell.progress.set_usenet_facts(UsenetFacts {
                pipelining: server.supports_pipelining,
                tls: server.tls,
                tls_cipher_suite,
            });
            let memory = tokio::task::spawn_blocking(crate::runtime::system_probe::detect_memory)
                .await
                .ok();
            let buffer_profile = weaver_nntp::connection::NntpBufferProfile::adaptive(
                memory
                    .map(|memory| memory.cgroup_limit.unwrap_or(memory.available_bytes))
                    .unwrap_or(0),
                usize::from(connections),
            );
            let runtime = match inner.runtime() {
                Ok(runtime) => runtime,
                Err(error) => return Outcome::Failed(error.to_string()),
            };
            let mut legs = Vec::new();
            for leg in &plan.legs {
                let stage = match plan.draft.leg_stage(leg.position) {
                    Ok(stage) => stage,
                    Err(error) => return Outcome::Failed(error),
                };
                legs.push(UsenetLeg {
                    position: leg.position,
                    connections: leg.target,
                    stage,
                    egress: leg.egress,
                    rung_map: leg.rung_map.clone(),
                    egress_control: runtime
                        .network
                        .egress_controls
                        .get(StableServerId(leg.egress)),
                });
            }
            inner.set_phase(&cell, SpeedTestPhase::Measuring);
            cell.progress.start();
            let sampler = spawn_sampler(cell.progress.clone(), Some(inner.handle.clone()));
            usenet::run(
                UsenetPlan {
                    server: server.clone(),
                    legs,
                    articles,
                    server_control: plan.server_control.clone(),
                    buffer_profile,
                },
                cell.progress.clone(),
                metrics,
                measure_stop(&stop),
            )
            .await;
            sampler.abort();
        }
        (None, _, Some((url, bytes))) => {
            let runtime = match inner.runtime() {
                Ok(runtime) => runtime,
                Err(error) => return Outcome::Failed(error.to_string()),
            };
            let mut streams = Vec::new();
            for leg in &plan.legs {
                let http = match http_leg(leg) {
                    Ok(http) => http,
                    Err(error) => return Outcome::Failed(error),
                };
                for _ in 0..leg.target {
                    streams.push(BandwidthStream {
                        leg: http.clone(),
                        recorder: LegRecorder {
                            progress: cell.progress.clone(),
                            leg: leg.position,
                            egress: leg.egress,
                            rung_map: leg.rung_map.clone(),
                        },
                        egress_control: runtime
                            .network
                            .egress_controls
                            .get(StableServerId(leg.egress)),
                    });
                }
            }
            inner.set_phase(&cell, SpeedTestPhase::Measuring);
            cell.progress.start();
            let sampler = spawn_sampler(cell.progress.clone(), None);
            bandwidth::run(
                streams,
                url,
                bytes,
                options,
                cell.progress.clone(),
                measure_stop(&stop),
            )
            .await;
            sampler.abort();
        }
        _ => return Outcome::Failed("the run has nothing to measure".into()),
    }
    cell.progress.finish();
    cell.progress.sample();
    if stop.cancelled() {
        return Outcome::Cancelled;
    }
    if cell.progress.bytes() == 0 {
        let first_error = cell
            .progress
            .result(cell.usenet(), None)
            .and_then(|result| result.connections.into_iter().find_map(|c| c.error));
        return Outcome::Failed(match first_error {
            Some(error) => format!("no bytes were received: {error}"),
            None => "no bytes were received".into(),
        });
    }
    Outcome::Completed
}

// Close a sample bin every second, and for Usenet read the header gauge.
fn spawn_sampler(
    progress: Arc<Progress>,
    gauge: Option<SchedulerHandle>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Some(handle) = &gauge {
                progress.note_gauge(handle.get_metrics().current_download_speed);
            }
            progress.sample();
        }
    })
}

// Counters the Prometheus exporter reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpeedTestMetricsSnapshot {
    // Finished runs, indexed by `SpeedTestKindLabel::index` and then
    // `SpeedTestStatus::finished_index`, so every pair is always present.
    pub runs: [[u64; SpeedTestStatus::FINISHED.len()]; SpeedTestKindLabel::ALL.len()],
    // Average bytes per second of the last completed run, by consumer key
    // and kind.
    pub last_bps: BTreeMap<(String, SpeedTestKindLabel), f64>,
}

static METRICS: Mutex<Option<SpeedTestMetricsSnapshot>> = Mutex::new(None);

fn record_metrics(cell: &RunCell, status: SpeedTestStatus, result: Option<&SpeedTestResult>) {
    let Some(finished) = status.finished_index() else {
        return;
    };
    let mut metrics = METRICS.lock().expect("speed test metrics");
    let metrics = metrics.get_or_insert_with(SpeedTestMetricsSnapshot::default);
    let kind = cell.request.kind.label();
    metrics.runs[kind.index()][finished] += 1;
    if status == SpeedTestStatus::Completed
        && let Some(result) = result
    {
        metrics
            .last_bps
            .insert((cell.consumer.key(), kind), result.average_bps);
    }
}

// `weaver_speed_test_runs_total{kind,status}` and
// `weaver_speed_test_last_bps{consumer,kind}`.
pub fn speed_test_metrics_snapshot() -> SpeedTestMetricsSnapshot {
    METRICS
        .lock()
        .expect("speed test metrics")
        .clone()
        .unwrap_or_default()
}
