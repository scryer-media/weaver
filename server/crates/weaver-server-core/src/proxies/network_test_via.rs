// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A test reaches what it tests through a route the operator drafted for it,
// and through nothing else. Every path through the draft is tried on its
// own: each leg's direct line, each rung, each member of a pool rung. A
// proxy under test sits on top of the path the way a chain carries it, so a
// proxy that is only reachable through a tunnel is tested through that
// tunnel. A pool under test sits there whole, and carries a target through
// the member it picks.
use super::*;
pub use weaver_tunnel::resolution::record::{
    HostResolution, LookupError, LookupFailure, ResolvedBy,
};
use weaver_tunnel::{fault, pipe::Purpose, resolution::record::Recording};

use crate::speed_test::torrent::{P2pPath, P2pPlan, probe as p2p_probe};
use crate::speed_test::{P2pReport, TestTorrentSource, gateway_of};
use crate::torrents::net::LegKind;

// The most paths one test tries, counting a pool under test by its members.
pub const MAX_TEST_PATHS: usize = 64;

// How long a test of the route alone waits for its target.
const ROUTE_TEST_WAIT: Duration = Duration::from_secs(15);

// What a test reaches through the drafted route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestSubject {
    // The target, over the route alone.
    Route,
    Proxy(u32),
    // The pool, carrying the target through the member it picks; without a
    // target, each member on its own.
    Pool(u32),
    // Each member of the pool on its own.
    Members(u32),
}

// What one path came to.
#[derive(Debug)]
pub struct PathTest {
    // The first leg the path belongs to.
    pub leg: usize,
    pub egress_id: u32,
    // The proxies beneath the one under test, from the egress up; none on
    // the leg's direct line.
    pub carriers: Vec<u32>,
    // The proxy under test, unless the route was tested alone; under a pool
    // that carried the target, the member it picked.
    pub proxy_id: Option<u32>,
    // The pool under test, when it carried the target.
    pub pool_id: Option<u32>,
    pub outcome: Result<ProxyTestPass, ProxyFault>,
    // The proxy a failure belongs to, when one hop is to blame.
    pub failing_proxy: Option<u32>,
    pub source_address: Option<std::net::SocketAddr>,
    pub elapsed: Duration,
    // How the host the path's last dial named was resolved: the target,
    // else the DNS server or proxy endpoint the test reached.
    pub resolution: Option<HostResolution>,
    // The test torrent's rungs, on a torrent route's path that reached its
    // target.
    pub p2p: Option<P2pReport>,
}

// A test torrent run over each path that reaches its target.
struct P2pTest {
    source: TestTorrentSource,
    // The port a tunnel's gateway is asked to map.
    internal_port: u16,
}

// A path's stages: the one beneath the proxy under test, and that proxy on
// top of it. A path that cannot be built says why and whose fault it is.
type Stages = Result<(Arc<dyn Dialer>, Option<Arc<dyn Dialer>>), (ProxyFault, Option<u32>)>;

struct Probe {
    leg: usize,
    egress_id: u32,
    carriers: Vec<u32>,
    subject: Option<ProxyProfile>,
    // The pool on top of the path, which carries the target itself.
    pool: Option<u32>,
    // How long the path's last hop waits.
    wait: Duration,
    stages: Stages,
    // How a torrent session would ride the path.
    kind: LegKind,
    // The gateway of the WireGuard tunnel the path ends in.
    gateway: Option<std::net::IpAddr>,
}

// The runtime a test builds its paths in. It is shut down when the test
// ends, or when the test is abandoned.
struct ProbeRuntime(Option<Arc<NetworkRuntime>>);
impl ProbeRuntime {
    fn runtime(&self) -> &NetworkRuntime {
        self.0.as_deref().expect("probe runtime")
    }
    async fn shutdown(mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown().await;
        }
    }
}
impl Drop for ProbeRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            let handle = runtime.handle.clone();
            handle.spawn(async move { runtime.shutdown().await });
        }
    }
}

// Every distinct path through `route`, as its first leg, its egress and the
// proxies it rides.
pub(super) fn paths(route: &Route, pools: &HashMap<u32, ProxyPool>) -> Vec<(usize, u32, Vec<u32>)> {
    let mut seen = HashSet::new();
    let mut paths = Vec::new();
    for (position, leg) in route.legs.iter().enumerate() {
        let mut carriers = Vec::new();
        match &leg.path {
            LegPath::Direct => carriers.push(Vec::new()),
            LegPath::Ladder {
                rungs,
                direct_fallback,
            } => {
                for rung in rungs {
                    match rung {
                        Rung::Proxy { id } => carriers.push(vec![*id]),
                        Rung::Chain { ids } => carriers.push(ids.clone()),
                        Rung::Pool { id } => carriers.extend(
                            pools
                                .get(id)
                                .into_iter()
                                .flat_map(|pool| &pool.member_ids)
                                .map(|member| vec![*member]),
                        ),
                    }
                }
                if *direct_fallback {
                    carriers.push(Vec::new());
                }
            }
        }
        for carriers in carriers {
            if seen.insert((leg.egress_id, carriers.clone())) {
                paths.push((position, leg.egress_id, carriers));
            }
        }
    }
    paths
}

// Whether `kind` may ride on `beneath` the way a chain would carry it: only
// WireGuard carries the UDP that WireGuard and HTTP/3 travel over, and only
// WireGuard can use it.
fn carried(kind: ProxyKind, beneath: Option<ProxyKind>) -> bool {
    match (kind, beneath) {
        (_, None) => true,
        (ProxyKind::WireGuard, Some(ProxyKind::WireGuard)) => true,
        (ProxyKind::WireGuard | ProxyKind::Http3Connect, Some(_)) => false,
        _ => true,
    }
}

impl NetworkRuntime {
    // Tests `subject` through each path of the drafted `route`. A test of
    // the route alone reaches `target`; a proxy reaches `target` when one is
    // given, and otherwise shows how far it got by itself.
    pub async fn test_via(
        &self,
        route: &Route,
        subject: TestSubject,
        target: Option<(String, u16)>,
    ) -> Result<Vec<PathTest>, NetworkRefusal> {
        self.test_paths(route, subject, target, None).await
    }

    // Tests a torrent route the same way, and then runs the test torrent
    // over each path that reached its target, the source's host when no
    // target is given. A tunnel's gateway is asked to map `internal_port`.
    pub async fn test_via_torrent(
        &self,
        route: &Route,
        subject: TestSubject,
        target: Option<(String, u16)>,
        source: TestTorrentSource,
        internal_port: u16,
    ) -> Result<Vec<PathTest>, NetworkRefusal> {
        let target = target
            .filter(|(host, _)| !host.trim().is_empty())
            .or_else(|| Some(source.target()));
        let p2p = P2pTest {
            source,
            internal_port,
        };
        self.test_paths(route, subject, target, Some(p2p)).await
    }

    async fn test_paths(
        &self,
        route: &Route,
        subject: TestSubject,
        target: Option<(String, u16)>,
        p2p: Option<P2pTest>,
    ) -> Result<Vec<PathTest>, NetworkRefusal> {
        let target = target.filter(|(host, _)| !host.trim().is_empty());
        let probe = ProbeRuntime(Some(self.isolated_probe_runtime()));
        let (probes, concurrency) = probe
            .runtime()
            .plan_test(route, subject, target.is_some())?;
        let tests = run_probes(probes, target, concurrency, p2p.map(Arc::new)).await;
        probe.shutdown().await;
        for test in &tests {
            if let Err(fault) = &test.outcome {
                tracing::warn!(
                    egress_id = test.egress_id,
                    carriers = ?test.carriers,
                    proxy_id = ?test.proxy_id,
                    failing_proxy = ?test.failing_proxy,
                    code = fault.code(),
                    error = %fault,
                    "test through a drafted route failed"
                );
            }
        }
        Ok(tests)
    }

    // Validates the test and builds each path's stages in this probe
    // runtime, with how many paths may be tried at once.
    fn plan_test(
        &self,
        route: &Route,
        subject: TestSubject,
        targeted: bool,
    ) -> Result<(Vec<Probe>, usize), NetworkRefusal> {
        // A test asks about a proxy whether or not routes may use it yet, so
        // this runtime's own copy enables the proxies under test.
        let subjects: Vec<Option<u32>> = {
            let mut config = self.configuration.write().expect("network configuration");
            let ids = match subject {
                TestSubject::Route if !targeted => return Err(NetworkRefusal::TestTargetRequired),
                TestSubject::Route => Vec::new(),
                TestSubject::Proxy(id) => vec![id],
                TestSubject::Pool(id) | TestSubject::Members(id) => config
                    .pools
                    .get(&id)
                    .ok_or(NetworkRefusal::PoolGone)?
                    .member_ids
                    .clone(),
            };
            if ids.is_empty() && matches!(subject, TestSubject::Pool(_) | TestSubject::Members(_)) {
                return Err(NetworkRefusal::RoutePoolEmpty);
            }
            for id in &ids {
                let profile = config
                    .profiles
                    .get_mut(id)
                    .ok_or(NetworkRefusal::ProxyNotFound)?;
                profile.validate()?;
                profile.enabled = true;
            }
            match subject {
                TestSubject::Route => vec![None],
                _ => ids.into_iter().map(Some).collect(),
            }
        };
        let config = self.configuration.read().expect("network configuration");
        route.validate_references(&config.egresses, &config.profiles, &config.pools)?;
        let paths = paths(route, &config.pools);
        let kind_of = |id: &u32| config.profiles.get(id).map(|profile| profile.kind);
        for (_, _, carriers) in &paths {
            for id in subjects.iter().flatten() {
                if carriers.contains(id) {
                    return Err(NetworkRefusal::TestSubjectInRoute);
                }
                // The proxy under test tops the path's chain.
                if carriers.len() >= 3 {
                    return Err(NetworkRefusal::RouteChainLength);
                }
                if !kind_of(id).is_some_and(|kind| carried(kind, carriers.last().and_then(kind_of)))
                {
                    return Err(NetworkRefusal::RouteChainTransport);
                }
            }
        }
        if paths.len() * subjects.len() > MAX_TEST_PATHS {
            return Err(NetworkRefusal::TestPathCount {
                limit: MAX_TEST_PATHS,
            });
        }
        // A WireGuard server keeps one endpoint per key: a test must not
        // reach a WireGuard proxy by a path other than the one a saved route
        // holds it on, or the two sessions take the endpoint from each other.
        let mut routes: Vec<Route> = config.policies.values().map(RoutingPolicy::route).collect();
        routes.push(Route {
            legs: paths
                .iter()
                .flat_map(|(_, egress_id, carriers)| {
                    subjects.iter().filter_map(move |subject| {
                        let ids: Vec<u32> = carriers.iter().copied().chain(*subject).collect();
                        let rung = match ids.as_slice() {
                            [] => return None,
                            [id] => Rung::Proxy { id: *id },
                            _ => Rung::Chain { ids },
                        };
                        Some(RouteLeg {
                            egress_id: *egress_id,
                            weight: 1,
                            path: LegPath::Ladder {
                                rungs: vec![rung],
                                direct_fallback: false,
                            },
                        })
                    })
                })
                .collect(),
            failover: Failover::default(),
        });
        validate_wireguard_paths(&routes, &config.profiles, &config.pools)?;
        // A pool carries the target through the member it picks.
        let pool = match subject {
            TestSubject::Pool(id) if targeted => Some(id),
            _ => None,
        };
        let wireguard = paths
            .iter()
            .flat_map(|(_, _, carriers)| carriers)
            .chain(subjects.iter().flatten())
            .any(|id| kind_of(id) == Some(ProxyKind::WireGuard));
        // A pool races every member at once, each with a tunnel of its own.
        let tunnels = if pool.is_some() { subjects.len() } else { 1 };
        let concurrency = if wireguard {
            (max_wireguard_instances() / tunnels).clamp(1, 4)
        } else {
            4
        };
        // The gateway of a WireGuard proxy, asked for a port mapping when a
        // path ends in it.
        let gateway = |id: Option<&u32>| {
            id.and_then(|id| config.profiles.get(id))
                .filter(|profile| profile.kind == ProxyKind::WireGuard)
                .and_then(|profile| gateway_of(&profile.tunnel_addresses))
        };
        let mut probes = Vec::new();
        for (leg, egress_id, carriers) in paths {
            let bottom = Self::bottom(&config, egress_id, ROUTE_TEST_WAIT)
                .map_err(|_| NetworkRefusal::RouteEgressMissing)?;
            let direct = match bottom.binding() {
                SocketEgress::System => LegKind::System,
                SocketEgress::Interface(name) => LegKind::Interface(name.clone()),
                SocketEgress::SourceAddress(address) => LegKind::SourceAddress(*address),
            };
            let beneath = self.carriers(&config, &carriers, &bottom);
            if let Some(pool) = pool {
                let stages = beneath.and_then(|stage| {
                    let members = subjects
                        .iter()
                        .flatten()
                        .map(|id| {
                            self.hop(&config, *id, stage.clone(), bottom.clone(), &carriers)
                                .map(|hop| (*id, hop))
                                .map_err(|detail| (ProxyFault::Other { detail }, Some(*id)))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let top: Arc<dyn Dialer> = PoolStage::new(pool, members, self.handle.clone());
                    Ok((stage, Some(top)))
                });
                probes.push(Probe {
                    leg,
                    egress_id,
                    carriers,
                    subject: None,
                    pool: Some(pool),
                    kind: LegKind::Ladder,
                    gateway: None,
                    wait: subjects
                        .iter()
                        .flatten()
                        .filter_map(|id| config.profiles.get(id))
                        .map(ProxyProfile::timeout)
                        .max()
                        .unwrap_or(ROUTE_TEST_WAIT),
                    stages,
                });
                continue;
            }
            for subject in &subjects {
                let stages = beneath.clone().and_then(|stage| match subject {
                    None => Ok((stage, None)),
                    Some(id) => self
                        .hop(&config, *id, stage.clone(), bottom.clone(), &carriers)
                        .map(|top| (stage, Some(top)))
                        .map_err(|detail| (ProxyFault::Other { detail }, Some(*id))),
                });
                let kind = match (subject, carriers.is_empty()) {
                    (None, true) => direct.clone(),
                    _ => LegKind::Ladder,
                };
                let gateway = gateway(subject.as_ref().or(carriers.last()));
                let subject = subject.and_then(|id| config.profiles.get(&id).cloned());
                probes.push(Probe {
                    leg,
                    egress_id,
                    kind,
                    gateway,
                    carriers: carriers.clone(),
                    wait: subject
                        .as_ref()
                        .map_or(ROUTE_TEST_WAIT, ProxyProfile::timeout),
                    subject,
                    pool: None,
                    stages,
                });
            }
        }
        Ok((probes, concurrency))
    }

    // The stage a path's carriers make, from the egress up.
    fn carriers(
        &self,
        config: &Configuration,
        carriers: &[u32],
        bottom: &Arc<Egress>,
    ) -> Result<Arc<dyn Dialer>, (ProxyFault, Option<u32>)> {
        let mut stage: Arc<dyn Dialer> = bottom.clone();
        for (index, id) in carriers.iter().enumerate() {
            if !config
                .profiles
                .get(id)
                .is_some_and(|profile| profile.enabled)
            {
                return Err((ProxyFault::ProxyUnavailable, Some(*id)));
            }
            stage = self
                .hop(config, *id, stage, bottom.clone(), &carriers[..index])
                .map_err(|detail| (ProxyFault::Other { detail }, Some(*id)))?;
        }
        Ok(stage)
    }
}

async fn run_probes(
    probes: Vec<Probe>,
    target: Option<(String, u16)>,
    concurrency: usize,
    p2p: Option<Arc<P2pTest>>,
) -> Vec<PathTest> {
    let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let target = Arc::new(target);
    let count = probes.len();
    let mut tasks = tokio::task::JoinSet::new();
    for (index, probe) in probes.into_iter().enumerate() {
        let permits = permits.clone();
        let target = target.clone();
        let p2p = p2p.clone();
        tasks.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (index, probe.run((*target).as_ref(), p2p.as_deref()).await)
        });
    }
    let mut tests: Vec<Option<PathTest>> = (0..count).map(|_| None).collect();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((index, test)) => tests[index] = Some(test),
            Err(error) => std::panic::resume_unwind(error.into_panic()),
        }
    }
    tests.into_iter().flatten().collect()
}

// Why a probe stopped short.
enum Stopped {
    Dial(DialError),
    // A proxy's own endpoint could not be reached from the stage beneath it.
    Endpoint(DialError),
}

impl Probe {
    async fn run(self, target: Option<&(String, u16)>, p2p: Option<&P2pTest>) -> PathTest {
        let started = tokio::time::Instant::now();
        let wait = self.wait;
        let mut resolution = None;
        let mut picked = None;
        let (outcome, failing_proxy, source_address) = match &self.stages {
            Err((fault, proxy)) => (Err(fault.clone()), *proxy, None),
            Ok((beneath, top)) => {
                match last(beneath, top.as_ref(), self.subject.as_ref(), target) {
                    Err(fault) => (Err(fault), None, None),
                    Ok(last) => {
                        let stage = top.as_ref().unwrap_or(beneath);
                        // The test's own lookups note how they went; no other
                        // dial's do.
                        let recording = Arc::new(Recording::default());
                        let reached = recording
                            .scope(tokio::time::timeout(stage.budget(), last.reach()))
                            .await;
                        let said = match &reached {
                            Ok(Err(Stopped::Dial(error) | Stopped::Endpoint(error))) => {
                                Some(error.to_string())
                            }
                            _ => None,
                        };
                        let (host, port) = last.named();
                        resolution = recording.finish(host, port, said.as_deref());
                        match reached {
                            Ok(Ok((pass, source, member))) => {
                                picked = member;
                                (Ok(pass), None, source)
                            }
                            Ok(Err(Stopped::Dial(error))) => {
                                let (fault, proxy) = blame(stage, &error, wait);
                                (Err(fault), proxy, None)
                            }
                            Ok(Err(Stopped::Endpoint(error))) => {
                                let (fault, proxy) = blame(beneath, &error, wait);
                                let proxy =
                                    proxy.or(self.subject.as_ref().map(|subject| subject.id));
                                (Err(fault), proxy, None)
                            }
                            Err(_) => match stage.failing_hop() {
                                Some(hop) => (Err(hop.fault), Some(hop.proxy), None),
                                None => (Err(self.timed_out(wait)), None, None),
                            },
                        }
                    }
                }
            }
        };
        // The test torrent runs over a path that reached its target.
        let p2p = match (p2p, &outcome, &self.stages) {
            (Some(test), Ok(_), Ok((beneath, top))) => {
                let stop = std::pin::pin!(std::future::pending::<()>());
                Some(
                    p2p_probe(
                        P2pPath {
                            stage: top.as_ref().unwrap_or(beneath).clone(),
                            kind: self.kind.clone(),
                            gateway: self.gateway,
                            internal_port: test.internal_port,
                        },
                        &P2pPlan {
                            source: test.source.clone(),
                            upload_budget: None,
                        },
                        stop,
                    )
                    .await,
                )
            }
            _ => None,
        };
        PathTest {
            p2p,
            leg: self.leg,
            egress_id: self.egress_id,
            carriers: self.carriers,
            proxy_id: self.subject.map(|subject| subject.id).or(picked),
            pool_id: self.pool,
            outcome,
            failing_proxy,
            source_address,
            elapsed: started.elapsed(),
            resolution,
        }
    }

    fn timed_out(&self, wait: Duration) -> ProxyFault {
        let seconds = fault::seconds(wait);
        match self.subject.as_ref().map(|subject| subject.kind) {
            // QUIC rides UDP, which a network can drop without a word.
            Some(ProxyKind::Http3Connect) => ProxyFault::UdpTimedOut { seconds },
            _ => ProxyFault::TimedOut { seconds },
        }
    }
}

// What a failed dial came to, and the proxy to blame: the first hop on the
// path known to be failing, else the hop the error names.
fn blame(stage: &Arc<dyn Dialer>, error: &DialError, wait: Duration) -> (ProxyFault, Option<u32>) {
    if let Some(hop) = stage.failing_hop() {
        return (hop.fault, Some(hop.proxy));
    }
    let proxy = match error {
        DialError::Hop { proxy, .. } => Some(*proxy),
        _ => None,
    };
    (error.fault(wait), proxy)
}

// The last thing a path does: the dial it ends at, or bringing a tunnel
// up to its endpoint.
enum Last<'a> {
    Dial {
        stage: &'a Arc<dyn Dialer>,
        target: Target,
        pass: ProxyTestPass,
        // Whether the target is the endpoint of the proxy under test, so
        // failing to reach it is that proxy's failure.
        endpoint: bool,
    },
    Prepare {
        stage: &'a Arc<dyn Dialer>,
        endpoint: (String, u16),
    },
}

impl Last<'_> {
    // The host and port the path ends at.
    fn named(&self) -> (&str, u16) {
        match self {
            Self::Dial { target, .. } => (&target.host, target.port),
            Self::Prepare { endpoint, .. } => (&endpoint.0, endpoint.1),
        }
    }

    // What the path came to, the address it left from, and the member a
    // pool picked to carry it.
    async fn reach(
        &self,
    ) -> Result<(ProxyTestPass, Option<std::net::SocketAddr>, Option<u32>), Stopped> {
        match self {
            Self::Dial {
                stage,
                target,
                pass,
                endpoint,
            } => {
                let dialed = stage.dial(target).await.map_err(|error| {
                    if *endpoint {
                        Stopped::Endpoint(error)
                    } else {
                        Stopped::Dial(error)
                    }
                })?;
                Ok((*pass, dialed.source, dialed.path.member))
            }
            Self::Prepare { stage, .. } => {
                stage.prepare().await.map_err(Stopped::Dial)?;
                Ok((ProxyTestPass::Connected, None, None))
            }
        }
    }
}

// How far one path goes. The route alone reaches the target, and a pool
// carries it there. A proxy reaches the target when one is given; otherwise
// an SSH or WireGuard tunnel comes up, and a stream proxy carries a
// connection to a DNS server it resolves with or, with none, answers at its
// endpoint.
fn last<'a>(
    beneath: &'a Arc<dyn Dialer>,
    top: Option<&'a Arc<dyn Dialer>>,
    subject: Option<&ProxyProfile>,
    target: Option<&(String, u16)>,
) -> Result<Last<'a>, ProxyFault> {
    let probe = |host: String, port: u16| Target {
        host,
        port,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    };
    let dial = |stage, target, pass| Last::Dial {
        stage,
        target,
        pass,
        endpoint: false,
    };
    let (Some(top), Some(profile)) = (top, subject) else {
        let Some((host, port)) = target else {
            return Err(ProxyFault::Other {
                detail: NetworkRefusal::TestTargetRequired.to_string(),
            });
        };
        return Ok(match top {
            Some(pool) => dial(pool, probe(host.clone(), *port), ProxyTestPass::Forwarded),
            None => dial(
                beneath,
                probe(host.clone(), *port),
                ProxyTestPass::Connected,
            ),
        });
    };
    if let Some((host, port)) = target {
        return Ok(dial(
            top,
            probe(host.clone(), *port),
            ProxyTestPass::Forwarded,
        ));
    }
    if matches!(profile.kind, ProxyKind::Ssh | ProxyKind::WireGuard) {
        return Ok(Last::Prepare {
            stage: top,
            endpoint: (profile.host.clone(), profile.port),
        });
    }
    if let Some(dns) = profile.proxy_dns_servers().first() {
        return Ok(dial(
            top,
            probe(dns.to_string(), 53),
            ProxyTestPass::Forwarded,
        ));
    }
    if profile.kind == ProxyKind::Http3Connect {
        return Err(ProxyFault::TestNeedsDns);
    }
    Ok(Last::Dial {
        stage: beneath,
        target: Target {
            host: profile.host.clone(),
            port: profile.port,
            purpose: Purpose::ProxyEndpoint { proxy: profile.id },
            addresses: Vec::new(),
        },
        pass: ProxyTestPass::Reachable,
        endpoint: true,
    })
}
