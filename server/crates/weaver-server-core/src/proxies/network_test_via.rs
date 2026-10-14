// A test reaches what it tests through a route the operator drafted for it,
// and through nothing else. Every path through the draft is tried on its
// own: each leg's direct line, each rung, each member of a pool rung. A
// proxy under test sits on top of the path the way a chain carries it, so a
// proxy that is only reachable through a tunnel is tested through that
// tunnel.
use super::*;
use weaver_tunnel::{fault, pipe::Purpose};

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
    // Each member of the pool.
    Pool(u32),
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
    // The proxy under test, unless the route was tested alone.
    pub proxy_id: Option<u32>,
    pub outcome: Result<ProxyTestPass, ProxyFault>,
    // The proxy a failure belongs to, when one hop is to blame.
    pub failing_proxy: Option<u32>,
    pub source_address: Option<std::net::SocketAddr>,
    pub elapsed: Duration,
}

// A path's stages: the one beneath the proxy under test, and that proxy on
// top of it. A path that cannot be built says why and whose fault it is.
type Stages = Result<(Arc<dyn Dialer>, Option<Arc<dyn Dialer>>), (ProxyFault, Option<u32>)>;

struct Probe {
    leg: usize,
    egress_id: u32,
    carriers: Vec<u32>,
    subject: Option<ProxyProfile>,
    stages: Stages,
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
        let target = target.filter(|(host, _)| !host.trim().is_empty());
        let probe = ProbeRuntime(Some(self.isolated_probe_runtime()));
        let (probes, concurrency) = probe
            .runtime()
            .plan_test(route, subject, target.is_some())?;
        let tests = run_probes(probes, target, concurrency).await;
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
                TestSubject::Pool(id) => config
                    .pools
                    .get(&id)
                    .ok_or(NetworkRefusal::PoolGone)?
                    .member_ids
                    .clone(),
            };
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
        let wireguard = paths
            .iter()
            .flat_map(|(_, _, carriers)| carriers)
            .chain(subjects.iter().flatten())
            .any(|id| kind_of(id) == Some(ProxyKind::WireGuard));
        let concurrency = if wireguard {
            max_wireguard_instances().clamp(1, 4)
        } else {
            4
        };
        let mut probes = Vec::new();
        for (leg, egress_id, carriers) in paths {
            let bottom = Self::bottom(&config, egress_id, ROUTE_TEST_WAIT)
                .map_err(|_| NetworkRefusal::RouteEgressMissing)?;
            let beneath = self.carriers(&config, &carriers, &bottom);
            for subject in &subjects {
                let stages = beneath.clone().and_then(|stage| match subject {
                    None => Ok((stage, None)),
                    Some(id) => self
                        .hop(&config, *id, stage.clone(), bottom.clone(), &carriers)
                        .map(|top| (stage, Some(top)))
                        .map_err(|detail| (ProxyFault::Other { detail }, Some(*id))),
                });
                probes.push(Probe {
                    leg,
                    egress_id,
                    carriers: carriers.clone(),
                    subject: subject.and_then(|id| config.profiles.get(&id).cloned()),
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
) -> Vec<PathTest> {
    let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let target = Arc::new(target);
    let count = probes.len();
    let mut tasks = tokio::task::JoinSet::new();
    for (index, probe) in probes.into_iter().enumerate() {
        let permits = permits.clone();
        let target = target.clone();
        tasks.spawn(async move {
            let _permit = permits.acquire_owned().await;
            (index, probe.run((*target).as_ref()).await)
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
    Fault(ProxyFault),
}

impl Probe {
    async fn run(self, target: Option<&(String, u16)>) -> PathTest {
        let started = tokio::time::Instant::now();
        let wait = self
            .subject
            .as_ref()
            .map_or(ROUTE_TEST_WAIT, ProxyProfile::timeout);
        let (outcome, failing_proxy, source_address) = match &self.stages {
            Err((fault, proxy)) => (Err(fault.clone()), *proxy, None),
            Ok((beneath, top)) => {
                let stage = top.as_ref().unwrap_or(beneath);
                let reached = tokio::time::timeout(
                    stage.budget(),
                    reach(beneath, top.as_ref(), self.subject.as_ref(), target),
                )
                .await;
                match reached {
                    Ok(Ok((pass, source))) => (Ok(pass), None, source),
                    Ok(Err(Stopped::Fault(fault))) => (Err(fault), None, None),
                    Ok(Err(Stopped::Dial(error))) => {
                        let (fault, proxy) = blame(stage, &error, wait);
                        (Err(fault), proxy, None)
                    }
                    Ok(Err(Stopped::Endpoint(error))) => {
                        let (fault, proxy) = blame(beneath, &error, wait);
                        let proxy = proxy.or(self.subject.as_ref().map(|subject| subject.id));
                        (Err(fault), proxy, None)
                    }
                    Err(_) => match stage.failing_hop() {
                        Some(hop) => (Err(hop.fault), Some(hop.proxy), None),
                        None => (Err(self.timed_out(wait)), None, None),
                    },
                }
            }
        };
        PathTest {
            leg: self.leg,
            egress_id: self.egress_id,
            carriers: self.carriers,
            proxy_id: self.subject.map(|subject| subject.id),
            outcome,
            failing_proxy,
            source_address,
            elapsed: started.elapsed(),
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

// How far one path gets. The route alone reaches the target. A proxy
// reaches the target when one is given; otherwise an SSH or WireGuard
// tunnel comes up, and a stream proxy carries a connection to a DNS server
// it resolves with or, with none, answers at its endpoint.
async fn reach(
    beneath: &Arc<dyn Dialer>,
    top: Option<&Arc<dyn Dialer>>,
    subject: Option<&ProxyProfile>,
    target: Option<&(String, u16)>,
) -> Result<(ProxyTestPass, Option<std::net::SocketAddr>), Stopped> {
    let probe = |host: String, port: u16| Target {
        host,
        port,
        purpose: Purpose::Probe,
        addresses: Vec::new(),
    };
    let (Some(top), Some(profile)) = (top, subject) else {
        let Some((host, port)) = target else {
            return Err(Stopped::Fault(ProxyFault::Other {
                detail: NetworkRefusal::TestTargetRequired.to_string(),
            }));
        };
        let dialed = beneath
            .dial(&probe(host.clone(), *port))
            .await
            .map_err(Stopped::Dial)?;
        return Ok((ProxyTestPass::Connected, dialed.source));
    };
    if let Some((host, port)) = target {
        let dialed = top
            .dial(&probe(host.clone(), *port))
            .await
            .map_err(Stopped::Dial)?;
        return Ok((ProxyTestPass::Forwarded, dialed.source));
    }
    if matches!(profile.kind, ProxyKind::Ssh | ProxyKind::WireGuard) {
        top.prepare().await.map_err(Stopped::Dial)?;
        return Ok((ProxyTestPass::Connected, None));
    }
    if let Some(dns) = profile.proxy_dns_servers().first() {
        let dialed = top
            .dial(&probe(dns.to_string(), 53))
            .await
            .map_err(Stopped::Dial)?;
        return Ok((ProxyTestPass::Forwarded, dialed.source));
    }
    if profile.kind == ProxyKind::Http3Connect {
        return Err(Stopped::Fault(ProxyFault::TestNeedsDns));
    }
    let dialed = beneath
        .dial(&Target {
            host: profile.host.clone(),
            port: profile.port,
            purpose: Purpose::ProxyEndpoint { proxy: profile.id },
            addresses: Vec::new(),
        })
        .await
        .map_err(Stopped::Endpoint)?;
    Ok((ProxyTestPass::Reachable, dialed.source))
}
