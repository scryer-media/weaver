use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use weaver_nntp::route_dialer::{AddressPlanned, RouteDialer};
use weaver_tunnel::{
    Http3TunnelProvider, SshTunnelProvider, TunnelProvider, WireGuardTunnelProvider,
    egress::SocketEgress,
    pipe::{
        DialError, DialPath, Dialed, Dialer, Egress, Fallback, InnerTransport, Revocable,
        SessionHop, Target, TransportHop,
    },
    transport::{TransportKind, TransportProxy},
};

use super::*;
use crate::Database;

#[path = "network_feed.rs"]
mod feed;
pub use feed::FeedAttempt;
#[path = "network_draft.rs"]
mod draft;
pub use draft::DraftNetworkRoute;

#[derive(Clone)]
struct Configuration {
    consumers: std::collections::HashSet<String>,
    consumer_labels: Vec<(Consumer, String, u16)>,
    egresses: HashMap<u32, EgressInterface>,
    profiles: HashMap<u32, ProxyProfile>,
    pools: HashMap<u32, ProxyPool>,
    policies: HashMap<String, RoutingPolicy>,
    warnings: HashMap<(String, usize), String>,
}
impl Configuration {
    fn load(db: &Database) -> Result<Self, String> {
        let mut configuration = Self {
            warnings: HashMap::new(),
            consumer_labels: db
                .list_servers()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|server| (Consumer::Server(server.id), server.host, server.connections))
                .chain(
                    db.list_rss_feeds()
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .map(|feed| (Consumer::Rss(feed.id), feed.name, 1)),
                )
                .collect(),
            consumers: ProxyRuntime::load_consumers(db).map_err(|e| e.to_string())?,
            egresses: db
                .list_egress_interfaces()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|e| (e.id, e))
                .collect(),
            profiles: db
                .list_proxy_profiles()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|p| (p.id, p))
                .collect(),
            pools: db
                .list_proxy_pools()
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|p| (p.id, p))
                .collect(),
            policies: db
                .list_proxy_routes()
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect(),
        };
        for (consumer, policy) in &mut configuration.policies {
            for (position, leg) in policy.legs.iter_mut().enumerate() {
                if !configuration.egresses.contains_key(&leg.egress_id) {
                    configuration.warnings.insert(
                        (consumer.clone(), position),
                        format!("Restored egress {} is missing; this leg is Down until its egress is repaired.", leg.egress_id),
                    );
                }
            }
        }
        Ok(configuration)
    }
    /// The health a leg's egress contributes before any dial evidence. A
    /// missing egress is Down under the warning recorded for that leg, so
    /// the flow names which egress vanished instead of a generic removal.
    fn egress_health(
        &self,
        consumer: &str,
        position: usize,
        egress_id: u32,
        snapshot: &InterfaceSnapshot,
    ) -> EgressHealth {
        match self.egresses.get(&egress_id) {
            Some(egress) => snapshot.health(egress, None),
            None => EgressHealth::Down(self.missing_egress_warning(consumer, position, egress_id)),
        }
    }
    fn missing_egress_warning(&self, consumer: &str, position: usize, egress_id: u32) -> String {
        self.warnings
            .get(&(consumer.to_owned(), position))
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "Egress {egress_id} is missing; this leg is Down until its egress is repaired."
                )
            })
    }
}

/// A leg of a configured route whose consumer has not asked for a dialer,
/// such as an inactive server. It carries no connections, but the route the
/// operator configured and the health of its egress are still facts the
/// flow reports.
pub struct DormantLeg {
    pub consumer: Consumer,
    pub position: usize,
    pub definition: RouteLeg,
    pub health: LegHealthState,
}

pub struct LiveLeg {
    pub definition: RouteLeg,
    pub warning: Option<String>,
    pub stage: Arc<Revocable>,
    pub address_plan: Option<Arc<AddressPlanned>>,
    ladder: Option<Arc<Fallback>>,
    signature: String,
}
impl LiveLeg {
    pub fn rung_states(&self) -> Vec<&'static str> {
        self.ladder
            .as_ref()
            .map_or_else(Vec::new, |ladder| ladder.rung_states())
    }
}
pub struct LiveNetworkRoute {
    pub consumer: Consumer,
    pub weighted: Arc<Weighted>,
    pub legs: RwLock<Vec<LiveLeg>>,
    cap: std::sync::atomic::AtomicU16,
    timeout: Duration,
}

pub struct NetworkProbe {
    pub source_address: Option<std::net::SocketAddr>,
    pub elapsed: Duration,
}

type StagedPoolMembers = HashMap<(u32, u32), Vec<(u32, Arc<dyn Dialer>)>>;

pub struct NetworkRuntime {
    pub egress_controls: Arc<weaver_nntp::transfer::ServerTransferRegistry>,
    #[cfg(test)]
    fixture_providers: Mutex<HashMap<u32, Arc<dyn TunnelProvider>>>,
    db: Database,
    handle: tokio::runtime::Handle,
    configuration: RwLock<Configuration>,
    sessions: Mutex<HashMap<String, Arc<dyn Dialer>>>,
    /// Live sessions a probe runtime may use but does not own. A probe of a
    /// saved profile shares the session already holding its WireGuard permit
    /// instead of competing with it for the budget, and never shuts it down.
    borrowed_sessions: HashMap<String, Arc<dyn Dialer>>,
    pools: Mutex<HashMap<(u32, u32), Arc<PoolStage>>>,
    routes: Mutex<HashMap<String, Arc<LiveNetworkRoute>>>,
    interfaces: Arc<InterfaceMonitor>,
    compiling: bool,
    pool_updates: Mutex<StagedPoolMembers>,
    poll: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl NetworkRuntime {
    pub fn new(db: Database, handle: tokio::runtime::Handle) -> Result<Arc<Self>, String> {
        let configuration = Configuration::load(&db)?;
        let interfaces = Arc::new(InterfaceMonitor::start(
            Arc::new(SystemInterfaceSource),
            &handle,
        ));
        let egress_controls = Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new());
        for egress in configuration.egresses.values() {
            egress_controls.configure(
                weaver_nntp::transfer::StableServerId(egress.id),
                weaver_nntp::transfer::ServerTransferConfig {
                    rate_bytes_per_sec: egress.max_download_speed,
                    quota: None,
                },
            );
        }
        let runtime = Arc::new(Self {
            egress_controls,
            #[cfg(test)]
            fixture_providers: Mutex::new(HashMap::new()),
            db,
            handle: handle.clone(),
            configuration: RwLock::new(configuration),
            sessions: Mutex::new(HashMap::new()),
            borrowed_sessions: HashMap::new(),
            pools: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            interfaces,
            compiling: false,
            pool_updates: Mutex::new(HashMap::new()),
            poll: Mutex::new(None),
        });
        let weak = Arc::downgrade(&runtime);
        *runtime.poll.lock().expect("network health task") = Some(handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let Some(runtime) = weak.upgrade() else {
                    break;
                };
                runtime.refresh_health();
                runtime.retire_unused_sessions().await;
            }
        }));
        Ok(runtime)
    }
    pub fn interfaces(&self) -> InterfaceSnapshot {
        self.interfaces.snapshot()
    }
    pub fn configuration_snapshot(
        &self,
    ) -> (Vec<EgressInterface>, Vec<ProxyProfile>, Vec<ProxyPool>) {
        let config = self.configuration.read().expect("network configuration");
        (
            config.egresses.values().cloned().collect(),
            config.profiles.values().cloned().collect(),
            config.pools.values().cloned().collect(),
        )
    }
    pub fn consumer_snapshot(&self) -> Vec<(Consumer, String, u16, RoutingPolicy)> {
        let config = self.configuration.read().expect("network configuration");
        config
            .consumer_labels
            .iter()
            .map(|(consumer, name, cap)| {
                (
                    *consumer,
                    name.clone(),
                    *cap,
                    config
                        .policies
                        .get(&consumer.key())
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect()
    }
    pub fn live_routes(&self) -> Vec<Arc<LiveNetworkRoute>> {
        self.routes
            .lock()
            .expect("network routes")
            .values()
            .cloned()
            .collect()
    }
    /// Legs of every configured route without a live counterpart.
    pub fn dormant_legs(&self) -> Vec<DormantLeg> {
        let snapshot = self.interfaces();
        let config = self.configuration.read().expect("network configuration");
        let routes = self.routes.lock().expect("network routes");
        let mut legs = Vec::new();
        for (consumer, _, _) in &config.consumer_labels {
            let key = consumer.key();
            if routes.contains_key(&key) {
                continue;
            }
            let definition = config
                .policies
                .get(&key)
                .cloned()
                .unwrap_or_default()
                .route();
            for (position, leg) in definition.legs.into_iter().enumerate() {
                let health = LegHealthState::of_egress(config.egress_health(
                    &key,
                    position,
                    leg.egress_id,
                    &snapshot,
                ));
                legs.push(DormantLeg {
                    consumer: *consumer,
                    position,
                    definition: leg,
                    health,
                });
            }
        }
        legs
    }
    pub fn pool_status(
        &self,
    ) -> Vec<(
        u32,
        u32,
        weaver_nntp::candidate_plan::PlanSnapshot<u32>,
        Vec<PoolMemberStatus>,
    )> {
        self.pools
            .lock()
            .expect("network pools")
            .iter()
            .map(|(&(pool, egress), stage)| {
                let (plan, members) = stage.snapshot();
                (pool, egress, plan, members)
            })
            .collect()
    }
    fn refresh_health(&self) {
        let snapshot = self.interfaces();
        let config = self.configuration.read().expect("network configuration");
        for route in self.routes.lock().expect("network routes").values() {
            for (position, leg) in route.legs.read().expect("route legs").iter().enumerate() {
                let health = config.egress_health(
                    &route.consumer.key(),
                    position,
                    leg.definition.egress_id,
                    &snapshot,
                );
                route.weighted.set_egress_health(position, health);
            }
        }
    }
    fn bottom(config: &Configuration, id: u32, timeout: Duration) -> Result<Arc<Egress>, String> {
        let egress = config.egresses.get(&id).ok_or("egress does not exist")?;
        let binding = match &egress.binding {
            EgressBinding::System => SocketEgress::System,
            EgressBinding::Interface { name } => SocketEgress::Interface(name.clone()),
            EgressBinding::SourceAddress { address } => SocketEgress::SourceAddress(*address),
        };
        Ok(Arc::new(Egress {
            id,
            binding,
            timeout,
        }))
    }
    fn hop(
        &self,
        config: &Configuration,
        id: u32,
        inner: Arc<dyn Dialer>,
        bottom: Arc<Egress>,
        prefix: &[u32],
    ) -> Result<Arc<dyn Dialer>, String> {
        let profile = config.profiles.get(&id).ok_or("proxy does not exist")?;
        if !profile.enabled {
            return Ok(Arc::new(Unavailable(format!("proxy {id} is disabled"))));
        }
        // A canonical hop has the profile's timeout, independent of its first consumer.
        let bottom = Arc::new(Egress {
            id: bottom.id,
            binding: bottom.binding.clone(),
            timeout: profile.timeout(),
        });
        let inner: Arc<dyn Dialer> = if prefix.is_empty() {
            bottom.clone()
        } else {
            inner
        };
        let mut ids = prefix.to_vec();
        ids.push(id);
        let revisions: Vec<_> = ids
            .iter()
            .map(|id| (*id, config.profiles.get(id).map(|p| p.revision)))
            .collect();
        let key = format!("{}:{:?}:{revisions:?}", bottom.id, bottom.binding);
        let mut sessions = self.sessions.lock().expect("network sessions");
        if let Some(stage) = sessions
            .get(&key)
            .or_else(|| self.borrowed_sessions.get(&key))
        {
            return Ok(stage.clone());
        }
        let observer = Arc::new(super::runtime::Observer {
            db: self.db.clone(),
            id,
            revision: profile.revision,
        });
        let mut transport = None;
        let mut resolver = None;
        let stage: Arc<dyn Dialer> = match profile.kind {
            ProxyKind::Socks5 | ProxyKind::HttpConnect => Arc::new(TransportHop {
                id,
                spec: TransportProxy {
                    kind: if profile.kind == ProxyKind::Socks5 {
                        TransportKind::Socks5
                    } else {
                        TransportKind::HttpConnect
                    },
                    host: profile.host.clone(),
                    port: profile.port,
                    username: profile.secrets.username.clone(),
                    password: profile.secrets.password.clone(),
                },
                inner,
                timeout: profile.timeout(),
            }),
            _ => {
                let provider: Arc<dyn TunnelProvider> = match profile.kind {
                    ProxyKind::Ssh => {
                        let input = Arc::new(InnerTransport {
                            inner: inner.clone(),
                            proxy: id,
                            connected: Mutex::new(None),
                        });
                        transport = Some(input.clone());
                        Arc::new(
                            SshTunnelProvider::new(profile.ssh_spec(), observer)
                                .with_transport(input),
                        )
                    }
                    ProxyKind::WireGuard => {
                        if !prefix.is_empty() {
                            return Err("WireGuard must be the first hop on an egress".into());
                        }
                        let provider = Arc::new(
                            WireGuardTunnelProvider::new(
                                profile.wireguard_spec().map_err(|e| e.to_string())?,
                                observer,
                            )
                            .with_udp_factory(bottom.clone()),
                        );
                        resolver = Some(provider.clone());
                        provider
                    }
                    ProxyKind::Http3Connect => {
                        if !prefix.is_empty() {
                            return Err("HTTP/3 must be the first hop on an egress".into());
                        }
                        Arc::new(
                            Http3TunnelProvider::new(
                                profile.http3_spec().map_err(|e| e.to_string())?,
                            )
                            .map_err(|e| e.to_string())?
                            .with_udp_factory(bottom.clone()),
                        )
                    }
                    _ => unreachable!(),
                };
                #[cfg(test)]
                let provider = self
                    .fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .get(&id)
                    .cloned()
                    .unwrap_or(provider);
                Arc::new(SessionHop {
                    capacity: if profile.kind == ProxyKind::WireGuard {
                        weaver_tunnel::pipe::SessionCapacity::new(wireguard_session_budget())
                    } else {
                        Default::default()
                    },
                    activity: Default::default(),
                    id,
                    provider,
                    inner,
                    transport,
                    path: DialPath {
                        egress: bottom.id,
                        proxies: prefix.to_vec(),
                        ..Default::default()
                    },
                    endpoint: profile
                        .host
                        .parse()
                        .ok()
                        .map(|ip| std::net::SocketAddr::new(ip, profile.port)),
                    timeout: profile.timeout(),
                    resolver,
                })
            }
        };
        sessions.insert(key, stage.clone());
        Ok(stage)
    }
    fn pool(
        &self,
        config: &Configuration,
        id: u32,
        bottom: Arc<Egress>,
    ) -> Result<Arc<dyn Dialer>, String> {
        let pool = config.pools.get(&id).ok_or("proxy pool does not exist")?;
        if !pool.enabled {
            if let Some(stage) = self
                .pools
                .lock()
                .expect("network pools")
                .get(&(id, bottom.id))
            {
                if self.compiling {
                    self.pool_updates
                        .lock()
                        .expect("staged pools")
                        .insert((id, bottom.id), Vec::new());
                } else {
                    stage.set_members(Vec::new());
                }
            }
            return Ok(Arc::new(Unavailable(format!("pool {id} is disabled"))));
        }
        let members = pool
            .member_ids
            .iter()
            .filter(|id| config.profiles.get(id).is_some_and(|p| p.enabled))
            .map(|id| {
                Ok((
                    *id,
                    self.hop(config, *id, bottom.clone(), bottom.clone(), &[])?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut pools = self.pools.lock().expect("network pools");
        let stage = if let Some(stage) = pools.get(&(id, bottom.id)) {
            if self.compiling {
                self.pool_updates
                    .lock()
                    .expect("staged pools")
                    .insert((id, bottom.id), members);
            } else {
                let revision = stage.revision();
                stage.set_members(members);
                if stage.revision() != revision {
                    stage.prewarm();
                }
            }
            stage.clone()
        } else {
            let stage = PoolStage::new(id, members, self.handle.clone());
            pools.insert((id, bottom.id), stage.clone());
            if !self.compiling {
                stage.prewarm();
            }
            stage
        };
        Ok(stage)
    }
    fn leg(
        &self,
        config: &Configuration,
        consumer: Consumer,
        position: usize,
        leg: &RouteLeg,
        timeout: Duration,
    ) -> Result<LiveLeg, String> {
        if !config.egresses.contains_key(&leg.egress_id) {
            let warning = config.missing_egress_warning(&consumer.key(), position, leg.egress_id);
            return Ok(LiveLeg {
                definition: leg.clone(),
                warning: Some(warning.clone()),
                stage: Arc::new(Revocable::new(Arc::new(Unavailable(warning)))),
                address_plan: None,
                ladder: None,
                signature: format!("missing:{}", leg.egress_id),
            });
        }
        let bottom = Self::bottom(config, leg.egress_id, timeout)?;
        let mut address_plan = None;
        let mut ladder = None;
        let mut versions = Vec::new();
        let mut pool_versions = Vec::new();
        let stage: Arc<dyn Dialer> = match &leg.path {
            LegPath::Direct => {
                if matches!(consumer, Consumer::Server(_)) {
                    let plan = Arc::new(AddressPlanned::new(
                        format!("{} leg {}", consumer.key(), position),
                        bottom.clone(),
                    ));
                    address_plan = Some(plan.clone());
                    plan
                } else {
                    bottom.clone()
                }
            }
            LegPath::Ladder {
                rungs,
                direct_fallback,
            } => {
                let mut stages = Vec::<Arc<dyn Dialer>>::new();
                for rung in rungs {
                    stages.push(match rung {
                        Rung::Proxy { id } => {
                            versions.push((*id, config.profiles.get(id).map(|p| p.revision)));
                            self.hop(config, *id, bottom.clone(), bottom.clone(), &[])?
                        }
                        Rung::Pool { id } => {
                            let pool = config.pools.get(id).ok_or("proxy pool does not exist")?;
                            pool_versions.push((pool.id, pool.enabled, pool.member_ids.clone()));
                            for member in &pool.member_ids {
                                versions.push((
                                    *member,
                                    config.profiles.get(member).map(|p| p.revision),
                                ));
                            }
                            self.pool(config, *id, bottom.clone())?
                        }
                        Rung::Chain { ids } => {
                            let mut stage: Arc<dyn Dialer> = bottom.clone();
                            for (index, id) in ids.iter().enumerate() {
                                versions.push((*id, config.profiles.get(id).map(|p| p.revision)));
                                stage =
                                    self.hop(config, *id, stage, bottom.clone(), &ids[..index])?;
                            }
                            stage
                        }
                    });
                }
                if *direct_fallback {
                    stages.push(bottom.clone());
                }
                let fallback = Arc::new(Fallback::with_rung_cooldown(
                    stages,
                    weaver_nntp::plan_timing::timing().rung_cooldown,
                ));
                ladder = Some(fallback.clone());
                fallback
            }
        };
        let signature = format!(
            "{}:{:?}:{}:{versions:?}:{pool_versions:?}",
            leg.egress_id,
            bottom.binding,
            serde_json::to_string(&leg.path).map_err(|e| e.to_string())?
        );
        Ok(LiveLeg {
            definition: leg.clone(),
            warning: config.warnings.get(&(consumer.key(), position)).cloned(),
            stage: Arc::new(Revocable::new(stage)),
            address_plan,
            ladder,
            signature,
        })
    }
    pub fn route(
        &self,
        consumer: Consumer,
        cap: u16,
        timeout: Duration,
    ) -> Result<Arc<LiveNetworkRoute>, String> {
        let config = self.configuration.read().expect("network configuration");
        let mut routes = self.routes.lock().expect("network routes");
        if !config.consumers.contains(&consumer.key()) {
            return Err("routing consumer no longer exists".into());
        }
        if let Some(route) = routes.get(&consumer.key()) {
            let definition = config
                .policies
                .get(&consumer.key())
                .cloned()
                .unwrap_or_default()
                .route();
            route.weighted.reweight(definition, cap)?;
            route.cap.store(cap, std::sync::atomic::Ordering::Release);
            return Ok(route.clone());
        }
        let definition = config
            .policies
            .get(&consumer.key())
            .cloned()
            .unwrap_or_default()
            .route();
        let legs = definition
            .legs
            .iter()
            .enumerate()
            .map(|(position, leg)| self.leg(&config, consumer, position, leg, timeout))
            .collect::<Result<Vec<_>, _>>()?;
        let weighted = Weighted::new(
            definition,
            legs.iter()
                .map(|leg| leg.stage.clone() as Arc<dyn Dialer>)
                .collect(),
            cap,
            &self.handle,
        )?;
        let snapshot = self.interfaces();
        for (position, leg) in legs.iter().enumerate() {
            weighted.set_egress_health(
                position,
                config.egress_health(
                    &consumer.key(),
                    position,
                    leg.definition.egress_id,
                    &snapshot,
                ),
            );
        }
        let route = Arc::new(LiveNetworkRoute {
            consumer,
            weighted,
            legs: RwLock::new(legs),
            cap: std::sync::atomic::AtomicU16::new(cap),
            timeout,
        });
        routes.insert(consumer.key(), route.clone());
        Ok(route)
    }
    pub fn nntp_dialer(
        &self,
        id: u32,
        cap: u16,
        timeout: Duration,
    ) -> Result<Arc<RouteDialer>, String> {
        let route = self.route(Consumer::Server(id), cap, timeout)?;
        Ok(Arc::new(RouteDialer {
            inner: route.weighted.clone(),
            egress_controls: self.egress_controls.clone(),
            runtime: self.handle.clone(),
            server: id,
        }))
    }
    pub async fn reload(&self) -> Result<(), String> {
        let db = self.db.clone();
        let next = tokio::task::spawn_blocking(move || Configuration::load(&db))
            .await
            .map_err(|e| e.to_string())??;
        self.apply_configuration(next)?;
        self.retire_unused_sessions().await;
        self.refresh_health();
        Ok(())
    }
    fn apply_configuration(&self, next: Configuration) -> Result<(), String> {
        // Route lookups must see configuration and compiled paths from the same reload.
        let mut configuration = self.configuration.write().expect("network configuration");
        let mut staging = Self {
            db: self.db.clone(),
            handle: self.handle.clone(),
            configuration: RwLock::new(next.clone()),
            sessions: Mutex::new(self.sessions.lock().expect("network sessions").clone()),
            borrowed_sessions: HashMap::new(),
            pools: Mutex::new(self.pools.lock().expect("network pools").clone()),
            routes: Mutex::new(HashMap::new()),
            interfaces: self.interfaces.clone(),
            compiling: true,
            pool_updates: Mutex::new(HashMap::new()),
            poll: Mutex::new(None),
            egress_controls: self.egress_controls.clone(),
            #[cfg(test)]
            fixture_providers: Mutex::new(
                self.fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .clone(),
            ),
        };
        let mut updates = Vec::new();
        let mut removed = Vec::new();
        let mut routes = self.live_routes();
        routes.sort_by_key(|route| route.consumer.key());
        for route in routes {
            if !next.consumers.contains(&route.consumer.key()) {
                removed.push(route);
                continue;
            }
            let definition = next
                .policies
                .get(&route.consumer.key())
                .cloned()
                .unwrap_or_default()
                .route();
            let mut fresh = definition
                .legs
                .iter()
                .enumerate()
                .map(|(position, leg)| {
                    staging.leg(&next, route.consumer, position, leg, route.timeout)
                })
                .collect::<Result<Vec<_>, _>>()?;
            {
                let old = route.legs.read().expect("route legs");
                for (position, new) in fresh.iter_mut().enumerate() {
                    if let Some(prior) = old
                        .get(position)
                        .filter(|prior| prior.signature == new.signature)
                    {
                        new.stage = prior.stage.clone();
                        new.address_plan = prior.address_plan.clone();
                        new.ladder = prior.ladder.clone();
                    }
                }
            }
            let allocation = Weighted::prepare_update(
                definition,
                fresh
                    .iter()
                    .map(|leg| leg.stage.clone() as Arc<dyn Dialer>)
                    .collect(),
                route.cap.load(std::sync::atomic::Ordering::Acquire),
            )?;
            updates.push((route, fresh, allocation));
        }
        // Everything below commits already validated objects; preparation above never
        // changes live membership, allocations, sessions, or revocation state.
        let prior_revisions: HashMap<(u32, u32), u64> = self
            .pools
            .lock()
            .expect("network pools")
            .iter()
            .map(|(key, stage)| (*key, stage.revision()))
            .collect();
        *self.sessions.lock().expect("network sessions") =
            std::mem::take(staging.sessions.get_mut().expect("staged sessions"));
        *self.pools.lock().expect("network pools") =
            std::mem::take(staging.pools.get_mut().expect("staged pools"));
        for (key, members) in std::mem::take(staging.pool_updates.get_mut().expect("staged pools"))
        {
            self.pools.lock().expect("network pools")[&key].set_members(members);
        }
        let snapshot = self.interfaces();
        for (route, fresh, allocation) in updates {
            let mut old = route.legs.write().expect("route legs");
            route.weighted.apply_update(allocation);
            for (position, leg) in fresh.iter().enumerate() {
                route.weighted.set_egress_health(
                    position,
                    next.egress_health(
                        &route.consumer.key(),
                        position,
                        leg.definition.egress_id,
                        &snapshot,
                    ),
                );
            }
            for (position, prior) in old.iter().enumerate() {
                if fresh
                    .get(position)
                    .is_none_or(|new| !Arc::ptr_eq(&new.stage, &prior.stage))
                {
                    prior.stage.revoke();
                }
            }
            *old = fresh;
        }
        for route in removed {
            for leg in route.legs.read().expect("route legs").iter() {
                leg.stage.revoke();
            }
            self.routes
                .lock()
                .expect("network routes")
                .remove(&route.consumer.key());
        }
        for egress in next.egresses.values() {
            self.egress_controls.configure(
                weaver_nntp::transfer::StableServerId(egress.id),
                weaver_nntp::transfer::ServerTransferConfig {
                    rate_bytes_per_sec: egress.max_download_speed,
                    quota: None,
                },
            );
        }
        // A pool cache owns its member sessions. Drop unused plans before
        // pruning sessions so deleted routes do not retain tunnel buffers.
        let retired_pools = {
            let mut pools = self.pools.lock().expect("network pools");
            let keys: Vec<_> = pools
                .iter()
                .filter(|(_, stage)| Arc::strong_count(stage) == 1)
                .map(|(key, _)| *key)
                .collect();
            keys.into_iter()
                .filter_map(|key| pools.remove(&key))
                .collect::<Vec<_>>()
        };
        for pool in retired_pools {
            pool.set_members(Vec::new());
        }
        *configuration = next;
        // Only new or re-membered pools warm up; an unchanged idle member must
        // not handshake again on every reload.
        for (key, pool) in self.pools.lock().expect("network pools").iter() {
            if prior_revisions.get(key) != Some(&pool.revision()) {
                pool.prewarm();
            }
        }
        Ok(())
    }
    async fn retire_unused_sessions(&self) {
        loop {
            let candidates: Vec<_> = self
                .sessions
                .lock()
                .expect("network sessions")
                .iter()
                .filter(|(_, stage)| Arc::strong_count(stage) == 1)
                .map(|(key, stage)| (key.clone(), stage.clone()))
                .collect();
            let mut removed = false;
            for (key, stage) in candidates {
                if !stage.retire_idle().await {
                    continue;
                }
                let mut sessions = self.sessions.lock().expect("network sessions");
                if sessions
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &stage))
                    && Arc::strong_count(&stage) == 2
                {
                    sessions.remove(&key);
                    removed = true;
                }
            }
            if !removed {
                break;
            }
        }
    }
    pub async fn test_path(
        &self,
        id: u32,
        proxy: Option<u32>,
        host: Option<&str>,
        port: Option<u16>,
    ) -> Result<NetworkProbe, String> {
        let isolated = self.isolated_probe_runtime();
        let stage = {
            let config = isolated
                .configuration
                .read()
                .expect("network configuration");
            let egress = config.egresses.get(&id).ok_or("egress does not exist")?;
            if !egress.enabled {
                return Err("egress is disabled".into());
            }
            let bottom = Self::bottom(&config, id, Duration::from_secs(15))?;
            match proxy {
                Some(proxy) => isolated.hop(&config, proxy, bottom.clone(), bottom, &[])?,
                None => bottom as Arc<dyn Dialer>,
            }
        };
        let started = tokio::time::Instant::now();
        let result = async {
            let source_address = match (host, port) {
                (Some(host), Some(port)) => {
                    let dialed = stage
                        .dial(&Target {
                            host: host.into(),
                            port,
                            purpose: weaver_tunnel::pipe::Purpose::Probe,
                            addresses: Vec::new(),
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                    dialed.source
                }
                (None, None) if proxy.is_some() => {
                    stage.prepare().await.map_err(|e| e.to_string())?;
                    None
                }
                _ => return Err("host and port must be provided together".into()),
            };
            Ok(NetworkProbe {
                source_address,
                elapsed: started.elapsed(),
            })
        }
        .await;
        isolated.shutdown().await;
        result
    }

    pub async fn shutdown(&self) {
        if let Some(task) = self.poll.lock().expect("network health task").take() {
            task.abort();
        }
        for route in self.live_routes() {
            for leg in route.legs.read().expect("route legs").iter() {
                leg.stage.revoke();
            }
        }
        let pools: Vec<_> = self
            .pools
            .lock()
            .expect("network pools")
            .values()
            .cloned()
            .collect();
        for pool in pools {
            pool.shutdown().await;
        }
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .expect("network sessions")
            .values()
            .cloned()
            .collect();
        for session in sessions {
            session.shutdown().await;
        }
    }
}
impl Drop for NetworkRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.poll.get_mut().expect("network health task").take() {
            task.abort();
        }
    }
}

#[cfg(test)]
impl NetworkRuntime {
    pub(crate) fn install_fixture(&self, id: u32, provider: Arc<dyn TunnelProvider>) {
        assert!(self.sessions.lock().unwrap().is_empty());
        self.fixture_providers.lock().unwrap().insert(id, provider);
    }
}

struct Unavailable(String);
#[async_trait::async_trait]
impl Dialer for Unavailable {
    async fn prepare(&self) -> Result<(), DialError> {
        Err(DialError::Skipped(self.0.clone()))
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        Err(DialError::Skipped(self.0.clone()))
    }
    fn budget(&self) -> Duration {
        Duration::ZERO
    }
    fn describe(&self) -> String {
        self.0.clone()
    }
}

#[cfg(test)]
#[path = "network_runtime_tests.rs"]
mod tests;
