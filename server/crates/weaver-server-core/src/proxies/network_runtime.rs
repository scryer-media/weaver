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
                    configuration.warnings.insert((consumer.clone(),position),format!("Restored egress {} is missing; using System. Edit this route to choose an egress.",leg.egress_id));
                    leg.egress_id = 0;
                }
            }
        }
        Ok(configuration)
    }
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

pub struct NetworkRuntime {
    pub egress_controls: Arc<weaver_nntp::transfer::ServerTransferRegistry>,
    #[cfg(test)]
    fixture_providers: Mutex<HashMap<u32, Arc<dyn TunnelProvider>>>,
    db: Database,
    handle: tokio::runtime::Handle,
    configuration: RwLock<Configuration>,
    sessions: Mutex<HashMap<String, Arc<dyn Dialer>>>,
    pools: Mutex<HashMap<(u32, u32), Arc<PoolStage>>>,
    routes: Mutex<HashMap<String, Arc<LiveNetworkRoute>>>,
    interfaces: InterfaceMonitor,
    poll: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl NetworkRuntime {
    pub fn new(db: Database, handle: tokio::runtime::Handle) -> Result<Arc<Self>, String> {
        let configuration = Configuration::load(&db)?;
        let interfaces = InterfaceMonitor::start(Arc::new(SystemInterfaceSource), &handle);
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
            pools: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            interfaces,
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
                let health = config
                    .egresses
                    .get(&leg.definition.egress_id)
                    .map(|e| snapshot.health(e, None))
                    .unwrap_or_else(|| EgressHealth::Down("Egress was removed".into()));
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
        if let Some(stage) = sessions.get(&key) {
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
                stage.set_members(Vec::new());
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
            let revision = stage.revision();
            stage.set_members(members);
            if stage.revision() != revision {
                stage.prewarm();
            }
            stage.clone()
        } else {
            let stage = PoolStage::new(id, members, self.handle.clone());
            pools.insert((id, bottom.id), stage.clone());
            stage.prewarm();
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
        let bottom = Self::bottom(config, leg.egress_id, timeout)?;
        let mut address_plan = None;
        let mut ladder = None;
        let mut versions = Vec::new();
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
                        Rung::Pool { id } => self.pool(config, *id, bottom.clone())?,
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
                let fallback = Arc::new(Fallback::new(stages));
                ladder = Some(fallback.clone());
                fallback
            }
        };
        let signature = format!(
            "{}:{:?}:{}:{versions:?}",
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
                snapshot.health(&config.egresses[&leg.definition.egress_id], None),
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
        for route in self.live_routes() {
            if !next.consumers.contains(&route.consumer.key()) {
                for leg in route.legs.read().expect("route legs").iter() {
                    leg.stage.revoke();
                }
                self.routes
                    .lock()
                    .expect("network routes")
                    .remove(&route.consumer.key());
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
                    self.leg(&next, route.consumer, position, leg, route.timeout)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut old = route.legs.write().expect("route legs");
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
            route.weighted.update(
                definition,
                fresh
                    .iter()
                    .map(|leg| leg.stage.clone() as Arc<dyn Dialer>)
                    .collect(),
                route.cap.load(std::sync::atomic::Ordering::Acquire),
            )?;
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
        Ok(())
    }
    async fn retire_unused_sessions(&self) {
        loop {
            let retired = {
                let mut sessions = self.sessions.lock().expect("network sessions");
                let keys: Vec<_> = sessions
                    .iter()
                    .filter(|(_, stage)| Arc::strong_count(stage) == 1)
                    .map(|(key, _)| key.clone())
                    .collect();
                keys.into_iter()
                    .filter_map(|key| sessions.remove(&key))
                    .collect::<Vec<_>>()
            };
            if retired.is_empty() {
                break;
            }
            for stage in retired {
                stage.retire().await;
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
