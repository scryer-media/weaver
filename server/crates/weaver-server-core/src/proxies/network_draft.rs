// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    pipe::{Purpose, Resolution, Transports},
};

pub struct DraftNetworkRoute {
    pub policy: RoutingPolicy,
    legs: Vec<LiveLeg>,
    handle: tokio::runtime::Handle,
    bridges: Mutex<HashMap<usize, Arc<Bridge>>>,
    isolated: Arc<NetworkRuntime>,
}
impl DraftNetworkRoute {
    pub fn bridge(&self) -> Result<Arc<Bridge>, String> {
        self.bridge_for_leg(0)
    }
    pub fn leg_count(&self) -> usize {
        self.legs.len()
    }
    // The dialing stage of one leg, as drafted.
    pub fn leg_stage(&self, position: usize) -> Result<Arc<dyn Dialer>, String> {
        Ok(self
            .legs
            .get(position)
            .ok_or("draft leg does not exist")?
            .stage
            .clone())
    }
    // How a torrent session rides the leg at `position`, as a saved
    // route's session would: its kind, why it carries nothing now when it
    // does not, and who resolves its names.
    pub fn torrent_leg(
        &self,
        position: usize,
    ) -> Result<
        (
            crate::torrents::net::LegKind,
            Option<crate::torrents::sessions::RouteFailure>,
            Option<ResolvedBy>,
        ),
        String,
    > {
        use crate::torrents::sessions::{leg_kind, leg_unavailable, names_on};
        let leg = &self
            .legs
            .get(position)
            .ok_or("draft leg does not exist")?
            .definition;
        Ok((
            leg_kind(&self.isolated, leg.egress_id, &leg.path),
            leg_unavailable(&self.isolated, leg.egress_id),
            names_on(&self.isolated, &leg.path),
        ))
    }
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
    // The dialer of a connection test of `server` over the leg at
    // `position`. Its dials are probes of the server, never connections of
    // its pool.
    pub fn nntp_dialer_for_leg(
        &self,
        position: usize,
        server: u32,
    ) -> Result<Arc<weaver_nntp::route_dialer::RouteDialer>, String> {
        let leg = self.legs.get(position).ok_or("draft leg does not exist")?;
        Ok(Arc::new(weaver_nntp::route_dialer::RouteDialer {
            inner: Arc::new(ServerProbe {
                inner: leg.stage.clone(),
                leg: position,
            }),
            plan: leg.address_plan.as_ref().map(|plan| plan.plan.clone()),
            egress_controls: self.isolated.egress_controls.clone(),
            runtime: self.handle.clone(),
            server,
        }))
    }
    pub fn bridge_for_leg(&self, position: usize) -> Result<Arc<Bridge>, String> {
        let mut bridges = self.bridges.lock().expect("draft bridges");
        if let Some(bridge) = bridges.get(&position) {
            return Ok(bridge.clone());
        }
        let stage = self
            .legs
            .get(position)
            .ok_or("draft leg does not exist")?
            .stage
            .clone();
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
        let created = Bridge::start(
            &self.handle,
            Arc::new(ProbeProvider(stage.clone())),
            stage.budget(),
            ("weaver".into(), hex::encode(secret)),
        )
        .map_err(|e| e.to_string())?;
        bridges.insert(position, created.clone());
        Ok(created)
    }
    pub async fn revoke(&self) {
        for leg in &self.legs {
            leg.stage.revoke();
        }
        let bridges = std::mem::take(&mut *self.bridges.lock().expect("draft bridges"));
        for bridge in bridges.into_values() {
            bridge.revoke().await;
        }
        self.isolated.shutdown().await;
    }
}
impl Drop for DraftNetworkRoute {
    fn drop(&mut self) {
        for leg in &self.legs {
            leg.stage.revoke();
        }
        let bridges = std::mem::take(self.bridges.get_mut().expect("draft bridges"));
        let isolated = self.isolated.clone();
        self.handle.spawn(async move {
            for bridge in bridges.into_values() {
                bridge.revoke().await;
            }
            isolated.shutdown().await;
        });
    }
}
// A connection test of a usenet server over one drafted leg. It dials as a
// probe of the server, which a WireGuard tunnel charges to the room it keeps
// for probes. The usenet share is sized to the connections of the servers
// already on, so a server being added, edited while off, or turned on has
// none of it yet; and the servers that are on may hold all of it.
struct ServerProbe {
    inner: Arc<dyn Dialer>,
    leg: usize,
}
impl ServerProbe {
    fn purpose(&self, purpose: &Purpose) -> Purpose {
        match purpose {
            Purpose::Nntp { server, .. } | Purpose::NntpProbe { server, .. } => {
                Purpose::NntpProbe {
                    server: *server,
                    leg: self.leg,
                }
            }
            other => other.clone(),
        }
    }
}
#[async_trait::async_trait]
impl Dialer for ServerProbe {
    async fn prepare(&self) -> Result<(), DialError> {
        self.inner.prepare().await
    }
    fn has_capacity(&self) -> bool {
        self.inner.has_capacity()
    }
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        self.inner.room(&self.purpose(purpose))
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.inner
            .dial(&Target {
                purpose: self.purpose(&target.purpose),
                ..target.clone()
            })
            .await
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.inner.resolve(host).await
    }
    fn offers(&self) -> Transports {
        self.inner.offers()
    }
    fn failing_hop(&self) -> Option<weaver_tunnel::pipe::FailingHop> {
        self.inner.failing_hop()
    }
    fn budget(&self) -> Duration {
        self.inner.budget()
    }
    fn describe(&self) -> String {
        format!("server test: {}", self.inner.describe())
    }
}
struct ProbeProvider(Arc<dyn Dialer>);
#[async_trait::async_trait]
impl TunnelProvider for ProbeProvider {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        outcome: Arc<ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let dialed = self
            .0
            .dial(&Target {
                host: host.into(),
                port,
                purpose: Purpose::Probe,
                addresses: Vec::new(),
            })
            .await
            .map_err(|e| match e {
                DialError::Fatal(e) => e,
                e => TunnelError::Engine(e.to_string()),
            })?;
        let upstream = dialed.outcome.clone();
        outcome.on_failure(move || upstream.failed());
        let upstream = dialed.outcome.clone();
        outcome.on_close(move || upstream.closed());
        Ok(dialed.into_observed_stream())
    }
    fn describe(&self) -> String {
        "network route probe".into()
    }
}
impl NetworkRuntime {
    pub(super) fn isolated_probe_runtime(&self) -> Arc<Self> {
        Arc::new(Self {
            db: self.db.clone(),
            handle: self.handle.clone(),
            configuration: RwLock::new(
                self.configuration
                    .read()
                    .expect("network configuration")
                    .clone(),
            ),
            sessions: Mutex::new(HashMap::new()),
            session_sockets: Arc::default(),
            borrowed_sessions: self.sessions.lock().expect("network sessions").clone(),
            pools: Arc::default(),
            routes: Mutex::new(HashMap::new()),
            interfaces: self.interfaces.clone(),
            compiling: false,
            pool_updates: Mutex::new(HashMap::new()),
            poll: Mutex::new(None),
            quota_watch: Mutex::new(None),
            cap_watch: Mutex::new(None),
            health_inputs: Mutex::new(None),
            torrent_legs: Mutex::default(),
            route_health: None,
            probing: true,
            reloads: tokio::sync::watch::Sender::new(0),
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::with_scope(
                weaver_nntp::transfer::TransferScope::Egress,
            )),
            quota_policy: None,
            #[cfg(test)]
            fixture_providers: Mutex::new(
                self.fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .clone(),
            ),
        })
    }
    pub fn validate_policy(&self, policy: &RoutingPolicy) -> Result<(), NetworkRefusal> {
        policy.validate()?;
        let config = self.configuration.read().expect("network configuration");
        if policy.legs.is_empty() && policy.proxy_ids.is_empty() && !policy.allow_direct {
            return Ok(());
        }
        policy
            .route()
            .validate_references(&config.egresses, &config.profiles, &config.pools)
    }
    pub fn draft_route(
        &self,
        consumer: Consumer,
        policy: RoutingPolicy,
        timeout: Duration,
    ) -> Result<Arc<DraftNetworkRoute>, String> {
        self.draft_route_without_members(consumer, policy, timeout, &[])
    }
    // A draft whose pools leave out `removed` proxies. Only the draft's own
    // copy of the configuration changes.
    pub fn draft_route_without_members(
        &self,
        consumer: Consumer,
        policy: RoutingPolicy,
        timeout: Duration,
        removed: &[u32],
    ) -> Result<Arc<DraftNetworkRoute>, String> {
        self.validate_policy(&policy)?;
        // Drafts share unchanged live carriers. Carried tunnels are sized
        // by the provider in the same way before and after saving the route.
        let isolated = self.isolated_probe_runtime();
        if !removed.is_empty() {
            let mut config = isolated
                .configuration
                .write()
                .expect("network configuration");
            for pool in config.pools.values_mut() {
                pool.member_ids.retain(|id| !removed.contains(id));
            }
        }
        let config = isolated
            .configuration
            .read()
            .expect("network configuration");
        let definition = policy.route();
        let legs = definition
            .legs
            .iter()
            .enumerate()
            .map(|(i, l)| isolated.leg(&config, consumer, i, l, timeout))
            .collect::<Result<Vec<_>, _>>()?;
        drop(config);
        Ok(Arc::new(DraftNetworkRoute {
            policy,
            legs,
            handle: self.handle.clone(),
            bridges: Mutex::new(HashMap::new()),
            isolated,
        }))
    }
}
