use super::*;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    pipe::Purpose,
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
            pools: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            interfaces: InterfaceMonitor::start(Arc::new(SystemInterfaceSource), &self.handle),
            poll: Mutex::new(None),
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
            #[cfg(test)]
            fixture_providers: Mutex::new(
                self.fixture_providers
                    .lock()
                    .expect("fixture providers")
                    .clone(),
            ),
        })
    }
    pub fn validate_policy(
        &self,
        consumer: Consumer,
        policy: &RoutingPolicy,
    ) -> Result<(), String> {
        policy.validate()?;
        let config = self.configuration.read().expect("network configuration");
        if policy.legs.is_empty() && policy.proxy_ids.is_empty() && !policy.allow_direct {
            return Ok(());
        }
        policy.route().validate_references(
            &config.egresses,
            &config.profiles,
            &config.pools,
            matches!(consumer, Consumer::Rss(_)),
        )
    }
    pub fn draft_route(
        &self,
        consumer: Consumer,
        policy: RoutingPolicy,
        timeout: Duration,
    ) -> Result<Arc<DraftNetworkRoute>, String> {
        self.validate_policy(consumer, &policy)?;
        let isolated = self.isolated_probe_runtime();
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
