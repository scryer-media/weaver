use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
#[cfg(test)]
mod tests;
use super::*;
use crate::Database;
use weaver_tunnel::{
    Http3TunnelProvider, SshTunnelProvider, TunnelError, TunnelObserver, TunnelProvider,
    TunnelStream, WireGuardTunnelProvider,
    bridge::Bridge,
    transport::{TransportKind, TransportProxy},
};

fn bridge_credentials() -> Result<(String, String), String> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
    Ok(("weaver".into(), hex::encode(secret)))
}

struct Observer {
    db: Database,
    id: u32,
    revision: u64,
}
impl TunnelObserver for Observer {
    fn tunnel_dial_failed(&self, _: &str, _: &str) {}
    fn tunnel_dial_succeeded(&self, _: &str) {}
    fn host_key_pinned(&self, _: &str, _: &str) {}
    fn persisted_host_key(&self, _: &str) -> Result<Option<String>, TunnelError> {
        self.db
            .proxy_host_key(self.id, self.revision)
            .map_err(|_| TunnelError::Engine("SSH profile trust changed or is unavailable".into()))
    }
    fn persist_host_key(&self, _: &str, fingerprint: &str) -> Result<(), TunnelError> {
        self.db
            .pin_proxy_host_key(self.id, self.revision, fingerprint)
            .map_err(|_| TunnelError::Engine("could not persist SSH host-key trust".into()))
    }
}

pub struct ProxyHop {
    pub profile: ProxyProfile,
    pub provider: Arc<dyn TunnelProvider>,
    wireguard: Option<Arc<WireGuardTunnelProvider>>,
}
impl ProxyHop {
    fn new(profile: ProxyProfile, db: &Database) -> Result<Arc<Self>, String> {
        profile.validate().map_err(|e| e.to_string())?;
        let observer = Arc::new(Observer {
            db: db.clone(),
            id: profile.id,
            revision: profile.revision,
        });
        let mut wireguard = None;
        let provider: Arc<dyn TunnelProvider> = match profile.kind {
            ProxyKind::Http3Connect => Arc::new(
                Http3TunnelProvider::new(profile.http3_spec().map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?,
            ),
            ProxyKind::Ssh => Arc::new(SshTunnelProvider::new(profile.ssh_spec(), observer)),
            ProxyKind::WireGuard => {
                let wg = Arc::new(WireGuardTunnelProvider::new(
                    profile.wireguard_spec().map_err(|e| e.to_string())?,
                    observer,
                ));
                wireguard = Some(wg.clone());
                wg
            }
            kind @ (ProxyKind::HttpConnect | ProxyKind::Socks5) => Arc::new(TransportProxy {
                kind: if kind == ProxyKind::Socks5 {
                    TransportKind::Socks5
                } else {
                    TransportKind::HttpConnect
                },
                host: profile.host.clone(),
                port: profile.port,
                username: profile.secrets.username.clone(),
                password: profile.secrets.password.clone(),
            }),
        };
        Ok(Arc::new(Self {
            profile,
            provider,
            wireguard,
        }))
    }
    pub async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, TunnelError> {
        if let Some(wg) = &self.wireguard {
            wg.resolve_host(host).await
        } else {
            weaver_tunnel::dns::resolve(self.provider.as_ref(), &self.profile.dns_servers, host)
                .await
        }
    }
}

struct Cooldown {
    until: Instant,
    probing: bool,
}
pub struct ConsumerRoute {
    pub policy: RoutingPolicy,
    pub hops: Vec<Option<Arc<ProxyHop>>>,
    cooldowns: Mutex<HashMap<u32, Cooldown>>,
    status: Mutex<RoutingStatus>,
    revoked: AtomicBool,
    cancellation: tokio::sync::Notify,
    pub sockets: Arc<weaver_nntp::revocation::SocketRegistry>,
    bridges: Mutex<Vec<Arc<Bridge>>>,
    http_bridges: Mutex<HashMap<u32, Arc<Bridge>>>,
    handle: tokio::runtime::Handle,
    timeout: Duration,
}

impl ConsumerRoute {
    pub async fn cancelled(&self) {
        let notified = self.cancellation.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_revoked() {
            notified.await;
        }
    }
    pub fn status(&self) -> RoutingStatus {
        if self.policy.proxy_ids.is_empty() && !self.policy.allow_direct {
            return RoutingStatus {
                state: RouteState::Blocked,
                ..Default::default()
            };
        }
        self.status.lock().expect("route status").clone()
    }
    pub fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }
    pub fn begin(self: &Arc<Self>, id: u32) -> Option<RouteAttempt> {
        if self.is_revoked() {
            return None;
        }
        let mut cooldowns = self.cooldowns.lock().expect("route cooldown");
        if let Some(entry) = cooldowns.get_mut(&id) {
            if entry.until > Instant::now() || entry.probing {
                return None;
            }
            entry.probing = true;
        }
        Some(RouteAttempt {
            route: self.clone(),
            id,
            finished: false,
        })
    }
    pub fn fail(&self, id: u32, message: &str) {
        self.cooldowns.lock().expect("route cooldown").insert(
            id,
            Cooldown {
                until: Instant::now() + Duration::from_secs(30),
                probing: false,
            },
        );
        let mut status = self.status.lock().expect("route status");
        status.failures.retain(|(prior, _)| *prior != id);
        status
            .failures
            .push((id, message.chars().take(256).collect()));
        status.failures.truncate(8);
    }
    pub fn success(&self, id: Option<u32>) {
        if let Some(id) = id {
            self.cooldowns.lock().expect("route cooldown").remove(&id);
        }
        let mut status = self.status.lock().expect("route status");
        status.state = if id.is_some() {
            RouteState::Proxy
        } else {
            RouteState::Direct
        };
        status.selected_proxy_id = id;
        if let Some(id) = id {
            status.failures.retain(|(prior, _)| *prior != id);
        }
    }
    pub fn blocked(&self) {
        let mut s = self.status.lock().expect("route status");
        s.state = RouteState::Blocked;
        s.selected_proxy_id = None;
    }
    pub fn bridge(self: &Arc<Self>) -> Result<Arc<Bridge>, String> {
        let mut bridges = self.bridges.lock().expect("route bridges");
        if self.is_revoked() {
            return Err("proxy route was revoked".into());
        }
        if let Some(bridge) = bridges.first() {
            return Ok(bridge.clone());
        }
        let provider: Arc<dyn TunnelProvider> = Arc::new(BridgeLadder(Arc::downgrade(self)));
        let bridge = Bridge::start(
            &self.handle,
            provider,
            self.timeout * (self.hops.len() as u32 + 1),
            bridge_credentials()?,
        )
        .map_err(|e| e.to_string())?;
        bridges.push(bridge.clone());
        Ok(bridge)
    }
    pub fn hop_bridge(&self, hop: &Arc<ProxyHop>) -> Result<Arc<Bridge>, String> {
        self.http_bridge(hop.profile.id, hop.provider.clone())
    }
    pub fn direct_bridge(&self) -> Result<Arc<Bridge>, String> {
        if !self.policy.allow_direct {
            return Err("direct access is blocked".into());
        }
        self.http_bridge(0, Arc::new(DirectProvider))
    }
    fn http_bridge(
        &self,
        id: u32,
        provider: Arc<dyn TunnelProvider>,
    ) -> Result<Arc<Bridge>, String> {
        let mut bridges = self.http_bridges.lock().expect("HTTP route bridges");
        if self.is_revoked() {
            return Err("proxy route was revoked".into());
        }
        if let Some(bridge) = bridges.get(&id) {
            return Ok(bridge.clone());
        }
        if bridges.len() >= 9 {
            return Err("too many HTTP route bridges".into());
        }
        let bridge = Bridge::start(&self.handle, provider, self.timeout, bridge_credentials()?)
            .map_err(|e| e.to_string())?;
        bridges.insert(id, bridge.clone());
        Ok(bridge)
    }
    pub async fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        self.cancellation.notify_waiters();
        self.sockets.revoke();
        let bridges = std::mem::take(&mut *self.bridges.lock().expect("route bridges"));
        for bridge in bridges {
            bridge.revoke().await;
        }
        let bridges = std::mem::take(&mut *self.http_bridges.lock().expect("HTTP route bridges"));
        for (_, bridge) in bridges {
            bridge.revoke().await;
        }
    }
}

pub struct RouteAttempt {
    route: Arc<ConsumerRoute>,
    id: u32,
    finished: bool,
}
impl RouteAttempt {
    pub fn success(mut self) {
        self.route.success(Some(self.id));
        self.finished = true;
    }
    pub fn fail(mut self, message: &str) {
        self.route.fail(self.id, message);
        self.finished = true;
    }
}
impl Drop for RouteAttempt {
    fn drop(&mut self) {
        if !self.finished
            && let Some(entry) = self
                .route
                .cooldowns
                .lock()
                .expect("route cooldown")
                .get_mut(&self.id)
        {
            entry.probing = false;
        }
    }
}

struct Ladder(Arc<ConsumerRoute>);
// The bridge task must not retain its owner through a route → bridge → task cycle.
struct BridgeLadder(std::sync::Weak<ConsumerRoute>);
#[async_trait::async_trait]
impl TunnelProvider for BridgeLadder {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        outcome: Arc<weaver_tunnel::bridge::ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let route = self
            .0
            .upgrade()
            .ok_or_else(|| TunnelError::Engine("proxy route was closed".into()))?;
        let ladder = Ladder(route.clone());
        tokio::select! {
            biased;
            _ = route.cancelled() => Err(TunnelError::Engine("proxy route was revoked".into())),
            result = ladder.dial_observed(host, port, outcome) => result,
        }
    }
    fn describe(&self) -> String {
        "consumer routing ladder".into()
    }
}
struct DirectProvider;
#[async_trait::async_trait]
impl TunnelProvider for DirectProvider {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        tokio::net::TcpStream::connect((host, port))
            .await
            .map(|s| Box::new(s) as Box<dyn TunnelStream>)
            .map_err(|_| TunnelError::Engine("direct connection failed".into()))
    }
    fn describe(&self) -> String {
        "direct host access".into()
    }
}
#[async_trait::async_trait]
impl TunnelProvider for Ladder {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        outcome: Arc<weaver_tunnel::bridge::ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let route = &self.0;
        for (id, hop) in route.policy.proxy_ids.iter().zip(&route.hops) {
            let Some(attempt) = route.begin(*id) else {
                continue;
            };
            let Some(hop) = hop.as_ref().filter(|h| h.profile.enabled) else {
                attempt.fail("proxy is disabled or unavailable");
                continue;
            };
            match tokio::time::timeout(route.timeout, hop.provider.dial(host, port)).await {
                Ok(Ok(stream)) => {
                    attempt.success();
                    let route = Arc::downgrade(route);
                    let id = *id;
                    outcome.on_failure(move || {
                        if let Some(route) = route.upgrade().filter(|r| !r.is_revoked()) {
                            route.fail(id, "NNTP response transport failed");
                        }
                    });
                    return Ok(stream);
                }
                Ok(Err(TunnelError::HostKeyMismatch { .. })) => {
                    attempt.fail("SSH host key changed; verify and reset trust");
                    route.blocked();
                    return Err(TunnelError::Engine(
                        "SSH host key verification failed".into(),
                    ));
                }
                _ => attempt.fail("proxy connection failed"),
            }
        }
        if route.policy.allow_direct
            && !route.is_revoked()
            && let Ok(Ok(stream)) =
                tokio::time::timeout(route.timeout, tokio::net::TcpStream::connect((host, port)))
                    .await
        {
            route.success(None);
            return Ok(Box::new(stream));
        }
        route.blocked();
        Err(TunnelError::Engine(
            "routing ladder exhausted; no permitted connection succeeded".into(),
        ))
    }
    fn describe(&self) -> String {
        "consumer routing ladder".into()
    }
}

pub struct ProxyRuntime {
    db: Database,
    handle: tokio::runtime::Handle,
    profiles: RwLock<HashMap<u32, Arc<ProxyHop>>>,
    policies: RwLock<HashMap<String, RoutingPolicy>>,
    consumers: RwLock<std::collections::HashSet<String>>,
    routes: Mutex<HashMap<String, Arc<ConsumerRoute>>>,
    pub mutations: tokio::sync::Mutex<()>,
    updating: AtomicBool,
    stopped: AtomicBool,
}
impl ProxyRuntime {
    #[cfg(test)]
    pub(crate) fn install_http3_fixture(&self, id: u32, provider: Arc<dyn TunnelProvider>) {
        assert!(self.routes.lock().unwrap().is_empty());
        let mut profiles = self.profiles.write().unwrap();
        let profile = profiles.get(&id).unwrap().profile.clone();
        assert_eq!(profile.kind, ProxyKind::Http3Connect);
        profiles.insert(
            id,
            Arc::new(ProxyHop {
                profile,
                provider,
                wireguard: None,
            }),
        );
    }

    pub fn nntp_sockets(
        &self,
        id: u32,
    ) -> Result<Arc<weaver_nntp::revocation::SocketRegistry>, String> {
        Ok(self
            .route(Consumer::Server(id), Duration::from_secs(30))?
            .sockets
            .clone())
    }
    pub fn nntp_bridge(&self, id: u32) -> Result<Option<Arc<Bridge>>, String> {
        let route = self.route(Consumer::Server(id), Duration::from_secs(30))?;
        if route.policy.is_direct() {
            Ok(None)
        } else {
            route.bridge().map(Some)
        }
    }
    pub fn new(db: Database, handle: tokio::runtime::Handle) -> Result<Arc<Self>, String> {
        let profiles = db
            .list_proxy_profiles()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|p| Ok((p.id, ProxyHop::new(p, &db)?)))
            .collect::<Result<_, String>>()?;
        let policies = db
            .list_proxy_routes()
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect();
        let consumers = Self::load_consumers(&db).map_err(|e| e.to_string())?;
        Ok(Arc::new(Self {
            db,
            handle,
            profiles: RwLock::new(profiles),
            policies: RwLock::new(policies),
            consumers: RwLock::new(consumers),
            routes: Mutex::new(HashMap::new()),
            mutations: tokio::sync::Mutex::new(()),
            updating: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }))
    }
    fn load_consumers(
        db: &Database,
    ) -> Result<std::collections::HashSet<String>, crate::StateError> {
        Ok(db
            .list_servers()?
            .into_iter()
            .map(|s| Consumer::Server(s.id).key())
            .chain(
                db.list_rss_feeds()?
                    .into_iter()
                    .map(|f| Consumer::Rss(f.id).key()),
            )
            .collect())
    }
    pub fn policy(&self, consumer: Consumer) -> RoutingPolicy {
        self.policies
            .read()
            .expect("proxy policies")
            .get(&consumer.key())
            .cloned()
            .unwrap_or_default()
    }
    pub fn validate_policy(
        &self,
        consumer: Consumer,
        policy: &RoutingPolicy,
    ) -> Result<(), String> {
        policy.validate()?;
        let profiles = self.profiles.read().expect("proxy profiles");
        for id in &policy.proxy_ids {
            let hop = profiles
                .get(id)
                .ok_or("routing policy references a missing proxy")?;
            if matches!(consumer, Consumer::Rss(_)) && hop.profile.dns_servers.is_empty() {
                return Err("RSS proxy routes require configured DNS servers".into());
            }
        }
        Ok(())
    }
    pub fn route(
        &self,
        consumer: Consumer,
        timeout: Duration,
    ) -> Result<Arc<ConsumerRoute>, String> {
        let key = consumer.key();
        let mut routes = self.routes.lock().expect("proxy routes");
        if self.updating.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
            return Err("proxy runtime is changing or stopped".into());
        }
        if !self
            .consumers
            .read()
            .expect("proxy consumers")
            .contains(&key)
        {
            return Err("routing consumer no longer exists".into());
        }
        if let Some(route) = routes.get(&key) {
            return Ok(route.clone());
        }
        let route = self.draft_route(self.policy(consumer), timeout)?;
        if routes.len() >= 4096 {
            return Err("too many active consumer routes".into());
        }
        routes.insert(key, route.clone());
        Ok(route)
    }
    pub fn draft_route(
        &self,
        policy: RoutingPolicy,
        timeout: Duration,
    ) -> Result<Arc<ConsumerRoute>, String> {
        policy.validate()?;
        let profiles = self.profiles.read().expect("proxy profiles");
        let hops = policy
            .proxy_ids
            .iter()
            .map(|id| profiles.get(id).cloned())
            .collect();
        Ok(Arc::new(ConsumerRoute {
            policy,
            hops,
            cooldowns: Mutex::new(HashMap::new()),
            status: Mutex::new(RoutingStatus::default()),
            revoked: AtomicBool::new(false),
            cancellation: tokio::sync::Notify::new(),
            sockets: Arc::new(weaver_nntp::revocation::SocketRegistry::default()),
            bridges: Mutex::new(Vec::new()),
            http_bridges: Mutex::new(HashMap::new()),
            handle: self.handle.clone(),
            timeout,
        }))
    }
    pub async fn reload(&self) -> Result<(), String> {
        if self.updating.swap(true, Ordering::AcqRel) {
            return Err("proxy runtime reload already in progress".into());
        }
        struct Reset<'a>(&'a AtomicBool);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _reset = Reset(&self.updating);
        let db = self.db.clone();
        let (profiles, policies, consumers) = tokio::task::spawn_blocking(move || {
            Ok::<_, crate::StateError>((
                db.list_proxy_profiles()?,
                db.list_proxy_routes()?,
                Self::load_consumers(&db)?,
            ))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        let profiles = {
            let previous = self.profiles.read().expect("proxy profiles");
            profiles
                .into_iter()
                .map(|p| {
                    let hop = match previous
                        .get(&p.id)
                        .filter(|old| old.profile.revision == p.revision)
                    {
                        Some(old) => old.clone(),
                        None => ProxyHop::new(p, &self.db)?,
                    };
                    Ok((hop.profile.id, hop))
                })
                .collect::<Result<HashMap<_, _>, String>>()?
        };
        let policies: HashMap<_, _> = policies.into_iter().collect();
        let stale_profiles: Vec<_> = self
            .profiles
            .read()
            .expect("proxy profiles")
            .iter()
            .filter(|(id, old)| profiles.get(id).is_none_or(|new| !Arc::ptr_eq(old, new)))
            .map(|(_, hop)| hop.clone())
            .collect();
        let obsolete =
            {
                let mut routes = self.routes.lock().expect("proxy routes");
                let keys: Vec<_> =
                    routes
                        .iter()
                        .filter(|(key, route)| {
                            !consumers.contains(*key)
                                || route.policy != policies.get(*key).cloned().unwrap_or_default()
                                || route.policy.proxy_ids.iter().zip(&route.hops).any(
                                    |(id, prior)| match (prior, profiles.get(id)) {
                                        (Some(a), Some(b)) => !Arc::ptr_eq(a, b),
                                        (None, None) => false,
                                        _ => true,
                                    },
                                )
                        })
                        .map(|(key, _)| key.clone())
                        .collect();
                let obsolete: Vec<_> = keys
                    .into_iter()
                    .filter_map(|key| routes.remove(&key))
                    .collect();
                for route in &obsolete {
                    route.revoked.store(true, Ordering::Release);
                    route.sockets.revoke();
                }
                *self.profiles.write().expect("proxy profiles") = profiles;
                *self.policies.write().expect("proxy policies") = policies;
                *self.consumers.write().expect("proxy consumers") = consumers;
                obsolete
            };
        for route in obsolete {
            route.revoke().await;
        }
        for hop in stale_profiles {
            hop.provider.shutdown().await;
        }
        Ok(())
    }
    pub async fn stop_all(&self) {
        self.stopped.store(true, Ordering::Release);
        let routes = std::mem::take(&mut *self.routes.lock().expect("proxy routes"));
        for (_, route) in routes {
            route.revoke().await;
        }
        let profiles: Vec<_> = self
            .profiles
            .read()
            .expect("proxy profiles")
            .values()
            .cloned()
            .collect();
        for hop in profiles {
            hop.provider.shutdown().await;
        }
    }
    pub async fn test_profile(&self, profile: &ProxyProfile) -> Result<String, String> {
        profile.validate().map_err(|e| e.to_string())?;
        if profile.kind == ProxyKind::Http3Connect {
            let dns = profile.dns_servers.first().ok_or(
                "Configure a routed DNS server IP to test HTTP/3 forwarding, or test an assigned NNTP server",
            )?;
            let hop = ProxyHop::new(profile.clone(), &self.db)?;
            let result = tokio::time::timeout(profile.timeout(), async {
                let _stream = hop.provider.dial(&dns.to_string(), 53).await?;
                Ok::<_, TunnelError>(())
            })
            .await;
            hop.provider.shutdown().await;
            return match result {
                Ok(Ok(())) => Ok("HTTP/3 proxy TLS, authentication and forwarding succeeded".into()),
                _ => Err("HTTP/3 proxy connection failed; check UDP reachability, TLS certificate, credentials and routed DNS".into()),
            };
        }
        let observer = Arc::new(Observer {
            db: self.db.clone(),
            id: profile.id,
            revision: profile.revision,
        });
        let result = tokio::time::timeout(profile.timeout(), async {
            match profile.kind {
                ProxyKind::Ssh => { SshTunnelProvider::handshake(profile.ssh_spec(), observer).await?; }
                ProxyKind::WireGuard => { WireGuardTunnelProvider::handshake(profile.wireguard_spec()?, observer).await?; }
                _ => {
                    let hop = ProxyHop::new(profile.clone(), &self.db).map_err(TunnelError::Configuration)?;
                    if let Some(dns) = profile.dns_servers.first() { let _stream = hop.provider.dial(&dns.to_string(), 53).await?; }
                    else { tokio::net::TcpStream::connect((profile.host.as_str(), profile.port)).await.map_err(|_| TunnelError::Engine("proxy endpoint unreachable".into()))?; return Ok("Endpoint reachable; configure routed DNS or test an assigned consumer to verify forwarding".to_string()); }
                }
            }
            Ok::<_, TunnelError>("Proxy connection succeeded".to_string())
        }).await;
        match result {
            Ok(Ok(message)) => Ok(message),
            Ok(Err(TunnelError::HostKeyMismatch { .. })) => {
                Err("SSH host key changed; verify the server before resetting trust".into())
            }
            _ => Err("Proxy connection failed; check endpoint and credentials".into()),
        }
    }
}
