// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::HashMap,
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
use weaver_tunnel::{TunnelError, TunnelObserver};

pub(super) struct Observer {
    pub(super) db: Database,
    pub(super) id: u32,
    pub(super) revision: u64,
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

struct Cooldown {
    until: Instant,
    probing: bool,
}
// Whether a policy change touches what a legacy route is built from. Its
// legs and failover live in the network runtime, which applies them to the
// live route in place: a reweight moves connections between legs, a changed
// path revokes that leg alone. Rebuilding the legacy route for them would
// revoke every socket it tracks for a change that was meant to keep them.
fn legacy_policy_changed(current: &RoutingPolicy, next: &RoutingPolicy) -> bool {
    current.proxy_ids != next.proxy_ids || current.allow_direct != next.allow_direct
}

pub struct ConsumerRoute {
    pub policy: RoutingPolicy,
    cooldowns: Mutex<HashMap<u32, Cooldown>>,
    status: Mutex<RoutingStatus>,
    revoked: AtomicBool,
    cancellation: tokio::sync::Notify,
    pub sockets: Arc<weaver_nntp::revocation::SocketRegistry>,
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
    pub fn fail(&self, id: u32, fault: ProxyFault) {
        self.cooldowns.lock().expect("route cooldown").insert(
            id,
            Cooldown {
                until: Instant::now() + weaver_nntp::plan_timing::timing().route_cooldown,
                probing: false,
            },
        );
        let mut status = self.status.lock().expect("route status");
        status.failures.retain(|(prior, _)| *prior != id);
        status.failures.push((id, fault));
        status.failures.truncate(8);
    }
    // Records a failed dial through proxy `id`, by what it came to in this
    // route's time.
    pub fn fail_dial(&self, id: u32, error: &weaver_tunnel::pipe::DialError) {
        self.fail(id, error.fault(self.timeout));
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
    pub async fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        self.cancellation.notify_waiters();
        self.sockets.revoke();
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
    pub fn fail(mut self, fault: ProxyFault) {
        self.route.fail(self.id, fault);
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

pub struct ProxyRuntime {
    pub network: Arc<NetworkRuntime>,
    db: Database,
    policies: RwLock<HashMap<String, RoutingPolicy>>,
    consumers: RwLock<std::collections::HashSet<Consumer>>,
    routes: Mutex<HashMap<Consumer, Arc<ConsumerRoute>>>,
    pub mutations: tokio::sync::Mutex<()>,
    updating: AtomicBool,
    // A feed was turned away while the routes were changing; the reload's
    // end wakes the feed poller for it. Set and taken under `routes`.
    feeds_passed_over: AtomicBool,
    stopped: AtomicBool,
    // Whether feeds and Default outbound may reach private addresses: the
    // one switch, `WEAVER_RSS_ALLOW_PRIVATE_NETWORK`.
    allow_private_network: AtomicBool,
}
impl ProxyRuntime {
    pub fn nntp_sockets(
        &self,
        id: u32,
    ) -> Result<Arc<weaver_nntp::revocation::SocketRegistry>, String> {
        Ok(self
            .route(
                Consumer::Server(id),
                crate::servers::connectivity::SERVER_CONNECT_TIMEOUT,
            )?
            .sockets
            .clone())
    }
    pub fn new(db: Database, handle: tokio::runtime::Handle) -> Result<Arc<Self>, String> {
        Self::with_quota_policy(db, handle, None)
    }
    // A runtime whose egresses are metered by the long-lived download
    // policies, so every egress quota is enforced.
    pub fn with_quota_policy(
        db: Database,
        handle: tokio::runtime::Handle,
        quota_policy: Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>>,
    ) -> Result<Arc<Self>, String> {
        let policies = db
            .list_proxy_routes()
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect();
        let consumers = Self::load_consumers(&db).map_err(|e| e.to_string())?;
        Ok(Arc::new(Self {
            network: NetworkRuntime::with_quota_policy(db.clone(), handle, quota_policy)?,
            db,
            policies: RwLock::new(policies),
            consumers: RwLock::new(consumers),
            routes: Mutex::new(HashMap::new()),
            mutations: tokio::sync::Mutex::new(()),
            updating: AtomicBool::new(false),
            feeds_passed_over: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            allow_private_network: AtomicBool::new(true),
        }))
    }
    pub(super) fn load_consumers(
        db: &Database,
    ) -> Result<std::collections::HashSet<Consumer>, crate::StateError> {
        Ok(db
            .list_servers()?
            .into_iter()
            .map(|s| Consumer::Server(s.id))
            .chain(
                db.list_rss_feeds()?
                    .into_iter()
                    .map(|f| Consumer::Rss(f.id)),
            )
            .chain(std::iter::once(Consumer::Outbound))
            .collect())
    }
    // Whether the runtime knows this consumer, so a route for it can be
    // drafted and governed. A feed row that exists before the next reload
    // is not yet a consumer, and polling it would bypass its kill switch.
    pub fn routes_consumer(&self, consumer: Consumer) -> bool {
        self.consumers
            .read()
            .expect("proxy consumers")
            .contains(&consumer)
    }
    // Whether a feed can be polled now: it is a consumer and the routes are
    // not mid-reload. A feed turned away by a reload is polled when the
    // reload ends, through the wake it sends.
    pub fn feed_routable(&self, consumer: Consumer) -> bool {
        let _routes = self.routes.lock().expect("proxy routes");
        if self.changing(consumer) {
            return false;
        }
        self.routes_consumer(consumer)
    }
    // Whether the routes are mid-reload, noting a feed turned away for it.
    // Called with `routes` held, so the reload's end cannot slip between.
    fn changing(&self, consumer: Consumer) -> bool {
        let changing = self.updating.load(Ordering::Acquire);
        if changing && matches!(consumer, Consumer::Rss(_)) {
            self.feeds_passed_over.store(true, Ordering::Release);
        }
        changing
    }
    pub fn set_allow_private_network(&self, allow: bool) {
        self.allow_private_network.store(allow, Ordering::Release);
    }
    pub fn allow_private_network(&self) -> bool {
        self.allow_private_network.load(Ordering::Acquire)
    }
    pub fn policy(&self, consumer: Consumer) -> RoutingPolicy {
        self.policies
            .read()
            .expect("proxy policies")
            .get(&consumer.key())
            .cloned()
            .unwrap_or_default()
    }
    pub fn route(
        &self,
        consumer: Consumer,
        timeout: Duration,
    ) -> Result<Arc<ConsumerRoute>, String> {
        let mut routes = self.routes.lock().expect("proxy routes");
        if self.stopped.load(Ordering::Acquire) {
            return Err("proxy runtime is stopped".into());
        }
        if self.changing(consumer) {
            return Err(ROUTES_CHANGING.into());
        }
        if !self
            .consumers
            .read()
            .expect("proxy consumers")
            .contains(&consumer)
        {
            return Err("routing consumer no longer exists".into());
        }
        if let Some(route) = routes.get(&consumer) {
            return Ok(route.clone());
        }
        let route = self.draft_route(self.policy(consumer), timeout)?;
        if routes.len() >= 4096 {
            return Err("too many active consumer routes".into());
        }
        routes.insert(consumer, route.clone());
        Ok(route)
    }
    pub fn draft_route(
        &self,
        policy: RoutingPolicy,
        timeout: Duration,
    ) -> Result<Arc<ConsumerRoute>, String> {
        policy.validate()?;
        Ok(Arc::new(ConsumerRoute {
            policy,
            cooldowns: Mutex::new(HashMap::new()),
            status: Mutex::new(RoutingStatus::default()),
            revoked: AtomicBool::new(false),
            cancellation: tokio::sync::Notify::new(),
            sockets: Arc::new(weaver_nntp::revocation::SocketRegistry::default()),
            timeout,
        }))
    }
    // Marks the routes as changing until the returned guard drops.
    pub(crate) fn begin_reload(&self) -> Result<Reloading<'_>, String> {
        if self.updating.swap(true, Ordering::AcqRel) {
            return Err("proxy runtime reload already in progress".into());
        }
        Ok(Reloading {
            runtime: self,
            new_feeds: false,
        })
    }
    pub async fn reload(&self) -> Result<(), String> {
        let mut reloading = self.begin_reload()?;
        let db = self.db.clone();
        let (policies, consumers) = tokio::task::spawn_blocking(move || {
            Ok::<_, crate::StateError>((db.list_proxy_routes()?, Self::load_consumers(&db)?))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        let policies: HashMap<_, _> = policies.into_iter().collect();
        let (obsolete, new_feeds) = {
            let mut routes = self.routes.lock().expect("proxy routes");
            let keys: Vec<_> = routes
                .iter()
                .filter(|(consumer, route)| {
                    !consumers.contains(*consumer)
                        || legacy_policy_changed(
                            &route.policy,
                            &policies.get(&consumer.key()).cloned().unwrap_or_default(),
                        )
                })
                .map(|(consumer, _)| *consumer)
                .collect();
            let obsolete: Vec<_> = keys
                .into_iter()
                .filter_map(|consumer| routes.remove(&consumer))
                .collect();
            for route in &obsolete {
                route.revoked.store(true, Ordering::Release);
                route.sockets.revoke();
            }
            *self.policies.write().expect("proxy policies") = policies;
            let previous = std::mem::replace(
                &mut *self.consumers.write().expect("proxy consumers"),
                consumers,
            );
            let new_feeds =
                self.consumers
                    .read()
                    .expect("proxy consumers")
                    .iter()
                    .any(|consumer| {
                        matches!(consumer, Consumer::Rss(_)) && !previous.contains(consumer)
                    });
            (obsolete, new_feeds)
        };
        for route in obsolete {
            route.revoke().await;
        }
        // The poller skipped these feeds while they had no route; once the
        // reload ends, poll them through it without waiting a tick.
        reloading.new_feeds = new_feeds;
        self.network.reload().await?;
        Ok(())
    }
    pub async fn stop_all(&self) {
        self.network.shutdown().await;
        self.stopped.store(true, Ordering::Release);
        let routes = std::mem::take(&mut *self.routes.lock().expect("proxy routes"));
        for (_, route) in routes {
            route.revoke().await;
        }
    }
}

// The refusal a consumer meets while the routes are mid-reload. It is a short
// internal state, not a failure of the consumer: a feed meeting it is left
// due and polled when the reload ends.
pub const ROUTES_CHANGING: &str = "proxy routes are changing";

// A reload in progress. Dropping it, on success or error, ends the reload and
// then wakes the feed poller if a feed was turned away meanwhile or new feeds
// appeared, so the wake never lands while the routes still refuse.
pub(crate) struct Reloading<'a> {
    runtime: &'a ProxyRuntime,
    new_feeds: bool,
}
impl Drop for Reloading<'_> {
    fn drop(&mut self) {
        let runtime = self.runtime;
        let passed_over = {
            let _routes = runtime
                .routes
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            runtime.updating.store(false, Ordering::Release);
            runtime.feeds_passed_over.swap(false, Ordering::AcqRel)
        };
        if passed_over || self.new_feeds {
            runtime.db.wake_rss_scheduler();
        }
    }
}
