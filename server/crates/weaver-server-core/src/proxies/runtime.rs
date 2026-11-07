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

// The reloads asked for and the last pass that answered them. One pass runs
// at a time; a reload asked for while one runs is answered by a pass that
// reads the rows after it was asked.
struct Reloads {
    asked: u64,
    answered: u64,
    running: bool,
    outcome: Result<(), String>,
}

pub struct ProxyRuntime {
    pub network: Arc<NetworkRuntime>,
    db: Database,
    policies: RwLock<HashMap<String, RoutingPolicy>>,
    consumers: RwLock<std::collections::HashSet<Consumer>>,
    routes: Mutex<HashMap<Consumer, Arc<ConsumerRoute>>>,
    pub mutations: tokio::sync::Mutex<()>,
    updating: AtomicBool,
    reloads: Mutex<Reloads>,
    // Sent whenever a reload's pass ends, for the reloads waiting on it.
    reloaded: tokio::sync::watch::Sender<()>,
    // Under test, the next reload's pass, once it has read the rows, says so
    // on the first and waits on the second.
    #[cfg(test)]
    pub(crate) pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
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
        let runtime = Arc::new(Self {
            network: NetworkRuntime::with_quota_policy(db.clone(), handle.clone(), quota_policy)?,
            db,
            policies: RwLock::new(policies),
            consumers: RwLock::new(consumers),
            routes: Mutex::new(HashMap::new()),
            mutations: tokio::sync::Mutex::new(()),
            updating: AtomicBool::new(false),
            reloads: Mutex::new(Reloads {
                asked: 0,
                answered: 0,
                running: false,
                outcome: Ok(()),
            }),
            reloaded: tokio::sync::watch::Sender::new(()),
            #[cfg(test)]
            pause: Mutex::new(None),
            stopped: AtomicBool::new(false),
            allow_private_network: AtomicBool::new(true),
        });
        let task = handle.spawn(Self::follow_socket_cap(
            Arc::downgrade(&runtime),
            super::follow_wireguard_socket_cap(),
        ));
        runtime.network.hold_cap_watch(task);
        Ok(runtime)
    }
    // Reload whenever the socket cap moves, so the tunnels are rebuilt to
    // it and every tunnel's socket shares are budgeted against it. The
    // reload joins any a save asked for, as every other reload does.
    async fn follow_socket_cap(
        runtime: std::sync::Weak<Self>,
        mut cap: tokio::sync::watch::Receiver<usize>,
    ) {
        while cap.changed().await.is_ok() {
            let Some(runtime) = runtime.upgrade() else {
                break;
            };
            if runtime.network.socket_cap() == *cap.borrow_and_update() {
                continue;
            }
            if let Err(error) = runtime.reload().await {
                tracing::warn!(
                    error,
                    "rebuilding the WireGuard tunnels for a new socket cap failed"
                );
            }
        }
    }
    // Everyone who owns a route, each with its name and how many connections
    // it holds: every server and feed, default outbound, the default torrent
    // route, and each category and tracker rule that routes its torrents
    // itself. A category or rule without its own route is no consumer.
    pub(super) fn consumer_labels(
        db: &Database,
    ) -> Result<Vec<(Consumer, String, u16)>, crate::StateError> {
        Ok(db
            .list_servers()?
            .into_iter()
            .map(|server| (Consumer::Server(server.id), server.host, server.connections))
            .chain(
                db.list_rss_feeds()?
                    .into_iter()
                    .map(|feed| (Consumer::Rss(feed.id), feed.name, 1)),
            )
            .chain(std::iter::once((
                Consumer::Outbound,
                "Default outbound".to_owned(),
                1,
            )))
            .chain(std::iter::once((
                Consumer::Torrent(TorrentScope::Default),
                "Default torrent".to_owned(),
                1,
            )))
            .chain(
                db.list_categories()?
                    .into_iter()
                    .filter(|category| category.torrent_route)
                    .map(|category| {
                        (
                            Consumer::Torrent(TorrentScope::Category(category.id)),
                            category.name,
                            1,
                        )
                    }),
            )
            .chain(
                db.list_tracker_rules()?
                    .into_iter()
                    .filter(|rule| rule.torrent_route)
                    .map(|rule| {
                        (
                            Consumer::Torrent(TorrentScope::TrackerRule(rule.id)),
                            rule.host_pattern,
                            1,
                        )
                    }),
            )
            .collect())
    }
    pub(super) fn load_consumers(
        db: &Database,
    ) -> Result<std::collections::HashSet<Consumer>, crate::StateError> {
        Ok(Self::consumer_labels(db)?
            .into_iter()
            .map(|(consumer, _, _)| consumer)
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
        if self.updating.load(Ordering::Acquire) || self.stopped.load(Ordering::Acquire) {
            return Err("proxy runtime is changing or stopped".into());
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
    // Reads the saved routes and consumers again and applies them, returning
    // once a pass that read them after this call began has applied. Asked
    // while another reload runs, that one reads the rows again before it
    // ends; if it is dropped first, this one runs the pass itself.
    pub async fn reload(&self) -> Result<(), String> {
        let mut ended = self.reloaded.subscribe();
        let asked = {
            let mut reloads = self.reloads.lock().expect("proxy reloads");
            reloads.asked += 1;
            reloads.asked
        };
        loop {
            {
                let mut reloads = self.reloads.lock().expect("proxy reloads");
                if reloads.answered >= asked {
                    return reloads.outcome.clone();
                }
                if !reloads.running {
                    reloads.running = true;
                    break;
                }
            }
            // The sender lives as long as `self`.
            let _ = ended.changed().await;
        }
        self.run_reloads().await
    }

    // Passes until none is asked for that the last one did not cover.
    async fn run_reloads(&self) -> Result<(), String> {
        struct Running<'a>(&'a ProxyRuntime);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.reloads.lock().expect("proxy reloads").running = false;
                self.0.updating.store(false, Ordering::Release);
                self.0.reloaded.send_replace(());
            }
        }
        let _running = Running(self);
        self.updating.store(true, Ordering::Release);
        loop {
            let covered = self.reloads.lock().expect("proxy reloads").asked;
            let outcome = self.reload_pass().await;
            let again = {
                let mut reloads = self.reloads.lock().expect("proxy reloads");
                reloads.answered = covered;
                reloads.outcome = outcome.clone();
                reloads.asked > covered
            };
            if !again {
                return outcome;
            }
            self.reloaded.send_replace(());
        }
    }

    async fn reload_pass(&self) -> Result<(), String> {
        let db = self.db.clone();
        let (policies, consumers) = tokio::task::spawn_blocking(move || {
            Ok::<_, crate::StateError>((db.list_proxy_routes()?, Self::load_consumers(&db)?))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        #[cfg(test)]
        {
            let pause = self.pause.lock().expect("proxy reload pause").take();
            if let Some((read, release)) = pause {
                let _ = read.send(());
                let _ = release.await;
            }
        }
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
        self.network.reload().await?;
        if new_feeds {
            // The poller skipped these feeds while they had no route; now
            // that one exists, poll them through it without waiting a tick.
            self.db.wake_rss_scheduler();
        }
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
