// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::PoisonError;

use super::*;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    endpoint::DatagramTransport,
    pipe::{Dialed, Purpose, Resolution},
};

/// A torrent session's leg: the first leg of its consumer's live route.
/// The stage belongs to the route, which revokes it when the leg changes;
/// the bridge an HTTP client rides belongs to this leg, and stops with it.
pub struct TorrentLeg {
    pub stage: Arc<Revocable>,
    route: Arc<super::weighted::Weighted>,
    handle: tokio::runtime::Handle,
    bridge: Mutex<Option<Arc<Bridge>>>,
}
impl TorrentLeg {
    /// The leg's dialer, whose connections the route counts as its own.
    pub fn dialer(&self) -> Arc<dyn Dialer> {
        Arc::new(Carried {
            stage: self.stage.clone(),
            route: self.route.clone(),
        })
    }
    // The bridge onto the leg's stage, started the first time it is asked for.
    pub fn bridge(&self) -> Result<Arc<Bridge>, String> {
        // A panic while starting the bridge left it unstarted: start it.
        let mut bridge = self.bridge.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(bridge) = bridge.as_ref() {
            return Ok(bridge.clone());
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
        let started = Bridge::start(
            &self.handle,
            Arc::new(TorrentProvider(self.dialer())),
            self.stage.budget(),
            ("weaver".into(), hex::encode(secret)),
        )
        .map_err(|e| e.to_string())?;
        *bridge = Some(started.clone());
        Ok(started)
    }
}
impl Drop for TorrentLeg {
    fn drop(&mut self) {
        if let Some(bridge) = self
            .bridge
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            self.handle.spawn(async move { bridge.revoke().await });
        }
    }
}
// A session's connections on the first leg of its route. The session keeps
// its own peer limit, so they pass the route's allocation by, but the route
// counts them, and their bytes, as the leg's.
struct Carried {
    stage: Arc<Revocable>,
    route: Arc<super::weighted::Weighted>,
}
#[async_trait::async_trait]
impl Dialer for Carried {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let mut dialed = self.stage.dial(target).await?;
        self.route.carry(0, &mut dialed);
        Ok(dialed)
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.stage.resolve(host).await
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        Arc::clone(&self.stage).datagrams()
    }
    fn budget(&self) -> Duration {
        self.stage.budget()
    }
    fn describe(&self) -> String {
        self.stage.describe()
    }
}
struct TorrentProvider(Arc<dyn Dialer>);
#[async_trait::async_trait]
impl TunnelProvider for TorrentProvider {
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
                purpose: Purpose::Torrent,
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
        "torrent route".into()
    }
}
/// What torrent sessions hold on a leg they run on their own sockets, which
/// no route carries: their peer connections, and the bytes per second they
/// receive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionTraffic {
    pub open: u64,
    pub bytes_per_second: u64,
}

impl NetworkRuntime {
    /// Count bytes a torrent session moved over `egress_id` as every byte
    /// through an egress is counted: those received in its download total,
    /// those sent in its upload count.
    pub fn record_torrent_bytes(&self, egress_id: u32, received: u64, sent: u64) {
        if let Some(control) = self
            .egress_controls
            .get(weaver_nntp::transfer::StableServerId(egress_id))
        {
            control.record_received_bytes(received);
        }
        if sent > 0
            && let Some(policy) = &self.quota_policy
        {
            policy.record_egress_upload(egress_id, sent);
        }
    }

    /// What torrent sessions hold now on the legs they run on their own
    /// sockets, by consumer and leg position, in place of what they held
    /// before.
    pub fn hold_torrent_traffic(
        &self,
        legs: impl IntoIterator<Item = ((Consumer, usize), SessionTraffic)>,
    ) {
        let legs = legs
            .into_iter()
            .map(|((consumer, position), traffic)| ((consumer.key(), position), traffic))
            .collect();
        *self
            .torrent_legs
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = legs;
    }

    pub(crate) fn torrent_traffic(&self) -> HashMap<(String, usize), SessionTraffic> {
        self.torrent_legs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Bytes ever uploaded over `egress_id`, as its upload count holds them.
    pub fn egress_upload_bytes(&self, egress_id: u32) -> u64 {
        self.quota_policy
            .as_ref()
            .and_then(|policy| policy.egress_upload_bytes(egress_id))
            .unwrap_or(0)
    }

    /// The leg a torrent scope's session runs on: the scope's live route's
    /// leg at `position`, which allows it `peers` connections.
    pub fn torrent_leg(
        &self,
        scope: TorrentScope,
        position: usize,
        peers: u16,
        timeout: Duration,
    ) -> Result<Arc<TorrentLeg>, String> {
        let route = self.route(Consumer::Torrent(scope), peers, timeout)?;
        let stage = route
            .legs
            .read()
            .expect("network route legs")
            .get(position)
            .ok_or("the route has no such leg")?
            .stage
            .clone();
        Ok(Arc::new(TorrentLeg {
            stage,
            route: route.weighted.clone(),
            handle: self.handle.clone(),
            bridge: Mutex::new(None),
        }))
    }
}
