// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::PoisonError;

use super::*;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    pipe::Purpose,
};

/// A torrent session's leg: the first leg of its consumer's live route.
/// The stage belongs to the route, which revokes it when the leg changes;
/// the bridge an HTTP client rides belongs to this leg, and stops with it.
pub struct TorrentLeg {
    pub stage: Arc<Revocable>,
    handle: tokio::runtime::Handle,
    bridge: Mutex<Option<Arc<Bridge>>>,
}
impl TorrentLeg {
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
            Arc::new(TorrentProvider(self.stage.clone())),
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
struct TorrentProvider(Arc<Revocable>);
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
impl NetworkRuntime {
    /// The leg a torrent scope's session runs on: the first leg of the
    /// scope's live route.
    pub fn torrent_leg(
        &self,
        scope: TorrentScope,
        timeout: Duration,
    ) -> Result<Arc<TorrentLeg>, String> {
        let route = self.route(Consumer::Torrent(scope), 1, timeout)?;
        let stage = route
            .legs
            .read()
            .expect("network route legs")
            .first()
            .ok_or("the route has no leg")?
            .stage
            .clone();
        Ok(Arc::new(TorrentLeg {
            stage,
            handle: self.handle.clone(),
            bridge: Mutex::new(None),
        }))
    }
}
