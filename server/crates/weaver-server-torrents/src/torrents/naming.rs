// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Who answers each torrent's names on its session's leg. Torrent routes on
//! one leg share its session whatever their DNS profiles, so each torrent's
//! own lookups, its trackers' and web seeds', name by its route's profile
//! rather than the leg's: they dial through the leg with that route's names
//! in scope, read at each lookup from a slot a save puts new names in. What
//! the session looks up for no torrent, its DHT's bootstrap nodes, names as
//! the leg does.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use weaver_torrent::network::{BoxFuture, DatagramSocket, Resolved, Resolver};
use weaver_torrent::{ClientShape, Egress, HttpClients, HttpTarget, TorrentNaming};
use weaver_tunnel::pipe::{
    Capacity, DialError, Dialed, Dialer, Names, NamesSlot, Purpose, Resolution, Target, Transports,
};

use crate::proxies::{PathBridge, TorrentLeg, TorrentScope};
use crate::torrents::net::{LegKind, LegNames, SeedBridge, tracker_client};

/// How the torrents of one route look names up and ask over HTTP on a
/// session's leg: a resolver and HTTP clients of their own.
#[derive(Clone)]
pub struct ScopeNaming(Arc<Naming>);

// On a ladder leg a route's own bridge rides inside `http`, held up by its
// clients: it ends once the last torrent naming by the route, and the last
// client built on it, are gone.
struct Naming {
    resolver: Arc<dyn Resolver>,
    http: Arc<dyn HttpClients>,
}

impl ScopeNaming {
    fn of(resolver: Arc<dyn Resolver>, http: Arc<dyn HttpClients>) -> Self {
        Self(Arc::new(Naming { resolver, http }))
    }

    /// A naming that answers nothing, saying why.
    pub(crate) fn refused(why: &'static str) -> Self {
        let refused = Arc::new(Refused(why));
        Self::of(refused.clone(), refused)
    }
}

/// The namings of a session's leg: the session's own, and one for each
/// torrent route on it, built the first time a torrent of it is added and
/// kept while a torrent names by it. A route's naming holds its own HTTP
/// clients and, on a ladder, its own bridge, so what one route's lookups
/// found is never what another's dials keep to.
pub struct LegNaming {
    kind: LegKind,
    dialer: Arc<dyn Dialer>,
    leg: Option<Arc<TorrentLeg>>,
    session: ScopeNaming,
    scopes: Mutex<HashMap<TorrentScope, Weak<Naming>>>,
}

impl LegNaming {
    pub fn new(
        kind: LegKind,
        dialer: Arc<dyn Dialer>,
        leg: Option<Arc<TorrentLeg>>,
        resolver: Arc<dyn Resolver>,
        http: Arc<dyn HttpClients>,
    ) -> Self {
        Self {
            kind,
            dialer,
            leg,
            session: ScopeNaming::of(resolver, http),
            scopes: Mutex::default(),
        }
    }

    /// The naming of `scope`'s torrents, whose lookups name by `names`: the
    /// session's own when there are none.
    pub fn of(
        &self,
        scope: TorrentScope,
        names: Option<Arc<NamesSlot>>,
    ) -> Result<ScopeNaming, String> {
        let Some(names) = names else {
            return Ok(self.session.clone());
        };
        let mut scopes = self.scopes.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(naming) = scopes.get(&scope).and_then(Weak::upgrade) {
            return Ok(ScopeNaming(naming));
        }
        // A scope no torrent names by any more is forgotten here, when
        // another is built: there are as many as the leg's routes.
        scopes.retain(|_, naming| naming.strong_count() > 0);
        let dialer: Arc<dyn Dialer> = Arc::new(Scoped {
            leg: self.dialer.clone(),
            names,
        });
        let (bridge, seeds) = match (&self.kind, &self.leg) {
            (LegKind::Ladder, Some(leg)) => (
                Some(leg.bridge_on(dialer.clone(), Purpose::TorrentHttp)?),
                Some(scoped_seeds(leg, &dialer)),
            ),
            _ => (None, None),
        };
        let naming = Arc::new(Naming {
            resolver: Arc::new(LegNames(dialer.clone())),
            http: tracker_client(&self.kind, dialer, bridge, seeds)?,
        });
        scopes.insert(scope, Arc::downgrade(&naming));
        Ok(ScopeNaming(naming))
    }

    /// The scopes some torrent names by now.
    #[cfg(test)]
    pub fn held_scopes(&self) -> Vec<TorrentScope> {
        let scopes = self.scopes.lock().unwrap_or_else(PoisonError::into_inner);
        scopes
            .iter()
            .filter(|(_, naming)| naming.strong_count() > 0)
            .map(|(scope, _)| *scope)
            .collect()
    }
}

// A route's web seed transfers ride a bridge of their own onto `dialer`,
// the leg's dialer under that route's names, apart from its trackers': it
// dials for web seeds as the leg's own seed bridge does, on the torrent side
// of the same tunnel, so a long transfer never holds what announces keep. It
// starts the first time a web seed is asked and is held by the route's
// clients; it holds the leg weakly, so the leg's drop revokes it with the
// others, and a web seed asked after that fails as a revoked route does.
fn scoped_seeds(leg: &Arc<TorrentLeg>, dialer: &Arc<dyn Dialer>) -> SeedBridge {
    let leg = Arc::downgrade(leg);
    let dialer = Arc::clone(dialer);
    let started: Mutex<Option<PathBridge>> = Mutex::new(None);
    Arc::new(move || {
        let leg = leg.upgrade().ok_or_else(|| "leg revoked".to_owned())?;
        // A panic while starting the bridge left it unstarted: start it.
        let mut started = started.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(bridge) = started.as_ref() {
            return Ok(bridge.clone());
        }
        let bridge = leg.bridge_on(Arc::clone(&dialer), Purpose::TorrentWebSeed)?;
        *started = Some(bridge.clone());
        Ok(bridge)
    })
}

/// One torrent's naming, which follows it to another route on the same
/// session: its resolver and HTTP clients read the naming in it at each
/// lookup and request.
pub struct TorrentNames(RwLock<ScopeNaming>);

impl TorrentNames {
    pub fn new(naming: ScopeNaming) -> Arc<Self> {
        Arc::new(Self(RwLock::new(naming)))
    }

    /// Name by `naming` from the next lookup on.
    pub fn follow(&self, naming: ScopeNaming) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = naming;
    }

    /// What the session is handed for the torrent.
    pub fn naming(self: &Arc<Self>) -> TorrentNaming {
        TorrentNaming {
            resolver: self.clone(),
            http_clients: self.clone(),
        }
    }

    fn now(&self) -> ScopeNaming {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Resolver for TorrentNames {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        let resolver = self.now().0.resolver.clone();
        Box::pin(async move { resolver.resolve(host, port).await })
    }
}

impl HttpClients for TorrentNames {
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        self.now().0.http.client(target, shape)
    }

    fn egress(&self, to: SocketAddr) -> Egress {
        self.now().0.http.egress(to)
    }

    fn ipv6_sources(&self) -> Option<io::Result<weaver_torrent::Ipv6Sources>> {
        self.now().0.http.ipv6_sources()
    }

    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        let http = self.now().0.http.clone();
        Box::pin(async move { http.gateway_socket(local).await })
    }

    fn ssdp_socket<'a>(
        &'a self,
        from: std::net::Ipv4Addr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        let http = self.now().0.http.clone();
        Box::pin(async move { http.ssdp_socket(from).await })
    }
}

// The leg's dialer under one route's names: what it dials and resolves is
// named as that route says, whichever route's leg it is.
struct Scoped {
    leg: Arc<dyn Dialer>,
    names: Arc<NamesSlot>,
}

#[async_trait::async_trait]
impl Dialer for Scoped {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        Names::scope(self.names.get(), self.leg.dial(target)).await
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        Names::scope(self.names.get(), self.leg.resolve(host)).await
    }
    fn offers(&self) -> Transports {
        self.leg.offers()
    }
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<tokio::sync::Notify>>> {
        self.leg.room(purpose)
    }
    fn wait_room(&self, purpose: &Purpose) -> Result<(), Capacity> {
        self.leg.wait_room(purpose)
    }
    fn bind_local_udp(&self, local: SocketAddr) -> Option<io::Result<std::net::UdpSocket>> {
        self.leg.bind_local_udp(local)
    }
    fn source_toward(&self, to: SocketAddr) -> Option<io::Result<std::net::IpAddr>> {
        self.leg.source_toward(to)
    }
    fn ipv6_sources(&self) -> Option<io::Result<weaver_tunnel::egress::Ipv6Sources>> {
        self.leg.ipv6_sources()
    }
    fn budget(&self) -> Duration {
        self.leg.budget()
    }
    fn describe(&self) -> String {
        self.leg.describe()
    }
}

// Answers nothing: every lookup and request fails, saying why.
struct Refused(&'static str);

impl Resolver for Refused {
    fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        let why = self.0;
        Box::pin(async move { Err(io::Error::other(why)) })
    }
}

impl HttpClients for Refused {
    fn client(
        &self,
        _target: Option<HttpTarget<'_>>,
        _shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        anyhow::bail!(self.0)
    }

    fn egress(&self, _to: SocketAddr) -> Egress {
        Egress::NoRoute
    }

    fn gateway_socket<'a>(
        &'a self,
        _local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        let why = self.0;
        Box::pin(async move { Err(io::Error::other(why)) })
    }

    fn ssdp_socket<'a>(
        &'a self,
        _from: std::net::Ipv4Addr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        let why = self.0;
        Box::pin(async move { Err(io::Error::other(why)) })
    }
}
