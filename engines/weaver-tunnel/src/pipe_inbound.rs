// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Stream connections that arrive through a WireGuard session.
//
// A session listening inside its tunnel holds the session's activity guard
// and budget permit for as long as the listener lives, as a socket carried
// through it does, so the session is never retired as idle while it listens.
// Each accepted connection holds them too, and reports to its own outcome.
//
// When the tunnel is replaced its stack stops and the listener fails. The
// next accept listens again on the same port inside the new tunnel, so a
// session never keeps a listener on a tunnel that is gone.
//
// A provider that forwards a port to the tunnel decides which port that
// is. A fixed forwarded port is listened on in place of the one asked for.
// A port the gateway forwards over NAT-PMP is asked for once the listener
// is up and kept while it lives; when the gateway forwards another port,
// the listener moves to it, and each outcome is published for the session
// to announce.
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex as SyncMutex, PoisonError},
};

use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedSemaphorePermit, watch};

use super::natpmp::{self, NATPMP_PORT};
pub use super::natpmp::{PortMapping, gateway_of};
use super::{
    ConnectionOutcome, DialError, Dialed, DialedStream, Dialer, OutcomeStream, SessionHop,
    SocketClass,
};
use crate::{TunnelError, wireguard::WgTcpListener};

/// Connections a listener inside a WireGuard tunnel holds before it accepts
/// them. The tunnel has no listen queue: a listener is a pool of this many
/// sockets on its port, each taking one of the tunnel's sockets for as long
/// as the listener lives.
pub const LISTEN_BACKLOG: usize = 16;

/// How the provider at a WireGuard hop's far end forwards a port to it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PortForwarding {
    /// No port is forwarded: the session listens on the port it asks for.
    #[default]
    None,
    /// The provider forwards this port, always the same one.
    Fixed(u16),
    /// The gateway forwards a port of its choosing over NAT-PMP.
    NatPmp { gateway: Ipv4Addr },
}

// Where stream connections arrive for one hop.
#[async_trait::async_trait]
pub trait Inbound: Send + Sync {
    // Listen on `port` inside the hop's tunnel; 0 takes a free port. The
    // `forwarded` listener, the one peers reach through the port the hop's
    // provider forwards, listens on that port instead when there is one.
    async fn listen(
        &self,
        port: u16,
        forwarded: bool,
    ) -> Result<Box<dyn InboundListener>, TunnelError>;
    // How the hop's provider forwards a port to its listener. Peers outside
    // the tunnel reach the listener only through such a port: with none,
    // nothing they send arrives.
    fn forwarding(&self) -> PortForwarding;
}

#[async_trait::async_trait]
pub trait InboundListener: Send + Sync {
    // The address peers reach inside the tunnel, now.
    fn local_addr(&self) -> SocketAddr;
    async fn accept(&self) -> Result<Dialed, TunnelError>;
    // What the gateway last said of the port it forwards, for a listener
    // whose gateway forwards one on request.
    fn mapping(&self) -> Option<watch::Receiver<PortMapping>> {
        None
    }
}

pub(super) struct SessionInbound(Arc<SessionHop>);

impl SessionInbound {
    pub(super) fn new(session: Arc<SessionHop>) -> Self {
        Self(session)
    }
}

fn tunnel_error(error: DialError) -> TunnelError {
    match error {
        DialError::Fatal(error) => error,
        error => TunnelError::Engine(error.to_string()),
    }
}

// Listen inside the session's tunnel, retiring it when it cannot come up.
// A probe lent another session's tunnel listens in that one, and gets back
// the guard that holds the lender busy.
async fn listen_in(
    session: &SessionHop,
    port: u16,
) -> Result<(WgTcpListener, Option<OwnedRwLockReadGuard<()>>), TunnelError> {
    let Some(provider) = session.resolver.as_ref() else {
        return Err(TunnelError::Engine(format!(
            "proxy {} cannot accept connections",
            session.id
        )));
    };
    session.prepare_carrier().await.map_err(tunnel_error)?;
    let (provider, lent) = match super::identity::claim(session).await {
        Some(lent) => (lent.resolver, Some(lent.activity)),
        None => (provider.clone(), None),
    };
    match provider.listen(port, LISTEN_BACKLOG).await {
        Ok(listener) => Ok((listener, lent)),
        // A tunnel that could not come up to listen is the hop's fault, as
        // one that could not come up to dial is.
        Err(error) => {
            session.failure.blame(&error, session.timeout);
            session.retire_idle().await;
            Err(error)
        }
    }
}

// A task that ends when its handle is dropped.
struct Abort(tokio::task::JoinHandle<()>);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[async_trait::async_trait]
impl Inbound for SessionInbound {
    async fn listen(
        &self,
        port: u16,
        forwarded: bool,
    ) -> Result<Box<dyn InboundListener>, TunnelError> {
        let session = Arc::clone(&self.0);
        let forwarding = if forwarded {
            session.forwarding
        } else {
            PortForwarding::None
        };
        let port = match forwarding {
            PortForwarding::Fixed(forwarded) => forwarded,
            PortForwarding::None | PortForwarding::NatPmp { .. } => port,
        };
        let (listener, lent) = listen_in(&session, port).await?;
        let (activity, capacity) = match lent {
            Some(activity) => (activity, None),
            None => {
                let activity = session.activity.clone().read_owned().await;
                match session.capacity.acquire() {
                    Ok(capacity) => (activity, capacity),
                    Err(error) => {
                        drop(activity);
                        return Err(tunnel_error(error));
                    }
                }
            }
        };
        let (mapping, keeper) = match forwarding {
            PortForwarding::NatPmp { gateway } => {
                let (published, mapping) = watch::channel(PortMapping::default());
                let transport = Arc::new(super::datagram::SessionDatagrams::new(session.clone()));
                let keeper = tokio::spawn(natpmp::keep(
                    transport,
                    SocketAddr::new(gateway.into(), NATPMP_PORT),
                    published,
                ));
                (Some(mapping), Some(Abort(keeper)))
            }
            PortForwarding::None | PortForwarding::Fixed(_) => (None, None),
        };
        Ok(Box::new(SessionListener {
            local: SyncMutex::new(listener.local_addr()),
            state: Mutex::new(Listening {
                listener: Some(listener),
                mapping: mapping.clone(),
            }),
            mapping,
            session,
            _keeper: keeper,
            _activity: activity,
            _capacity: capacity,
        }))
    }
    fn forwarding(&self) -> PortForwarding {
        self.0.forwarding
    }
}

// A listener inside a session's tunnel, holding that session open.
struct SessionListener {
    session: Arc<SessionHop>,
    // The address it listens on, kept across tunnel replacements.
    local: SyncMutex<SocketAddr>,
    state: Mutex<Listening>,
    mapping: Option<watch::Receiver<PortMapping>>,
    _keeper: Option<Abort>,
    _activity: OwnedRwLockReadGuard<()>,
    _capacity: Option<Arc<OwnedSemaphorePermit>>,
}

struct Listening {
    // None once the tunnel it listened in has stopped.
    listener: Option<WgTcpListener>,
    // The gateway's word on the port it forwards, watched for a move.
    mapping: Option<watch::Receiver<PortMapping>>,
}

impl SessionListener {
    fn local(&self) -> SocketAddr {
        *self.local.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// Until `mapping` changes; never, without one.
async fn moved(mapping: &mut Option<watch::Receiver<PortMapping>>) {
    let Some(mapping) = mapping else {
        return std::future::pending().await;
    };
    if mapping.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl InboundListener for SessionListener {
    fn local_addr(&self) -> SocketAddr {
        self.local()
    }

    fn mapping(&self) -> Option<watch::Receiver<PortMapping>> {
        self.mapping.clone()
    }

    async fn accept(&self) -> Result<Dialed, TunnelError> {
        let mut state = self.state.lock().await;
        let Listening { listener, mapping } = &mut *state;
        let (stream, share) = loop {
            // The port the gateway forwards now, else the one listened on.
            let port = mapping
                .as_mut()
                .and_then(|mapping| mapping.borrow_and_update().external_port)
                .unwrap_or_else(|| self.local().port());
            if listener
                .as_ref()
                .is_some_and(|current| current.local_addr().port() != port)
            {
                *listener = None;
            }
            let current = match listener.as_mut() {
                Some(current) => current,
                None => {
                    // The listener already holds the session it listens in
                    // busy.
                    let (fresh, _) = listen_in(&self.session, port).await?;
                    *self.local.lock().unwrap_or_else(PoisonError::into_inner) = fresh.local_addr();
                    listener.insert(fresh)
                }
            };
            tokio::select! {
                accepted = current.accept() => match accepted {
                    // A peer past the torrent share is turned away and the
                    // listener keeps listening.
                    Ok(stream) => match self.session.sockets.take(SocketClass::Torrent) {
                        Ok(share) => break (stream, share),
                        Err(_) => drop(stream),
                    },
                    // The tunnel stopped: listen again inside its replacement.
                    Err(_) => *listener = None,
                },
                () = moved(mapping) => {}
            }
        };
        drop(state);
        let session = &self.session;
        let peer = stream.peer_addr();
        let local = self.local();
        let activity = session.activity.clone().read_owned().await;
        let capacity = session.capacity.acquire().map_err(tunnel_error)?;
        let outcome = Arc::new(ConnectionOutcome::default());
        let mut path = session.path.clone();
        path.proxies.push(session.id);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(
                OutcomeStream::new(
                    DialedStream::Tunnel(Box::new(stream)),
                    outcome.clone(),
                    Some(activity),
                    capacity,
                )
                .holding(share),
            )),
            outcome,
            path,
            peer: Some(peer),
            source: Some(local),
            setup: None,
        })
    }
}
