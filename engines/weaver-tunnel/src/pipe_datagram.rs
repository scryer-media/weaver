//! A WireGuard session as the carrier of another WireGuard tunnel.
//!
//! The tunnel stacked on top sends its encrypted packets through a UDP socket
//! inside the session beneath it. Each such socket holds the session's
//! activity guard and budget permit for as long as it lives, so the session
//! beneath is never retired as idle while a tunnel still rides on it.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit};

use super::{DialError, Dialer, Resolution, SessionHop};
use crate::{
    TunnelError,
    endpoint::{DatagramSocket, DatagramTransport},
};

pub(super) struct SessionDatagrams(Arc<SessionHop>);

impl SessionDatagrams {
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

#[async_trait::async_trait]
impl DatagramTransport for SessionDatagrams {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, TunnelError> {
        match self.0.resolve(host).await.map_err(tunnel_error)? {
            Resolution::Addresses(addresses) => Ok(addresses),
            Resolution::Unresolved => Err(TunnelError::Engine(format!(
                "proxy {} cannot resolve names",
                self.0.id
            ))),
        }
    }

    async fn bind(&self, peer: SocketAddr) -> Result<Arc<dyn DatagramSocket>, TunnelError> {
        let session = &self.0;
        let Some(provider) = session.resolver.as_ref() else {
            return Err(TunnelError::Engine(format!(
                "proxy {} cannot carry datagrams",
                session.id
            )));
        };
        let activity = session.activity.clone().read_owned().await;
        let capacity = session.capacity.acquire().map_err(tunnel_error)?;
        match provider.0.bind(peer).await {
            Ok(socket) => Ok(Arc::new(HeldSocket {
                socket,
                _activity: activity,
                _capacity: capacity,
            })),
            Err(error) => {
                drop(capacity);
                drop(activity);
                session.retire_idle().await;
                Err(error)
            }
        }
    }
}

/// A socket inside the session beneath, holding that session open.
struct HeldSocket {
    socket: Arc<dyn DatagramSocket>,
    _activity: OwnedRwLockReadGuard<()>,
    _capacity: Option<Arc<OwnedSemaphorePermit>>,
}

#[async_trait::async_trait]
impl DatagramSocket for HeldSocket {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> std::io::Result<()> {
        self.socket.send_to(data, target).await
    }
    async fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buffer).await
    }
    fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.local_addr()
    }
    fn link_mtu(&self) -> Option<u16> {
        self.socket.link_mtu()
    }
    fn is_closed(&self) -> bool {
        self.socket.is_closed()
    }
    async fn closed(&self) {
        self.socket.closed().await;
    }
}
