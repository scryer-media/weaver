// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A WireGuard session as the carrier of another WireGuard tunnel.
//
// The tunnel stacked on top sends its encrypted packets through a UDP socket
// inside the session beneath it. Each such socket holds the session's
// activity guard and budget permit for as long as it lives, so the session
// beneath is never retired as idle while a tunnel still rides on it.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit};

use super::{DialError, Dialer, Resolution, SessionHop, SocketClass, SocketPermit};
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

pub(super) fn tunnel_error(error: DialError) -> TunnelError {
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
        let (provider, activity, capacity) = match super::identity::claim(session).await {
            Some(lent) => (lent.resolver, lent.activity, None),
            None => (
                provider.clone(),
                session.activity.clone().read_owned().await,
                session.capacity.acquire().map_err(tunnel_error)?,
            ),
        };
        let share = session
            .sockets
            .take(SocketClass::ReserveCore)
            .map_err(|_| {
                TunnelError::Engine(format!(
                    "proxy {}: the tunnel's {} share of sockets is full",
                    session.id,
                    SocketClass::ReserveCore.as_str()
                ))
            })?;
        match provider.0.bind(peer).await {
            Ok(socket) => {
                crate::resolution::record::bound(peer, true);
                Ok(Arc::new(HeldSocket {
                    socket,
                    _activity: activity,
                    _capacity: capacity,
                    _share: share,
                }))
            }
            // A tunnel that could not come up to carry datagrams is the
            // hop's fault, as one that could not come up to dial is.
            Err(error) => {
                session.failure.blame(&error, session.timeout);
                drop(share);
                drop(capacity);
                drop(activity);
                session.retire_idle().await;
                Err(error)
            }
        }
    }
}

// A socket inside the session beneath, holding that session open.
struct HeldSocket {
    socket: Arc<dyn DatagramSocket>,
    _activity: OwnedRwLockReadGuard<()>,
    _capacity: Option<Arc<OwnedSemaphorePermit>>,
    _share: Option<SocketPermit>,
}

#[async_trait::async_trait]
impl DatagramSocket for HeldSocket {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> std::io::Result<()> {
        self.socket.send_to(data, target).await
    }
    // A batch goes to the socket beneath whole, so a socket that sends
    // several datagrams for less than several single sends still does.
    async fn send_batch_to(
        &self,
        datagrams: &[&[u8]],
        target: SocketAddr,
    ) -> std::io::Result<usize> {
        self.socket.send_batch_to(datagrams, target).await
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // A socket that records each call it is handed.
    #[derive(Default)]
    struct Recording {
        singles: Mutex<Vec<Vec<u8>>>,
        batches: Mutex<Vec<(Vec<Vec<u8>>, SocketAddr)>>,
    }

    #[async_trait::async_trait]
    impl DatagramSocket for Recording {
        async fn send_to(&self, data: &[u8], _target: SocketAddr) -> std::io::Result<()> {
            self.singles.lock().unwrap().push(data.to_vec());
            Ok(())
        }
        async fn send_batch_to(
            &self,
            datagrams: &[&[u8]],
            target: SocketAddr,
        ) -> std::io::Result<usize> {
            let batch = datagrams.iter().map(|datagram| datagram.to_vec()).collect();
            self.batches.lock().unwrap().push((batch, target));
            Ok(datagrams.len())
        }
        async fn recv_from(&self, _buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            std::future::pending().await
        }
        fn local_addr(&self) -> Option<SocketAddr> {
            None
        }
        fn link_mtu(&self) -> Option<u16> {
            None
        }
        fn is_closed(&self) -> bool {
            false
        }
        async fn closed(&self) {
            std::future::pending().await
        }
    }

    // A batch sent through a held socket reaches the socket beneath as one
    // batch, in order, to the same target, and not as single sends.
    #[tokio::test]
    async fn a_held_socket_hands_a_batch_down_whole() {
        let inner = Arc::new(Recording::default());
        let held = HeldSocket {
            socket: inner.clone(),
            _activity: Arc::new(tokio::sync::RwLock::new(())).read_owned().await,
            _capacity: None,
            _share: None,
        };
        let target: SocketAddr = "10.0.0.2:51820".parse().unwrap();
        let datagrams: [&[u8]; 3] = [b"one", b"two", b"three"];

        assert_eq!(held.send_batch_to(&datagrams, target).await.unwrap(), 3);

        let batches = inner.batches.lock().unwrap();
        assert_eq!(
            *batches,
            [(
                vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
                target
            )]
        );
        assert!(inner.singles.lock().unwrap().is_empty());
    }
}
