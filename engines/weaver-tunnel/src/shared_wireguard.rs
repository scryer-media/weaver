// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::shared::Observer;
use crate::{TunnelError, TunnelObserver, TunnelProvider, TunnelStream};
pub use proxy_tunnels::wireguard::*;
use std::sync::Arc;

// The key pair a WireGuard tunnel handshakes with: its own public key and its
// peer's. A server keeps one session, and one endpoint, per peer key, so two
// tunnels with one identity contend for a single session at that server.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WireGuardIdentity {
    own: String,
    peer: [u8; 32],
}

impl WireGuardIdentity {
    pub fn of(spec: &WireGuardSpec) -> Self {
        Self {
            own: spec.public_key(),
            peer: spec.peer_public_key,
        }
    }
}

pub struct WireGuardTunnelProvider(
    pub(crate) proxy_tunnels::WireGuardTunnelProvider,
    WireGuardIdentity,
);

impl WireGuardTunnelProvider {
    pub fn with_udp_factory(self, factory: Arc<dyn crate::endpoint::UdpSocketFactory>) -> Self {
        Self(self.0.with_udp_factory(factory), self.1)
    }
    // Carry this tunnel's packets through `transport` instead of an OS
    // socket, such as a WireGuard session beneath it.
    pub fn with_datagram_transport(
        self,
        transport: Arc<dyn crate::endpoint::DatagramTransport>,
    ) -> Self {
        Self(self.0.with_datagram_transport(transport), self.1)
    }
    // Resolve an endpoint written as a name with `resolver`.
    pub fn with_endpoint_resolver(
        self,
        resolver: Arc<dyn crate::endpoint::EndpointResolver>,
    ) -> Self {
        Self(self.0.with_endpoint_resolver(resolver), self.1)
    }
    pub fn new(spec: WireGuardSpec, observer: Arc<dyn TunnelObserver>) -> Self {
        let identity = WireGuardIdentity::of(&spec);
        Self(
            proxy_tunnels::WireGuardTunnelProvider::new(spec, Arc::new(Observer(observer, None)))
                .with_download_tuning(),
            identity,
        )
    }
    pub fn identity(&self) -> &WireGuardIdentity {
        &self.1
    }
    // Accept stream connections on `port` inside the tunnel, bringing it up
    // first; 0 takes a free port.
    pub async fn listen(&self, port: u16, backlog: usize) -> Result<WgTcpListener, TunnelError> {
        self.0.listen(port, backlog).await
    }
    // Whether the live tunnel that is current now holds a listener on `port`.
    pub async fn listens_on(&self, port: u16) -> bool {
        self.0.listens_on(port).await
    }
    pub async fn resolve_host(&self, host: &str) -> Result<Vec<std::net::IpAddr>, TunnelError> {
        self.0.resolve_host(host).await
    }
}

#[async_trait::async_trait]
impl TunnelProvider for WireGuardTunnelProvider {
    async fn prepare(&self) -> Result<(), TunnelError> {
        proxy_tunnels::TunnelProvider::prepare(&self.0).await
    }
    async fn retire(&self) {
        proxy_tunnels::TunnelProvider::retire(&self.0).await;
    }
    fn kind(&self) -> Option<crate::metrics::TunnelKind> {
        Some(crate::metrics::TunnelKind::WireGuard)
    }
    fn source_address(&self) -> Option<std::net::SocketAddr> {
        proxy_tunnels::TunnelProvider::source_address(&self.0)
    }
    async fn shutdown(&self) {
        proxy_tunnels::TunnelProvider::shutdown(&self.0).await;
    }
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        proxy_tunnels::TunnelProvider::dial(&self.0, host, port).await
    }
    fn describe(&self) -> String {
        proxy_tunnels::TunnelProvider::describe(&self.0)
    }
}
