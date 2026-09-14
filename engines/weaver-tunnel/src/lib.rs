//! Weaver adapters for shared tunnels and consumer routing.
//!
//! SSH, WireGuard and HTTP/3 protocols live in the Git-pinned `proxy-tunnels` crate.
//! This crate adapts persisted host-key trust and consumer outcomes, implements
//! HTTP CONNECT/SOCKS5 upstream transports and routed DNS, and supplies owned
//! revocable streams. NNTP uses direct in-process I/O; RSS uses authenticated
//! loopback SOCKS bridges. The server owns policy, persistence and the runtime.

pub mod bridge;
pub mod direct;
pub mod dns;
mod error;
#[path = "shared_http3.rs"]
mod http3;
mod provider;
#[cfg(any(test, feature = "test-support"))]
mod registry;
mod shared;
pub mod socks5;
#[path = "shared_ssh.rs"]
mod ssh;
pub mod transport;
#[path = "shared_wireguard.rs"]
pub mod wireguard;

#[cfg(any(test, feature = "test-support"))]
#[path = "shared_test_support.rs"]
pub mod test_support;

pub use error::TunnelError;
pub use http3::{Http3ProxyCredentials, Http3TunnelProvider, Http3TunnelSpec};
pub use provider::{
    ED25519_ONLY_PRIVATE_KEY_MESSAGE, NoopTunnelObserver, TunnelObserver, TunnelProvider,
    TunnelSpec, TunnelStream,
};
#[cfg(any(test, feature = "test-support"))]
pub use registry::TunnelRegistry;
pub use ssh::{SshTunnelProvider, TunnelHandshake, validate_private_key};
pub use wireguard::{
    DEFAULT_WIREGUARD_KEEPALIVE, DEFAULT_WIREGUARD_MTU, IpCidr, MAX_WIREGUARD_MTU,
    MIN_WIREGUARD_MTU, WIREGUARD_KEY_MESSAGE, WgStack, WgTcpStream, WgUdpSocket,
    WireGuardHandshake, WireGuardSpec, WireGuardTunnelProvider, parse_key, public_key_of,
    validate_wireguard_keys,
};
