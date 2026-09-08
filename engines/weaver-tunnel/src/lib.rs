//! Application-independent TCP proxy and userspace tunnel protocols.
//!
//! A transport proxy already exists when a consumer starts talking to it — the HTTP
//! client is handed `socks5://gateway:1080` and that is the whole story. A
//! tunnel does not exist until its owner establishes it, so something has to run
//! the session and then publish an endpoint the ordinary transport client
//! factories can dial. That is this crate.
//!
//! ```text
//!   TCP consumer  ──SOCKS5──▶  loopback SOCKS5 front  ──▶  TunnelProvider
//!   (any consumer)                127.0.0.1:<ephemeral>        (SSH or WireGuard)
//! ```
//!
//! Three pieces, deliberately separated:
//!
//! * [`TunnelProvider`] — "open a byte stream to `host:port` on the far side".
//!   [`SshTunnelProvider`] runs an SSH session and forwards `direct-tcpip`
//!   channels; [`WireGuardTunnelProvider`] runs a userspace WireGuard device
//!   with a userspace IP stack on top of it. Both plug into the same seam, and
//!   everything below inherits both without knowing which is which.
//! * [`socks5`] — a hand-rolled, loopback-only, authenticated CONNECT SOCKS5
//!   front. It is what makes a tunnel look like an ordinary proxy to every
//!   existing egress site.
//! * [`bridge::Bridge`] — an owned, revocable front reachable from blocking
//!   and async callers. The legacy registry is retained only for fixtures.
//!
//! ## Why this is a separate crate
//!
//! Protocols accept [`TunnelSpec`] without application persistence or routing
//! types. The owning runtime supplies its executor, policy and observer.
//!
//! ## What this crate never does
//!
//! It never logs key material, never persists anything, and never decides
//! policy. Host-key pins and health outcomes are handed to a
//! [`TunnelObserver`] supplied by the caller, which owns the ledgers and the
//! repository.

pub mod bridge;
pub mod dns;
mod error;
mod provider;
#[cfg(any(test, feature = "test-support"))]
mod registry;
pub mod socks5;
mod ssh;
pub mod transport;
pub mod wireguard;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use error::TunnelError;
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
