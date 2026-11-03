// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Shared SSH/WireGuard fixtures plus Weaver's HTTP/SOCKS and DNS fixtures.
pub use proxy_tunnels::test_support::*;
#[path = "test_support/dns.rs"]
pub mod dns;
#[cfg(feature = "test-support")]
#[path = "test_support/http3.rs"]
pub mod http3;
#[cfg(feature = "test-support")]
#[path = "test_support/http_client.rs"]
pub mod http_client;
#[path = "test_support/natpmp.rs"]
pub mod natpmp;

/// The host's own egress, alone, as a route with nothing saved leaves, and
/// with no connect deadline: a test waits on what it checks.
pub fn system_dialer() -> std::sync::Arc<dyn crate::pipe::Dialer> {
    std::sync::Arc::new(crate::pipe::Egress::new(
        0,
        crate::egress::SocketEgress::System,
        std::time::Duration::MAX,
    ))
}
#[path = "test_support/proxy.rs"]
pub mod proxy;
#[cfg(feature = "test-support")]
#[path = "test_support/tls.rs"]
pub mod tls;
