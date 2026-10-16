// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Shared SSH/WireGuard fixtures plus Weaver's HTTP/SOCKS and DNS fixtures.
pub use proxy_tunnels::test_support::*;
#[path = "test_support/dns.rs"]
pub mod dns;
#[cfg(feature = "test-support")]
#[path = "test_support/http3.rs"]
pub mod http3;
#[path = "test_support/proxy.rs"]
pub mod proxy;
#[cfg(feature = "test-support")]
#[path = "test_support/tls.rs"]
pub mod tls;
