//! Shared SSH/WireGuard fixtures plus Weaver's HTTP/SOCKS and DNS fixtures.
pub use proxy_tunnels::test_support::*;
#[path = "test_support/dns.rs"]
pub mod dns;
#[path = "test_support/proxy.rs"]
pub mod proxy;
