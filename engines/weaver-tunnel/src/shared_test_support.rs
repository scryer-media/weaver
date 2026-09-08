//! Shared SSH/WireGuard fixtures plus Weaver's HTTP/SOCKS and DNS fixtures.
pub use proxy_tunnels::test_support::*;
#[path = "test_support/dns.rs"]
pub mod dns;
#[cfg(feature = "test-support")]
#[path = "test_support/http3.rs"]
pub mod http3;
#[path = "test_support/proxy.rs"]
pub mod proxy;
