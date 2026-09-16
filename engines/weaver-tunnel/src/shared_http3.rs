//! HTTP/3 uses the same native stream and revocation paths as SSH and WireGuard.
use crate::{TunnelError, shared::SharedProvider};
pub use proxy_tunnels::{Http3ProxyCredentials, Http3TunnelSpec};

pub type Http3TunnelProvider = SharedProvider<proxy_tunnels::Http3TunnelProvider>;

impl Http3TunnelProvider {
    pub fn new(spec: Http3TunnelSpec) -> Result<Self, TunnelError> {
        proxy_tunnels::Http3TunnelProvider::new(spec).map(Self)
    }
}
