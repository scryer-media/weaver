use crate::shared::{Observer, SharedProvider};
use crate::{TunnelError, TunnelObserver};
pub use proxy_tunnels::wireguard::*;
use std::sync::Arc;

pub type WireGuardTunnelProvider = SharedProvider<proxy_tunnels::WireGuardTunnelProvider>;

impl WireGuardTunnelProvider {
    pub fn new(spec: WireGuardSpec, observer: Arc<dyn TunnelObserver>) -> Self {
        Self(
            proxy_tunnels::WireGuardTunnelProvider::new(spec, Arc::new(Observer(observer, None)))
                .with_download_tuning(),
        )
    }
    pub async fn handshake(
        spec: WireGuardSpec,
        observer: Arc<dyn TunnelObserver>,
    ) -> Result<WireGuardHandshake, TunnelError> {
        proxy_tunnels::WireGuardTunnelProvider::handshake(spec, Arc::new(Observer(observer, None)))
            .await
    }
    pub async fn resolve_host(&self, host: &str) -> Result<Vec<std::net::IpAddr>, TunnelError> {
        self.0.resolve_host(host).await
    }
}
