use crate::shared::{Observer, SharedProvider};
use crate::{TunnelError, TunnelObserver, TunnelSpec};
pub use proxy_tunnels::{TunnelHandshake, validate_private_key};
use std::sync::Arc;

pub type SshTunnelProvider = SharedProvider<proxy_tunnels::SshTunnelProvider>;

impl SshTunnelProvider {
    pub fn new(spec: TunnelSpec, observer: Arc<dyn TunnelObserver>) -> Self {
        let observer = Arc::new(Observer(observer, Some((spec.host.clone(), spec.port))));
        Self(proxy_tunnels::SshTunnelProvider::new(spec, observer))
    }
    pub async fn handshake(
        spec: TunnelSpec,
        observer: Arc<dyn TunnelObserver>,
    ) -> Result<TunnelHandshake, TunnelError> {
        let observer = Arc::new(Observer(observer, Some((spec.host.clone(), spec.port))));
        proxy_tunnels::SshTunnelProvider::handshake(spec, observer).await
    }
}
