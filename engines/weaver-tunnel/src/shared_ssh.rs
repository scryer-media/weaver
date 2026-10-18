// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::shared::{Observer, SharedProvider};
use crate::{TunnelObserver, TunnelSpec};
pub use proxy_tunnels::validate_private_key;
use std::sync::Arc;

pub type SshTunnelProvider = SharedProvider<proxy_tunnels::SshTunnelProvider>;

impl crate::shared::SessionKind for proxy_tunnels::SshTunnelProvider {
    const KIND: crate::metrics::TunnelKind = crate::metrics::TunnelKind::Ssh;
}

impl SshTunnelProvider {
    pub fn with_transport(self, transport: Arc<dyn crate::endpoint::EndpointTransport>) -> Self {
        Self(self.0.with_transport(transport))
    }
    pub fn new(spec: TunnelSpec, observer: Arc<dyn TunnelObserver>) -> Self {
        let observer = Arc::new(Observer(observer, Some((spec.host.clone(), spec.port))));
        Self(proxy_tunnels::SshTunnelProvider::new(spec, observer))
    }
}
