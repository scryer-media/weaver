use crate::error::TunnelError;
pub use proxy_tunnels::{ED25519_ONLY_PRIVATE_KEY_MESSAGE, TunnelSpec, TunnelStream};

/// Something that can carry a TCP connection to `host:port` on the far side of
/// a tunnel.
///
/// This is the seam a second tunnel technology plugs into. A WireGuard
/// implementation (smoltcp over a userspace device) implements exactly this
/// and inherits the SOCKS5 front, the registry, the lifecycle and every
/// consumer without another line changing.
#[async_trait::async_trait]
pub trait TunnelProvider: Send + Sync {
    /// Stop the owned session. Callers close their streams before awaiting this.
    async fn shutdown(&self) {}
    /// Open a stream to `host:port`, resolving `host` **on the far side**.
    ///
    /// `host` may be a name; that is the point of a tunnel. A seedbox's
    /// `localhost` must mean the seedbox.
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError>;

    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        _outcome: std::sync::Arc<crate::bridge::ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial(host, port).await
    }

    /// Short human description of where this tunnel goes, for health text and
    /// tracing. Must not contain credentials.
    fn describe(&self) -> String;
}

/// Where a tunnel reports what it observed.
///
/// The engine cannot reach a repository (it runs on egress paths that have no
/// handle to one, including a blocking plugin worker thread), so it hands
/// observations to the caller, which owns the ledgers that async flows drain.
/// This is the same convention the challenge-solver health path established.
pub trait TunnelObserver: Send + Sync {
    /// A connection through the tunnel failed. `message` is already
    /// operator-facing and free of secrets.
    fn tunnel_dial_failed(&self, proxy_config_id: &str, message: &str);

    /// A connection through the tunnel succeeded.
    fn tunnel_dial_succeeded(&self, proxy_config_id: &str);

    /// Trust-on-first-use: this fingerprint was learned from the first
    /// successful handshake and should be persisted as the pin.
    fn host_key_pinned(&self, proxy_config_id: &str, fingerprint: &str);

    fn persisted_host_key(&self, _proxy_config_id: &str) -> Result<Option<String>, TunnelError> {
        Ok(None)
    }

    fn persist_host_key(
        &self,
        proxy_config_id: &str,
        fingerprint: &str,
    ) -> Result<(), TunnelError> {
        self.host_key_pinned(proxy_config_id, fingerprint);
        Ok(())
    }
}

/// Observer that discards everything. For tests and for callers that only want
/// the transport.
pub struct NoopTunnelObserver;

impl TunnelObserver for NoopTunnelObserver {
    fn tunnel_dial_failed(&self, _proxy_config_id: &str, _message: &str) {}
    fn tunnel_dial_succeeded(&self, _proxy_config_id: &str) {}
    fn host_key_pinned(&self, _proxy_config_id: &str, _fingerprint: &str) {}
}
