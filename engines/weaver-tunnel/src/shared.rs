//! Weaver's persistence and outcome adapters for the shared tunnel engine.
use crate::{TunnelError, TunnelObserver, TunnelProvider, TunnelStream};
use std::sync::Arc;

pub(crate) struct Observer(
    pub(crate) Arc<dyn TunnelObserver>,
    pub(crate) Option<(String, u16)>,
);

impl proxy_tunnels::TunnelObserver for Observer {
    fn tunnel_dial_failed(&self, id: &str, message: &str) {
        self.0.tunnel_dial_failed(id, message);
    }
    fn tunnel_dial_succeeded(&self, id: &str) {
        self.0.tunnel_dial_succeeded(id);
    }
    fn host_key_pinned(&self, _id: &str, _fingerprint: &str) {
        // The fallible authorization hook persists trust before forwarding.
    }
    fn authorize_host_key(&self, id: &str, fingerprint: &str) -> Result<(), TunnelError> {
        self.0.persist_host_key(id, fingerprint).map_err(|error| {
            // Preserve a typed trust failure even if a competing handshake
            // persisted its pin after this provider began authenticating.
            if let Some((host, port)) = &self.1
                && let Ok(Some(expected)) = self.0.persisted_host_key(id)
                && expected != fingerprint
            {
                return TunnelError::HostKeyMismatch {
                    host: host.clone(),
                    port: *port,
                    expected,
                    actual: fingerprint.into(),
                };
            }
            error
        })
    }
}

/// Attach Weaver's outcome-aware provider interface to a shared provider.
pub struct SharedProvider<T>(pub(crate) T);

#[async_trait::async_trait]
impl<T: proxy_tunnels::TunnelProvider> TunnelProvider for SharedProvider<T> {
    async fn shutdown(&self) {
        self.0.shutdown().await;
    }
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.0.dial(host, port).await
    }
    fn describe(&self) -> String {
        self.0.describe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{SshServerDouble, SshServerOptions, TunnelledOrigin, spec_for};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    struct Trust {
        pin: Mutex<Option<String>>,
        revoked: AtomicBool,
    }
    impl TunnelObserver for Trust {
        fn tunnel_dial_failed(&self, _: &str, _: &str) {}
        fn tunnel_dial_succeeded(&self, _: &str) {}
        fn host_key_pinned(&self, _: &str, _: &str) {}
        fn persisted_host_key(&self, _: &str) -> Result<Option<String>, TunnelError> {
            Ok(self.pin.lock().unwrap().clone())
        }
        fn persist_host_key(&self, _: &str, fingerprint: &str) -> Result<(), TunnelError> {
            let mut pin = self.pin.lock().unwrap();
            if self.revoked.load(Ordering::Acquire)
                || pin.as_ref().is_some_and(|pin| pin != fingerprint)
            {
                return Err(TunnelError::Engine("persisted trust rejected".into()));
            }
            *pin = Some(fingerprint.into());
            Ok(())
        }
    }

    #[tokio::test]
    async fn shared_ssh_rechecks_persisted_trust_before_each_forward() {
        let server = SshServerDouble::start(SshServerOptions::default()).await;
        let origin = TunnelledOrigin::start("fixture").await;
        let observer = Arc::new(Trust::default());
        let provider = crate::SshTunnelProvider::new(
            spec_for("trust", &server.host(), server.port()),
            observer.clone(),
        );
        let stream = provider
            .dial("127.0.0.1", origin.addr().port())
            .await
            .unwrap();
        drop(stream);
        observer.revoked.store(true, Ordering::Release);
        assert!(
            provider
                .dial("127.0.0.1", origin.addr().port())
                .await
                .is_err()
        );
        assert_eq!(server.forwarded_targets().len(), 1);
        assert_eq!(server.accepted_auth(), vec!["publickey"]);
        provider.shutdown().await;
        assert!(
            provider
                .dial("127.0.0.1", origin.addr().port())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_competing_pin_remains_a_typed_trust_failure() {
        let server = SshServerDouble::start(SshServerOptions::default()).await;
        let observer = Arc::new(Trust::default());
        let provider = crate::SshTunnelProvider::new(
            spec_for("trust", &server.host(), server.port()),
            observer.clone(),
        );
        *observer.pin.lock().unwrap() = Some("SHA256:competing".into());
        let result = provider.dial("unresolved.invalid", 119).await;
        assert!(matches!(result, Err(TunnelError::HostKeyMismatch { .. })));
        assert!(server.forwarded_targets().is_empty());
        provider.shutdown().await;
    }
}
