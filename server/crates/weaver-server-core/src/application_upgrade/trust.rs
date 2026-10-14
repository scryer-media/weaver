// Process-wide TLS and Sigstore trust setup for upgrade downloads.
//
// `application-updater` builds its HTTP client on reqwest's
// `rustls-no-provider` feature, and `artifact-trust` refreshes the Sigstore
// trust roots over the same stack, so both depend on the host installing the
// process default crypto provider. Weaver's own TLS always passes a provider
// explicitly, which is why nothing else in the workspace installs one.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::proxies::{Consumer, ProxyRuntime, with_routed_client};

// How long a successful refresh is trusted before the next one.
const TRUST_ROOT_REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
// How long to wait after a failed refresh. Failure is not fatal: verification
// falls back to the trust snapshot embedded in `artifact-trust`.
const TRUST_ROOT_RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);

// Install the process default rustls crypto provider, once.
//
// Weaver compiles rustls against aws-lc-rs everywhere, so that is the provider
// installed here. Losing the race is expected — parallel tests and any earlier
// caller install the same provider — so `AlreadyInstalled` is ignored.
pub fn install_default_rustls_provider() {
    static INSTALLED: OnceLock<()> = OnceLock::new();

    INSTALLED.get_or_init(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

// Start refreshing the Sigstore trust roots in the background.
//
// Detached on purpose: an upgrade must be installable on a host that cannot
// reach the Sigstore TUF repository, and startup must not wait on the network.
// The refresh goes out on the Default outbound route, like the upgrade it
// serves.
pub fn spawn_sigstore_trust_root_priming(proxies: Arc<ProxyRuntime>) -> JoinHandle<()> {
    spawn_trust_root_priming(move || refresh_trust_roots(Arc::clone(&proxies)))
}

async fn refresh_trust_roots(proxies: Arc<ProxyRuntime>) -> Result<(), String> {
    with_routed_client(
        &proxies,
        Consumer::Outbound,
        "Sigstore trust root",
        trust_root_http_client,
        |client| async move {
            artifact_trust::prime_sigstore_trust_roots_with(client)
                .await
                .map_err(|error| error.to_string())
        },
    )
    .await?
}

// The TUF repository serves HTTPS only, and the refresh bounds its own run.
fn trust_root_http_client() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .connect_timeout(Duration::from_secs(30))
}

fn spawn_trust_root_priming<Refresh, Refreshing>(refresh: Refresh) -> JoinHandle<()>
where
    Refresh: Fn() -> Refreshing + Send + 'static,
    Refreshing: Future<Output = Result<(), String>> + Send,
{
    install_default_rustls_provider();
    tokio::spawn(async move {
        // The first success replaces the embedded snapshot, which is news. The
        // refresher reports nothing about what later ones changed, so those
        // stay at debug.
        let mut refreshed_before = false;
        loop {
            match refresh().await {
                Ok(()) => {
                    if refreshed_before {
                        debug!("sigstore trust roots refreshed");
                    } else {
                        info!("sigstore trust roots refreshed");
                    }
                    refreshed_before = true;
                    tokio::time::sleep(TRUST_ROOT_REFRESH_INTERVAL).await;
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to refresh the sigstore trust roots; upgrades keep using the embedded snapshot"
                    );
                    tokio::time::sleep(TRUST_ROOT_RETRY_INTERVAL).await;
                }
            }
        }
    })
}

#[cfg(all(test, unix))]
pub(crate) fn spawn_trust_root_priming_for_test<Refresh, Refreshing>(
    refresh: Refresh,
) -> JoinHandle<()>
where
    Refresh: Fn() -> Refreshing + Send + 'static,
    Refreshing: Future<Output = Result<(), String>> + Send,
{
    spawn_trust_root_priming(refresh)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installing_the_default_provider_twice_is_harmless() {
        install_default_rustls_provider();
        install_default_rustls_provider();
    }

    // The refresh leaves on the Default outbound route. When that route's only
    // proxy refuses it, the refresh fails and the repository was asked for
    // through that proxy alone.
    #[tokio::test]
    async fn the_trust_root_refresh_leaves_on_the_outbound_route() {
        install_default_rustls_provider();
        let proxy = weaver_tunnel::test_support::proxy::ProxyServerDouble::start(
            weaver_tunnel::transport::TransportKind::Socks5,
            None,
            std::collections::HashMap::new(),
        )
        .await;
        let runtime = crate::proxies::outbound_only_through(
            crate::proxies::ProxyKind::Socks5,
            proxy.addr.port(),
        );
        let error = refresh_trust_roots(runtime.clone()).await.unwrap_err();
        assert_eq!(
            error,
            "Sigstore trust root request failed on all permitted routes"
        );
        let targets = proxy.targets.lock().unwrap().clone();
        assert!(!targets.is_empty());
        assert!(
            targets
                .iter()
                .all(|target| *target == ("tuf-repo-cdn.sigstore.dev".to_owned(), 443))
        );
        runtime.stop_all().await;
    }
}
