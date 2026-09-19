//! Process-wide TLS and Sigstore trust setup for upgrade downloads.
//!
//! `application-updater` builds its HTTP client on reqwest's
//! `rustls-no-provider` feature, and `artifact-trust` refreshes the Sigstore
//! trust roots over the same stack, so both depend on the host installing the
//! process default crypto provider. Weaver's own TLS always passes a provider
//! explicitly, which is why nothing else in the workspace installs one.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{info, warn};

/// How long a successful refresh is trusted before the next one.
const TRUST_ROOT_REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
/// How long to wait after a failed refresh. Failure is not fatal: verification
/// falls back to the trust snapshot embedded in `artifact-trust`.
const TRUST_ROOT_RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Install the process default rustls crypto provider, once.
///
/// Weaver compiles rustls against aws-lc-rs everywhere, so that is the provider
/// installed here. Losing the race is expected — parallel tests and any earlier
/// caller install the same provider — so `AlreadyInstalled` is ignored.
pub fn install_default_rustls_provider() {
    static INSTALLED: OnceLock<()> = OnceLock::new();

    INSTALLED.get_or_init(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Start refreshing the Sigstore trust roots in the background.
///
/// Detached on purpose: an upgrade must be installable on a host that cannot
/// reach the Sigstore TUF repository, and startup must not wait on the network.
pub fn spawn_sigstore_trust_root_priming() -> JoinHandle<()> {
    spawn_trust_root_priming(refresh_trust_roots)
}

async fn refresh_trust_roots() -> Result<(), String> {
    artifact_trust::prime_sigstore_trust_roots()
        .await
        .map_err(|error| error.to_string())
}

fn spawn_trust_root_priming<Refresh, Refreshing>(refresh: Refresh) -> JoinHandle<()>
where
    Refresh: Fn() -> Refreshing + Send + 'static,
    Refreshing: Future<Output = Result<(), String>> + Send,
{
    install_default_rustls_provider();
    tokio::spawn(async move {
        loop {
            match refresh().await {
                Ok(()) => {
                    info!("sigstore trust roots refreshed");
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
}
