// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use std::time::Duration;

use weaver_tunnel::bridge::Bridge;
use weaver_tunnel::pipe::{DialError, Dialer};

use super::failure::SpeedTestFailure;
use super::progress::{LegRecorder, StreamProvider};
use crate::proxies::{AdmitError, Admitted, admit, pinned, start_bridge, through_bridge};

// The test NZB may be no larger than this.
pub(crate) const NZB_LIMIT: usize = 16 * 1024 * 1024;
// Same-host redirects a speed-test GET follows.
const REDIRECT_LIMIT: usize = 3;
// Names a PEM file of root certificates a speed test trusts beside the
// platform's: the way an operator whose traffic passes a TLS-intercepting
// proxy adds that proxy's root. Read each time a speed-test client is built.
pub(crate) const EXTRA_ROOTS_ENV: &str = "WEAVER_SPEED_TEST_EXTRA_ROOTS";

// One leg of the tested route, as an HTTP fetch sees it.
#[derive(Clone)]
pub(crate) struct HttpLeg {
    pub stage: Arc<dyn Dialer>,
}

#[derive(Clone, Default)]
pub(crate) struct HttpOptions {
    pub allow_private_network: bool,
    // Trust only these roots; only tests set it. Empty uses the platform's
    // and any named by `EXTRA_ROOTS_ENV`.
    pub roots: Vec<Vec<u8>>,
}

// A response whose body is still to be read, with the bridge that carries it.
pub(crate) struct RoutedResponse {
    pub response: reqwest::Response,
    pub bridge: Arc<Bridge>,
}

impl RoutedResponse {
    pub(crate) async fn close(self) {
        drop(self.response);
        self.bridge.revoke().await;
    }
}

// GET `url` once through one leg.
pub(crate) async fn routed_get(
    leg: &HttpLeg,
    url: &reqwest::Url,
    options: &HttpOptions,
) -> Result<RoutedResponse, String> {
    let client = RoutedClient::connect(leg, url, options, None)
        .await
        .map_err(|(_, error)| error.to_string())?;
    match client.get(url).await {
        Ok(response) => Ok(RoutedResponse {
            response,
            bridge: client.bridge,
        }),
        Err(error) => {
            client.bridge.revoke().await;
            Err(error.to_string())
        }
    }
}

// A client whose GETs all go through one leg, over one bridge that stays up
// until the client is closed.
pub(crate) struct RoutedClient {
    client: reqwest::Client,
    host: String,
    pub bridge: Arc<Bridge>,
    pub provider: Arc<StreamProvider>,
}

impl RoutedClient {
    // Set up the way every routed fetch is: resolve on the leg, apply the
    // egress block rules unless private destinations are allowed, pin the
    // checked addresses, and point reqwest at the leg's bridge.
    pub(crate) async fn connect(
        leg: &HttpLeg,
        url: &reqwest::Url,
        options: &HttpOptions,
        recorder: Option<LegRecorder>,
    ) -> Result<Self, (Option<Arc<StreamProvider>>, SpeedTestFailure)> {
        let provider = StreamProvider::new(leg.stage.clone(), recorder);
        let admitted = admit(provider.as_ref(), url, options.allow_private_network)
            .await
            .map_err(|error| {
                let host = url
                    .host_str()
                    .unwrap_or_default()
                    .trim_matches(['[', ']'])
                    .to_owned();
                let failure = match error {
                    AdmitError::Url(message) => message.into(),
                    // The local resolver found nothing for the host.
                    AdmitError::Resolve(DialError::Destination(error)) => {
                        SpeedTestFailure::Unresolved {
                            host,
                            reason: Some(error.to_string()),
                        }
                    }
                    AdmitError::Resolve(error) => error.to_string().into(),
                    AdmitError::Empty => SpeedTestFailure::Unresolved { host, reason: None },
                    AdmitError::Private => SpeedTestFailure::AddressNotAllowed { host },
                };
                (None, failure)
            })?;
        let bridge = start_bridge(provider.clone())
            .map_err(|error| (Some(provider.clone()), error.into()))?;
        match Self::build(&bridge, &admitted, options) {
            Ok(client) => Ok(Self {
                client,
                host: admitted.host,
                bridge,
                provider,
            }),
            Err(error) => {
                bridge.revoke().await;
                Err((Some(provider), error.into()))
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the client rides the leg's bridge"
    )]
    fn build(
        bridge: &Bridge,
        admitted: &Admitted,
        options: &HttpOptions,
    ) -> Result<reqwest::Client, String> {
        let same_host = admitted.host.clone();
        let mut builder = reqwest::Client::builder()
            .user_agent(concat!("weaver/", env!("CARGO_PKG_VERSION")))
            .read_timeout(Duration::from_secs(30))
            // Ask for the body as stored: a compressed transfer would count
            // decoded bytes, not the bytes the route carried.
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() > REDIRECT_LIMIT {
                    return attempt.error("too many redirects");
                }
                let url = attempt.url();
                if url.scheme() == "http"
                    && attempt
                        .previous()
                        .iter()
                        .any(|previous| previous.scheme() == "https")
                {
                    return attempt.error("HTTPS speed-test redirects must remain HTTPS");
                }
                if matches!(url.scheme(), "http" | "https")
                    && url.host_str().map(|h| h.trim_matches(['[', ']']))
                        == Some(same_host.as_str())
                {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }));
        builder = through_bridge(builder, bridge)?;
        if !options.roots.is_empty() {
            let roots = options
                .roots
                .iter()
                .map(|der| reqwest::Certificate::from_der(der))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            builder = builder.tls_certs_only(roots);
        } else {
            let extra = extra_roots(std::env::var_os(EXTRA_ROOTS_ENV).as_deref())?;
            if !extra.is_empty() {
                builder = builder.tls_certs_merge(extra);
            }
        }
        pinned(builder, &admitted.host, admitted.port, &admitted.addresses)
            .build()
            .map_err(|e| e.to_string())
    }

    // One GET, refused unless it answers with a success status.
    pub(crate) async fn get(
        &self,
        url: &reqwest::Url,
    ) -> Result<reqwest::Response, SpeedTestFailure> {
        let response = self.client.get(url.clone()).send().await.map_err(|error| {
            if weaver_nntp::tls::is_tls_error(&error) {
                SpeedTestFailure::TlsVerification {
                    host: self.host.clone(),
                }
            } else {
                format!("GET {url} failed: {error}").into()
            }
        })?;
        if !response.status().is_success() {
            return Err(SpeedTestFailure::answered(url, response.status()));
        }
        Ok(response)
    }

    // One POST of `body`, refused unless it answers with a success status.
    pub(crate) async fn post(
        &self,
        url: &reqwest::Url,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, SpeedTestFailure> {
        let response = self
            .client
            .post(url.clone())
            .body(body)
            .send()
            .await
            .map_err(|error| {
                if weaver_nntp::tls::is_tls_error(&error) {
                    SpeedTestFailure::TlsVerification {
                        host: self.host.clone(),
                    }
                } else {
                    format!("POST {url} failed: {error}").into()
                }
            })?;
        if !response.status().is_success() {
            return Err(SpeedTestFailure::answered(url, response.status()));
        }
        Ok(response)
    }

    pub(crate) async fn close(self) {
        drop(self.client);
        self.bridge.revoke().await;
    }
}

// The roots the PEM file at `path` holds, each checked to be a certificate a
// TLS client can anchor on. No path trusts the platform's roots alone; a path
// that cannot be read, or holds no certificate or a broken one, is refused.
pub(crate) fn extra_roots(
    path: Option<&std::ffi::OsStr>,
) -> Result<Vec<reqwest::Certificate>, String> {
    use tokio_rustls::rustls::RootCertStore;
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::pem::PemObject;

    let Some(path) = path.filter(|path| !path.is_empty()) else {
        return Ok(Vec::new());
    };
    let shown = std::path::Path::new(path).display();
    let refused = |reason: String| format!("{EXTRA_ROOTS_ENV} file {shown}: {reason}");
    let pem = std::fs::read(path).map_err(|error| refused(format!("cannot be read: {error}")))?;
    let ders = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| refused(format!("is not readable PEM: {error}")))?;
    if ders.is_empty() {
        return Err(refused("holds no certificate".into()));
    }
    let mut store = RootCertStore::empty();
    ders.iter()
        .map(|der| {
            store.add(der.clone()).map_err(|error| {
                refused(format!("holds a certificate that is not usable: {error}"))
            })?;
            reqwest::Certificate::from_der(der).map_err(|error| refused(error.to_string()))
        })
        .collect()
}

// Read a small body whole, refusing one larger than `limit`.
pub(crate) async fn read_capped(
    mut routed: RoutedResponse,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if routed
        .response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        routed.close().await;
        return Err(format!("the body is larger than {limit} bytes"));
    }
    let mut body = Vec::new();
    loop {
        match routed.response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > limit {
                    routed.close().await;
                    return Err(format!("the body is larger than {limit} bytes"));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(error) => {
                routed.close().await;
                return Err(format!("reading the body failed: {error}"));
            }
        }
    }
    routed.close().await;
    Ok(body)
}

// One article of the test NZB.
#[derive(Debug, Clone)]
pub(crate) struct TestArticle {
    pub message_id: String,
    pub groups: Arc<[String]>,
    pub bytes: u32,
}

// Fetch and parse the test NZB through one leg. Nothing is persisted.
pub(crate) async fn fetch_test_nzb(
    leg: &HttpLeg,
    url: &reqwest::Url,
    options: &HttpOptions,
) -> Result<Vec<TestArticle>, String> {
    let routed = routed_get(leg, url, options)
        .await
        .map_err(|error| format!("fetching the test NZB failed: {error}"))?;
    let body = read_capped(routed, NZB_LIMIT)
        .await
        .map_err(|error| format!("fetching the test NZB failed: {error}"))?;
    parse_test_nzb(&body)
}

// The articles of a test NZB, read the same way whether it was fetched or
// uploaded.
pub(crate) fn parse_test_nzb(body: &[u8]) -> Result<Vec<TestArticle>, String> {
    let nzb = weaver_nzb::parse_nzb(body)
        .map_err(|error| format!("the test NZB is not readable: {error}"))?;
    let articles: Vec<TestArticle> = nzb
        .files
        .into_iter()
        .flat_map(|file| {
            let groups: Arc<[String]> = file.groups.into();
            file.segments.into_iter().map(move |segment| TestArticle {
                message_id: segment.message_id().to_string(),
                groups: groups.clone(),
                bytes: segment.bytes,
            })
        })
        .collect();
    if articles.is_empty() {
        return Err("the test NZB lists no articles".into());
    }
    Ok(articles)
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
