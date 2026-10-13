use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use weaver_tunnel::bridge::Bridge;
use weaver_tunnel::pipe::{Dialer, Resolution};

use super::progress::{LegRecorder, StreamProvider};
use crate::security::{ResolvedFetchTarget, is_blocked_egress_ip};

// The test NZB may be no larger than this.
pub(crate) const NZB_LIMIT: usize = 16 * 1024 * 1024;
// Same-host redirects a speed-test GET follows.
const REDIRECT_LIMIT: usize = 3;

// One leg of the tested route, as an HTTP fetch sees it.
#[derive(Clone)]
pub(crate) struct HttpLeg {
    pub stage: Arc<dyn Dialer>,
    // The DNS servers of the proxies the leg exits through, for a ladder
    // that cannot resolve on its own.
    pub dns_servers: Vec<IpAddr>,
}

#[derive(Clone, Default)]
pub(crate) struct HttpOptions {
    pub allow_private_network: bool,
    // Trust only these roots. Empty uses the platform's; only tests set it.
    pub roots: Vec<Vec<u8>>,
}

// A response whose body is still to be read, with the path that carries it.
pub(crate) struct RoutedResponse {
    pub response: reqwest::Response,
    pub bridge: Arc<Bridge>,
    pub provider: Arc<StreamProvider>,
}

impl RoutedResponse {
    pub(crate) async fn close(self) {
        drop(self.response);
        self.bridge.revoke().await;
    }
}

// GET `url` through one leg, the way the RSS routed GET does: resolve on the
// leg, apply the egress block rules unless private destinations are allowed,
// pin the checked addresses, and send reqwest through the leg's bridge.
pub(crate) async fn routed_get(
    leg: &HttpLeg,
    url: &reqwest::Url,
    options: &HttpOptions,
    recorder: Option<LegRecorder>,
) -> Result<RoutedResponse, (Option<Arc<StreamProvider>>, String)> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err((None, "URL must use http or https".into()));
    }
    let host = url
        .host_str()
        .ok_or((None, "URL must include a host".to_string()))?
        .trim_matches(['[', ']'])
        .to_owned();
    let port = url
        .port_or_known_default()
        .ok_or((None, "URL has no port".to_string()))?;
    let addresses = match leg
        .stage
        .resolve(&host)
        .await
        .map_err(|e| (None, e.to_string()))?
    {
        Resolution::Addresses(addresses) => addresses,
        Resolution::Unresolved => {
            if host.parse::<IpAddr>().is_err() && leg.dns_servers.is_empty() {
                return Err((
                    None,
                    format!(
                        "the leg's proxies have no DNS servers, so {host} cannot be resolved through the route"
                    ),
                ));
            }
            let resolver = StreamProvider::new(leg.stage.clone(), None);
            weaver_tunnel::dns::resolve(resolver.as_ref(), &leg.dns_servers, &host)
                .await
                .map_err(|e| (None, format!("could not resolve {host}: {e}")))?
        }
    };
    if addresses.is_empty() {
        return Err((None, format!("no address resolved for {host}")));
    }
    if !options.allow_private_network && addresses.iter().any(|ip| is_blocked_egress_ip(*ip)) {
        return Err((
            None,
            format!("{host} resolves to an address that is not allowed"),
        ));
    }
    let provider = StreamProvider::new(leg.stage.clone(), recorder);
    provider.pin(&host, addresses.clone());
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret)
        .map_err(|_| (None, "could not generate bridge credentials".to_string()))?;
    let bridge = Bridge::start(
        &tokio::runtime::Handle::current(),
        provider.clone(),
        leg.stage.budget(),
        ("weaver".into(), hex::encode(secret)),
    )
    .map_err(|e| (Some(provider.clone()), e.to_string()))?;
    let fail = |message: String| (Some(provider.clone()), message);
    let addr = bridge.addr().map_err(|e| fail(e.to_string()))?;
    let same_host = host.clone();
    let mut builder = reqwest::Client::builder()
        .no_proxy()
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
            if matches!(url.scheme(), "http" | "https")
                && url.host_str().map(|h| h.trim_matches(['[', ']'])) == Some(same_host.as_str())
            {
                attempt.follow()
            } else {
                attempt.stop()
            }
        }))
        .proxy(
            reqwest::Proxy::all(format!("socks5h://{addr}"))
                .map_err(|e| fail(e.to_string()))?
                .basic_auth(bridge.credentials().0, bridge.credentials().1),
        );
    if !options.roots.is_empty() {
        let roots = options
            .roots
            .iter()
            .map(|der| reqwest::Certificate::from_der(der))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| fail(e.to_string()))?;
        builder = builder.tls_certs_only(roots);
    }
    let target = ResolvedFetchTarget {
        url: url.clone(),
        host: host.clone(),
        addrs: addresses
            .iter()
            .map(|ip| SocketAddr::new(*ip, port))
            .collect(),
    };
    let client = target
        .apply_dns_override(builder)
        .build()
        .map_err(|e| fail(e.to_string()))?;
    let response = match client.get(url.clone()).send().await {
        Ok(response) => response,
        Err(error) => {
            bridge.revoke().await;
            let message = if weaver_nntp::tls::is_tls_error(&error) {
                format!("TLS verification failed for {host}")
            } else {
                format!("GET {url} failed: {error}")
            };
            return Err(fail(message));
        }
    };
    if !response.status().is_success() {
        let status = response.status();
        drop(response);
        bridge.revoke().await;
        return Err(fail(format!("GET {url} answered HTTP {status}")));
    }
    Ok(RoutedResponse {
        response,
        bridge,
        provider,
    })
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
    let routed = routed_get(leg, url, options, None)
        .await
        .map_err(|(_, error)| format!("fetching the test NZB failed: {error}"))?;
    let body = read_capped(routed, NZB_LIMIT)
        .await
        .map_err(|error| format!("fetching the test NZB failed: {error}"))?;
    let nzb = weaver_nzb::parse_nzb(&body)
        .map_err(|error| format!("the test NZB is not readable: {error}"))?;
    let articles: Vec<TestArticle> = nzb
        .files
        .into_iter()
        .flat_map(|file| {
            let groups: Arc<[String]> = file.groups.into();
            file.segments.into_iter().map(move |segment| TestArticle {
                message_id: segment.message_id,
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
