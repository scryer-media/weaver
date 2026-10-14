use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use weaver_tunnel::{
    TunnelProvider,
    bridge::Bridge,
    pipe::{DialError, Resolution},
};

use super::{Consumer, ProxyRuntime};
use crate::security::{ResolvedFetchTarget, is_blocked_egress_ip, resolve_fetch_target};

// One concrete path an HTTP fetch can take: it resolves names the way its
// exit does, and dials the addresses the fetch pinned for a name.
#[async_trait::async_trait]
pub trait FetchPath: TunnelProvider {
    // The addresses `host` is reached at, as the path's exit resolves it, or
    // `Unresolved` when the exit proxy is handed the name.
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError>;
    // Pins the checked addresses a dial of `host` uses.
    fn pin(&self, host: &str, addresses: Vec<IpAddr>);
    fn budget(&self) -> Duration;
    // A response that came over this path failed while its body was read.
    fn body_failed(&self, _error: &DialError) {}
}

pub struct FetchOptions {
    // Names the fetch in its errors.
    pub label: &'static str,
    // Bounds one route attempt, every redirect included.
    pub timeout: Duration,
    pub redirects: usize,
    pub user_agent: &'static str,
    pub https_only: bool,
    pub allow_private_network: bool,
}

// Why a path refused a destination before any request went out.
pub(crate) enum AdmitError {
    Url(String),
    // The path's resolver failed.
    Resolve(DialError),
    // The path's resolver found no address.
    Empty,
    // The host resolves to an address private destinations are refused at.
    Private,
}

pub(crate) struct Admitted {
    pub host: String,
    pub port: u16,
    // Empty when the exit proxy resolves the name.
    pub addresses: Vec<IpAddr>,
}

// Resolves the URL's host the way `path` will reach it, refuses a private
// address unless allowed, and pins what it checked on the path. An address
// named in the URL is always checked, whoever resolves names.
pub(crate) async fn admit(
    path: &dyn FetchPath,
    url: &reqwest::Url,
    allow_private_network: bool,
) -> Result<Admitted, AdmitError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AdmitError::Url("URL must use http or https".into()));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AdmitError::Url("URL must include a host".into()))?
        .trim_matches(['[', ']'])
        .to_owned();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| AdmitError::Url("invalid URL port".into()))?;
    let addresses = match path.resolve(&host).await.map_err(AdmitError::Resolve)? {
        Resolution::Addresses(addresses) => {
            if addresses.is_empty() {
                return Err(AdmitError::Empty);
            }
            if !allow_private_network && addresses.iter().any(|ip| is_blocked_egress_ip(*ip)) {
                return Err(AdmitError::Private);
            }
            path.pin(&host, addresses.clone());
            addresses
        }
        Resolution::Unresolved => Vec::new(),
    };
    Ok(Admitted {
        host,
        port,
        addresses,
    })
}

// A local SOCKS bridge onto `path`, up until it is dropped or revoked.
pub(crate) fn start_bridge(path: Arc<dyn FetchPath>) -> Result<Arc<Bridge>, String> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
    let budget = path.budget();
    Bridge::start(
        &tokio::runtime::Handle::current(),
        path,
        budget,
        ("weaver".into(), hex::encode(secret)),
    )
    .map_err(|e| e.to_string())
}

// Sends every request of `builder` through `bridge`. reqwest hands the bridge
// the hostname, for Host and SNI; the path dials what it pinned for that
// name, or hands the name to its exit.
pub(crate) fn through_bridge(
    builder: reqwest::ClientBuilder,
    bridge: &Bridge,
) -> Result<reqwest::ClientBuilder, String> {
    let addr = bridge.addr().map_err(|e| e.to_string())?;
    let (user, password) = bridge.credentials();
    Ok(builder.no_proxy().proxy(
        reqwest::Proxy::all(format!("socks5h://{addr}"))
            .map_err(|e| e.to_string())?
            .basic_auth(user, password),
    ))
}

// Pins `host` to the checked `addresses` for a client that dials it itself.
pub(crate) fn pinned(
    builder: reqwest::ClientBuilder,
    url: &reqwest::Url,
    host: &str,
    port: u16,
    addresses: &[IpAddr],
) -> reqwest::ClientBuilder {
    ResolvedFetchTarget {
        url: url.clone(),
        host: host.to_owned(),
        addrs: addresses
            .iter()
            .map(|ip| SocketAddr::new(*ip, port))
            .collect(),
    }
    .apply_dns_override(builder)
}

// The path a routed response came over, and the bridge that carries its
// body, held until the response is dropped.
#[derive(Clone)]
pub struct RoutedBody {
    path: Arc<dyn FetchPath>,
    label: &'static str,
    _bridge: Arc<Bridge>,
}

impl RoutedBody {
    pub fn failed(&self) {
        self.path
            .body_failed(&DialError::Destination(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("{} response transport failed", self.label),
            )));
    }
}

// Reads a whole body, refusing one larger than `limit`. A body that breaks
// off counts against the path it came over.
pub async fn read_body(mut response: reqwest::Response, limit: u64) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(format!("response exceeds {limit} bytes"));
    }
    let route = response.extensions().get::<RoutedBody>().cloned();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        // reqwest labels every frame-read failure as Decode, including a
        // truncated TCP response. Parsing happens after this complete read.
        if let Some(route) = &route {
            route.failed();
        }
        "body read failed".to_string()
    })? {
        let next_len = body.len().saturating_add(chunk.len());
        if next_len as u64 > limit {
            return Err(format!("response exceeds {limit} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Debug)]
pub(crate) enum AttemptError {
    // The path failed to carry the request.
    Route,
    Routed(Arc<DialError>),
    // The destination is refused whichever path it is reached over.
    Destination(String),
    // The path carried a response, but the fetch cannot go on from it.
    Answered(String),
}

// GETs `url` for `consumer` on the first of its route's paths that carries
// it, in the order the route tries them, following redirects on that path.
// No runtime means a direct fetch. `request` dresses each hop's request; it
// is handed the hop's URL and its index.
pub async fn routed_fetch<F>(
    runtime: Option<&ProxyRuntime>,
    consumer: Consumer,
    url: &reqwest::Url,
    options: &FetchOptions,
    request: F,
) -> Result<reqwest::Response, String>
where
    F: Fn(reqwest::RequestBuilder, &reqwest::Url, usize) -> reqwest::RequestBuilder + Sync,
{
    let label = options.label;
    let Some(runtime) = runtime else {
        return match tokio::time::timeout(options.timeout, fetch_on(None, url, options, &request))
            .await
        {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(AttemptError::Destination(message) | AttemptError::Answered(message))) => {
                Err(message)
            }
            _ => Err(format!("{label} request failed")),
        };
    };
    let status = runtime.route(consumer, Duration::from_secs(30))?;
    let attempts = runtime.network.fetch_attempts(consumer, status)?;
    for attempt in attempts {
        let path: Arc<dyn FetchPath> = attempt.clone();
        let result = tokio::select! {
            biased;
            _ = attempt.cancelled() => continue,
            result = tokio::time::timeout(
                options.timeout,
                fetch_on(Some(&path), url, options, &request),
            ) => result,
        };
        if let Some(error) = attempt.take_transport_error() {
            attempt.report(Some(&error));
            if matches!(error.as_ref(), DialError::Fatal(_)) {
                return Err(error.to_string());
            }
            continue;
        }
        match result {
            Ok(Ok(response)) => {
                attempt.report(None);
                return Ok(response);
            }
            Ok(Err(AttemptError::Answered(message))) => {
                attempt.report(None);
                return Err(message);
            }
            Ok(Err(AttemptError::Destination(message))) => return Err(message),
            Ok(Err(AttemptError::Routed(error))) => {
                attempt.report(Some(&error));
                if matches!(error.as_ref(), DialError::Fatal(_)) {
                    return Err(error.to_string());
                }
            }
            Ok(Err(AttemptError::Route)) => {
                attempt.report(Some(&DialError::Egress(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    format!("{label} transport or routed DNS failed"),
                ))))
            }
            Err(_) => attempt.report(Some(&DialError::Timeout {
                stage: format!("{label} route"),
            })),
        }
    }
    Err(format!("{label} request failed on all permitted routes"))
}

const REDIRECT_STATUSES: [u16; 5] = [301, 302, 303, 307, 308];

// One attempt: every hop of the fetch on `path`, or direct without one. Each
// redirect is resolved and checked through the same path before it is
// followed.
pub(crate) async fn fetch_on<F>(
    path: Option<&Arc<dyn FetchPath>>,
    url: &reqwest::Url,
    options: &FetchOptions,
    request: &F,
) -> Result<reqwest::Response, AttemptError>
where
    F: Fn(reqwest::RequestBuilder, &reqwest::Url, usize) -> reqwest::RequestBuilder + Sync,
{
    let label = options.label;
    let bridge = path
        .map(|path| start_bridge(path.clone()))
        .transpose()
        .map_err(|_| AttemptError::Route)?;
    let mut current = url.clone();
    for hop in 0..=options.redirects {
        if options.https_only && current.scheme() != "https" {
            return Err(AttemptError::Destination(format!(
                "{label} URL must use https"
            )));
        }
        let builder = reqwest::Client::builder()
            .no_proxy()
            .timeout(options.timeout)
            .user_agent(options.user_agent)
            .redirect(reqwest::redirect::Policy::none())
            .https_only(options.https_only)
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .zstd(true);
        let builder = match (path, &bridge) {
            (Some(path), Some(bridge)) => {
                let admitted = admit(path.as_ref(), &current, options.allow_private_network)
                    .await
                    .map_err(|error| match error {
                        AdmitError::Url(message) => AttemptError::Destination(message),
                        AdmitError::Resolve(error) => AttemptError::Routed(Arc::new(error)),
                        AdmitError::Empty => AttemptError::Route,
                        AdmitError::Private => AttemptError::Destination(format!(
                            "{label} destination address is not allowed"
                        )),
                    })?;
                let builder = through_bridge(builder, bridge).map_err(|_| AttemptError::Route)?;
                pinned(
                    builder,
                    &current,
                    &admitted.host,
                    admitted.port,
                    &admitted.addresses,
                )
            }
            _ => resolve_fetch_target(&current, options.allow_private_network)
                .await
                .map_err(AttemptError::Destination)?
                .apply_dns_override(builder),
        };
        let client = builder.build().map_err(|_| AttemptError::Route)?;
        let mut response = request(client.get(current.clone()), &current, hop)
            .send()
            .await
            .map_err(|error| {
                if weaver_nntp::tls::is_tls_error(&error) {
                    AttemptError::Destination(format!("{label} TLS verification failed"))
                } else {
                    AttemptError::Route
                }
            })?;
        if !REDIRECT_STATUSES.contains(&response.status().as_u16()) {
            if let (Some(path), Some(bridge)) = (path, bridge) {
                response.extensions_mut().insert(RoutedBody {
                    path: path.clone(),
                    label,
                    _bridge: bridge,
                });
            }
            return Ok(response);
        }
        if hop == options.redirects {
            break;
        }
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| AttemptError::Answered("redirect missing Location".into()))?;
        current = current
            .join(location)
            .map_err(|e| AttemptError::Answered(format!("invalid redirect: {e}")))?;
    }
    Err(AttemptError::Answered("too many redirects".into()))
}

#[cfg(test)]
#[path = "routed_http_tests.rs"]
mod tests;
