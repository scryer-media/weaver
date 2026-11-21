// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use weaver_tunnel::{
    TunnelError, TunnelProvider, TunnelStream,
    bridge::{Bridge, ConnectionOutcome},
    pipe::{DialError, Resolution},
};

use super::{Consumer, FetchAttempt, ProxyRuntime};
use weaver_server_base::security::is_blocked_egress_ip;

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
    // A response that came over this path failed while its body was read:
    // the destination's failure, not the path's.
    fn body_failed(&self) {}
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
pub enum AdmitError {
    Url(String),
    // The path's resolver failed.
    Resolve(DialError),
    // The path's resolver found no address.
    Empty,
    // The host resolves to an address private destinations are refused at.
    Private,
}

pub struct Admitted {
    pub host: String,
    pub port: u16,
    // Empty when the exit proxy resolves the name.
    pub addresses: Vec<IpAddr>,
}

// Resolves the URL's host the way `path` will reach it, refuses a private
// address unless allowed, and pins what it checked on the path. An address
// named in the URL is always checked, whoever resolves names.
pub async fn admit(
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
    let addresses = check_host(path, &host, allow_private_network).await?;
    Ok(Admitted {
        host,
        port,
        addresses,
    })
}

async fn check_host(
    path: &dyn FetchPath,
    host: &str,
    allow_private_network: bool,
) -> Result<Vec<IpAddr>, AdmitError> {
    match path.resolve(host).await.map_err(AdmitError::Resolve)? {
        Resolution::Addresses(addresses) => {
            if addresses.is_empty() {
                return Err(AdmitError::Empty);
            }
            if !allow_private_network && addresses.iter().any(|ip| is_blocked_egress_ip(*ip)) {
                return Err(AdmitError::Private);
            }
            path.pin(host, addresses.clone());
            Ok(addresses)
        }
        Resolution::Unresolved => Ok(Vec::new()),
    }
}

// A local SOCKS bridge onto `path`, up until it is dropped or revoked.
pub fn start_bridge(path: Arc<dyn FetchPath>) -> Result<Arc<Bridge>, String> {
    let budget = path.budget();
    bridge_onto(path, budget)
}

// A bridge onto `path`, with the path itself: a client pins on the path
// what it checked for a name, then hands the bridge the name, so whichever
// leg of the path dials is held to the checked addresses.
#[derive(Clone)]
pub struct PathBridge {
    pub bridge: Arc<Bridge>,
    pub path: Arc<dyn FetchPath>,
    // What keeps the bridge up: every client built on it holds this, so a
    // bridge that is let go once nothing holds it outlives its clients.
    pub keep: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl PathBridge {
    pub fn onto(path: Arc<dyn FetchPath>) -> Result<Self, String> {
        Ok(Self {
            bridge: start_bridge(path.clone())?,
            path,
            keep: None,
        })
    }
    pub async fn revoke(&self) {
        self.bridge.revoke().await;
    }
}

pub fn bridge_onto(
    provider: Arc<dyn TunnelProvider>,
    budget: Duration,
) -> Result<Arc<Bridge>, String> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|_| "could not generate bridge credentials")?;
    Bridge::start(
        &tokio::runtime::Handle::current(),
        provider,
        budget,
        ("weaver".into(), hex::encode(secret)),
    )
    .map_err(|e| e.to_string())
}

// A path for a client that follows redirects itself: each name is resolved
// and checked through the path, and pinned there, before it is dialed.
pub struct Checked {
    pub path: Arc<dyn FetchPath>,
    pub label: &'static str,
    pub allow_private_network: bool,
    // The path failed to resolve a name, which is evidence against the path.
    pub failure: std::sync::Mutex<Option<Arc<DialError>>>,
}

impl Checked {
    fn take_failure(&self) -> Option<Arc<DialError>> {
        self.failure.lock().expect("checked path failure").take()
    }
    fn fail(&self, error: DialError) -> TunnelError {
        let message = error.to_string();
        *self.failure.lock().expect("checked path failure") = Some(Arc::new(error));
        TunnelError::Engine(message)
    }
}

#[async_trait::async_trait]
impl TunnelProvider for Checked {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        observed: Arc<ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let label = self.label;
        let name = host.trim_matches(['[', ']']);
        match check_host(self.path.as_ref(), name, self.allow_private_network).await {
            Ok(_) => {}
            Err(AdmitError::Resolve(error)) => return Err(self.fail(error)),
            Err(AdmitError::Empty) => {
                return Err(self.fail(DialError::Egress(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    format!("{label} transport or routed DNS failed"),
                ))));
            }
            Err(AdmitError::Private) => {
                return Err(TunnelError::Engine(format!(
                    "{label} destination address is not allowed"
                )));
            }
            Err(AdmitError::Url(message)) => return Err(TunnelError::Engine(message)),
        }
        self.path.dial_observed(host, port, observed).await
    }
    fn describe(&self) -> String {
        self.path.describe()
    }
}

// Runs `operation` with a client whose every connection goes over one path
// of `consumer`'s route, trying the paths in the order the route tries them.
// The client follows redirects itself, and each name it dials is resolved
// and checked through the path first. The operation's own result ends the
// walk unless the path failed to carry it.
pub async fn with_routed_client<T, E, F, Fut>(
    runtime: &ProxyRuntime,
    consumer: Consumer,
    label: &'static str,
    client: impl Fn() -> reqwest::ClientBuilder,
    operation: F,
) -> Result<Result<T, E>, String>
where
    F: Fn(reqwest::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let status = runtime.route(consumer, Duration::from_secs(30))?;
    let attempts = runtime.network.fetch_attempts(consumer, status)?;
    for attempt in attempts {
        // A leg the route stopped running carries nothing more.
        if !attempt.leg_current() {
            continue;
        }
        let checked = Arc::new(Checked {
            path: attempt.clone(),
            label,
            allow_private_network: runtime.allow_private_network(),
            failure: Default::default(),
        });
        #[expect(
            clippy::disallowed_methods,
            reason = "the client rides the attempt's bridge"
        )]
        let built = bridge_onto(checked.clone(), attempt.budget()).and_then(|bridge| {
            let client = through_bridge(client(), &bridge)?
                .build()
                .map_err(|e| e.to_string())?;
            Ok((bridge, client))
        });
        let Ok((_bridge, client)) = built else {
            attempt.report(Some(&DialError::Egress(std::io::Error::other(format!(
                "{label} route bridge failed"
            )))));
            continue;
        };
        let result = tokio::select! {
            biased;
            _ = attempt.cancelled() => continue,
            result = operation(client) => result,
        };
        let failure = checked
            .take_failure()
            .or_else(|| attempt.take_transport_error());
        match (result, failure) {
            (Err(_), Some(error)) => {
                attempt.report(Some(&error));
                if matches!(error.as_ref(), DialError::Fatal(_)) {
                    return Err(error.to_string());
                }
            }
            (result, _) => {
                attempt.report(None);
                return Ok(result);
            }
        }
    }
    Err(format!("{label} request failed on all permitted routes"))
}

// Sends every request of `builder` through `bridge`. reqwest hands the bridge
// the hostname, for Host and SNI; the path dials what it pinned for that
// name, or hands the name to its exit.
pub fn through_bridge(
    builder: reqwest::ClientBuilder,
    bridge: &Bridge,
) -> Result<reqwest::ClientBuilder, String> {
    bridged(builder, bridge, "socks5h")
}

fn bridged(
    builder: reqwest::ClientBuilder,
    bridge: &Bridge,
    scheme: &str,
) -> Result<reqwest::ClientBuilder, String> {
    let addr = bridge.addr().map_err(|e| e.to_string())?;
    let (user, password) = bridge.credentials();
    Ok(builder.no_proxy().proxy(
        reqwest::Proxy::all(format!("{scheme}://{addr}"))
            .map_err(|e| e.to_string())?
            .basic_auth(user, password),
    ))
}

// Pins `host` to the checked `addresses` for a client that dials it itself.
pub fn pinned(
    builder: reqwest::ClientBuilder,
    host: &str,
    port: u16,
    addresses: &[IpAddr],
) -> reqwest::ClientBuilder {
    // No addresses: the path's exit is handed the name.
    if host.parse::<IpAddr>().is_ok() || addresses.is_empty() {
        return builder;
    }
    let addresses: Vec<SocketAddr> = addresses
        .iter()
        .map(|ip| SocketAddr::new(*ip, port))
        .collect();
    builder.resolve_to_addrs(host, &addresses)
}

// The path a routed response came over, and the bridge that carries its
// body, held until the response is dropped.
#[derive(Clone)]
pub struct RoutedBody {
    path: Arc<dyn FetchPath>,
    _bridge: Arc<Bridge>,
}

impl RoutedBody {
    pub fn failed(&self) {
        self.path.body_failed();
    }
}

// Reads a whole body, refusing one larger than `limit`. A body that breaks
// off fails the read only: the path carried it.
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
pub enum AttemptError {
    // The path failed to carry the request.
    Route,
    Routed(Arc<DialError>),
    // The request failed once it was handed to the client, before its
    // response came: the path's failure unless the attempt's dial went
    // through it. `destination` is the hop's scheme, host and port.
    Sent { destination: String, cause: String },
    // The attempt's own time ran out on a hop to `destination`, its scheme,
    // host and port: the path's unless the attempt's dial went through it,
    // as for a request that fails once handed over.
    TimedOut { destination: String },
    // The destination is refused whichever path it is reached over.
    Destination(String),
    // The path carried a response, but the fetch cannot go on from it.
    Answered(String),
}

// GETs `url` for `consumer` on the first of its route's paths that carries
// it, in the order the route tries them, following redirects on that path.
// `request` dresses each hop's request; it is handed the hop's URL and its
// index.
pub async fn routed_fetch<F>(
    runtime: &ProxyRuntime,
    consumer: Consumer,
    url: &reqwest::Url,
    options: &FetchOptions,
    request: F,
) -> Result<reqwest::Response, String>
where
    F: Fn(reqwest::RequestBuilder, &reqwest::Url, usize) -> reqwest::RequestBuilder + Sync,
{
    let label = options.label;
    let status = runtime.route(consumer, Duration::from_secs(30))?;
    let attempts = runtime.network.fetch_attempts(consumer, status)?;
    // What the destination failed a request of, kept for when no later
    // attempt carries the fetch.
    let mut refused = None;
    // The last attempt whose path carried a request the destination failed.
    // The rest of its rung, such as the other members of a pool, would only
    // ask the same destination again from another exit.
    let mut spent: Option<Arc<FetchAttempt>> = None;
    for attempt in attempts {
        // A leg the route stopped running carries nothing more.
        if !attempt.leg_current()
            || spent
                .as_ref()
                .is_some_and(|spent| spent.same_rung(&attempt))
        {
            continue;
        }
        let path: Arc<dyn FetchPath> = attempt.clone();
        // Each attempt has the whole of the fetch's timeout: one that runs
        // out on a rung leaves the next rung its own.
        let result = tokio::select! {
            biased;
            _ = attempt.cancelled() => continue,
            result = fetch_on(&path, url, options, &request) => result,
        };
        let verdict = judge(
            result,
            attempt.take_transport_error(),
            attempt.carried(),
            label,
        );
        match verdict.report {
            Report::Nothing => {}
            Report::Answered => attempt.report(None),
            Report::Carried(error) => {
                attempt.request_failed();
                refused.get_or_insert(error);
                spent = Some(attempt.clone());
            }
            Report::Failed(error) => attempt.report(Some(&error)),
        }
        if let Some(end) = verdict.end {
            return end;
        }
    }
    Err(refused.unwrap_or_else(|| format!("{label} request failed on all permitted routes")))
}

// What one attempt's outcome says about its path.
#[derive(Debug)]
pub enum Report {
    // Nothing: the fetch is refused whichever path carries it.
    Nothing,
    // The path carried the fetch to an answer.
    Answered,
    // The path carried a request its destination failed. The error is the
    // fetch's when no later attempt carries it.
    Carried(String),
    // The path failed to carry the fetch.
    Failed(Arc<DialError>),
}

pub struct Verdict {
    pub report: Report,
    // The fetch's result, when the outcome ends the walk.
    pub end: Option<Result<reqwest::Response, String>>,
}

// The one owner of what an attempt's outcome means. `transport` is the
// failure of the attempt's own dial or lookup, if one failed; `carried`
// says whether its latest dial went through the path.
pub fn judge(
    result: Result<reqwest::Response, AttemptError>,
    transport: Option<Arc<DialError>>,
    carried: bool,
    label: &'static str,
) -> Verdict {
    let failed = |error: Arc<DialError>| Verdict {
        end: matches!(error.as_ref(), DialError::Fatal(_)).then(|| Err(error.to_string())),
        report: Report::Failed(error),
    };
    if let Some(error) = transport {
        return failed(error);
    }
    match result {
        Ok(response) => Verdict {
            report: Report::Answered,
            end: Some(Ok(response)),
        },
        Err(AttemptError::Answered(message)) => Verdict {
            report: Report::Answered,
            end: Some(Err(message)),
        },
        Err(AttemptError::Destination(message)) => Verdict {
            report: Report::Nothing,
            end: Some(Err(message)),
        },
        Err(AttemptError::Routed(error)) => failed(error),
        // The path carried the connection, and the destination failed the
        // request on it, as when a body breaks off: no evidence about the
        // path, so the next path is tried.
        Err(AttemptError::Sent { destination, cause }) if carried => {
            tracing::warn!(
                fetch = label,
                %destination,
                error = %cause,
                "request failed after its route connected"
            );
            Verdict {
                report: Report::Carried(format!(
                    "{label} request failed after its route connected"
                )),
                end: None,
            }
        }
        // The path carried the connection, and the destination did not
        // answer in the attempt's time: the destination's, as above.
        Err(AttemptError::TimedOut { destination }) if carried => {
            tracing::warn!(
                fetch = label,
                %destination,
                "request timed out after its route connected"
            );
            Verdict {
                report: Report::Carried(format!(
                    "{label} request timed out after its route connected"
                )),
                end: None,
            }
        }
        Err(AttemptError::Route | AttemptError::Sent { .. }) => {
            failed(Arc::new(DialError::Egress(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("{label} transport or routed DNS failed"),
            ))))
        }
        // The time ran out before the attempt's dial went through: in the
        // lookup, the proxy, the tunnel or the connect.
        Err(AttemptError::TimedOut { .. }) => failed(Arc::new(DialError::Timeout {
            stage: format!("{label} route"),
        })),
    }
}

const REDIRECT_STATUSES: [u16; 5] = [301, 302, 303, 307, 308];

// One attempt: every hop of the fetch on `path`, within the options'
// timeout. Each redirect is resolved and checked through the same path
// before it is followed.
pub async fn fetch_on<F>(
    path: &Arc<dyn FetchPath>,
    url: &reqwest::Url,
    options: &FetchOptions,
    request: &F,
) -> Result<reqwest::Response, AttemptError>
where
    F: Fn(reqwest::RequestBuilder, &reqwest::Url, usize) -> reqwest::RequestBuilder + Sync,
{
    // The hop under way, as scheme, host and port, for when time runs out.
    let at = std::sync::Mutex::new(scheme_host_port(url));
    tokio::time::timeout(options.timeout, hops(path, url, options, request, &at))
        .await
        .unwrap_or_else(|_| {
            Err(AttemptError::TimedOut {
                destination: std::mem::take(&mut *at.lock().expect("fetch hop")),
            })
        })
}

async fn hops<F>(
    path: &Arc<dyn FetchPath>,
    url: &reqwest::Url,
    options: &FetchOptions,
    request: &F,
    at: &std::sync::Mutex<String>,
) -> Result<reqwest::Response, AttemptError>
where
    F: Fn(reqwest::RequestBuilder, &reqwest::Url, usize) -> reqwest::RequestBuilder + Sync,
{
    let label = options.label;
    let bridge = start_bridge(path.clone()).map_err(|_| AttemptError::Route)?;
    let mut current = url.clone();
    for hop in 0..=options.redirects {
        *at.lock().expect("fetch hop") = scheme_host_port(&current);
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
        let admitted = admit(path.as_ref(), &current, options.allow_private_network)
            .await
            .map_err(|error| match error {
                AdmitError::Url(message) => AttemptError::Destination(message),
                AdmitError::Resolve(error) => AttemptError::Routed(Arc::new(error)),
                AdmitError::Empty => AttemptError::Route,
                AdmitError::Private => {
                    AttemptError::Destination(format!("{label} destination address is not allowed"))
                }
            })?;
        let builder = through_bridge(builder, &bridge).map_err(|_| AttemptError::Route)?;
        let builder = pinned(builder, &admitted.host, admitted.port, &admitted.addresses);
        #[expect(
            clippy::disallowed_methods,
            reason = "the client rides the attempt's bridge"
        )]
        let client = builder.build().map_err(|_| AttemptError::Route)?;
        let mut response = request(client.get(current.clone()), &current, hop)
            .send()
            .await
            .map_err(|error| {
                if weaver_nntp::tls::is_certificate_error(&error) {
                    AttemptError::Destination(format!("{label} TLS verification failed"))
                } else if error.is_timeout() {
                    // The client's timeout is the attempt's: its time ran out.
                    AttemptError::TimedOut {
                        destination: scheme_host_port(&current),
                    }
                } else {
                    AttemptError::Sent {
                        destination: scheme_host_port(&current),
                        cause: causes(&error),
                    }
                }
            })?;
        if !REDIRECT_STATUSES.contains(&response.status().as_u16()) {
            response.extensions_mut().insert(RoutedBody {
                path: path.clone(),
                _bridge: bridge,
            });
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

// `url` as scheme, host and port only: no credentials, path or query, which
// carry tracker passkeys and indexer keys.
fn scheme_host_port(url: &reqwest::Url) -> String {
    format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or_default(),
        url.port_or_known_default().unwrap_or_default()
    )
}

// What a request failed of, below reqwest's own text, which names the URL.
fn causes(error: &reqwest::Error) -> String {
    let mut causes = Vec::new();
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        causes.push(error.to_string());
        source = error.source();
    }
    causes.join(": ")
}
