// Composable outbound paths shared by NNTP, feeds and connectivity probes.
#[path = "pipe_revocable.rs"]
mod revocable;
pub use revocable::Revocable;
#[path = "pipe_datagram.rs"]
mod datagram;

#[cfg(test)]
#[path = "pipe_tests.rs"]
mod tests;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Notify,
};

use crate::{
    TunnelError, TunnelProvider, TunnelStream,
    bridge::ConnectionOutcome,
    egress::SocketEgress,
    endpoint::{DatagramTransport, EndpointStream, EndpointTransport, UdpSocketFactory},
    transport::{ConnectFailure, TransportProxy},
};

#[derive(Clone, Debug)]
pub enum Purpose {
    Nntp { server: u32, leg: usize },
    NntpProbe { server: u32, leg: usize },
    ProxyEndpoint { proxy: u32 },
    Feed { feed: u32 },
    Probe,
}

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub purpose: Purpose,
    // The addresses `host` resolved to when the caller checked them, so a
    // dial connects exactly those instead of resolving `host` again; empty
    // when the dial, or the proxy it goes through, resolves for itself.
    pub addresses: Vec<IpAddr>,
}
impl Target {
    // The destinations a hop asks its proxy for, in order: each checked
    // address when the caller pinned them, so the proxy never resolves
    // `host` itself, else `host`.
    fn hop_destinations(&self) -> Vec<String> {
        if self.addresses.is_empty() {
            vec![self.host.clone()]
        } else {
            self.addresses.iter().map(|ip| ip.to_string()).collect()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transports {
    Tcp,
    TcpUdp,
}

#[derive(Debug)]
pub enum Resolution {
    Addresses(Vec<IpAddr>),
    Unresolved,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DialPath {
    pub egress: u32,
    pub leg: Option<usize>,
    pub rung: Option<usize>,
    pub pool: Option<u32>,
    pub member: Option<u32>,
    pub proxies: Vec<u32>,
}

pub enum DialedStream {
    Socket(tokio::net::TcpStream),
    Tunnel(Box<dyn TunnelStream>),
}

impl DialedStream {
    pub fn into_tunnel(self) -> Box<dyn TunnelStream> {
        match self {
            Self::Socket(socket) => Box::new(socket),
            Self::Tunnel(stream) => stream,
        }
    }
    pub fn tcp(&self) -> Option<&tokio::net::TcpStream> {
        match self {
            Self::Socket(socket) => Some(socket),
            Self::Tunnel(_) => None,
        }
    }
}

impl AsyncRead for DialedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Socket(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tunnel(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for DialedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Socket(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tunnel(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Socket(s) => Pin::new(s).poll_flush(cx),
            Self::Tunnel(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Socket(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tunnel(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// One-shot greeting/authentication evidence. Dropping an unreported setup is cancellation.
pub struct SetupHandle(Option<Box<dyn FnOnce(bool) + Send + Sync>>);
impl SetupHandle {
    pub fn new(report: impl FnOnce(bool) + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(report)))
    }
    pub fn complete(mut self, success: bool) {
        if let Some(report) = self.0.take() {
            report(success);
        }
    }
}

pub struct Dialed {
    pub stream: DialedStream,
    pub outcome: Arc<ConnectionOutcome>,
    pub path: DialPath,
    pub peer: Option<SocketAddr>,
    pub source: Option<SocketAddr>,
    pub setup: Option<SetupHandle>,
}
impl Dialed {
    // Preserve outcome ownership when adapting a pipe to a tunnel consumer.
    pub fn into_observed_stream(self) -> Box<dyn TunnelStream> {
        Box::new(OutcomeStream::new(self.stream, self.outcome, None, None))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("no address resolved for the destination")]
pub struct ResolutionFailed;

#[derive(Debug, thiserror::Error)]
pub enum DialError {
    #[error("{0}")]
    Skipped(String),
    #[error("all route legs are at capacity")]
    AtCapacity(Vec<Arc<Notify>>),
    #[error("egress binding failed: {0}")]
    Bind(io::Error),
    #[error("egress connection failed: {0}")]
    Egress(io::Error),
    #[error("proxy {proxy}: {source}")]
    Hop { proxy: u32, source: TunnelError },
    #[error("destination connection failed: {0}")]
    Destination(io::Error),
    #[error("destination refused the connection: {0}")]
    Refused(io::Error),
    #[error("{stage} timed out")]
    Timeout { stage: String },
    #[error("{0}")]
    Fatal(TunnelError),
}

impl DialError {
    pub fn is_evidence(&self) -> bool {
        !matches!(self, Self::Skipped(_) | Self::AtCapacity(_))
    }
    // Only positively attributed path failures can cool a leg or rung.
    pub fn is_path_evidence(&self) -> bool {
        matches!(
            self,
            Self::Bind(_)
                | Self::Egress(_)
                | Self::Hop { .. }
                | Self::Timeout { .. }
                | Self::Fatal(_)
        )
    }
    pub fn destination(error: io::Error) -> Self {
        if error
            .get_ref()
            .is_some_and(|source| source.is::<ResolutionFailed>())
        {
            return Self::Destination(error);
        }
        match error.kind() {
            io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NotFound
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::Unsupported
            | io::ErrorKind::InvalidInput => Self::Bind(error),
            io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset => {
                Self::Refused(error)
            }
            _ => Self::Destination(error),
        }
    }
    fn hop(proxy: u32, source: TunnelError) -> Self {
        if matches!(source, TunnelError::HostKeyMismatch { .. }) {
            Self::Fatal(source)
        } else if matches!(&source, TunnelError::Dial { detail, .. } if is_forwarding_refusal(detail))
        {
            // The proxy answered and refused to forward at all. That is the
            // proxy's policy, not the destination, so it is evidence against
            // the path, as an HTTP proxy refusing CONNECT is.
            Self::Hop { proxy, source }
        } else if matches!(source, TunnelError::Dial { .. }) {
            Self::Destination(io::Error::other(source))
        } else {
            Self::Hop { proxy, source }
        }
    }
}

// An SSH server refuses a forwarding request it will not serve with
// `administratively prohibited`; the destination was never tried.
fn is_forwarding_refusal(detail: &str) -> bool {
    detail.contains("AdministrativelyProhibited")
}

// A proxy hop known to be failing, and what its last attempt came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailingHop {
    pub proxy: u32,
    pub reason: String,
}

// What a proxy hop's own last attempt came to. The hop keeps it itself, so
// a path can name its first failing hop however the failure travelled up: a
// hop stacked on a broken one fails too, and its error names only itself.
#[derive(Default)]
pub struct HopFailure(Mutex<Option<String>>);
impl HopFailure {
    // Records `error` when it is evidence against the hop. A destination
    // the hop answered for clears the record, and an attempt that was never
    // made, such as one skipped for capacity, says nothing either way.
    fn note(&self, error: &DialError) {
        match error {
            // The hop is named beside its reason, so the reason leaves it out.
            DialError::Hop { source, .. } => self.blame(source),
            error if error.is_path_evidence() => self.record(error.to_string()),
            error if error.is_evidence() => self.clear(),
            _ => {}
        }
    }
    // The hop's own endpoint could not be reached from the stage beneath.
    fn unreachable(&self, source: &io::Error) {
        self.record(format!("endpoint unreachable: {source}"));
    }
    // Records what the hop's tunnel said. A stream proxy reports through
    // the engine variant, whose own wording is about an engine that could
    // not start, so what the proxy said stands alone.
    fn blame(&self, source: &TunnelError) {
        self.record(match source {
            TunnelError::Engine(said) => said.clone(),
            source => source.to_string(),
        });
    }
    fn record(&self, reason: String) {
        *self.0.lock().expect("hop failure") = Some(reason);
    }
    fn clear(&self) {
        self.0.lock().expect("hop failure").take();
    }
    fn of(&self, proxy: u32) -> Option<FailingHop> {
        let reason = self.0.lock().expect("hop failure").clone()?;
        Some(FailingHop { proxy, reason })
    }
}

#[async_trait::async_trait]
pub trait Dialer: Send + Sync {
    // Physical NNTP capacity by leg; independent of server health and work permits.
    fn leg_targets(&self) -> Option<tokio::sync::watch::Receiver<Vec<u16>>> {
        None
    }
    // Feedback from consumers whose request outlives connection setup.
    fn report_request(&self, _error: Option<&DialError>) {}
    fn needs_probe(&self) -> bool {
        false
    }
    async fn prepare(&self) -> Result<(), DialError> {
        Ok(())
    }
    async fn retire(&self) {}
    // Returns false when a shared active session could not be retired.
    async fn retire_idle(&self) -> bool {
        self.retire().await;
        true
    }
    fn over_limit_cleared(&self) {}
    fn has_capacity(&self) -> bool {
        true
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError>;
    async fn resolve(&self, _host: &str) -> Result<Resolution, DialError> {
        Ok(Resolution::Unresolved)
    }
    fn offers(&self) -> Transports {
        Transports::Tcp
    }
    // Datagrams through this stage, for a WireGuard tunnel stacked on it.
    // Only a WireGuard stage carries them.
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        None
    }
    // The first proxy hop on this stage's path, counted from the egress,
    // whose own last attempt failed.
    fn failing_hop(&self) -> Option<FailingHop> {
        None
    }
    fn budget(&self) -> Duration;
    async fn shutdown(&self) {}
    fn describe(&self) -> String;
}

pub struct Egress {
    pub id: u32,
    pub binding: SocketEgress,
    pub timeout: Duration,
}

impl Egress {
    pub fn connect_error(&self, error: io::Error) -> DialError {
        if !matches!(self.binding, SocketEgress::System)
            && matches!(
                error.kind(),
                io::ErrorKind::NetworkUnreachable | io::ErrorKind::HostUnreachable
            )
        {
            let via = match &self.binding {
                SocketEgress::Interface(name) => name.clone(),
                SocketEgress::SourceAddress(address) => address.to_string(),
                SocketEgress::System => String::new(),
            };
            return DialError::Egress(io::Error::new(
                error.kind(),
                format!("no route via {via}: {error}"),
            ));
        }
        DialError::destination(error)
    }
    pub async fn connect_address(&self, peer: SocketAddr) -> Result<Dialed, DialError> {
        let stream = tokio::time::timeout(self.timeout, self.binding.connect(peer))
            .await
            .map_err(|_| {
                DialError::Destination(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "destination connect timed out",
                ))
            })?
            .map_err(|error| self.connect_error(error))?;
        let source = stream.local_addr().ok();
        Ok(Dialed {
            stream: DialedStream::Socket(stream),
            outcome: Arc::new(ConnectionOutcome::default()),
            path: DialPath {
                egress: self.id,
                ..Default::default()
            },
            peer: Some(peer),
            source,
            setup: None,
        })
    }
}

impl UdpSocketFactory for Egress {
    fn bind(&self, peer: SocketAddr) -> io::Result<std::net::UdpSocket> {
        self.binding.bind_udp(peer)
    }
}

#[async_trait::async_trait]
impl Dialer for Egress {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let started = tokio::time::Instant::now();
        let attempt = async {
            let peers: Vec<_> = if target.addresses.is_empty() {
                tokio::net::lookup_host((target.host.as_str(), target.port))
                    .await
                    .map_err(DialError::Destination)?
                    .map(|peer| peer.ip())
                    .collect()
            } else {
                target.addresses.clone()
            };
            let peers: Vec<_> = peers
                .into_iter()
                .filter(|ip| self.binding.supports_address(*ip))
                .map(|ip| SocketAddr::new(ip, target.port))
                .collect();
            let mut last = DialError::Skipped("no compatible destination address".into());
            for (index, peer) in peers.iter().enumerate() {
                // Reserve part of the remaining budget for every other resolved address.
                let remaining = self.timeout.saturating_sub(started.elapsed());
                let attempt = Self {
                    id: self.id,
                    binding: self.binding.clone(),
                    timeout: remaining / (peers.len() - index) as u32,
                };
                match attempt.connect_address(*peer).await {
                    Ok(stream) => return Ok(stream),
                    Err(error) => last = error,
                }
            }
            Err(last)
        };
        let result = tokio::time::timeout(self.timeout, attempt)
            .await
            .unwrap_or_else(|_| {
                Err(DialError::Destination(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "destination resolution or connect timed out",
                )))
            });
        match (&target.purpose, result) {
            // Failure to reach a proxy endpoint is evidence about the route itself.
            (
                Purpose::ProxyEndpoint { .. },
                Err(DialError::Destination(error) | DialError::Refused(error)),
            ) => Err(DialError::Egress(error)),
            (_, result) => result,
        }
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        let peers = tokio::time::timeout(self.timeout, tokio::net::lookup_host((host, 0)))
            .await
            .map_err(|_| {
                DialError::Destination(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "local DNS timed out",
                ))
            })?
            .map_err(DialError::Destination)?;
        let mut addresses: Vec<_> = peers
            .map(|p| p.ip())
            .filter(|ip| self.binding.supports_address(*ip))
            .collect();
        addresses.sort();
        addresses.dedup();
        Ok(Resolution::Addresses(addresses))
    }
    fn offers(&self) -> Transports {
        Transports::TcpUdp
    }
    fn budget(&self) -> Duration {
        self.timeout
    }
    fn describe(&self) -> String {
        format!("egress {}", self.id)
    }
}

pub struct TransportHop {
    pub id: u32,
    pub spec: TransportProxy,
    pub inner: Arc<dyn Dialer>,
    pub timeout: Duration,
    pub failure: HopFailure,
}

impl TransportHop {
    // Asks the proxy for `destination` within `budget`.
    //
    // A proxy that was reached, and then reports that the next hop's
    // endpoint cannot be reached or never answers for it, has done its own
    // part. That failure is the next hop's, so the error names the next hop.
    // For any other destination it stays a failure of this hop's path.
    async fn negotiate(
        &self,
        stream: &mut DialedStream,
        destination: &str,
        target: &Target,
        budget: Duration,
    ) -> Result<(), DialError> {
        let deadline = tokio::time::Instant::now() + budget;
        let timed_out = || DialError::Timeout {
            stage: self.describe(),
        };
        tokio::time::timeout_at(deadline, self.spec.greet(stream))
            .await
            .map_err(|_| timed_out())?
            .map_err(|error| DialError::hop(self.id, error))?;
        let next = match target.purpose {
            Purpose::ProxyEndpoint { proxy } => Some(proxy),
            _ => None,
        };
        let unreachable = |proxy, reason: &str| DialError::Hop {
            proxy,
            source: TunnelError::Engine(format!("endpoint unreachable: {reason}")),
        };
        let connected = tokio::time::timeout_at(
            deadline,
            self.spec.connect(stream, destination, target.port),
        )
        .await;
        match (connected, next) {
            (Ok(Ok(())), _) => Ok(()),
            (Ok(Err(ConnectFailure::Proxy(error))), _) => Err(DialError::hop(self.id, error)),
            (Ok(Err(ConnectFailure::Unreachable(reason))), Some(proxy)) => {
                Err(unreachable(proxy, reason))
            }
            (Ok(Err(ConnectFailure::Unreachable(reason))), None) => Err(DialError::hop(
                self.id,
                self.spec.destination_unreachable(reason),
            )),
            (Err(_), Some(proxy)) => Err(unreachable(proxy, "no answer through the hop before it")),
            (Err(_), None) => Err(timed_out()),
        }
    }
}

#[async_trait::async_trait]
impl Dialer for TransportHop {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let endpoint = Target {
            host: self.spec.host.clone(),
            port: self.spec.port,
            purpose: Purpose::ProxyEndpoint { proxy: self.id },
            addresses: Vec::new(),
        };
        let destinations = target.hop_destinations();
        let started = tokio::time::Instant::now();
        let mut last = None;
        for (index, destination) in destinations.iter().enumerate() {
            let mut dialed = self
                .inner
                .dial(&endpoint)
                .await
                .inspect_err(|error| match error {
                    // The proxy's own endpoint was the destination of that
                    // dial, so failing to reach it is this hop's failure.
                    DialError::Egress(source)
                    | DialError::Destination(source)
                    | DialError::Refused(source) => self.failure.unreachable(source),
                    // The proxy hop beneath was reached and could not reach
                    // this endpoint through its own proxy.
                    DialError::Hop { proxy, source } if *proxy == self.id => {
                        self.failure.blame(source)
                    }
                    // Any other failure belongs to the stage beneath.
                    _ => {}
                })?;
            // Reserve part of the remaining budget for every other address.
            let remaining = self.timeout.saturating_sub(started.elapsed());
            let negotiated = self
                .negotiate(
                    &mut dialed.stream,
                    destination,
                    target,
                    remaining / (destinations.len() - index) as u32,
                )
                .await;
            crate::metrics::record_stream(self.spec.kind.tunnel_kind(), &negotiated);
            match &negotiated {
                Ok(()) => self.failure.clear(),
                // The proxy answered for the next hop, so it is working.
                Err(DialError::Hop { proxy, .. }) if *proxy != self.id => self.failure.clear(),
                Err(error) => self.failure.note(error),
            }
            match negotiated {
                Ok(()) => {
                    // A negotiated socket carries proxy framing and cannot use NNTP's raw TCP fast path.
                    dialed.stream = DialedStream::Tunnel(dialed.stream.into_tunnel());
                    dialed.path.proxies.push(self.id);
                    return Ok(dialed);
                }
                Err(error @ DialError::Fatal(_)) => return Err(error),
                Err(error) => last = Some(error),
            }
        }
        Err(last.expect("a hop dials at least one destination"))
    }
    fn failing_hop(&self) -> Option<FailingHop> {
        self.inner
            .failing_hop()
            .or_else(|| self.failure.of(self.id))
    }
    fn budget(&self) -> Duration {
        self.inner.budget().saturating_add(self.timeout)
    }
    // No shutdown: a stream proxy hop holds nothing of its own, and the
    // stage beneath belongs to whoever built it.
    fn describe(&self) -> String {
        format!("{} → proxy {}", self.inner.describe(), self.id)
    }
}

pub type ConnectedPath = (DialPath, Option<SocketAddr>, Option<SocketAddr>);

// Adapts an arbitrary inner stage to the shared SSH engine while retaining its metadata.
pub struct InnerTransport {
    pub inner: Arc<dyn Dialer>,
    pub proxy: u32,
    pub connected: Mutex<Option<ConnectedPath>>,
}

#[async_trait::async_trait]
impl EndpointTransport for InnerTransport {
    async fn connect(&self, host: &str, port: u16) -> Result<EndpointStream, TunnelError> {
        let target = Target {
            host: host.into(),
            port,
            purpose: Purpose::ProxyEndpoint { proxy: self.proxy },
            addresses: Vec::new(),
        };
        let dialed = self
            .inner
            .dial(&target)
            .await
            .map_err(|error| match error {
                DialError::Fatal(error) => error,
                // The hop beneath named this hop's endpoint as what it could
                // not reach, so the reason is already this hop's own.
                DialError::Hop { proxy, source } if proxy == self.proxy => source,
                error => TunnelError::Engine(error.to_string()),
            })?;
        *self.connected.lock().expect("inner path") =
            Some((dialed.path, dialed.peer, dialed.source));
        Ok(EndpointStream {
            source: dialed.source,
            stream: Box::new(OutcomeStream::new(
                dialed.stream,
                dialed.outcome,
                None,
                None,
            )),
        })
    }
}

struct OutcomeStream {
    stream: std::mem::ManuallyDrop<DialedStream>,
    outcome: Arc<ConnectionOutcome>,
    // The runtime the stream was dialed on. A tunnel stream closes itself
    // from a task it spawns when dropped, so it is dropped inside that
    // runtime wherever the drop happens, a blocking lane thread included.
    runtime: Option<tokio::runtime::Handle>,
    _session: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    _capacity: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}
impl OutcomeStream {
    fn new(
        stream: DialedStream,
        outcome: Arc<ConnectionOutcome>,
        session: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
        capacity: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Self {
        Self {
            stream: std::mem::ManuallyDrop::new(stream),
            outcome,
            runtime: tokio::runtime::Handle::try_current().ok(),
            _session: session,
            _capacity: capacity,
        }
    }
}
impl Drop for OutcomeStream {
    fn drop(&mut self) {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        // SAFETY: the stream is dropped exactly once, here, and never used
        // again; the guard above holds the runtime for the drop.
        unsafe { std::mem::ManuallyDrop::drop(&mut self.stream) };
        self.outcome.closed();
    }
}
impl AsyncRead for OutcomeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut *self.stream).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Err(_))) {
            self.outcome.failed();
        }
        result
    }
}
impl AsyncWrite for OutcomeStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut *self.stream).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Err(_))) {
            self.outcome.failed();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_shutdown(cx)
    }
}

#[derive(Default)]
pub struct SessionCapacity {
    pub budget: Option<Arc<tokio::sync::Semaphore>>,
    permit: Mutex<Option<Arc<tokio::sync::OwnedSemaphorePermit>>>,
}
impl SessionCapacity {
    fn acquire(&self) -> Result<Option<Arc<tokio::sync::OwnedSemaphorePermit>>, DialError> {
        let Some(budget) = &self.budget else {
            return Ok(None);
        };
        let mut permit = self.permit.lock().expect("session capacity");
        if permit.is_none() {
            *permit = Some(Arc::new(budget.clone().try_acquire_owned().map_err(|_| {
                DialError::Skipped("WireGuard session budget is exhausted; retry after an idle session retires".into())
            })?));
        }
        Ok(permit.clone())
    }
    fn release(&self) {
        self.permit.lock().expect("session capacity").take();
    }
    pub fn new(budget: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            budget: Some(budget),
            permit: Mutex::new(None),
        }
    }
}

pub struct SessionHop {
    pub capacity: SessionCapacity,
    // Shared by every use of this canonical session path.
    pub activity: Arc<tokio::sync::RwLock<()>>,
    pub id: u32,
    pub provider: Arc<dyn TunnelProvider>,
    pub inner: Arc<dyn Dialer>,
    pub transport: Option<Arc<InnerTransport>>,
    pub path: DialPath,
    pub endpoint: Option<SocketAddr>,
    pub timeout: Duration,
    pub resolver: Option<Arc<crate::WireGuardTunnelProvider>>,
    pub failure: HopFailure,
}

impl SessionHop {
    // A WireGuard hop carried by the WireGuard session beneath it.
    fn is_carried(&self) -> bool {
        self.resolver.is_some() && !self.path.proxies.is_empty()
    }

    // Bring the session beneath a carried hop up first. Its exhausted budget
    // or failed handshake then surfaces as its own error, attributed to it,
    // instead of as a failure of this hop's tunnel to come up.
    async fn prepare_carrier(&self) -> Result<(), DialError> {
        if !self.is_carried() {
            return Ok(());
        }
        tokio::time::timeout(self.inner.budget(), self.inner.prepare())
            .await
            .map_err(|_| DialError::Timeout {
                stage: self.inner.describe(),
            })?
    }
}

#[async_trait::async_trait]
impl Dialer for SessionHop {
    async fn prepare(&self) -> Result<(), DialError> {
        self.prepare_carrier().await?;
        let activity = self.activity.read().await;
        let capacity = self.capacity.acquire()?;
        let result = self
            .provider
            .prepare()
            .await
            .map_err(|e| DialError::hop(self.id, e));
        drop(capacity);
        drop(activity);
        if let Some(kind) = self.provider.kind() {
            crate::metrics::record_session_prepare(kind, result.is_ok());
        }
        match &result {
            Ok(()) => self.failure.clear(),
            Err(error) => self.failure.note(error),
        }
        if result.is_err() {
            self.retire_idle().await;
        }
        result
    }
    async fn retire(&self) {
        // Another pool or direct rung can share this session. An idle member
        // may retire it only when every channel on the canonical path is idle.
        self.retire_idle().await;
    }
    async fn retire_idle(&self) -> bool {
        if let Ok(_activity) = self.activity.try_write() {
            if let Some(kind) = self.provider.kind() {
                crate::metrics::record_session_retirement(kind);
            }
            self.provider.retire().await;
            self.capacity.release();
            true
        } else {
            false
        }
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let started = tokio::time::Instant::now();
        self.prepare_carrier().await?;
        let activity = self.activity.clone().read_owned().await;
        let capacity = self.capacity.acquire()?;
        let outcome = Arc::new(ConnectionOutcome::default());
        let destinations = target.hop_destinations();
        let mut connected = Err(DialError::Skipped("no destination".into()));
        for (index, destination) in destinations.iter().enumerate() {
            // Reserve part of the remaining budget for every other address.
            let remaining = self.budget().saturating_sub(started.elapsed());
            connected = tokio::time::timeout(
                remaining / (destinations.len() - index) as u32,
                self.provider
                    .dial_observed(destination, target.port, outcome.clone()),
            )
            .await
            .map_err(|_| DialError::Timeout {
                stage: self.describe(),
            })
            .and_then(|result| result.map_err(|error| DialError::hop(self.id, error)));
            if matches!(connected, Ok(_) | Err(DialError::Fatal(_))) {
                break;
            }
        }
        if let Some(kind) = self.provider.kind() {
            crate::metrics::record_stream(kind, &connected);
        }
        let stream = match connected {
            Ok(stream) => {
                self.failure.clear();
                stream
            }
            Err(error) => {
                self.failure.note(&error);
                drop(capacity);
                drop(activity);
                // A destination that refuses says nothing about the tunnel.
                if error.is_path_evidence() {
                    self.retire_idle().await;
                }
                return Err(error);
            }
        };
        let metadata = self
            .transport
            .as_ref()
            .and_then(|t| t.connected.lock().expect("inner path").clone());
        let (mut path, peer, source) = metadata.unwrap_or_else(|| {
            (
                self.path.clone(),
                self.endpoint,
                self.provider.source_address(),
            )
        });
        path.proxies.push(self.id);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(OutcomeStream::new(
                DialedStream::Tunnel(stream),
                outcome.clone(),
                Some(activity),
                capacity,
            ))),
            outcome,
            path,
            peer,
            source,
            setup: None,
        })
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.prepare_carrier().await?;
        let _activity = self.activity.read().await;
        let _capacity = self.capacity.acquire()?;
        match &self.resolver {
            Some(provider) => provider
                .resolve_host(host)
                .await
                .inspect(|_| {
                    crate::metrics::record_resolution(crate::metrics::Resolver::WireGuard, true)
                })
                .inspect_err(|_| {
                    crate::metrics::record_resolution(crate::metrics::Resolver::WireGuard, false)
                })
                .map(Resolution::Addresses)
                .map_err(|error| DialError::hop(self.id, error))
                .inspect_err(|error| self.failure.note(error)),
            None => Ok(Resolution::Unresolved),
        }
    }
    fn failing_hop(&self) -> Option<FailingHop> {
        self.inner
            .failing_hop()
            .or_else(|| self.failure.of(self.id))
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        self.resolver
            .is_some()
            .then(|| Arc::new(datagram::SessionDatagrams::new(self)) as Arc<dyn DatagramTransport>)
    }
    fn budget(&self) -> Duration {
        self.inner.budget().saturating_add(self.timeout)
    }
    // Stops this hop's own session only. The stage beneath may be another
    // session shared with other routes, or one a probe runtime borrowed;
    // whoever owns it shuts it down.
    async fn shutdown(&self) {
        self.provider.shutdown().await;
        self.capacity.release();
    }
    fn describe(&self) -> String {
        format!("{} → proxy {}", self.inner.describe(), self.id)
    }
}

struct RungState {
    until: Option<tokio::time::Instant>,
    failures: u32,
}

pub struct Fallback {
    rungs: Vec<Arc<dyn Dialer>>,
    states: Arc<Mutex<Vec<RungState>>>,
    // How long a rung whose path failed is left alone before it is tried
    // again. Flat: a rung is a whole path, and the ladder below it carries
    // the work meanwhile.
    rung_cooldown: Duration,
}
impl Fallback {
    pub fn rung_states(&self) -> Vec<&'static str> {
        let now = tokio::time::Instant::now();
        self.states
            .lock()
            .expect("rung state")
            .iter()
            .map(|state| {
                if state.until.is_some_and(|until| until > now) {
                    "COOLDOWN"
                } else if state.failures > 0 {
                    "FAILING"
                } else {
                    "STANDBY"
                }
            })
            .collect()
    }
    // For each rung, the first proxy hop on it known to be failing.
    pub fn failing_hops(&self) -> Vec<Option<FailingHop>> {
        self.rungs.iter().map(|rung| rung.failing_hop()).collect()
    }
    pub fn allows_rung(&self, index: usize, probe: bool) -> bool {
        self.states
            .lock()
            .expect("rung state")
            .get(index)
            .is_some_and(|state| {
                probe
                    || state
                        .until
                        .is_none_or(|until| until <= tokio::time::Instant::now())
            })
    }
    pub fn report_rung(&self, index: usize, error: Option<&DialError>) {
        let mut states = self.states.lock().expect("rung state");
        let Some(state) = states.get_mut(index) else {
            return;
        };
        if let Some(error) = error {
            let now = tokio::time::Instant::now();
            if error.is_path_evidence() && state.until.is_none_or(|until| until <= now) {
                crate::metrics::record_rung_cooldown(index);
                state.failures = state.failures.saturating_add(1);
                state.until = Some(now + self.rung_cooldown);
            }
        } else {
            state.failures = 0;
            state.until = None;
        }
    }
    // A ladder that leaves a failed rung alone for thirty seconds.
    pub fn new(rungs: Vec<Arc<dyn Dialer>>) -> Self {
        Self::with_rung_cooldown(rungs, PATH_COOLDOWN)
    }
    // A ladder that leaves a failed rung alone for `rung_cooldown`.
    pub fn with_rung_cooldown(rungs: Vec<Arc<dyn Dialer>>, rung_cooldown: Duration) -> Self {
        let states = (0..rungs.len())
            .map(|_| RungState {
                until: None,
                failures: 0,
            })
            .collect();
        Self {
            rungs,
            states: Arc::new(Mutex::new(states)),
            rung_cooldown,
        }
    }
}
#[async_trait::async_trait]
impl Dialer for Fallback {
    fn has_capacity(&self) -> bool {
        self.rungs
            .iter()
            .enumerate()
            .any(|(index, rung)| self.allows_rung(index, false) && rung.has_capacity())
    }

    fn over_limit_cleared(&self) {
        for rung in &self.rungs {
            rung.over_limit_cleared();
        }
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let mut last = DialError::Skipped("all rungs are unavailable".into());
        let mut capacity_changes = Vec::new();
        for (index, rung) in self.rungs.iter().enumerate() {
            if !matches!(target.purpose, Purpose::Probe | Purpose::NntpProbe { .. })
                && self.states.lock().expect("rung state")[index]
                    .until
                    .is_some_and(|t| t > tokio::time::Instant::now())
            {
                continue;
            }
            match rung.dial(target).await {
                Ok(mut dialed) => {
                    self.states.lock().expect("rung state")[index] = RungState {
                        until: None,
                        failures: 0,
                    };
                    dialed.path.rung = Some(index);
                    if index > 0 {
                        crate::metrics::record_rung_fallback(index);
                    }
                    return Ok(dialed);
                }
                Err(error @ DialError::Fatal(_)) => return Err(error),
                Err(DialError::AtCapacity(changes)) => capacity_changes.extend(changes),
                Err(error) => {
                    self.report_rung(index, Some(&error));
                    if !last.is_evidence() {
                        last = error;
                    }
                }
            }
        }
        if !last.is_evidence() && !capacity_changes.is_empty() {
            return Err(DialError::AtCapacity(capacity_changes));
        }
        Err(last)
    }
    fn budget(&self) -> Duration {
        self.rungs.iter().fold(Duration::ZERO, |sum, rung| {
            sum.saturating_add(rung.budget())
        })
    }
    async fn shutdown(&self) {
        for rung in &self.rungs {
            rung.shutdown().await;
        }
    }
    fn describe(&self) -> String {
        self.rungs
            .iter()
            .map(|r| r.describe())
            .collect::<Vec<_>>()
            .join(" or ")
    }
}

pub const PATH_COOLDOWN: Duration = Duration::from_secs(30);

pub fn cooldown(failures: u32) -> Duration {
    cooldown_from(PATH_COOLDOWN, failures)
}

// [`cooldown`] starting from `initial` instead of 30 seconds: doubling per
// failure, never longer than ten times `initial`.
pub fn cooldown_from(initial: Duration, failures: u32) -> Duration {
    initial
        .saturating_mul(1 << failures.saturating_sub(1).min(4))
        .min(initial.saturating_mul(10))
}
