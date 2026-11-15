// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Composable outbound paths shared by NNTP, feeds and connectivity probes.
#[path = "pipe_revocable.rs"]
mod revocable;
pub use revocable::Revocable;
#[path = "pipe_datagram.rs"]
mod datagram;
#[path = "pipe_identity.rs"]
mod identity;
#[path = "pipe_inbound.rs"]
mod inbound;
#[path = "pipe_natpmp.rs"]
mod natpmp;
#[path = "pipe_sockets.rs"]
mod sockets;
#[path = "pipe_socks5_udp.rs"]
mod socks5_udp;
pub use inbound::{
    Inbound, InboundListener, LISTEN_BACKLOG, PortForwarding, PortMapping, gateway_of,
};
pub use sockets::{SessionSockets, SocketClass, SocketPermit, SocketShares};

#[cfg(test)]
#[path = "pipe_tests.rs"]
mod tests;
use std::{
    cell::Cell,
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
    dns::{DnsServer, Trust, WireKind},
    egress::SocketEgress,
    endpoint::{DatagramTransport, EndpointStream, EndpointTransport, UdpSocketFactory},
    fault::{self, ProxyFault},
    metrics::Resolver,
    resolution::{
        Account, Lookup, Replicate, ResolutionCache, Ttl,
        record::{self, Asker, LookupFailure},
    },
    transport::{ConnectFailure, Reply, TransportProxy},
};

// What a lookup or an egress says when it fails, and `fault` reads back.
pub(crate) const NO_ADDRESS_RESOLVED: &str = "no address resolved for the destination";
pub(crate) const RESOLVE_TIMED_OUT: &str = "name resolution timed out";
pub(crate) const NO_ROUTE_VIA: &str = "no route via ";

thread_local! {
    // Whether this thread was handed over to blocking work by `blocking`.
    static MAY_BLOCK: Cell<bool> = const { Cell::new(false) };
}

// Run `work` on this thread, which the caller vouches may block: a runtime's
// blocking thread, as `spawn_blocking` gives, never one of its workers.
// `Egress::resolve_blocking` asks listed servers only inside it.
pub fn blocking<R>(work: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            MAY_BLOCK.with(|may| may.set(self.0));
        }
    }
    let _restore = Restore(MAY_BLOCK.with(|may| may.replace(true)));
    work()
}

#[derive(Clone, Debug)]
pub enum Purpose {
    // A usenet connection the server's pool keeps. The first one a leg
    // being probed again opens is `probing`: it tries rungs still cooling
    // down, as a probe does, and is a pooled connection all the same.
    Nntp {
        server: u32,
        leg: usize,
        probing: bool,
    },
    NntpProbe {
        server: u32,
        leg: usize,
    },
    // The connection to a proxy's endpoint. One that carries a single
    // connection through the proxy counts against that connection's share
    // of a tunnel beneath; one a proxy's session shares, as SSH and HTTP/3
    // do, is `None`.
    ProxyEndpoint {
        proxy: u32,
        carrying: Option<SocketClass>,
    },
    Feed {
        feed: u32,
    },
    // Any other fetch Weaver makes over HTTP.
    Outbound,
    // A torrent session's peers over TCP.
    Torrent,
    // A torrent session's trackers, metainfo downloads and web seeds over
    // HTTP.
    TorrentHttp,
    Probe,
    // A DNS query a hop asks to resolve a name dialled through it.
    Dns,
    // A datagram socket a torrent session binds: DHT, uTP, UDP trackers.
    Datagram,
}

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub purpose: Purpose,
    // The addresses `host` resolved to when the caller checked them, so a
    // dial connects exactly those instead of resolving `host` again; empty
    // when the dial, or the hop it goes through, resolves for itself.
    pub addresses: Vec<IpAddr>,
}

// How a proxy hop resolves a name dialled through it: the next hop's
// endpoint, or the destination when the hop is the last. Answers from DNS
// servers are held by the hop that asked for them, for as long as their TTL
// allows, and go with it when the hop is rebuilt.
#[derive(Clone)]
pub enum Naming {
    // Weaver resolves the name with the host's own DNS.
    Host,
    // Weaver asks these servers in order, from the route's egress, and
    // judges their answers by `trust`.
    Servers {
        servers: Vec<DnsServer>,
        trust: Trust,
        egress: Arc<dyn Dialer>,
        answers: Arc<ResolutionCache<DialError>>,
        // The TLS sessions these servers are resumed with, held apart from
        // every other naming's.
        sessions: Arc<crate::dns::DnsSessions>,
    },
    // The proxy is handed the name and resolves it.
    Proxy,
    // These servers are asked in order, through the proxy, and their
    // answers judged by `trust`.
    ProxyServers {
        servers: Vec<DnsServer>,
        trust: Trust,
        answers: Arc<ResolutionCache<DialError>>,
        sessions: Arc<crate::dns::DnsSessions>,
    },
}

impl Naming {
    pub fn servers(servers: Vec<DnsServer>, trust: Trust, egress: Arc<dyn Dialer>) -> Self {
        Self::Servers {
            servers,
            trust,
            egress,
            answers: Default::default(),
            sessions: Default::default(),
        }
    }
    pub fn proxy_servers(servers: Vec<DnsServer>, trust: Trust) -> Self {
        Self::ProxyServers {
            servers,
            trust,
            answers: Default::default(),
            sessions: Default::default(),
        }
    }
    // How long a lookup may take on top of a hop's connect: the same bound
    // `resolve` is given, and nothing when the proxy is handed the name.
    fn window(&self, timeout: Duration) -> Duration {
        match self {
            Self::Proxy => Duration::ZERO,
            Self::Host | Self::Servers { .. } | Self::ProxyServers { .. } => timeout,
        }
    }
    // What `host` resolves to for a dial through `hop`, the hop of proxy
    // `proxy`, or `Unresolved` when the proxy is handed the name.
    pub(crate) async fn resolve(
        &self,
        hop: &dyn Dialer,
        proxy: u32,
        host: &str,
        timeout: Duration,
    ) -> Result<Resolution, DialError> {
        if let Ok(ip) = host.parse() {
            return Ok(Resolution::Addresses(vec![ip]));
        }
        record::asking(host, || match self {
            Self::Host => Asker::host(),
            Self::Servers { servers, trust, .. } => Asker::servers(servers, *trust),
            Self::Proxy => Asker::proxy(proxy),
            Self::ProxyServers { servers, trust, .. } => {
                Asker::proxy_servers(proxy, servers, *trust)
            }
        });
        // Each naming value holds its own answers. A lookup says how it was
        // judged, and a caller that joins it or finds its answer held notes
        // the same. A lookup that was refused is an error, which the cache
        // never holds.
        let resolving = async {
            match self {
                Self::Proxy => Ok(None),
                Self::Host => lookup(host).await.map(Some),
                Self::Servers {
                    servers,
                    trust,
                    egress,
                    answers,
                    sessions,
                } => answers
                    .resolve_accounted(Resolver::Routed, host, || {
                        crate::dns::resolve_routed(egress.as_ref(), sessions, servers, *trust, host)
                    })
                    .await
                    .and_then(routed)
                    .map(Some),
                Self::ProxyServers {
                    servers,
                    trust,
                    answers,
                    sessions,
                } => answers
                    .resolve_accounted(Resolver::Routed, host, || {
                        crate::dns::resolve_routed(hop, sessions, servers, *trust, host)
                    })
                    .await
                    .and_then(routed)
                    .map(Some),
            }
        };
        let resolved = match tokio::time::timeout(timeout, resolving).await {
            Ok(resolved) => resolved.inspect_err(|error| record::failed_saying(host, error))?,
            Err(_) => {
                let said = format!("{RESOLVE_TIMED_OUT}: {host}");
                record::failed(host, LookupFailure::TimedOut, || said.clone());
                return Err(DialError::Destination(io::Error::new(
                    io::ErrorKind::TimedOut,
                    said,
                )));
            }
        };
        Ok(match resolved {
            None => Resolution::Unresolved,
            Some(addresses) if addresses.is_empty() => {
                record::failed(host, LookupFailure::NoAddress, || {
                    NO_ADDRESS_RESOLVED.to_owned()
                });
                return Err(DialError::Destination(io::Error::other(ResolutionFailed)));
            }
            Some(addresses) => {
                record::found(host, &addresses);
                Resolution::Addresses(addresses)
            }
        })
    }
    // Drops what this naming holds for `host`, after none of the addresses
    // it gave could be reached.
    fn forget(&self, host: &str) {
        match self {
            Self::Host => crate::resolution::forget_host(host),
            Self::Servers { answers, .. } | Self::ProxyServers { answers, .. } => {
                answers.forget(host)
            }
            Self::Proxy => {}
        }
    }
}

// DNS servers that all say the name has no address fail as one that
// answered nothing would.
fn routed(addresses: Vec<IpAddr>) -> Result<Vec<IpAddr>, DialError> {
    if addresses.is_empty() {
        return Err(crate::dns::unresolved());
    }
    Ok(addresses)
}

// Whether a WireGuard tunnel's own resolver can serve servers asked by
// `trust`. It speaks only plain DNS to port 53 of the servers the tunnel
// was built with, and passes on no AD bit, so any other wire, port or
// trust is asked by Weaver's own client through the hop instead. It also
// asks only the first server it was built with, so a list of more than one
// is asked by Weaver's client too, every server in its turn.
fn tunnel_resolver_serves(servers: &[DnsServer], trust: Trust) -> bool {
    trust == Trust::AsSent
        && servers.len() <= 1
        && servers
            .iter()
            .all(|server| *server == DnsServer::plain(server.address.ip()))
}

// What the host's own DNS resolves `host` to.
async fn lookup(host: &str) -> Result<Vec<IpAddr>, DialError> {
    let mut addresses = host_dns::lookup(host)
        .await
        .map_err(DialError::Destination)?;
    addresses.sort();
    Ok(addresses)
}

// The host's own DNS, asked only where a leg's naming is Host: a direct
// egress leg always, and a proxy hop whose profile says Host. These are the
// only callers of the host resolver, and nothing outside this module can
// reach them; every other lookup goes through a leg's naming.
mod host_dns {
    use std::{io, net::IpAddr};

    #[expect(
        clippy::disallowed_methods,
        reason = "the one caller of the host resolver, for a leg whose naming is Host"
    )]
    pub(super) async fn lookup(host: &str) -> io::Result<Vec<IpAddr>> {
        crate::resolution::host_lookup(host).await
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the one blocking caller of the host resolver, for a leg whose naming is Host"
    )]
    pub(super) fn lookup_blocking(host: &str) -> io::Result<Vec<IpAddr>> {
        crate::resolution::host_lookup_blocking(host)
    }
}

// Whether a dial that resolved `target` itself failed at the destination,
// so the addresses it was given may be stale.
fn unreached(target: &Target, error: &DialError) -> bool {
    target.addresses.is_empty()
        && match error {
            DialError::Destination(_) | DialError::Refused(_) => true,
            // A proxy answered that it could not reach an address it was handed.
            DialError::Hop { source, .. } => crate::transport::is_destination_unreachable(source),
            _ => false,
        }
}

// The destinations a hop asks its proxy for, in order: each address the
// caller pinned, else each address the hop resolves the host to, else the
// host itself, for the proxy to resolve.
async fn hop_destinations(hop: &dyn Dialer, target: &Target) -> Result<Vec<String>, DialError> {
    if !target.addresses.is_empty() {
        return Ok(target.addresses.iter().map(ToString::to_string).collect());
    }
    Ok(match hop.resolve(&target.host).await? {
        Resolution::Addresses(addresses) => addresses.iter().map(ToString::to_string).collect(),
        Resolution::Unresolved => vec![target.host.clone()],
    })
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
#[error("{}", NO_ADDRESS_RESOLVED)]
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
        } else if let TunnelError::AtCapacity { freed, .. } = &source {
            // A full tunnel is working; it has room again once a socket closes.
            Self::AtCapacity(vec![freed.notify()])
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

impl Replicate for DialError {
    fn replicate(&self) -> Self {
        match self {
            Self::Skipped(reason) => Self::Skipped(reason.clone()),
            Self::AtCapacity(waits) => Self::AtCapacity(waits.clone()),
            Self::Bind(error) => Self::Bind(error.replicate()),
            Self::Egress(error) => Self::Egress(error.replicate()),
            Self::Hop { proxy, source } => Self::Hop {
                proxy: *proxy,
                source: source.clone(),
            },
            Self::Destination(error) => Self::Destination(error.replicate()),
            Self::Refused(error) => Self::Refused(error.replicate()),
            Self::Timeout { stage } => Self::Timeout {
                stage: stage.clone(),
            },
            Self::Fatal(source) => Self::Fatal(source.clone()),
        }
    }
}

// An SSH server refuses a forwarding request it will not serve with
// `administratively prohibited`; the destination was never tried.
pub(crate) fn is_forwarding_refusal(detail: &str) -> bool {
    detail.contains("AdministrativelyProhibited")
}

// A proxy hop known to be failing, and what its last attempt came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailingHop {
    pub proxy: u32,
    pub fault: ProxyFault,
}

// What a proxy hop's own last attempt came to. The hop keeps it itself, so
// a path can name its first failing hop however the failure travelled up: a
// hop stacked on a broken one fails too, and its error names only itself.
// `wait` is the hop's own timeout, which a fault that ran out of time names.
//
// A watched hop tells its watcher each time it turns: it works after it was
// untried or failing, or it fails after it was untried or working. Failing
// for its egress and failing for itself count as two turns.
//
// A hop that answered within `LIVE_WINDOW` is alive. A wait that runs out on
// it is then the destination's, and says nothing of the hop.
#[derive(Default)]
pub struct HopFailure {
    state: Mutex<HopState>,
    watcher: Option<Arc<dyn HopWatcher>>,
}

#[derive(Default)]
enum HopState {
    #[default]
    Untried,
    // When the hop last answered, if it has since it last failed.
    Working(Option<tokio::time::Instant>),
    Failing(ProxyFault),
}

// How long a hop's last answer keeps it known alive. Two minutes is
// WireGuard's REKEY_AFTER_TIME, the window in which proxy-tunnels takes a
// handshake as proof that a tunnel is up. SSH keepalives (30 s, three missed)
// and QUIC's idle timeout (90 s) find a dead session sooner.
pub const LIVE_WINDOW: Duration = Duration::from_secs(120);

// Whether `error` is a wait that ran out on the hop, with no answer from it:
// Weaver's own timeout, or the HTTP/3 engine's deadline for a whole dial.
fn ran_out(error: &DialError) -> bool {
    match error {
        DialError::Timeout { .. } => true,
        DialError::Hop { source, .. } => fault::is_http3_dial_deadline(source),
        _ => false,
    }
}

// What a wait that ran out on a live hop comes to: the destination did not
// answer, or, when the destination is the next hop's endpoint, that hop did
// not.
fn unanswered(target: &Target, destination: &str, proxy: u32) -> DialError {
    match target.purpose {
        Purpose::ProxyEndpoint { proxy: next, .. } => DialError::Hop {
            proxy: next,
            source: crate::transport::hop_before(destination, target.port, Reply::Silent),
        },
        _ => DialError::Destination(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "{} did not answer in time through proxy {proxy}",
                fault::authority(destination, target.port)
            ),
        )),
    }
}

// Told when a watched hop turns. It is called with the hop's state held, so
// turns arrive in order; it must not dial or wait.
pub trait HopWatcher: Send + Sync {
    fn working(&self);
    fn failing(&self, fault: &ProxyFault);
}

impl HopFailure {
    // A record that tells `watcher` each time the hop turns.
    pub fn watched(watcher: Arc<dyn HopWatcher>) -> Self {
        Self {
            state: Mutex::default(),
            watcher: Some(watcher),
        }
    }
    // Records `error` when it is evidence against the hop. A destination
    // the hop answered for clears the record, and an attempt that was never
    // made, such as one skipped for capacity, says nothing either way.
    fn note(&self, error: &DialError, wait: Duration) {
        match error {
            DialError::Hop { source, .. } => self.blame(source, wait),
            error if error.is_path_evidence() => self.record(error.fault(wait)),
            error if error.is_evidence() => self.clear(),
            _ => {}
        }
    }
    // The hop's own endpoint could not be reached from the stage beneath.
    fn unreachable(&self, source: &io::Error, wait: Duration) {
        self.record(fault::from_io(source, wait));
    }
    // Records what the hop's tunnel said.
    fn blame(&self, source: &TunnelError, wait: Duration) {
        self.record(fault::classify(source, wait));
    }
    fn record(&self, fault: ProxyFault) {
        let mut state = self.state.lock().expect("hop failure");
        // Failing for the egress and failing for the hop itself are two
        // turns: the egress's fault says nothing of the proxy beyond it.
        let turned = match &*state {
            HopState::Failing(before) => before.is_egress_own() != fault.is_egress_own(),
            _ => true,
        };
        if turned && let Some(watcher) = &self.watcher {
            watcher.failing(&fault);
        }
        *state = HopState::Failing(fault);
    }
    // The hop answered.
    fn clear(&self) {
        self.work(Some(tokio::time::Instant::now()));
    }
    // The hop did its part without a word, such as a proxy that took the
    // request for the next hop and was silent: working, but not heard.
    fn spare(&self) {
        self.work(None);
    }
    fn work(&self, answered: Option<tokio::time::Instant>) {
        let mut state = self.state.lock().expect("hop failure");
        let heard = match *state {
            HopState::Working(heard) => heard,
            _ => {
                if let Some(watcher) = &self.watcher {
                    watcher.working();
                }
                None
            }
        };
        *state = HopState::Working(answered.or(heard));
    }
    // Whether the hop answered within `LIVE_WINDOW`.
    fn alive(&self) -> bool {
        matches!(
            *self.state.lock().expect("hop failure"),
            HopState::Working(Some(at)) if at.elapsed() < LIVE_WINDOW
        )
    }
    fn of(&self, proxy: u32) -> Option<FailingHop> {
        match &*self.state.lock().expect("hop failure") {
            HopState::Failing(fault) => Some(FailingHop {
                proxy,
                fault: fault.clone(),
            }),
            _ => None,
        }
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
    // Room for a dial for `purpose` in the share it counts against, or the
    // signals that fire when room may have freed. A path with no shares
    // always has room.
    fn room(&self, _purpose: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        Ok(())
    }
    fn has_room(&self, purpose: &Purpose) -> bool {
        self.room(purpose).is_ok()
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError>;
    async fn resolve(&self, _host: &str) -> Result<Resolution, DialError> {
        Ok(Resolution::Unresolved)
    }
    fn offers(&self) -> Transports {
        Transports::Tcp
    }
    // Datagrams through this stage, for a WireGuard tunnel stacked on it or
    // a torrent session's UDP. A WireGuard stage carries them, and so does a
    // SOCKS5 hop whose stage beneath carries datagrams to its relay.
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        None
    }
    // Datagrams a proxy stacked on this stage sends to its UDP relay. An
    // egress sends them from its own sockets; any other stage only through
    // what it carries.
    fn relay_datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        self.datagrams()
    }
    // Stream connections arriving through this stage, for a session that
    // listens on it. Only a WireGuard stage takes them: the hop at the top of
    // a path, so a chain listens at its last hop and only when that is one.
    fn inbound(self: Arc<Self>) -> Option<Arc<dyn Inbound>> {
        None
    }
    // A datagram socket a direct leg's consumer sends from itself, bound on
    // this stage's egress at `local`'s port. Only an egress binds one: past
    // a proxy, the consumer's datagrams ride the stage.
    fn bind_local_udp(&self, _local: SocketAddr) -> Option<io::Result<std::net::UdpSocket>> {
        None
    }
    // The address a datagram to `to` leaves this stage's egress from, or why
    // no route reaches it. Only an egress says: past a proxy, the exit picks.
    fn source_toward(&self, _to: SocketAddr) -> Option<io::Result<IpAddr>> {
        None
    }
    // The first proxy hop on this stage's path, counted from the egress,
    // whose own last attempt failed.
    fn failing_hop(&self) -> Option<FailingHop> {
        None
    }
    // The activity guard of the session this stage is, for a tunnel stacked
    // on it to name the session it rides on. Only a session hop has one.
    fn session_activity(&self) -> Option<Arc<tokio::sync::RwLock<()>>> {
        None
    }
    fn budget(&self) -> Duration;
    async fn shutdown(&self) {}
    fn describe(&self) -> String;
}

pub struct Egress {
    id: u32,
    binding: SocketEgress,
    timeout: Duration,
    // How a name dialled straight out of this egress is resolved, with no
    // proxy above it. None is the host's own DNS.
    naming: Option<Naming>,
}

impl Egress {
    // The egress a route leaves on. Only the proxy runtime builds one, from a
    // saved egress; a dialer that builds its own takes no route.
    pub fn new(id: u32, binding: SocketEgress, timeout: Duration) -> Self {
        Self {
            id,
            binding,
            timeout,
            naming: None,
        }
    }
    // This egress, resolving the names dialled straight out of it as
    // `naming` says. Host is the host's own DNS.
    pub fn with_naming(mut self, naming: Naming) -> Self {
        self.naming = match naming {
            Naming::Host => None,
            naming => Some(naming),
        };
        self
    }
    // A naming that asks `servers` in order from this egress, by address, and
    // judges their answers by `trust`. It holds its own answers and TLS
    // sessions, so every egress given it shares them and no other does.
    pub fn servers_naming(&self, servers: Vec<DnsServer>, trust: Trust) -> Naming {
        let carrier = Self {
            id: self.id,
            binding: self.binding.clone(),
            timeout: self.timeout,
            naming: None,
        };
        Naming::servers(servers, trust, Arc::new(carrier))
    }
    // Whether the names dialled out of this egress go to listed servers
    // rather than the host's own DNS.
    pub fn names_by_servers(&self) -> bool {
        self.naming.is_some()
    }
    // The naming that resolves names dialled straight out of this egress,
    // when it lists servers; None is the host's own DNS. A hop above the
    // egress that would ask the host is given this one, answers and all.
    pub fn naming(&self) -> Option<&Naming> {
        self.naming.as_ref()
    }
    // Drops what this egress's naming holds for `host`, after none of the
    // addresses it gave could be reached.
    pub fn forget(&self, host: &str) {
        match &self.naming {
            None => crate::resolution::forget_host(host),
            Some(naming) => naming.forget(host),
        }
    }
    // What this egress's naming resolves `host` to, as addresses it can
    // reach, in the order the naming gives them.
    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, DialError> {
        let addresses = match &self.naming {
            None => host_dns::lookup(host)
                .await
                .map_err(DialError::Destination)?,
            Some(naming) => match naming.resolve(self, 0, host, self.timeout).await? {
                Resolution::Addresses(addresses) => addresses,
                // A naming that hands the name on has no one to hand it to
                // here: the lookup fails rather than going anywhere else.
                Resolution::Unresolved => {
                    return Err(DialError::Destination(io::Error::other(ResolutionFailed)));
                }
            },
        };
        Ok(addresses
            .into_iter()
            .filter(|ip| self.binding.supports_address(*ip))
            .collect())
    }
    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn binding(&self) -> &SocketEgress {
        &self.binding
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
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
                format!("{NO_ROUTE_VIA}{via}: {error}"),
            ));
        }
        DialError::destination(error)
    }
    // What this egress's naming gives for `host` that the egress can reach,
    // in the order it gives them, for a direct leg's blocking dialer.
    //
    // The caller must be on a thread that may block, inside `blocking`: a
    // runtime's blocking thread, as `spawn_blocking` gives. Listed servers
    // are asked by blocking that thread on the runtime it belongs to. Asked
    // from anywhere else, a runtime's worker above all, where waiting on the
    // runtime would stall it, the lookup fails at once, and a debug build
    // stops on it. With no runtime the servers cannot be asked, and the
    // lookup fails.
    pub fn resolve_blocking(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        if self.naming.is_none() {
            return Ok(host_dns::lookup_blocking(host)?
                .into_iter()
                .filter(|ip| self.binding.supports_address(*ip))
                .collect());
        }
        let may_block = MAY_BLOCK.with(Cell::get);
        debug_assert!(
            may_block,
            "a blocking lookup through listed servers runs only inside `blocking`"
        );
        if !may_block {
            return Err(io::Error::other(
                "a blocking lookup through listed servers was asked from a thread that may not block",
            ));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        let looked_up = runtime.block_on(self.lookup(host));
        looked_up.map_err(|error| match error {
            DialError::Destination(error)
            | DialError::Egress(error)
            | DialError::Refused(error) => error,
            error => io::Error::other(error.to_string()),
        })
    }
    #[expect(
        clippy::disallowed_methods,
        reason = "an egress leg dials the destination"
    )]
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
    #[expect(clippy::disallowed_methods, reason = "an egress leg's datagram socket")]
    fn bind(&self, peer: SocketAddr) -> io::Result<std::net::UdpSocket> {
        self.binding
            .bind_udp(peer)
            .inspect(|_| record::bound(peer, false))
    }
}

#[async_trait::async_trait]
impl Dialer for Egress {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let started = tokio::time::Instant::now();
        let attempt = async {
            let peers: Vec<_> = if target.addresses.is_empty() {
                self.lookup(&target.host).await?
            } else {
                target
                    .addresses
                    .iter()
                    .copied()
                    .filter(|ip| self.binding.supports_address(*ip))
                    .collect()
            };
            let peers: Vec<_> = peers
                .into_iter()
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
                    naming: None,
                };
                match attempt.connect_address(*peer).await {
                    Ok(stream) => {
                        record::connected(&target.host, peer.ip());
                        return Ok(stream);
                    }
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
        if let Err(error) = &result
            && unreached(target, error)
        {
            self.forget(&target.host);
        }
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
        let mut addresses = tokio::time::timeout(self.timeout, self.lookup(host))
            .await
            .map_err(|_| {
                DialError::Destination(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{RESOLVE_TIMED_OUT}: {host}"),
                ))
            })??;
        addresses.sort();
        Ok(Resolution::Addresses(addresses))
    }
    fn offers(&self) -> Transports {
        Transports::TcpUdp
    }
    fn relay_datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        Some(Arc::new(socks5_udp::EgressDatagrams(self)))
    }
    fn bind_local_udp(&self, local: SocketAddr) -> Option<io::Result<std::net::UdpSocket>> {
        Some(self.binding.bind_udp_at(local))
    }
    fn source_toward(&self, to: SocketAddr) -> Option<io::Result<IpAddr>> {
        Some(self.binding.source_toward(to))
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
    pub naming: Naming,
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
            Purpose::ProxyEndpoint { proxy, .. } => Some(proxy),
            _ => None,
        };
        let unreachable = |proxy, reply| DialError::Hop {
            proxy,
            source: crate::transport::hop_before(destination, target.port, reply),
        };
        let connected = tokio::time::timeout_at(
            deadline,
            self.spec.connect(stream, destination, target.port),
        )
        .await;
        match (connected, next) {
            (Ok(Ok(())), _) => Ok(()),
            (Ok(Err(ConnectFailure::Proxy(error))), _) => Err(DialError::hop(self.id, error)),
            (Ok(Err(ConnectFailure::Unreachable(reply))), Some(proxy)) => {
                Err(unreachable(proxy, reply))
            }
            (Ok(Err(ConnectFailure::Unreachable(reply))), None) => Err(DialError::hop(
                self.id,
                self.spec
                    .destination_unreachable(destination, target.port, reply),
            )),
            (Err(_), Some(proxy)) => Err(unreachable(proxy, Reply::Silent)),
            // A SOCKS5 proxy that answered its greeting is up, so the
            // destination is the one that did not answer. An HTTP proxy says
            // nothing before its reply.
            (Err(_), None) if matches!(self.spec.kind, crate::transport::TransportKind::Socks5) => {
                Err(unanswered(target, destination, self.id))
            }
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
            // The endpoint connection carries this one alone.
            purpose: Purpose::ProxyEndpoint {
                proxy: self.id,
                carrying: Some(SocketClass::of(&target.purpose)),
            },
            addresses: Vec::new(),
        };
        // The lookup has a window of its own in `budget`, so a slow one
        // leaves the connect its whole timeout.
        let destinations = hop_destinations(self, target).await?;
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
                    | DialError::Refused(source) => self.failure.unreachable(source, self.timeout),
                    // The proxy hop beneath was reached and could not reach
                    // this endpoint through its own proxy.
                    DialError::Hop { proxy, source } if *proxy == self.id => {
                        self.failure.blame(source, self.timeout)
                    }
                    // Any other failure belongs to the stage beneath.
                    _ => {}
                })?;
            // Reserve part of the remaining budget for every other address.
            let remaining = self.timeout.saturating_sub(started.elapsed());
            let mut negotiated = self
                .negotiate(
                    &mut dialed.stream,
                    destination,
                    target,
                    remaining / (destinations.len() - index) as u32,
                )
                .await;
            crate::metrics::record_stream(self.spec.kind.tunnel_kind(), &negotiated);
            // A wait on a proxy that answered lately is the destination's,
            // and says nothing new of the proxy. A SOCKS5 proxy's own wait is
            // the exception: `negotiate` turns its quiet CONNECT into the
            // destination's error, so the only timeout it returns for a SOCKS5
            // hop is the proxy silent at its own greeting.
            let socks = matches!(self.spec.kind, crate::transport::TransportKind::Socks5);
            let silent_greeting = socks && matches!(negotiated, Err(DialError::Timeout { .. }));
            let waited = matches!(&negotiated, Err(error) if ran_out(error))
                && !silent_greeting
                && self.failure.alive();
            match &negotiated {
                _ if waited => {}
                Ok(()) => self.failure.clear(),
                // The proxy took the request for the next hop, so it is
                // working. An HTTP proxy that kept quiet for it was not heard.
                Err(DialError::Hop { proxy, source }) if *proxy != self.id => {
                    if socks || !crate::transport::is_silent_hop_before(source) {
                        self.failure.clear();
                    } else {
                        self.failure.spare();
                    }
                }
                Err(error) => self.failure.note(error, self.timeout),
            }
            if waited {
                negotiated = Err(unanswered(target, destination, self.id));
            }
            match negotiated {
                Ok(()) => {
                    // A negotiated socket carries proxy framing and cannot use NNTP's raw TCP fast path.
                    dialed.stream = DialedStream::Tunnel(dialed.stream.into_tunnel());
                    dialed.path.proxies.push(self.id);
                    if let Ok(ip) = destination.parse() {
                        record::connected(&target.host, ip);
                    }
                    return Ok(dialed);
                }
                Err(error @ DialError::Fatal(_)) => return Err(error),
                Err(error) => last = Some(error),
            }
        }
        let error = last.expect("a hop dials at least one destination");
        if unreached(target, &error) {
            self.naming.forget(&target.host);
        }
        Err(error)
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.naming.resolve(self, self.id, host, self.timeout).await
    }
    // Each connection through the proxy opens one endpoint connection in
    // the share of the tunnel beneath.
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        self.inner.room(purpose)
    }
    fn failing_hop(&self) -> Option<FailingHop> {
        self.inner
            .failing_hop()
            .or_else(|| self.failure.of(self.id))
    }
    // A SOCKS5 hop carries datagrams by UDP ASSOCIATE when the stage beneath
    // carries them to its relay. An HTTP CONNECT hop carries none.
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        if !matches!(self.spec.kind, crate::transport::TransportKind::Socks5) {
            return None;
        }
        let relay = Arc::clone(&self.inner).relay_datagrams()?;
        Some(Arc::new(socks5_udp::Socks5Datagrams::new(self, relay)))
    }
    fn budget(&self) -> Duration {
        self.inner
            .budget()
            .saturating_add(self.timeout)
            .saturating_add(self.naming.window(self.timeout))
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
            purpose: Purpose::ProxyEndpoint {
                proxy: self.proxy,
                carrying: None,
            },
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
    _socket: Option<SocketPermit>,
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
            _socket: None,
        }
    }
    // The stream holds `socket` of its session's share while it lives.
    fn holding(mut self, socket: Option<SocketPermit>) -> Self {
        self._socket = socket;
        self
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
    held: Arc<identity::Held>,
}
impl SessionCapacity {
    fn acquire(&self) -> Result<Option<Arc<tokio::sync::OwnedSemaphorePermit>>, DialError> {
        let Some(budget) = &self.budget else {
            return Ok(None);
        };
        let mut permit = self.held.permit.lock().expect("session capacity");
        if permit.is_none() {
            *permit = Some(Arc::new(budget.clone().try_acquire_owned().map_err(|_| {
                DialError::Skipped("WireGuard session budget is exhausted; retry after an idle session retires".into())
            })?));
        }
        Ok(permit.clone())
    }
    fn release(&self) {
        self.held.release();
    }
    pub fn new(budget: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            budget: Some(budget),
            held: Default::default(),
        }
    }
    // The capacity of a session built to probe a path. A probe never brings
    // up a second tunnel for an identity a session already holds: it asks
    // through that session's tunnel instead.
    pub fn probing(self) -> Self {
        Self {
            budget: self.budget,
            held: Arc::new(identity::Held::probe()),
        }
    }
}

pub struct SessionHop {
    pub capacity: SessionCapacity,
    // The share of the tunnel's sockets each kind of traffic may hold.
    pub sockets: Arc<SessionSockets>,
    // Shared by every use of this canonical session path.
    pub activity: Arc<tokio::sync::RwLock<()>>,
    pub id: u32,
    pub provider: Arc<dyn TunnelProvider>,
    pub inner: Arc<dyn Dialer>,
    pub transport: Option<Arc<InnerTransport>>,
    pub path: DialPath,
    pub endpoint: Option<SocketAddr>,
    pub timeout: Duration,
    pub naming: Naming,
    pub resolver: Option<Arc<crate::WireGuardTunnelProvider>>,
    // How the provider at a WireGuard hop's far end forwards a port to it.
    pub forwarding: PortForwarding,
    pub failure: HopFailure,
}

impl SessionHop {
    // A permit of `class`'s share of the tunnel's sockets. A full share
    // says the session is at capacity until one of its sockets closes.
    fn take_socket(&self, class: SocketClass) -> Result<Option<SocketPermit>, DialError> {
        self.sockets.take(class).map_err(|freed| {
            tracing::debug!(
                proxy = self.id,
                share = class.as_str(),
                "the tunnel's socket share is full"
            );
            DialError::AtCapacity(vec![freed])
        })
    }

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
        if let Some(lent) = identity::claim(self).await {
            // The lender's tunnel is up or comes up under its own claim.
            return lent
                .provider
                .prepare()
                .await
                .map_err(|e| DialError::hop(self.id, e));
        }
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
            Err(error) => self.failure.note(error, self.timeout),
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
            identity::release(self);
            true
        } else {
            false
        }
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let started = tokio::time::Instant::now();
        self.prepare_carrier().await?;
        // Resolving through the proxy dials it, so the name is resolved
        // before this dial holds the session. The lookup has a window of its
        // own in `budget`, so a slow one does not shorten the connect.
        let resolving = tokio::time::Instant::now();
        let destinations = hop_destinations(self, target).await?;
        let resolved = resolving.elapsed();
        // A probe lent another session's tunnel dials through it, and holds
        // that session busy, not its own; the lender's budget already counts
        // the tunnel.
        let (provider, activity, capacity) = match identity::claim(self).await {
            Some(lent) => (lent.provider, lent.activity, None),
            None => (
                self.provider.clone(),
                self.activity.clone().read_owned().await,
                self.capacity.acquire()?,
            ),
        };
        let socket = self.take_socket(SocketClass::of(&target.purpose))?;
        let outcome = Arc::new(ConnectionOutcome::default());
        let mut connected = Err(DialError::Skipped("no destination".into()));
        let mut tried = target.host.as_str();
        for (index, destination) in destinations.iter().enumerate() {
            tried = destination.as_str();
            // Reserve part of the remaining budget for every other address.
            let remaining = self
                .inner
                .budget()
                .saturating_add(self.timeout)
                .saturating_sub(started.elapsed().saturating_sub(resolved));
            connected = tokio::time::timeout(
                remaining / (destinations.len() - index) as u32,
                provider.dial_observed(destination, target.port, outcome.clone()),
            )
            .await
            .map_err(|_| DialError::Timeout {
                stage: self.describe(),
            })
            .and_then(|result| result.map_err(|error| DialError::hop(self.id, error)));
            if connected.is_ok()
                && let Ok(ip) = destination.parse()
            {
                record::connected(&target.host, ip);
            }
            if matches!(connected, Ok(_) | Err(DialError::Fatal(_))) {
                break;
            }
        }
        if let Some(kind) = self.provider.kind() {
            crate::metrics::record_stream(kind, &connected);
        }
        // A wait on a session that answered lately is the destination's, and
        // says nothing new of the session.
        let waited = matches!(&connected, Err(error) if ran_out(error)) && self.failure.alive();
        let stream = match connected {
            Ok(stream) => {
                self.failure.clear();
                stream
            }
            Err(error) => {
                let error = if waited {
                    unanswered(target, tried, self.id)
                } else {
                    self.failure.note(&error, self.timeout);
                    error
                };
                if unreached(target, &error) {
                    self.naming.forget(&target.host);
                }
                drop(capacity);
                drop(activity);
                // A destination that refuses or keeps quiet says nothing
                // about the tunnel.
                if !waited && error.is_path_evidence() {
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
            stream: DialedStream::Tunnel(Box::new(
                OutcomeStream::new(
                    DialedStream::Tunnel(stream),
                    outcome.clone(),
                    Some(activity),
                    capacity,
                )
                .holding(socket),
            )),
            outcome,
            path,
            peer,
            source,
            setup: None,
        })
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        // A WireGuard tunnel asks its DNS servers from inside the tunnel. It
        // was built with the same servers this naming lists, so it asks
        // them itself when it can serve the naming; otherwise Weaver's own
        // client asks them through this hop.
        let (
            Naming::ProxyServers {
                servers,
                trust,
                answers,
                ..
            },
            Some(provider),
        ) = (&self.naming, &self.resolver)
        else {
            return self.naming.resolve(self, self.id, host, self.timeout).await;
        };
        if !tunnel_resolver_serves(servers, *trust) {
            return self.naming.resolve(self, self.id, host, self.timeout).await;
        }
        if host.parse::<IpAddr>().is_err() {
            record::asking(host, || Asker::proxy_servers(self.id, servers, *trust));
        }
        // The tunnel's resolver is given one server, asked in plain.
        let from = servers.first().map(record::Answerer::of);
        answers
            .resolve_accounted(Resolver::WireGuard, host, || async {
                let found = async {
                    self.prepare_carrier().await?;
                    let (resolver, _activity, _capacity) = match identity::claim(self).await {
                        Some(lent) => (lent.resolver, lent.activity, None),
                        None => (
                            provider.clone(),
                            self.activity.clone().read_owned().await,
                            self.capacity.acquire()?,
                        ),
                    };
                    resolver
                        .resolve_host(host)
                        .await
                        .inspect(|_| crate::metrics::record_resolution(Resolver::WireGuard, true))
                        .inspect_err(|_| {
                            crate::metrics::record_resolution(Resolver::WireGuard, false)
                        })
                        .inspect_err(|error| match error {
                            // The tunnel's resolver asked, and says it found nothing.
                            TunnelError::Dial { detail, .. } => record::tunnel_failed(host, detail),
                            TunnelError::Configuration(said) => {
                                record::failed(host, LookupFailure::Other, || said.clone())
                            }
                            // The tunnel never came up to ask.
                            error => {
                                record::failed(host, LookupFailure::NoReply, || error.to_string())
                            }
                        })
                        // The tunnel's resolver gives no TTL.
                        .map(|addresses| Lookup {
                            addresses,
                            ttl: Ttl::Unknown,
                            nonexistent: false,
                        })
                        .map_err(|error| DialError::hop(self.id, error))
                        .inspect_err(|error| self.failure.note(error, self.timeout))
                }
                .await;
                // An address came from the listed servers, as sent: counted as
                // Weaver's own client counts an answer.
                let judged = found.is_ok().then_some(record::TrustOutcome::AsSent);
                if let Some(judged) = judged {
                    record::answered(host, from, judged);
                    crate::metrics::record_dns_wire(WireKind::Plain);
                    crate::metrics::record_dns_trust(judged);
                }
                let account = Account {
                    judged,
                    failure: None,
                    from: judged.and(from),
                };
                (found, account)
            })
            .await
            .inspect(|addresses| record::found(host, addresses))
            .inspect_err(|error| record::failed_saying(host, error))
            .map(Resolution::Addresses)
    }
    fn failing_hop(&self) -> Option<FailingHop> {
        self.inner
            .failing_hop()
            .or_else(|| self.failure.of(self.id))
    }
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        self.sockets
            .room(SocketClass::of(purpose))
            .map_err(|freed| vec![freed])
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        self.resolver
            .is_some()
            .then(|| Arc::new(datagram::SessionDatagrams::new(self)) as Arc<dyn DatagramTransport>)
    }
    fn inbound(self: Arc<Self>) -> Option<Arc<dyn Inbound>> {
        self.resolver
            .is_some()
            .then(|| Arc::new(inbound::SessionInbound::new(self)) as Arc<dyn Inbound>)
    }
    fn budget(&self) -> Duration {
        self.inner
            .budget()
            .saturating_add(self.timeout)
            .saturating_add(self.naming.window(self.timeout))
    }
    // Stops this hop's own session only. The stage beneath may be another
    // session shared with other routes, or one a probe runtime borrowed;
    // whoever owns it shuts it down.
    async fn shutdown(&self) {
        self.provider.shutdown().await;
        self.capacity.release();
        identity::release(self);
    }
    fn session_activity(&self) -> Option<Arc<tokio::sync::RwLock<()>>> {
        Some(self.activity.clone())
    }
    fn describe(&self) -> String {
        format!("{} → proxy {}", self.inner.describe(), self.id)
    }
}

struct RungState {
    until: Option<tokio::time::Instant>,
    failures: u32,
    // Raised when the rung's cooldown ends, so a dial that skipped the rung
    // while it cooled can try it again.
    cooled: Arc<Notify>,
    // The timer that raises `cooled` at `until`.
    timer: Option<tokio::task::AbortHandle>,
}
impl RungState {
    fn new() -> Self {
        Self {
            until: None,
            failures: 0,
            cooled: Arc::new(Notify::new()),
            timer: None,
        }
    }
    // Cools the rung until `until`, raising `cooled` then.
    fn cool(&mut self, until: tokio::time::Instant) {
        self.until = Some(until);
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let cooled = self.cooled.clone();
            self.timer = Some(
                runtime
                    .spawn(async move {
                        tokio::time::sleep_until(until).await;
                        cooled.notify_waiters();
                    })
                    .abort_handle(),
            );
        }
    }
    // Clears the rung's failures and any cooldown it is in.
    fn clear(&mut self) {
        self.failures = 0;
        if self.until.take().is_some() {
            if let Some(timer) = self.timer.take() {
                timer.abort();
            }
            self.cooled.notify_waiters();
        }
    }
}
impl Drop for RungState {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
    }
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
                state.cool(now + self.rung_cooldown);
            }
        } else {
            state.clear();
        }
    }
    // A ladder that leaves a failed rung alone for thirty seconds.
    pub fn new(rungs: Vec<Arc<dyn Dialer>>) -> Self {
        Self::with_rung_cooldown(rungs, PATH_COOLDOWN)
    }
    // A ladder that leaves a failed rung alone for `rung_cooldown`.
    pub fn with_rung_cooldown(rungs: Vec<Arc<dyn Dialer>>, rung_cooldown: Duration) -> Self {
        let states = (0..rungs.len()).map(|_| RungState::new()).collect();
        Self {
            rungs,
            states: Arc::new(Mutex::new(states)),
            rung_cooldown,
        }
    }
}
impl Fallback {
    // The rung whose naming answers a caller resolving through the ladder:
    // the first one a dial that is not a probe would try.
    fn answering(&self) -> Option<usize> {
        (0..self.rungs.len()).find(|index| self.allows_rung(*index, false))
    }
}

// `target` as `rung`, a ladder rung or a route leg, dials it. Addresses
// pinned for a name were resolved by the answering stage's naming; any other
// asks its own naming and keeps only the pinned addresses it also gives, or
// is handed the name when its proxy resolves names. A stage whose naming
// gives none of them is skipped.
pub async fn as_named_by<'a>(
    rung: &dyn Dialer,
    target: &'a Target,
    answered: bool,
) -> Result<std::borrow::Cow<'a, Target>, DialError> {
    if answered || target.addresses.is_empty() || target.host.parse::<IpAddr>().is_ok() {
        return Ok(std::borrow::Cow::Borrowed(target));
    }
    let addresses = match rung.resolve(&target.host).await? {
        Resolution::Unresolved => Vec::new(),
        Resolution::Addresses(own) => {
            let kept: Vec<_> = target
                .addresses
                .iter()
                .copied()
                .filter(|ip| own.contains(ip))
                .collect();
            if kept.is_empty() {
                return Err(DialError::Skipped(format!(
                    "{}: this rung resolves it to none of the addresses checked",
                    target.host
                )));
            }
            kept
        }
    };
    Ok(std::borrow::Cow::Owned(Target {
        addresses,
        ..target.clone()
    }))
}

// Whether a dial for `purpose` probes its path, and so tries rungs still
// cooling.
fn probes(purpose: &Purpose) -> bool {
    matches!(
        purpose,
        Purpose::Probe | Purpose::NntpProbe { .. } | Purpose::Nntp { probing: true, .. }
    )
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
        let answering = self.answering();
        for (index, rung) in self.rungs.iter().enumerate() {
            if !self.allows_rung(index, probes(&target.purpose)) {
                continue;
            }
            let dialed = match as_named_by(rung.as_ref(), target, answering == Some(index)).await {
                Ok(target) => rung.dial(&target).await,
                Err(error) => Err(error),
            };
            match dialed {
                Ok(mut dialed) => {
                    self.states.lock().expect("rung state")[index].clear();
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
    // What the first rung a dial would try resolves `host` to.
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        match self.answering() {
            Some(index) => self.rungs[index].resolve(host).await,
            None => Ok(Resolution::Unresolved),
        }
    }
    // Room on the first rung a dial for `purpose` would try: a probe tries
    // the rungs still cooling too. With none on it, the dial waits for that
    // rung's share and for each rung it skipped as cooling to finish its
    // cooldown or free a socket of its own.
    fn room(&self, purpose: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        let probe = probes(purpose);
        let mut skipped = Vec::new();
        for (index, rung) in self.rungs.iter().enumerate() {
            if !self.allows_rung(index, probe) {
                skipped.push(
                    self.states.lock().expect("rung state")[index]
                        .cooled
                        .clone(),
                );
                if let Err(freed) = rung.room(purpose) {
                    skipped.extend(freed);
                }
                continue;
            }
            return rung.room(purpose).map_err(|mut freed| {
                freed.extend(skipped);
                freed
            });
        }
        Ok(())
    }
    // The datagrams of the first rung a dial would try.
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        let index = (0..self.rungs.len()).find(|index| self.allows_rung(*index, false))?;
        Arc::clone(&self.rungs[index]).datagrams()
    }
    // The inbound connections of the first rung a dial would try.
    fn inbound(self: Arc<Self>) -> Option<Arc<dyn Inbound>> {
        let index = (0..self.rungs.len()).find(|index| self.allows_rung(*index, false))?;
        Arc::clone(&self.rungs[index]).inbound()
    }
    // The failing hop of the first rung a dial would try, else of the first
    // rung that has one.
    fn failing_hop(&self) -> Option<FailingHop> {
        self.answering()
            .and_then(|index| self.rungs[index].failing_hop())
            .or_else(|| self.rungs.iter().find_map(|rung| rung.failing_hop()))
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
