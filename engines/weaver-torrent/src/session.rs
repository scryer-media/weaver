// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use ports::AnnouncePort;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::Read,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::Identity;
use crate::buffers::ByteBuf;
use crate::clone_to_owned::CloneToOwned;
use crate::core::{
    hash_id::InfoHashes,
    net::{ClientShape, HttpClients, Resolver, TorrentNaming},
    spawn_utils::spawn_with_cancel,
    torrent_metainfo::TorrentMetaV1Owned,
};
use crate::dht::{Dht, Id20};
use crate::lsd::LocalServiceDiscovery;
use crate::meter::Meter;
use crate::peer_protocol::Handshake;
use crate::ssl::{SslIdentity, SslTorrent};
use crate::torrent_state::live::{RateTick, choker::Choker};
use crate::tracker::{
    HttpAnnounces, ListenPort, StoppedAnnounces, Tracker, TrackerTask, UdpTrackerClient,
};
use crate::{
    api::TorrentIdOrHash,
    bitv_factory::BitVFactory,
    connection_slots::ConnectionSlots,
    ip_ranges::IpRanges,
    limits::Limits,
    peer_connection::PeerConnectionOptions,
    read_buf::ReadBuf,
    session_persistence::SessionPersistenceStore,
    session_stats::SessionStats,
    spawn_utils::BlockingSpawner,
    storage::BoxStorageFactory,
    stream_connect::{ConnectionKind, StreamConnector},
    torrent_state::{ManagedTorrentHandle, TorrentMetadata, live::peers::PeerList},
    type_aliases::{BoxAsyncReadVectored, PeerStream},
};
use anyhow::{Context, bail};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};
use tracing::{debug, info, trace};

#[cfg(test)]
use crate::{
    connection_slots::{DEFAULT_CONNECTIONS_LIMIT, DEFAULT_CONNECTIONS_SLACK},
    stream_connect::ConnectionOptions,
};

mod actions;
mod add;
mod construction;
pub(crate) mod external_ip;
mod listeners;
mod options;
mod peer_sources;
mod ports;
mod turnover;
pub use options::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, DhtSessionConfig, IpFilter,
    ListOnlyResponse, SessionLimits, SessionOptions, SessionPersistenceConfig, SharedLimits,
};
pub use peer_sources::{Discovery, FetchPhase, MetadataFetch};
pub(crate) use peer_sources::{a_tracker_answered, speaks_pex, trackers_only};

use external_ip::ExternalIps;

pub const SUPPORTED_SCHEMES: [&str; 3] = ["http:", "https:", "magnet:"];

pub type TorrentId = usize;

// How long a listener rests after a failed accept before it accepts again.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(10);

struct ParsedTorrentFile {
    meta: TorrentMetaV1Owned,
    torrent_bytes: Bytes,
}

fn torrent_from_bytes(bytes: Bytes) -> anyhow::Result<ParsedTorrentFile> {
    let parsed = crate::core::torrent_metainfo::torrent_from_bytes(&bytes)?;
    // Its shape alone: the values carry the trackers' and seeds' passkeys.
    trace!(
        bytes = bytes.len(),
        info_bytes = parsed.info.raw_bytes.0.len(),
        keys = ?metainfo_keys(&bytes),
        info_hash = ?parsed.info_hash,
        "torrent read"
    );
    Ok(ParsedTorrentFile {
        meta: parsed.clone_to_owned(Some(&bytes)),
        torrent_bytes: bytes,
    })
}

// The names of a metainfo's top-level keys, sorted.
fn metainfo_keys(bytes: &[u8]) -> Vec<String> {
    match crate::bencode::dyn_from_bytes::<ByteBuf>(bytes) {
        Ok(crate::bencode::BencodeValue::Dict(dict)) => {
            let mut keys: Vec<String> = dict.keys().map(ToString::to_string).collect();
            keys.sort();
            keys
        }
        _ => Vec::new(),
    }
}

#[derive(Default)]
pub struct SessionDatabase {
    torrents: HashMap<TorrentId, ManagedTorrentHandle>,
    index: TorrentIndex,
    // Rings as a torrent is added: a magnet's torrent enters the session
    // once its metadata is received.
    added: Arc<Notify>,
}

impl SessionDatabase {
    fn add_torrent(&mut self, torrent: ManagedTorrentHandle, id: TorrentId) {
        self.index
            .insert(id, torrent.info_hashes(), torrent.encryption());
        self.torrents.insert(id, torrent);
        self.added.notify_waiters();
    }

    fn remove_torrent(&mut self, id: TorrentId) -> Option<ManagedTorrentHandle> {
        let removed = self.torrents.remove(&id)?;
        self.index
            .remove(id, removed.info_hashes(), removed.encryption());
        Some(removed)
    }

    /// The torrent that answers to `hash`, one of its 20-byte ids.
    fn by_hash(&self, hash: Id20) -> Option<(TorrentId, &ManagedTorrentHandle)> {
        let id = self.index.by_hash(hash)?;
        Some((id, self.torrents.get(&id)?))
    }
}

/// Each torrent's 20-byte ids, and `HASH('req2', id)` an encryption
/// handshake names each by, so an accepted connection finds its torrent
/// in one lookup (libtorrent `torrent_list::find_obfuscated`); and how many
/// torrents set each encryption policy over the session's.
#[derive(Default)]
struct TorrentIndex {
    by_hash: HashMap<Id20, TorrentId>,
    /// The id and the policy its torrent sets over the session's.
    by_obfuscated: HashMap<Id20, (Id20, TorrentId, Option<crate::mse::EncryptionPolicy>)>,
    /// Torrents setting `Disabled`, `Prefer` and `Require`.
    overrides: [usize; 3],
}

impl TorrentIndex {
    fn slot(policy: crate::mse::EncryptionPolicy) -> usize {
        match policy {
            crate::mse::EncryptionPolicy::Disabled => 0,
            crate::mse::EncryptionPolicy::Prefer => 1,
            crate::mse::EncryptionPolicy::Require => 2,
        }
    }

    fn insert(
        &mut self,
        id: TorrentId,
        hashes: InfoHashes,
        encryption: Option<crate::mse::EncryptionPolicy>,
    ) {
        for hash in hashes.ids20() {
            self.by_hash.insert(hash, id);
            self.by_obfuscated
                .insert(crate::mse::obfuscated_hash(&hash), (hash, id, encryption));
        }
        if let Some(policy) = encryption {
            self.overrides[Self::slot(policy)] += 1;
        }
    }

    fn remove(
        &mut self,
        id: TorrentId,
        hashes: InfoHashes,
        encryption: Option<crate::mse::EncryptionPolicy>,
    ) {
        for hash in hashes.ids20() {
            if self.by_hash.get(&hash) == Some(&id) {
                self.by_hash.remove(&hash);
            }
            let obfuscated = crate::mse::obfuscated_hash(&hash);
            if self.by_obfuscated.get(&obfuscated).map(|(_, t, _)| *t) == Some(id) {
                self.by_obfuscated.remove(&obfuscated);
            }
        }
        if let Some(policy) = encryption {
            let count = &mut self.overrides[Self::slot(policy)];
            *count = count.saturating_sub(1);
        }
    }

    fn by_hash(&self, hash: Id20) -> Option<TorrentId> {
        self.by_hash.get(&hash).copied()
    }

    /// The id an encryption handshake named by `HASH('req2', id)`, its
    /// torrent, and the torrent's policy under the session's `session`.
    fn by_obfuscated(
        &self,
        obfuscated: &Id20,
        session: crate::mse::EncryptionPolicy,
    ) -> Option<(Id20, TorrentId, crate::mse::EncryptionPolicy)> {
        let (hash, id, encryption) = *self.by_obfuscated.get(obfuscated)?;
        Some((hash, id, encryption.unwrap_or(session)))
    }

    /// What an accepted connection may be taken as under the session's
    /// `session` and every torrent's policy. A torrent with none of its own
    /// keeps `session`, which already counts.
    fn incoming(&self, session: crate::mse::EncryptionPolicy) -> crate::mse::Incoming {
        use crate::mse::EncryptionPolicy::{Disabled, Prefer, Require};
        let any = |policy| self.overrides[Self::slot(policy)] > 0;
        crate::mse::Incoming {
            encrypted: session != Disabled || any(Prefer) || any(Require),
            plaintext: session != Require || any(Disabled) || any(Prefer),
        }
    }
}

/// The peers the DHT finds for `info_hash`, announcing `port` to the nodes
/// closest to it. The DHT search reads the port when it starts, so a new
/// port starts a new search, which announces it (libtorrent
/// `torrent::dht_announce` reads the port at each announce), and so does
/// each change of `announce` (libtorrent `torrent_handle::force_dht_announce`).
fn dht_peers(
    dht: Dht,
    info_hash: Id20,
    mut port: ListenPort,
    mut announce: tokio::sync::watch::Receiver<()>,
) -> BoxStream<'static, SocketAddr> {
    async_stream::stream! {
        loop {
            let ports = *port.borrow_and_update();
            let mut peers = dht.get_peers_announcing(info_hash, ports.v4, ports.v6);
            announce.mark_unchanged();
            loop {
                tokio::select! {
                    peer = peers.next() => match peer {
                        Some(peer) => yield peer,
                        None => return,
                    },
                    Ok(()) = port.changed() => break,
                    Ok(()) = announce.changed() => break,
                }
            }
        }
    }
    .boxed()
}

pub struct Session {
    // Core state and services
    pub(crate) db: RwLock<SessionDatabase>,
    next_id: AtomicUsize,
    pub(crate) bitv_factory: Arc<dyn BitVFactory>,
    pub(crate) spawner: BlockingSpawner,

    // Network
    // The DHT's, and every torrent's that names none of its own.
    identity: Identity,
    announce_port: Arc<AnnouncePort>,
    listen_addr: Option<SocketAddr>,
    // Whether the DHT runs over the listener's uTP socket, whose UDP port
    // `announce_port` keeps the mapping of.
    dht_on_listener: bool,
    // The port SSL torrents announce: the SSL listener's, or the port a
    // mapper got for it, while the session announces a port at all.
    ssl_port: Arc<AnnouncePort>,
    ssl_listen_addr: Option<SocketAddr>,
    // Our address as the internet sees it, one for each family.
    pub(crate) external_ips: Arc<ExternalIps>,
    // When the session started, which dates the closes its peer lists
    // order connect candidates by (libtorrent `session_impl::m_created`).
    started: tokio::time::Instant,
    dht: Option<Dht>,
    pub(crate) connector: Arc<StreamConnector>,
    pub(crate) utp_sockets: Vec<crate::network::UtpSocket>,
    // The clients every request is asked through: metainfo and the allow
    // list by their URL, trackers and web seeds each held to the address
    // its request was checked at.
    pub(crate) http_clients: Arc<dyn HttpClients>,
    // Resolves web seeds' host names, so their addresses are known before
    // they are asked: the session's resolver.
    pub(crate) resolver: Arc<dyn Resolver>,
    udp_tracker_client: UdpTrackerClient,
    disable_trackers: bool,
    stopped_announces: StoppedAnnounces,
    // The turns HTTP announces take, shared with every session that shares
    // limits.
    http_announces: HttpAnnounces,
    // Rung as an announce table or a metadata fetch's phase changes.
    trackers_changed: Option<Arc<Notify>>,

    // Lifecycle management
    cancellation_token: CancellationToken,
    _cancellation_token_drop_guard: DropGuard,
    // Every task the session spawns: stop() returns once they are gone.
    tasks: TaskTracker,
    // What lets the swarm reach this session: its listeners, its DHT and its
    // local discovery. stop() ends them, and waits for them, before any
    // tracker hears stopped.
    swarm: CancellationToken,
    swarm_tasks: TaskTracker,

    // Runtime settings
    output_folder: PathBuf,
    peer_opts: PeerConnectionOptions,
    default_storage_factory: Option<BoxStorageFactory>,
    persistence: Option<Arc<dyn SessionPersistenceStore>>,
    trackers: HashSet<url::Url>,

    /// Local discovery, unless disabled; it may not have joined its groups
    /// yet (see [`Session::rejoin_local_discovery`]).
    pub(crate) lsd: Option<LocalServiceDiscovery>,

    // Limits and throttling
    pub(crate) concurrent_initialize_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) connection_slots: Arc<ConnectionSlots>,
    // Read by every torrent's peer list at each judgment; changed by
    // `set_allow_multiple_connections_per_ip`.
    pub(crate) allow_multiple_connections_per_ip: Arc<AtomicBool>,
    // `SessionOptions::allow_idna`.
    pub(crate) allow_idna: bool,
    // The addresses banned for bad data sent to any torrent, refused by
    // every torrent's peer list and at every incoming connection while the
    // session runs.
    pub(crate) bad_data_bans: Arc<crate::torrent_state::live::peers::SessionBans>,
    pub ratelimits: Limits,

    // Read wherever an address is judged; changed by `set_ip_filter`.
    ip_filter: RwLock<IpFilter>,
    pub allowlist: Option<IpRanges>,

    // Monitoring / tracing / logging
    pub(crate) stats: Arc<SessionStats>,
    root_span: Option<tracing::Span>,

    // Feature flags
    pub ipv4_only: bool,
    pub peer_limit: Option<usize>,
    // Hands out the session's upload slots to the peers of every torrent.
    pub(crate) choker: Arc<Choker>,
    // Samples the rates of the torrents that move bytes.
    pub(crate) rate_tick: Arc<RateTick>,
    pub(crate) disable_pex: bool,
    // Whether the session listens on uTP, and so connects over it.
    pub(crate) utp: bool,
}

/// A 40 character hex string is a v1 info hash. Checking the length alone
/// also matches URLs that happen to be 40 characters long, such as
/// https://nyaa.si/download/1234567.torrent.
fn is_bare_info_hash(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

async fn torrent_from_url(
    clients: &dyn HttpClients,
    url: &str,
) -> anyhow::Result<ParsedTorrentFile> {
    let mut response = clients
        .client(None, &ClientShape::FETCH)
        .context("error building HTTP(S) client")?
        .get(url)
        .send()
        .await
        .context("error downloading torrent metadata")?;
    if !response.status().is_success() {
        bail!("GET {} returned {}", url, response.status())
    }
    let limit = crate::core::torrent_metainfo::MAX_TORRENT_FILE_BYTES;
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        bail!("torrent at {url} is larger than {limit} bytes");
    }
    let mut b = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("error reading response body from {url}"))?
    {
        if b.len() + chunk.len() > limit {
            bail!("torrent at {url} is larger than {limit} bytes");
        }
        b.extend_from_slice(&chunk);
    }
    torrent_from_bytes(Bytes::from(b)).context("error decoding torrent")
}

// The DHT is Weaver's own, whichever client the session's torrents go as:
// BEP 5's "v" is Weaver's two letters followed by its major and minor version
// as bytes.
fn dht_client_version() -> Vec<u8> {
    let (major, minor, _, _) = crate::crate_version!();
    vec![b'W', b'V', major, minor]
}

pub fn read_local_file_including_stdin(filename: &str) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    if filename == "-" {
        std::io::stdin()
            .read_to_end(&mut buf)
            .context("error reading stdin")?;
    } else {
        std::fs::File::open(filename)
            .context("error opening")?
            .read_to_end(&mut buf)
            .context("error reading")?;
    }
    Ok(buf)
}

pub(crate) struct CheckedIncomingConnection {
    pub kind: ConnectionKind,
    pub addr: SocketAddr,
    pub reader: BoxAsyncReadVectored,
    pub writer: crate::mse::Outbound,
    pub read_buf: ReadBuf,
    pub handshake: Handshake,
    /// How the peer's handshake went (libtorrent
    /// `bt_peer_connection::m_encrypted`, `m_rc4_encrypted`).
    pub encryption: crate::mse::Encryption,
    /// What has crossed the socket so far, the handshakes included.
    pub meter: Meter,
}

/// What a torrent asks for peers with: the hashes and identity it goes by,
/// the trackers and sources it asks, and whether it announces itself.
struct PeerRequest<'a> {
    info_hashes: InfoHashes,
    identity: &'a Identity,
    naming: &'a TorrentNaming,
    tracker_tiers: Vec<Vec<Tracker>>,
    announce: bool,
    force_tracker_interval: Option<Duration>,
    initial_peers: Vec<SocketAddr>,
    trackers_only: bool,
    discovery: Discovery,
    dht_announce: tokio::sync::watch::Receiver<()>,
    ssl: Option<&'a Arc<SslTorrent>>,
    // A tracker task already running for the torrent, taken over in place
    // of starting its own.
    trackers: Option<TrackerTask>,
}

// What a torrent's tracker task starts from.
struct TrackerStart<'a> {
    info_hashes: InfoHashes,
    identity: &'a Identity,
    naming: &'a TorrentNaming,
    tracker_tiers: Vec<Vec<Tracker>>,
    force_tracker_interval: Option<Duration>,
    trackers_only: bool,
    ssl: Option<&'a Arc<SslTorrent>>,
}

impl Session {
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// How a torrent added with no naming of its own looks names up and
    /// asks over HTTP: the session's resolver and clients.
    pub fn naming(&self) -> TorrentNaming {
        TorrentNaming {
            resolver: self.resolver.clone(),
            http_clients: self.http_clients.clone(),
        }
    }

    pub fn get_dht(&self) -> Option<&Dht> {
        self.dht.as_ref()
    }

    /// Resolves once [`Session::stop`] has stopped the session's swarm: its
    /// listeners, DHT and local discovery are gone, and its DHT has written
    /// its table for the last time. Never before the stop begins. The
    /// future holds no reference to the session.
    pub fn swarm_stopped(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let swarm_tasks = self.swarm_tasks.clone();
        async move { swarm_tasks.wait().await }
    }

    /// Joins local service discovery's groups on the interfaces up now, for
    /// a caller that saw them change: again, or for the first time when no
    /// join has succeeded since the session opened, which starts its
    /// announces and its listening. Nothing to do when the session runs no
    /// local service discovery; on failure it stays as it was, on the
    /// groups joined before or on none.
    pub fn rejoin_local_discovery(&self) -> anyhow::Result<()> {
        match &self.lsd {
            Some(lsd) => lsd.join(),
            None => Ok(()),
        }
    }

    /// The limits the session holds to now.
    pub fn limits(&self) -> SessionLimits {
        SessionLimits {
            connections_limit: self.connection_slots.limit(),
            unchoke_slots_limit: self.choker.slots(),
            connection_speed: self.connection_slots.speed(),
        }
    }

    /// Hold the session to `limits` from now on.
    pub fn set_limits(&self, limits: SessionLimits) {
        self.connection_slots.set_limit(limits.connections_limit);
        self.choker.set_slots(limits.unchoke_slots_limit);
        self.connection_slots.set_speed(limits.connection_speed);
    }

    /// The encryption the session's connections are made under now, unless
    /// a torrent names its own.
    pub fn encryption(&self) -> crate::mse::EncryptionPolicy {
        self.connector.encryption()
    }

    /// Make the session's connections under `policy` from now on, and
    /// announce it from the next announce: the connections already made
    /// keep what they negotiated.
    pub fn set_encryption(&self, policy: crate::mse::EncryptionPolicy) {
        self.connector.set_encryption(policy);
    }

    /// Whether peers at one address may each hold a connection to a torrent
    /// now.
    pub fn allows_multiple_connections_per_ip(&self) -> bool {
        self.allow_multiple_connections_per_ip
            .load(Ordering::Relaxed)
    }

    /// Let peers at one address each hold a connection to a torrent, or
    /// not, from now on (libtorrent `allow_multiple_connections_per_ip`,
    /// which `apply_settings` changes with no callback: each
    /// `torrent::get_peer_list_state` reads it afresh). Every torrent's
    /// connect loop looks again, so a peer the gate held back is dialed
    /// without waiting for the connection at its address to close; the
    /// connections already made stay.
    pub fn set_allow_multiple_connections_per_ip(&self, allow: bool) {
        if self
            .allow_multiple_connections_per_ip
            .swap(allow, Ordering::Relaxed)
            == allow
        {
            return;
        }
        let torrents: Vec<_> = self.db.read().torrents.values().cloned().collect();
        for torrent in torrents {
            if let Some(live) = torrent.live() {
                live.connections_per_ip_changed();
            }
        }
    }

    /// The IP filter the session holds to now.
    pub fn ip_filter(&self) -> IpFilter {
        self.ip_filter.read().clone()
    }

    /// Hold the session to `filter` from now on: every torrent's peers it
    /// refuses leave the peer list, a connection to one closing first
    /// (libtorrent `session_impl::set_ip_filter` and
    /// `update_privileged_ports`, which run `torrent::ip_filter_updated` and
    /// `privileged_port_updated` over every torrent). Trackers are held to it
    /// from their next announce.
    pub fn set_ip_filter(&self, filter: IpFilter) {
        {
            let mut held = self.ip_filter.write();
            if *held == filter {
                return;
            }
            *held = filter;
        }
        let torrents: Vec<_> = self.db.read().torrents.values().cloned().collect();
        for torrent in torrents {
            if let Some(live) = torrent.live() {
                live.refilter();
            }
        }
    }

    /// Whether the session refuses a peer on `port`.
    pub(crate) fn refuses_port(&self, port: u16) -> bool {
        self.ip_filter.read().refuses_port(port)
    }

    fn merge_peer_opts(&self, other: Option<PeerConnectionOptions>) -> PeerConnectionOptions {
        let other = match other {
            Some(o) => o,
            None => self.peer_opts,
        };
        PeerConnectionOptions {
            connect_timeout: other.connect_timeout.or(self.peer_opts.connect_timeout),
            read_write_timeout: other
                .read_write_timeout
                .or(self.peer_opts.read_write_timeout),
            keep_alive_interval: other
                .keep_alive_interval
                .or(self.peer_opts.keep_alive_interval),
            handshake_timeout: other.handshake_timeout.or(self.peer_opts.handshake_timeout),
            max_request_window: other
                .max_request_window
                .or(self.peer_opts.max_request_window),
            encryption: other.encryption.or(self.peer_opts.encryption),
        }
    }

    /// Spawn a task in the context of the session.
    #[track_caller]
    pub fn spawn(
        &self,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        spawn_with_cancel(
            span,
            name,
            self.cancellation_token.clone(),
            self.tasks.track_future(fut),
        );
    }

    /// Spawn a task the swarm reaches this session through; stop() ends it
    /// before the trackers hear stopped.
    fn spawn_on_swarm(
        &self,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        spawn_with_cancel(
            span,
            name,
            self.swarm.clone(),
            self.swarm_tasks.track_future(fut),
        );
    }

    #[cfg(test)]
    pub(crate) fn stopped_announces(&self) -> &StoppedAnnounces {
        &self.stopped_announces
    }

    /// From now on no tracker hears `stopped` from this session: the address
    /// it would go out from no longer carries this session's traffic.
    pub fn withhold_stopped(&self) {
        self.stopped_announces.withhold();
    }

    pub(crate) fn rs(&self) -> Option<tracing::Id> {
        self.root_span.as_ref().and_then(|s| s.id())
    }

    /// Stop the session and all managed tasks. Each step logs as it starts,
    /// naming what it waits on. The swarm loses this session first: its
    /// listeners, DHT and local discovery are gone, and its torrents' peers
    /// dropped, before the trackers hear stopped, when they may.
    pub async fn stop(&self) {
        let listen = self.listen_addr();
        info!(
            ?listen,
            "stopping the session: closing its listeners, DHT and local discovery"
        );
        // Announces still waiting for their answers are dropped from here
        // on; the `stopped` announces below still go.
        self.stopped_announces.shut_down();
        self.swarm.cancel();
        self.swarm_tasks.close();
        self.swarm_tasks.wait().await;
        for socket in self.utp_sockets.iter().chain(self.connector.rebound_utp()) {
            if let Err(error) = socket.shutdown().await {
                debug!(%error, "error shutting down uTP socket");
            }
        }
        let torrents = self
            .db
            .read()
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        info!(
            ?listen,
            torrents = torrents.len(),
            "stopping the session: pausing its torrents"
        );
        for torrent in torrents {
            if let Err(e) = torrent.pause() {
                debug!("error pausing torrent: {e:#}");
            }
        }
        // Let the trackers hear `stopped` before the UDP tracker client goes away.
        let stopped = &self.stopped_announces;
        info!(
            ?listen,
            timeout = ?stopped.whole(),
            "stopping the session: waiting on its trackers' stopped announces"
        );
        if tokio::time::timeout(stopped.whole(), stopped.wait_idle())
            .await
            .is_err()
        {
            info!(
                ?listen,
                "stopping the session: not every tracker heard stopped in time"
            );
        }
        info!(
            ?listen,
            "stopping the session: closing its trackers and removing its port mappings"
        );
        self.cancellation_token.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        info!(?listen, "the session stopped");
    }

    /// Run a callback given the currently managed torrents.
    pub fn with_torrents<R>(
        &self,
        callback: impl Fn(&mut dyn Iterator<Item = (TorrentId, &ManagedTorrentHandle)>) -> R,
    ) -> R {
        callback(&mut self.db.read().torrents.iter().map(|(id, t)| (*id, t)))
    }

    pub fn get(&self, id: TorrentIdOrHash) -> Option<ManagedTorrentHandle> {
        match id {
            TorrentIdOrHash::Id(id) => self.db.read().torrents.get(&id).cloned(),
            TorrentIdOrHash::Hash(id) => self.db.read().by_hash(id).map(|(_, t)| t.clone()),
        }
    }

    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.listen_addr
    }

    /// Whether the session listens on uTP as well, on the listen address.
    pub fn listens_utp(&self) -> bool {
        self.utp
    }

    /// Whether the session speaks uTP at all: it listens on uTP, or it
    /// connects out over a uTP socket of its own.
    pub fn uses_utp(&self) -> bool {
        !self.utp_sockets.is_empty() || self.connector.rebound_utp().is_some()
    }

    /// Our address as the internet sees it, IPv4 first, of each family the
    /// vote has settled on.
    pub fn external_ips(&self) -> Vec<IpAddr> {
        self.external_ips.get()
    }

    /// Our identity under an SSL torrent's CA becomes `identity`: new
    /// connections and announces present it, running ones keep theirs. A
    /// torrent that waits for one starts; one left without one stops its
    /// run and waits. Refused for a torrent that is not an SSL torrent, and
    /// for a key that is not the certificate's.
    pub fn set_ssl_identity(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
        identity: Option<SslIdentity>,
    ) -> anyhow::Result<()> {
        let ssl = handle
            .shared()
            .ssl
            .as_ref()
            .context("the torrent is not an SSL torrent")?;
        ssl.set_identity(identity)?;
        if !ssl.has_identity() {
            return handle.wait_for_certificate();
        }
        if handle.waits_for_certificate() {
            handle.start(None, false)?;
        }
        Ok(())
    }
}

pub(crate) struct ResolveMagnetResult {
    pub metadata: TorrentMetadata,
    pub peer_rx: PeerStream,
    /// The torrent's peer list, as the metadata connections left it.
    pub peers: PeerList,
    pub seen_peers: Vec<SocketAddr>,
}

// Ad adapter for converting stats into the format that tracker_comms accepts.
struct PeerRxTorrentInfo {
    info_hashes: InfoHashes,
    // Weak for the reason `Session::reachable` gives.
    session: std::sync::Weak<Session>,
    // Kept once found, so the counters stay readable after the torrent is
    // removed from the session (for the `stopped` announce).
    torrent: std::sync::OnceLock<ManagedTorrentHandle>,
}

// The initial uTP connection limit when the caller left it unset.
fn session_utp_opts(
    utp: &mut weaver_utp::SocketOpts,
    connections_limit: usize,
    shards: Option<&weaver_utp::Shards>,
) {
    if utp.shards.is_none() {
        utp.shards = shards.cloned();
    }
    if utp.connections_limit.is_none() {
        utp.connections_limit = std::num::NonZeroUsize::new(connections_limit);
    }
}

// uTP threads per session: the first reads and sends on the session's uTP
// sockets and finds each datagram's stream; the streams of a kernel socket,
// and the peers on them, run on the others. The threads are the session's,
// not a torrent's or a socket's, and an idle one is parked, so the count
// does not grow with the torrents held. A tunnel's streams stay on the
// first.
pub(crate) const UTP_THREADS: usize = 2;

// The session's uTP threads, when it runs on a multi-threaded runtime:
// `UTP_THREADS`, held to the machine's cores, named apart from other
// sessions' within the 15 bytes Linux keeps of a thread's name
// (`utp{session}-{index}`). None start until a uTP socket places a task.
fn utp_shards() -> Option<weaver_utp::Shards> {
    static SESSIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    if tokio::runtime::Handle::current().runtime_flavor()
        != tokio::runtime::RuntimeFlavor::MultiThread
    {
        return None;
    }
    let session = SESSIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Some(weaver_utp::Shards::new(
        &format!("utp{session}"),
        UTP_THREADS,
    ))
}

#[cfg(test)]
mod incoming_tests;
#[cfg(test)]
mod local_tests;
#[cfg(test)]
mod tests;
