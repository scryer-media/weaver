// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::Read,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroU32,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::Identity;
use crate::bencode::bencode_serialize_to_writer;
use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::clone_to_owned::CloneToOwned;
use crate::core::{
    hash_id::InfoHashes,
    magnet::Magnet,
    spawn_utils::spawn_with_cancel,
    torrent_metainfo::{TorrentMetaV1Owned, ValidatedTorrentMetaV1Info, WebSeed, WebSeedKind},
};
use crate::dht::{
    Dht, DhtBuilder, DhtConfig, DhtPersistenceConfig, Id20, PersistentDht, dht_listen_addr,
};
use crate::lsd::{LocalServiceDiscovery, LocalServiceDiscoveryOptions};
use crate::natpmp::{NATPMP_PORT, NatPmpMapper};
use crate::peer_protocol::Handshake;
use crate::torrent_state::live::web_seed::WebSeeds;
use crate::tracker::{AutoScrape, ListenPort, StoppedAnnounces, TrackerComms, UdpTrackerClient};
use crate::upnp::{
    PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport, UpnpPortForwarder,
    UpnpPortForwarderOptions,
};
use crate::{
    ApiError, CreateTorrentOptions, FileInfos, ManagedTorrent, ManagedTorrentShared,
    api::TorrentIdOrHash,
    api_error::WithStatus,
    bitv_factory::{BitVFactory, NonPersistentBitVFactory},
    connection_slots::{ConnectionSlots, DEFAULT_CONNECTION_SPEED, DEFAULT_CONNECTIONS_LIMIT},
    create_torrent,
    create_torrent_file::CreateTorrentResult,
    dht_utils::{ReadMetainfoResult, read_metainfo_from_peer_receiver},
    ip_ranges::IpRanges,
    limits::{Limits, LimitsConfig},
    listen::{Accept, ListenerOptions},
    merge_streams::merge_streams,
    network::SocketFactory,
    peer_connection::PeerConnectionOptions,
    piece_priority::{PiecePriority, validate_piece_priorities},
    read_buf::ReadBuf,
    session_persistence::{SessionPersistenceStore, json::JsonSessionPersistenceStore},
    session_stats::SessionStats,
    spawn_utils::BlockingSpawner,
    storage::{
        BoxStorageFactory, StorageFactoryExt, TorrentStorage, filesystem::FilesystemStorageFactory,
    },
    stream_connect::{ConnectionKind, ConnectionOptions, StreamConnector, StreamConnectorArgs},
    torrent_state::{
        ManagedTorrentHandle, ManagedTorrentLocked, ManagedTorrentOptions, ManagedTorrentState,
        TorrentMetadata, TorrentStateLive,
        initializing::{TorrentStateInitializing, normalize_initial_have},
    },
    type_aliases::{BF, BoxAsyncReadVectored, BoxAsyncWrite, PeerSource, PeerStream},
};
use anyhow::{Context, bail};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};
use http::StatusCode;
use librqbit_utp::BindDevice;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};

pub const SUPPORTED_SCHEMES: [&str; 3] = ["http:", "https:", "magnet:"];

pub type TorrentId = usize;

struct ParsedTorrentFile {
    meta: TorrentMetaV1Owned,
    torrent_bytes: Bytes,
}

fn torrent_from_bytes(bytes: Bytes) -> anyhow::Result<ParsedTorrentFile> {
    trace!(
        "all fields in torrent: {:#?}",
        crate::bencode::dyn_from_bytes::<ByteBuf>(&bytes)
    );
    let parsed = crate::core::torrent_metainfo::torrent_from_bytes(&bytes)?;
    Ok(ParsedTorrentFile {
        meta: parsed.clone_to_owned(Some(&bytes)),
        torrent_bytes: bytes,
    })
}

#[derive(Default)]
pub struct SessionDatabase {
    torrents: HashMap<TorrentId, ManagedTorrentHandle>,
}

impl SessionDatabase {
    fn add_torrent(&mut self, torrent: ManagedTorrentHandle, id: TorrentId) {
        self.torrents.insert(id, torrent);
    }
}

/// The port the session announces (libtorrent `listen_socket_t::
/// tcp_external_port`): the TCP port a mapper got on the gateway, NAT-PMP's
/// before UPnP's, else the configured one. Announces subscribe to it and
/// read it as they go out, so a mapping that lands after a torrent starts
/// is announced by that torrent.
struct AnnouncePort {
    configured: Option<u16>,
    mapped: parking_lot::Mutex<MappedPorts>,
    port: tokio::sync::watch::Sender<Option<u16>>,
}

#[derive(Clone, Copy, Default)]
struct MappedPorts {
    natpmp: Option<u16>,
    upnp: Option<u16>,
}

impl AnnouncePort {
    fn new(configured: Option<u16>) -> Self {
        Self {
            configured,
            mapped: Default::default(),
            port: tokio::sync::watch::channel(configured).0,
        }
    }

    fn get(&self) -> Option<u16> {
        *self.port.borrow()
    }

    fn subscribe(&self) -> ListenPort {
        self.port.subscribe()
    }

    /// libtorrent `session_impl::on_port_mapping`: a mapper's TCP report,
    /// a failure included, sets that mapper's port.
    fn on_port_mapping(&self, event: &PortMapped) {
        if event.protocol != PortmapProtocol::Tcp {
            return;
        }
        let mut mapped = self.mapped.lock();
        let port = match event.transport {
            PortmapTransport::NatPmp => &mut mapped.natpmp,
            PortmapTransport::Upnp => &mut mapped.upnp,
        };
        *port = event.external_port;
        let announced = self
            .configured
            .map(|configured| mapped.natpmp.or(mapped.upnp).unwrap_or(configured));
        self.port.send_if_modified(|port| {
            let changed = *port != announced;
            *port = announced;
            changed
        });
    }
}

/// The peers the DHT finds for `info_hash`, announcing `port` to the nodes
/// closest to it. The DHT search reads the port when it starts, so a new
/// port starts a new search, which announces it (libtorrent
/// `torrent::dht_announce` reads the port at each announce).
fn dht_peers(dht: Dht, info_hash: Id20, mut port: ListenPort) -> BoxStream<'static, SocketAddr> {
    async_stream::stream! {
        loop {
            let mut peers = dht.get_peers(info_hash, *port.borrow_and_update());
            loop {
                tokio::select! {
                    peer = peers.next() => match peer {
                        Some(peer) => yield peer,
                        None => return,
                    },
                    Ok(()) = port.changed() => break,
                }
            }
        }
    }
    .boxed()
}

/// The protocols to map: those the session listens on (libtorrent
/// `session_impl::remap_ports` maps a socket only once it is open).
fn listening_protocols(listen: &crate::listen::ListenResult) -> Vec<PortmapProtocol> {
    [
        (listen.tcp_socket.is_some() || listen.custom_tcp_socket.is_some())
            .then_some(PortmapProtocol::Tcp),
        listen.utp_socket.is_some().then_some(PortmapProtocol::Udp),
    ]
    .into_iter()
    .flatten()
    .collect()
}

pub struct Session {
    // Core state and services
    pub(crate) db: RwLock<SessionDatabase>,
    next_id: AtomicUsize,
    pub(crate) bitv_factory: Arc<dyn BitVFactory>,
    spawner: BlockingSpawner,

    // Network
    // The DHT's, and every torrent's that names none of its own.
    identity: Identity,
    announce_port: Arc<AnnouncePort>,
    listen_addr: Option<SocketAddr>,
    dht: Option<Dht>,
    pub(crate) connector: Arc<StreamConnector>,
    pub(crate) reqwest_client: reqwest::Client,
    udp_tracker_client: UdpTrackerClient,
    disable_trackers: bool,
    stopped_announces: StoppedAnnounces,
    auto_scrape: AutoScrape,

    // Lifecycle management
    cancellation_token: CancellationToken,
    _cancellation_token_drop_guard: DropGuard,
    // Every task the session spawns and the DHT's worker: stop() returns once they are gone.
    tasks: TaskTracker,

    // Runtime settings
    output_folder: PathBuf,
    peer_opts: PeerConnectionOptions,
    default_storage_factory: Option<BoxStorageFactory>,
    persistence: Option<Arc<dyn SessionPersistenceStore>>,
    trackers: HashSet<url::Url>,

    lsd: Option<LocalServiceDiscovery>,

    // Limits and throttling
    pub(crate) concurrent_initialize_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) connection_slots: Arc<ConnectionSlots>,
    pub ratelimits: Limits,

    pub blocklist: IpRanges,
    pub allowlist: Option<IpRanges>,

    // Monitoring / tracing / logging
    pub(crate) stats: Arc<SessionStats>,
    root_span: Option<tracing::Span>,

    // Feature flags
    pub ipv4_only: bool,
    pub peer_limit: Option<usize>,
    pub(crate) unchoke_slots_limit: usize,
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
    reqwest_client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<ParsedTorrentFile> {
    let response = reqwest_client
        .get(url)
        .send()
        .await
        .context("error downloading torrent metadata")?;
    if !response.status().is_success() {
        bail!("GET {} returned {}", url, response.status())
    }
    let b = response
        .bytes()
        .await
        .with_context(|| format!("error reading response body from {url}"))?;
    torrent_from_bytes(b).context("error decoding torrent")
}

fn compute_only_files_regex<ByteBuf: AsRef<[u8]>>(
    torrent: &ValidatedTorrentMetaV1Info<ByteBuf>,
    filename_re: &str,
) -> anyhow::Result<Vec<usize>> {
    let filename_re = regex::Regex::new(filename_re).context("filename regex is incorrect")?;
    let mut only_files = Vec::new();
    for (idx, fd) in torrent.iter_file_details().enumerate() {
        let full_path = fd.filename.to_pathbuf();
        if filename_re.is_match(full_path.to_str().unwrap()) {
            only_files.push(idx);
        }
    }
    if only_files.is_empty() {
        bail!("none of the filenames match the given regex")
    }
    Ok(only_files)
}

fn compute_only_files(
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    only_files: Option<Vec<usize>>,
    only_files_regex: Option<String>,
    list_only: bool,
) -> anyhow::Result<Option<Vec<usize>>> {
    match (only_files, only_files_regex) {
        (Some(_), Some(_)) => {
            bail!("only_files and only_files_regex are mutually exclusive");
        }
        (Some(only_files), None) => {
            let total_files = info.iter_file_lengths().count();
            for id in only_files.iter().copied() {
                if id >= total_files {
                    bail!("file id {} is out of range", id);
                }
            }
            Ok(Some(only_files))
        }
        (None, Some(filename_re)) => {
            let only_files = compute_only_files_regex(info, &filename_re)?;
            for (idx, fd) in info.iter_file_details().enumerate() {
                if !only_files.contains(&idx) {
                    continue;
                }
                if !list_only {
                    info!(filename=?fd.filename, "will download");
                }
            }
            Ok(Some(only_files))
        }
        (None, None) => Ok(None),
    }
}

fn merge_two_optional_streams<T>(
    s1: Option<impl Stream<Item = T> + Unpin + Send + 'static>,
    s2: Option<impl Stream<Item = T> + Unpin + Send + 'static>,
) -> Option<BoxStream<'static, T>> {
    match (s1, s2) {
        (Some(s1), None) => Some(Box::pin(s1)),
        (None, Some(s2)) => Some(Box::pin(s2)),
        (Some(s1), Some(s2)) => Some(Box::pin(merge_streams(s1, s2))),
        (None, None) => None,
    }
}

/// Options for adding new torrents to the session.
//
// Serialize/deserialize is for Tauri.
#[derive(Serialize, Deserialize)]
pub struct AddTorrentOptions {
    /// Start in paused state.
    #[serde(default)]
    pub paused: bool,
    /// A regex to only download files matching it.
    pub only_files_regex: Option<String>,
    /// An explicit list of file IDs to download.
    /// To see the file indices, run with "list_only".
    pub only_files: Option<Vec<usize>>,
    /// Allow writing on top of existing files, including when resuming a torrent.
    /// You probably want to set it, however for safety it's not default.
    ///
    /// Even when all the torrent pieces have been written, `overwrite` needs to
    /// be enabled in order to resume/seed the torrent.
    #[serde(default)]
    pub overwrite: bool,
    /// Only list the files in the torrent without starting it.
    #[serde(default)]
    pub list_only: bool,
    /// The output folder for the torrent. If not set, the session's default one will be used.
    pub output_folder: Option<String>,
    /// Sub-folder within session's default output folder. Will error if "output_folder" if also set.
    /// By default, multi-torrent files are downloaded to a sub-folder.
    pub sub_folder: Option<String>,
    /// Peer connection options, timeouts etc. If not set, session's defaults will be used.
    pub peer_opts: Option<PeerConnectionOptions>,

    /// Force a refresh interval for polling trackers.
    pub force_tracker_interval: Option<Duration>,

    #[serde(default)]
    pub disable_trackers: bool,

    #[serde(default)]
    pub ratelimits: LimitsConfig,

    /// Initial peers to start of with.
    pub initial_peers: Option<Vec<SocketAddr>>,

    /// Web seeds beside those the torrent names; a magnet link's `ws` join
    /// them (libtorrent `add_torrent_params::url_seeds`).
    #[serde(default)]
    pub web_seeds: Vec<WebSeed>,

    /// Max concurrent connected peers.
    pub peer_limit: Option<usize>,

    /// This is used to restore the session from serialized state.
    pub preferred_id: Option<usize>,

    #[serde(skip)]
    pub storage_factory: Option<BoxStorageFactory>,

    // Custom trackers
    pub trackers: Option<Vec<String>>,

    /// The pieces the storage already holds and that the caller has already
    /// verified, as a BEP 3 bitfield: bit 0 (the high bit of the first byte)
    /// is piece 0. When set, the initial hash check does not run: these pieces
    /// are taken as have and every other piece as missing, and none of them is
    /// read back from the storage. It also takes precedence over any fastresume
    /// bitfield.
    ///
    /// Its length is either the piece count, or the piece count rounded up to
    /// whole bytes with the spare bits clear; anything else is refused.
    #[serde(skip)]
    pub initial_have: Option<BF>,

    /// The initial fetch priority of the given pieces, by piece index; every
    /// other piece is Normal. See [`PiecePriority`] and
    /// [`ManagedTorrent::set_piece_priorities`]. An index out of range is refused.
    pub piece_priorities: Option<Vec<(u32, PiecePriority)>>,

    /// How the torrent names itself to its peers and trackers. If not set,
    /// the session's identity is used.
    #[serde(skip)]
    pub identity: Option<Identity>,
    /// For a hybrid, verify a piece by its v2 hash alone, without SHA-1,
    /// wherever its v2 block hashes are known; a piece whose v2 hashes are
    /// not known yet is still verified by v1. No effect on a v1 or a v2
    /// torrent (libtorrent `torrent_flags::disable_v1_hashes`). On by
    /// default, where libtorrent's is off: hashing every byte of a hybrid
    /// twice is a cost Weaver does not pay.
    #[serde(default = "v1_hashes_disabled_by_default")]
    pub disable_v1_hashes: bool,
    /// Super-seed (BEP 16) while the torrent is a seed: peers are told of
    /// no piece but the one each is handed, the rarest it lacks, and a peer
    /// is handed the next once another peer reports the piece (libtorrent
    /// `torrent_flags::super_seeding`). Off by default.
    #[serde(default)]
    pub super_seeding: bool,
}

const fn v1_hashes_disabled_by_default() -> bool {
    true
}

impl Default for AddTorrentOptions {
    fn default() -> Self {
        Self {
            paused: false,
            only_files_regex: None,
            only_files: None,
            overwrite: false,
            list_only: false,
            output_folder: None,
            sub_folder: None,
            peer_opts: None,
            force_tracker_interval: None,
            disable_trackers: false,
            ratelimits: LimitsConfig::default(),
            initial_peers: None,
            web_seeds: Vec::new(),
            peer_limit: None,
            preferred_id: None,
            storage_factory: None,
            trackers: None,
            initial_have: None,
            piece_priorities: None,
            identity: None,
            disable_v1_hashes: v1_hashes_disabled_by_default(),
            super_seeding: false,
        }
    }
}

pub struct ListOnlyResponse {
    pub info_hash: Id20,
    pub info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
    pub only_files: Option<Vec<usize>>,
    pub output_folder: PathBuf,
    pub seen_peers: Vec<SocketAddr>,
    pub torrent_bytes: Bytes,
}

#[allow(clippy::large_enum_variant)]
pub enum AddTorrentResponse {
    AlreadyManaged(TorrentId, ManagedTorrentHandle),
    ListOnly(ListOnlyResponse),
    Added(TorrentId, ManagedTorrentHandle),
}

impl AddTorrentResponse {
    pub fn into_handle(self) -> Option<ManagedTorrentHandle> {
        match self {
            Self::AlreadyManaged(_, handle) => Some(handle),
            Self::ListOnly(_) => None,
            Self::Added(_, handle) => Some(handle),
        }
    }
}

// The DHT side of a configured peer id: the node id prefix, and the BEP 5 "v".
//
// An Azureus-style peer id (BEP 20) starts with the client's fingerprint "-XXvvvv-":
// the DHT node id starts with the same eight bytes, and "v" is the two client letters
// followed by the major and minor version as bytes. A peer id of another shape has no
// fingerprint to carry over: the node id is then entirely random and no "v" is sent.
// Without a configured peer id the DHT keeps its defaults.
fn dht_identity(peer_id: Option<&Id20>) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let Some(peer_id) = peer_id else {
        return (None, None);
    };
    match crate::core::peer_id::try_decode_peer_id(*peer_id) {
        Some(crate::core::peer_id::PeerId::AzureusStyle(a)) => {
            let p = &peer_id.0;
            (
                Some(p[..8].to_vec()),
                Some(vec![p[1], p[2], a.version[0], a.version[1]]),
            )
        }
        None => (Some(Vec::new()), None),
    }
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

pub enum AddTorrent<'a> {
    Url(Cow<'a, str>),
    TorrentFileBytes(Bytes),
}

impl<'a> AddTorrent<'a> {
    // Don't call this from HTTP API.
    #[inline(never)]
    pub fn from_cli_argument(path: &'a str) -> anyhow::Result<Self> {
        if SUPPORTED_SCHEMES.iter().any(|s| path.starts_with(s)) {
            return Ok(Self::Url(Cow::Borrowed(path)));
        }
        if path.len() == 40 && !Path::new(path).exists() && Magnet::parse(path).is_ok() {
            return Ok(Self::Url(Cow::Borrowed(path)));
        }
        Self::from_local_filename(path)
    }

    pub fn from_url(url: impl Into<Cow<'a, str>>) -> Self {
        Self::Url(url.into())
    }

    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self::TorrentFileBytes(bytes.into())
    }

    // Don't call this from HTTP API.
    #[inline(never)]
    pub fn from_local_filename(filename: &str) -> anyhow::Result<Self> {
        let file = read_local_file_including_stdin(filename)
            .with_context(|| format!("error reading local file {filename:?}"))?;
        Ok(Self::TorrentFileBytes(file.into()))
    }

    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Url(s) => s.into_owned().into_bytes().into(),
            Self::TorrentFileBytes(b) => b,
        }
    }
}

pub enum SessionPersistenceConfig {
    /// The folder for persistence.
    Json { folder: PathBuf },
}

/// Configuration for the DHT subsystem.
/// Set to `None` in `SessionOptions::dht` to disable DHT entirely.
pub struct DhtSessionConfig {
    /// Bootstrap nodes (host:port or ip:port). Uses built-in defaults if None.
    pub bootstrap_addrs: Option<Vec<String>>,
    /// The DHT listen port. Priority: this explicit port -> persisted port
    /// (when persistence is enabled) -> random. The bind IP is derived from
    /// `SessionOptions::ipv4_only` (`0.0.0.0` if true, `[::]` otherwise).
    /// Use `SessionOptions::bind_device_name` to scope the bind to a specific
    /// network interface.
    pub port: Option<u16>,
    /// Persistence behavior. If None, persistence is disabled.
    pub persistence: Option<DhtPersistenceConfig>,
}

impl Default for DhtSessionConfig {
    fn default() -> Self {
        Self {
            bootstrap_addrs: None,
            port: None,
            persistence: None,
        }
    }
}

pub struct SessionOptions {
    /// DHT configuration. Set to None to disable DHT entirely.
    /// Defaults to DHT enabled with persistence.
    pub dht: Option<DhtSessionConfig>,

    /// What network device to bind to for DHT, BT-UDP, BT-TCP, trackers and LSD.
    /// On OSX will use IP(V6)_BOUND_IF, on Linux will use SO_BINDTODEVICE.
    pub bind_device_name: Option<String>,

    /// Disable tracker communication
    pub disable_trackers: bool,

    /// How long the `stopped` announce a torrent sends to its trackers when it
    /// is paused, removed or the session stops may take. Session shutdown waits
    /// at most this long for them. Defaults to
    /// [`StoppedAnnounces::DEFAULT_TIMEOUT`].
    pub tracker_stopped_timeout: Option<Duration>,

    /// Enable fastresume, to restore state quickly after restart.
    pub fastresume: bool,

    /// Turn on to dump session contents into a file periodically, so that on next start
    /// all remembered torrents will continue where they left off.
    pub persistence: Option<SessionPersistenceConfig>,

    /// How the session names itself: its DHT node, and every torrent added
    /// without an identity of its own. Defaults to this build under a fresh
    /// peer id, and the DHT then keeps its own defaults.
    pub identity: Option<Identity>,

    /// Options for listening on TCP and/or uTP for incoming connections.
    pub listen: Option<ListenerOptions>,
    /// Options for connecting to peers (for outgiong connections).
    pub connect: Option<ConnectionOptions>,

    pub default_storage_factory: Option<BoxStorageFactory>,

    pub cancellation_token: Option<CancellationToken>,

    /// how many concurrent torrent initializations can happen
    pub concurrent_init_limit: Option<usize>,

    /// How many blocking threads does the tokio runtime have.
    /// Will limit blocking work to that number to avoid starving the runtime.
    pub runtime_worker_threads: Option<usize>,

    /// the root span to use. If not set will be None.
    pub root_span: Option<tracing::Span>,

    pub ratelimits: LimitsConfig,

    pub blocklist_url: Option<String>,
    pub allowlist_url: Option<String>,

    // The list of tracker URLs to always use for each torrent.
    pub trackers: HashSet<url::Url>,

    /// Default peer limit per torrent.
    pub peer_limit: Option<usize>,

    /// Peers each torrent uploads to at once, one in five of them and at least
    /// one on optimistic slots (libtorrent `unchoke_slots_limit`). Defaults
    /// to 8.
    pub unchoke_slots_limit: usize,

    /// Peer connections, incoming and outgoing, the session holds open at
    /// once across all its torrents. Outgoing connects wait for a free place;
    /// an incoming connection past it is closed. Defaults to 200.
    pub connections_limit: usize,

    /// Outgoing connects the session begins per second across all its
    /// torrents, handed to the torrents that want peers in turn. Defaults
    /// to 30.
    pub connection_speed: NonZeroU32,

    /// Disable LSD multicast
    pub disable_local_service_discovery: bool,

    /// Leave peer exchange (ut_pex) out: it is not advertised to peers, and
    /// none is sent or read, as in libtorrent without the ut_pex plugin.
    pub disable_pex: bool,

    /// Force IPv4 only.
    pub ipv4_only: bool,

    /// Create peer sockets with this factory instead of binding kernel
    /// sockets. See [`SocketFactory`] for what it covers.
    pub socket_factory: Option<Arc<dyn SocketFactory>>,

    /// Use this HTTP client (HTTP trackers, torrent, blocklist and allowlist
    /// downloads) instead of building one. When set, `bind_device_name` and the identity's User-Agent do not apply to HTTP
    /// requests; configure them on the client.
    ///
    /// The client also does its own DNS lookups: [`SocketFactory::resolver`]
    /// does not apply to it. If HTTP hostname lookups must be routed too, build
    /// the client with its own resolver (`reqwest::ClientBuilder::dns_resolver`).
    pub http_client: Option<reqwest::Client>,

    /// Message stream encryption for peer connections, outgoing and incoming.
    /// Defaults to [`EncryptionPolicy::Disabled`](crate::EncryptionPolicy::Disabled).
    pub encryption: crate::mse::EncryptionPolicy,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            dht: Some(DhtSessionConfig::default()),
            bind_device_name: None,
            disable_trackers: false,
            tracker_stopped_timeout: None,
            fastresume: false,
            persistence: None,
            identity: None,
            listen: None,
            connect: None,
            default_storage_factory: None,
            cancellation_token: None,
            concurrent_init_limit: None,
            runtime_worker_threads: None,
            root_span: None,
            ratelimits: LimitsConfig::default(),
            blocklist_url: None,
            allowlist_url: None,
            trackers: HashSet::new(),
            peer_limit: None,
            unchoke_slots_limit: crate::torrent_state::live::choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
            connections_limit: DEFAULT_CONNECTIONS_LIMIT,
            connection_speed: DEFAULT_CONNECTION_SPEED,
            disable_local_service_discovery: false,
            disable_pex: false,
            ipv4_only: false,
            socket_factory: None,
            http_client: None,
            encryption: crate::mse::EncryptionPolicy::Disabled,
        }
    }
}

fn torrent_file_from_info_bytes(
    info_bytes: &[u8],
    tracker_tiers: &[Vec<url::Url>],
) -> anyhow::Result<Bytes> {
    #[derive(Serialize)]
    struct Tmp<'a> {
        announce: &'a str,
        #[serde(rename = "announce-list")]
        announce_list: &'a [Vec<url::Url>],
        info: crate::bencode::raw_value::RawValue<&'a [u8]>,
    }

    let mut w = Vec::new();
    let v = Tmp {
        info: crate::bencode::raw_value::RawValue(info_bytes),
        announce: tracker_tiers
            .iter()
            .flatten()
            .next()
            .map(|s| s.as_str())
            .unwrap_or(""),
        announce_list: tracker_tiers,
    };
    bencode_serialize_to_writer(&v, &mut w)?;
    Ok(w.into())
}

pub(crate) struct CheckedIncomingConnection {
    pub kind: ConnectionKind,
    pub addr: SocketAddr,
    pub reader: BoxAsyncReadVectored,
    pub writer: BoxAsyncWrite,
    pub read_buf: ReadBuf,
    pub handshake: Handshake,
}

struct InternalAddResult {
    info_hashes: InfoHashes,
    metadata: Option<TorrentMetadata>,
    tracker_tiers: Vec<Vec<url::Url>>,
    name: Option<String>,
}

fn parse_tracker_url(tracker: &[u8]) -> Option<url::Url> {
    match std::str::from_utf8(tracker) {
        Ok(url) => url::Url::parse(url).ok(),
        Err(_) => {
            warn!("cannot parse tracker url as utf-8, ignoring");
            None
        }
    }
}

/// A torrent's own tracker tiers (BEP 12): `announce-list` when it names any
/// tracker, otherwise `announce` as the only tier.
fn metainfo_tracker_tiers(meta: &TorrentMetaV1Owned) -> Vec<Vec<url::Url>> {
    if meta.announce_list.iter().flatten().next().is_some() {
        meta.announce_list
            .iter()
            .map(|tier| {
                tier.iter()
                    .filter_map(|t| parse_tracker_url(t.as_ref()))
                    .collect()
            })
            .collect()
    } else {
        meta.announce
            .iter()
            .filter_map(|t| parse_tracker_url(t.as_ref()))
            .map(|t| vec![t])
            .collect()
    }
}

/// The tiers a torrent announces to: its own tiers in order, then each of
/// `extra` as a tier of its own, unless the torrent is private. A private
/// torrent (BEP 27) talks only to the trackers it came with. A tracker listed
/// more than once keeps its first place, and empty tiers are dropped.
fn merge_tracker_tiers(
    own: Vec<Vec<url::Url>>,
    extra: impl IntoIterator<Item = url::Url>,
    private: bool,
) -> Vec<Vec<url::Url>> {
    let extra = (!private).then(|| extra.into_iter().map(|t| vec![t]));
    let mut seen = HashSet::new();
    own.into_iter()
        .chain(extra.into_iter().flatten())
        .map(|tier| {
            tier.into_iter()
                .filter(|t| seen.insert(t.clone()))
                .collect::<Vec<_>>()
        })
        .filter(|tier| !tier.is_empty())
        .collect()
}

impl Session {
    /// Create a new session with default options.
    /// The passed in folder will be used as a default unless overridden per torrent.
    /// It will run a DHT server/client, a TCP listener and .
    #[inline(never)]
    pub fn new(default_output_folder: PathBuf) -> BoxFuture<'static, anyhow::Result<Arc<Self>>> {
        Self::new_with_opts(default_output_folder, SessionOptions::default())
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// The cadence paused torrents scrape their trackers on.
    pub(crate) fn auto_scrape(&self) -> &AutoScrape {
        &self.auto_scrape
    }

    /// Create a new session with options.
    #[inline(never)]
    pub fn new_with_opts(
        default_output_folder: PathBuf,
        mut opts: SessionOptions,
    ) -> BoxFuture<'static, anyhow::Result<Arc<Self>>> {
        async move {
            let (dht_node_id_prefix, dht_client_version) =
                dht_identity(opts.identity.as_ref().map(|identity| &identity.peer_id));
            let identity = opts.identity.take().unwrap_or_default();
            let token = opts.cancellation_token.take().unwrap_or_default();
            let tasks = TaskTracker::new();

            let bind_device = match opts.bind_device_name.as_ref() {
                Some(name) => Some(
                    BindDevice::new_from_name(name)
                        .with_context(|| format!("error creating bind device {name}"))?,
                ),
                None => None,
            };

            let listen_result = if let Some(listen_opts) = opts.listen.take() {
                Some(
                    listen_opts
                        .start(
                            opts.root_span.as_ref().and_then(|s| s.id()),
                            token.child_token(),
                            bind_device.as_ref(),
                            opts.socket_factory.as_ref(),
                        )
                        .await
                        .context("error starting listeners")?,
                )
            } else {
                None
            };

            let dht = if let Some(dht_config) = opts.dht.take() {
                // With a socket factory the DHT runs over a datagram socket the
                // factory binds; the factory decides the real address.
                let dht_socket = match opts.socket_factory.as_ref() {
                    Some(factory) => Some(
                        factory
                            .bind_udp(dht_listen_addr(dht_config.port, None, opts.ipv4_only))
                            .await
                            .context("error binding DHT socket through socket factory")?,
                    ),
                    None => None,
                };
                let dht_resolver = opts.socket_factory.as_ref().and_then(|f| f.resolver());
                let dht = if let Some(persistence_config) = dht_config.persistence {
                    PersistentDht::create_from_config(
                        persistence_config,
                        dht_config.port,
                        opts.ipv4_only,
                        DhtConfig {
                            bootstrap_addrs: dht_config.bootstrap_addrs,
                            cancellation_token: Some(token.clone()),
                            bind_device: bind_device.as_ref(),
                            socket: dht_socket,
                            resolver: dht_resolver,
                            node_id_prefix: dht_node_id_prefix,
                            client_version: dht_client_version,
                            tasks: Some(tasks.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .context("error initializing persistent DHT")?
                } else {
                    let listen_addr = dht_listen_addr(dht_config.port, None, opts.ipv4_only);
                    DhtBuilder::with_config(DhtConfig {
                        bootstrap_addrs: dht_config.bootstrap_addrs,
                        cancellation_token: Some(token.child_token()),
                        bind_device: bind_device.as_ref(),
                        listen_addr: Some(listen_addr),
                        socket: dht_socket,
                        resolver: dht_resolver,
                        node_id_prefix: dht_node_id_prefix,
                        client_version: dht_client_version,
                        tasks: Some(tasks.clone()),
                        ..Default::default()
                    })
                    .await
                    .context("error initializing DHT")?
                };

                Some(dht)
            } else {
                None
            };
            let peer_opts = opts
                .connect
                .as_ref()
                .and_then(|p| p.peer_opts)
                .unwrap_or_default();

            async fn persistence_factory(
                opts: &SessionOptions,
                spawner: BlockingSpawner,
            ) -> anyhow::Result<(
                Option<Arc<dyn SessionPersistenceStore>>,
                Arc<dyn BitVFactory>,
            )> {
                macro_rules! make_result {
                    ($store:expr) => {
                        if opts.fastresume {
                            Ok((Some($store.clone()), $store))
                        } else {
                            Ok((Some($store), Arc::new(NonPersistentBitVFactory {})))
                        }
                    };
                }

                match &opts.persistence {
                    Some(SessionPersistenceConfig::Json { folder }) => {
                        let s = Arc::new(
                            JsonSessionPersistenceStore::new(folder.clone(), spawner)
                                .await
                                .context("error initializing JsonSessionPersistenceStore")?,
                        );

                        make_result!(s)
                    }
                    None => Ok((None, Arc::new(NonPersistentBitVFactory {}))),
                }
            }

            const DEFAULT_BLOCKING_THREADS_IF_NOT_SET: usize = 8;
            let spawner = BlockingSpawner::new(
                opts.runtime_worker_threads
                    .unwrap_or(DEFAULT_BLOCKING_THREADS_IF_NOT_SET),
            );

            let (persistence, bitv_factory) = persistence_factory(&opts, spawner.clone())
                .await
                .context("error initializing session persistence store")?;

            let reqwest_client = if let Some(client) = opts.http_client.take() {
                client
            } else {
                #[allow(unused_mut)]
                let mut builder = reqwest::Client::builder();
                #[cfg(not(windows))]
                if let Some(bd) = opts.bind_device_name.as_ref() {
                    builder = builder.interface(bd);
                }

                builder
                    .user_agent(&identity.user_agent)
                    .build()
                    .context("error building HTTP(S) client")?
            };

            let stream_connector = Arc::new(
                StreamConnector::new(StreamConnectorArgs {
                    enable_tcp: opts.connect.as_ref().map(|c| c.enable_tcp).unwrap_or(true),
                    utp_socket: listen_result.as_ref().and_then(|l| l.utp_socket.clone()),
                    bind_device: bind_device.clone(),
                    ipv4_only: opts.ipv4_only,
                    socket_factory: opts.socket_factory.clone(),
                    encryption: opts.encryption,
                })
                .await
                .context("error creating stream connector")?,
            );

            let blocklist = if let Some(blocklist_url) = opts.blocklist_url {
                info!(url = blocklist_url, "loading p2p blocklist");
                let bl = IpRanges::load_from_url(&reqwest_client, &blocklist_url)
                    .await
                    .with_context(|| format!("error reading blocklist from {blocklist_url}"))?;
                info!(len = bl.len(), "loaded blocklist");
                bl
            } else {
                IpRanges::default()
            };

            let allowlist = if let Some(allowlist_url) = opts.allowlist_url {
                info!(url = allowlist_url, "loading p2p allowlist");
                let al = IpRanges::load_from_url(&reqwest_client, &allowlist_url)
                    .await
                    .with_context(|| format!("error reading allowlist from {allowlist_url}"))?;
                info!(len = al.len(), "loaded allowlist");
                Some(al)
            } else {
                None
            };

            let udp_tracker_client = match opts.socket_factory.as_ref() {
                Some(factory) => {
                    let addr: SocketAddr = if opts.ipv4_only {
                        (Ipv4Addr::UNSPECIFIED, 0).into()
                    } else {
                        (Ipv6Addr::UNSPECIFIED, 0).into()
                    };
                    let socket = factory
                        .bind_udp(addr)
                        .await
                        .context("error binding UDP tracker socket through socket factory")?;
                    UdpTrackerClient::new_with_socket(token.clone(), socket, factory.resolver())
                }
                None => UdpTrackerClient::new(token.clone(), bind_device.as_ref())
                    .await
                    .context("error creating UDP tracker client")?,
            };

            let lsd = {
                if opts.disable_local_service_discovery {
                    None
                } else {
                    LocalServiceDiscovery::new(LocalServiceDiscoveryOptions {
                        cancel_token: token.clone(),
                        bind_device: bind_device.as_ref(),
                        ..Default::default()
                    })
                    .await
                    .inspect_err(|e| warn!("error starting local service discovery: {e:#}"))
                    .ok()
                }
            };

            let session = Arc::new(Self {
                persistence,
                bitv_factory,
                identity,
                dht,
                peer_opts,
                spawner: spawner.clone(),
                output_folder: default_output_folder,
                next_id: AtomicUsize::new(0),
                db: RwLock::new(Default::default()),
                _cancellation_token_drop_guard: token.clone().drop_guard(),
                cancellation_token: token,
                tasks,
                announce_port: Arc::new(AnnouncePort::new(
                    listen_result.as_ref().and_then(|l| l.announce_port),
                )),
                listen_addr: listen_result.as_ref().map(|l| l.addr),
                utp: listen_result
                    .as_ref()
                    .is_some_and(|l| l.utp_socket.is_some()),
                default_storage_factory: opts.default_storage_factory,
                reqwest_client,
                connector: stream_connector,
                root_span: opts.root_span,
                stats: Arc::new(SessionStats::new()),
                concurrent_initialize_semaphore: Arc::new(tokio::sync::Semaphore::new(
                    opts.concurrent_init_limit.unwrap_or(3),
                )),
                udp_tracker_client,
                connection_slots: Arc::new(ConnectionSlots::new(
                    opts.connections_limit,
                    opts.connection_speed,
                )),
                ratelimits: Limits::new(opts.ratelimits),
                ipv4_only: opts.ipv4_only,
                trackers: opts.trackers,
                disable_trackers: opts.disable_trackers,
                stopped_announces: StoppedAnnounces::new(
                    opts.tracker_stopped_timeout
                        .unwrap_or(StoppedAnnounces::DEFAULT_TIMEOUT),
                ),
                auto_scrape: AutoScrape::default(),
                peer_limit: opts.peer_limit,
                unchoke_slots_limit: opts.unchoke_slots_limit,
                disable_pex: opts.disable_pex,
                blocklist,
                allowlist,
                lsd,
            });

            if let Some(mut listen) = listen_result {
                let protocols = listening_protocols(&listen);
                if let Some(tcp) = listen.tcp_socket.take() {
                    let max_pending_incoming_handshake_checks =
                        listen.max_pending_incoming_handshake_checks;
                    session.spawn(
                        debug_span!(parent: session.rs(), "tcp_listen", addr = ?listen.addr),
                        "tcp_listen",
                        {
                            let this = session.clone();
                            async move {
                                this.task_listener(tcp, max_pending_incoming_handshake_checks)
                                    .await
                            }
                        },
                    );
                }
                if let Some(tcp) = listen.custom_tcp_socket.take() {
                    let max_pending_incoming_handshake_checks =
                        listen.max_pending_incoming_handshake_checks;
                    session.spawn(
                        debug_span!(parent: session.rs(), "tcp_listen", addr = ?listen.addr),
                        "tcp_listen",
                        {
                            let this = session.clone();
                            async move {
                                this.task_listener(tcp, max_pending_incoming_handshake_checks)
                                    .await
                            }
                        },
                    );
                }
                if let Some(utp) = listen.utp_socket.take() {
                    let max_pending_incoming_handshake_checks =
                        listen.max_pending_incoming_handshake_checks;
                    session.spawn(
                        debug_span!(parent: session.rs(), "utp_listen", addr = ?listen.addr),
                        "utp_listen",
                        {
                            let this = session.clone();
                            async move {
                                this.task_listener(utp, max_pending_incoming_handshake_checks)
                                    .await
                            }
                        },
                    );
                }
                if listen.enable_upnp_port_forwarding
                    && let Some(announce_port) = listen.announce_port
                {
                    let requests = protocols
                        .into_iter()
                        .map(|protocol| PortMapRequest {
                            protocol,
                            local_port: listen.addr.port(),
                            external_port: announce_port,
                        })
                        .collect();
                    session.start_port_mapping(
                        requests,
                        bind_device.clone(),
                        opts.bind_device_name.as_deref(),
                    );
                }
            }

            if let Some(persistence) = session.persistence.as_ref() {
                info!("will use {persistence:?} for session persistence");

                let mut ps = persistence.stream_all().await?;
                let mut added_all = false;
                let mut futs = FuturesUnordered::new();

                while !added_all || !futs.is_empty() {
                    // NOTE: this closure exists purely to workaround rustfmt screwing up when inlining it.
                    let add_torrent_span = |info_hash: &Id20| -> tracing::Span {
                        debug_span!(parent: session.rs(), "add_torrent", info_hash=?info_hash)
                    };
                    tokio::select! {
                        Some(res) = futs.next(), if !futs.is_empty() => {
                            if let Err(e) = res {
                                error!("error adding torrent to session: {e:#}");
                            }
                        }
                        st = ps.next(), if !added_all => {
                            match st {
                                Some(st) => {
                                    let (id, st) = st?;
                                    let span = add_torrent_span(st.info_hash());
                                    let (add_torrent, mut opts) = st.into_add_torrent()?;
                                    opts.preferred_id = Some(id);
                                    let fut = session.add_torrent(add_torrent, Some(opts));
                                    let fut = fut.instrument(span);
                                    futs.push(fut);
                                },
                                None => added_all = true
                            };
                        }
                    };
                }
            }

            session.start_speed_estimator_updater();

            Ok(session)
        }
        .boxed()
    }

    async fn check_incoming_connection(
        self: Arc<Self>,
        addr: SocketAddr,
        kind: ConnectionKind,
        mut reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> anyhow::Result<(Arc<TorrentStateLive>, CheckedIncomingConnection)> {
        let handshake_timeout = self.peer_opts.handshake_timeout();

        let incoming_ip = addr.ip();
        if self.blocklist.has(incoming_ip) {
            self.stats
                .counters
                .blocked_incoming
                .fetch_add(1, Ordering::Relaxed);
            bail!("Incoming ip {incoming_ip} is in blocklist");
        }
        if self.allowlist.as_ref().is_some_and(|l| !l.has(incoming_ip)) {
            self.stats
                .counters
                .blocked_incoming
                .fetch_add(1, Ordering::Relaxed);
            bail!("Incoming ip {incoming_ip} is not in allowlist");
        }

        // With encryption enabled, an encrypted connection is unwrapped here
        // and a plaintext one passed through (or refused); with it disabled
        // the halves are left as they are.
        let mut writer = writer;
        crate::mse::upgrade_incoming(
            self.connector.encryption(),
            &mut reader,
            &mut writer,
            handshake_timeout,
            || {
                self.db
                    .read()
                    .torrents
                    .values()
                    .flat_map(|t| t.info_hashes().ids20())
                    .collect()
            },
        )
        .await
        .context("error in the encryption handshake")?;

        let mut read_buf = ReadBuf::new();
        let h = read_buf
            .read_handshake(&mut reader, handshake_timeout)
            .await
            .context("error reading handshake")?;
        trace!("received handshake from {addr}: {:?}", h);

        let (id, torrent) = self
            .db
            .read()
            .torrents
            .iter()
            .find(|(_, t)| t.info_hashes().answers_to(h.info_hash))
            .map(|(id, t)| (*id, t.clone()))
            .with_context(|| format!("didn't find a matching torrent {:?}", h.info_hash))?;
        if h.peer_id == torrent.shared().peer_id {
            bail!("seems like we are connecting to ourselves, ignoring");
        }

        let live = torrent
            .live_wait_initializing(Duration::from_secs(5))
            .await
            .with_context(|| format!("torrent {id} is not live, ignoring connection"))?;

        Ok((
            live,
            CheckedIncomingConnection {
                addr,
                reader,
                writer,
                kind,
                handshake: h,
                read_buf,
            },
        ))
    }

    async fn task_listener<A: Accept>(
        self: Arc<Self>,
        l: A,
        max_pending_incoming_handshake_checks: usize,
    ) -> anyhow::Result<()> {
        let mut futs = FuturesUnordered::new();
        let session = Arc::downgrade(&self);
        drop(self);

        loop {
            tokio::select! {
                r = l.accept(), if futs.len() < max_pending_incoming_handshake_checks => {
                    match r {
                        Ok((addr, (read, write))) => {
                            trace!("accepted connection from {addr}");
                            let session = session.upgrade().context("session is dead")?;
                            // Dropping the halves closes a connection past the session's cap.
                            let Some(slot) = session.connection_slots.try_open() else {
                                debug!(%addr, "at the session's connection limit, closing incoming connection");
                                continue;
                            };
                            let span = debug_span!(parent: session.rs(), "incoming", addr=%addr);
                            futs.push(
                                session.check_incoming_connection(addr, A::KIND, Box::new(read), Box::new(write))
                                    .map_ok(|(live, checked)| (live, checked, slot))
                                    .map_err(|e| {
                                        debug!("error checking incoming connection: {e:#}");
                                        e
                                    })
                                    .instrument(span)
                            );
                        }
                        Err(e) => {
                            warn!("error accepting: {e:#}");
                            // Whatever is the reason, ensure we are not stuck trying to
                            // accept indefinitely.
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            continue
                        }
                    }
                },
                Some(Ok((live, checked, slot))) = futs.next(), if !futs.is_empty() => {
                    let (addr, kind) = (checked.addr, checked.kind);
                    if let Err(e) = live.add_incoming_peer(checked, slot) {
                        warn!(?addr, ?kind, "error handing over incoming connection: {e:#}");
                    }
                },
                else => continue,
            }
        }
    }

    /// Keep the listen port mapped on the gateway over UPnP and over PCP /
    /// NAT-PMP at once (libtorrent `session_impl::start_upnp` and
    /// `start_natpmp`), and announce the TCP port a mapper got
    /// (`on_port_mapping`).
    fn start_port_mapping(
        &self,
        requests: Vec<PortMapRequest>,
        bind_device: Option<BindDevice>,
        bind_device_name: Option<&str>,
    ) {
        info!(?requests, "starting UPnP and NAT-PMP / PCP port mapping");
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let upnp_opts = UpnpPortForwarderOptions {
            description: self.identity.user_agent.clone(),
            ..Default::default()
        };
        match UpnpPortForwarder::new(requests.clone(), Some(upnp_opts), bind_device) {
            Ok(upnp) => self.spawn_port_mapper(
                "upnp_forward",
                upnp.run(self.cancellation_token.clone(), events_tx.clone()),
            ),
            Err(e) => warn!("error starting UPnP port mapping: {e:#}"),
        }
        match crate::natpmp::default_gateway(bind_device_name) {
            Ok(gateway) => {
                let mapper = NatPmpMapper::new((gateway, NATPMP_PORT).into(), requests, None);
                self.spawn_port_mapper(
                    "natpmp_forward",
                    mapper.run(self.cancellation_token.clone(), events_tx),
                );
            }
            Err(e) => debug!("no gateway for NAT-PMP / PCP: {e:#}"),
        }
        let announce_port = self.announce_port.clone();
        self.spawn_port_mapper("port_mapping", async move {
            while let Some(event) = events.recv().await {
                match &event.error {
                    None => info!(?event, "port mapping"),
                    Some(error) => {
                        warn!(?event.transport, ?event.protocol, "port mapping failed: {error}")
                    }
                }
                announce_port.on_port_mapping(&event);
            }
            Ok(())
        });
    }

    /// A mapper watches the session's token itself and removes its mappings
    /// on the way out, so it is not dropped on cancel; `stop` waits for it.
    fn spawn_port_mapper(
        &self,
        name: &'static str,
        fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        crate::core::spawn_utils::spawn(
            debug_span!(parent: self.rs(), "port_mapping", task = name),
            name,
            self.tasks.track_future(fut),
        );
    }

    pub fn get_dht(&self) -> Option<&Dht> {
        self.dht.as_ref()
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

    pub(crate) fn rs(&self) -> Option<tracing::Id> {
        self.root_span.as_ref().and_then(|s| s.id())
    }

    /// Stop the session and all managed tasks.
    pub async fn stop(&self) {
        let torrents = self
            .db
            .read()
            .torrents
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for torrent in torrents {
            if let Err(e) = torrent.pause() {
                debug!("error pausing torrent: {e:#}");
            }
        }
        // Let the trackers hear `stopped` before the UDP tracker client goes away.
        let stopped = &self.stopped_announces;
        if tokio::time::timeout(stopped.timeout(), stopped.wait_idle())
            .await
            .is_err()
        {
            debug!("not every tracker heard stopped before shutdown");
        }
        self.cancellation_token.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Run a callback given the currently managed torrents.
    pub fn with_torrents<R>(
        &self,
        callback: impl Fn(&mut dyn Iterator<Item = (TorrentId, &ManagedTorrentHandle)>) -> R,
    ) -> R {
        callback(&mut self.db.read().torrents.iter().map(|(id, t)| (*id, t)))
    }

    /// Add a torrent to the session.
    #[inline(never)]
    pub fn add_torrent<'a>(
        self: &'a Arc<Self>,
        add: AddTorrent<'a>,
        opts: Option<AddTorrentOptions>,
    ) -> BoxFuture<'a, anyhow::Result<AddTorrentResponse>> {
        async move {
            let mut opts = opts.unwrap_or_default();
            let add_res = match add {
                AddTorrent::Url(magnet)
                    if magnet.starts_with("magnet:") || is_bare_info_hash(&magnet) =>
                {
                    let magnet = Magnet::parse(&magnet)
                        .context("provided path is not a valid magnet URL")?;
                    // The link's peers join those the caller named
                    // (libtorrent `add_torrent_params::peers`).
                    opts.initial_peers
                        .get_or_insert_default()
                        .extend(magnet.peers.iter().copied());
                    opts.web_seeds
                        .extend(magnet.web_seeds.iter().map(|url| WebSeed {
                            url: url.clone(),
                            kind: WebSeedKind::Url,
                        }));
                    if let Some(so) = magnet.get_select_only() {
                        // Only overwrite opts.only_files if user didn't specify
                        if opts.only_files.is_none() {
                            opts.only_files = Some(so);
                        }
                    }

                    InternalAddResult {
                        info_hashes: magnet.info_hashes(),
                        // Each tracker of a magnet link is a tier of its own.
                        tracker_tiers: merge_tracker_tiers(
                            magnet
                                .trackers
                                .into_iter()
                                .filter_map(|t| url::Url::parse(&t).ok())
                                .map(|t| vec![t])
                                .collect(),
                            std::iter::empty(),
                            false,
                        ),
                        metadata: None,
                        name: magnet.name,
                    }
                }
                other => {
                    let torrent = match other {
                        AddTorrent::Url(url)
                            if url.starts_with("http://") || url.starts_with("https://") =>
                        {
                            torrent_from_url(&self.reqwest_client, &url).await?
                        }
                        AddTorrent::Url(url) => {
                            bail!(
                                "unsupported URL {:?}. Supporting magnet:, http:, and https",
                                url
                            )
                        }
                        AddTorrent::TorrentFileBytes(bytes) => {
                            torrent_from_bytes(bytes).context("error decoding torrent")?
                        }
                    };

                    let tracker_tiers = merge_tracker_tiers(
                        metainfo_tracker_tiers(&torrent.meta),
                        opts.trackers
                            .iter()
                            .flatten()
                            .filter_map(|t| url::Url::parse(t).ok()),
                        torrent.meta.info.data.private,
                    );

                    InternalAddResult {
                        info_hashes: torrent.meta.info_hashes,
                        metadata: Some(TorrentMetadata::from_torrent(
                            torrent.meta,
                            torrent.torrent_bytes,
                        )?),
                        tracker_tiers,
                        name: None,
                    }
                }
            };

            self.add_torrent_internal(add_res, opts).await
        }
        .instrument(debug_span!(parent: self.rs(), "add_torrent"))
        .boxed()
    }

    fn get_default_subfolder_for_torrent(
        &self,
        info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
        magnet_name: Option<&str>,
    ) -> anyhow::Result<Option<PathBuf>> {
        let files = info
            .iter_file_details()
            .map(|fd| Ok((fd.filename.to_pathbuf(), fd.len)))
            .collect::<anyhow::Result<Vec<(PathBuf, u64)>>>()?;
        if files.len() < 2 || info.is_single_file() {
            return Ok(None);
        }

        fn check_valid(pb: &Path) -> anyhow::Result<()> {
            if pb.components().any(|x| !matches!(x, Component::Normal(_))) {
                bail!("path traversal in torrent name detected")
            }
            Ok(())
        }

        if let Some(name) = info.name()
            && !name.is_empty()
        {
            let pb = PathBuf::from(name.as_ref());
            check_valid(&pb)?;
            return Ok(Some(pb));
        };
        if let Some(name) = magnet_name {
            let pb = PathBuf::from(name);
            check_valid(&pb)?;
            return Ok(Some(pb));
        }
        // Let the subfolder name be the longest filename
        let longest = files
            .iter()
            .max_by_key(|(_, l)| l)
            .unwrap()
            .0
            .file_stem()
            .context("can't determine longest filename")?;
        Ok::<_, anyhow::Error>(Some(PathBuf::from(longest)))
    }

    async fn add_torrent_internal(
        self: &Arc<Self>,
        add_res: InternalAddResult,
        mut opts: AddTorrentOptions,
    ) -> anyhow::Result<AddTorrentResponse> {
        let InternalAddResult {
            info_hashes,
            metadata,
            tracker_tiers,
            name,
        } = add_res;
        let identity = opts
            .identity
            .take()
            .unwrap_or_else(|| self.identity.clone());

        let make_peer_rx = |private: bool| {
            self.make_peer_rx(
                info_hashes,
                &identity,
                tracker_tiers.clone(),
                !opts.paused && !opts.list_only,
                opts.force_tracker_interval,
                opts.initial_peers.clone().unwrap_or_default(),
                private,
            )
        };

        let mut seen_peers = Vec::new();

        let (metadata, peer_rx) = {
            match metadata {
                Some(metadata) => {
                    let mut peer_rx = None;
                    if !opts.paused && !opts.list_only {
                        peer_rx = make_peer_rx(metadata.info.info().private);
                    }
                    (metadata, peer_rx)
                }
                None => {
                    // Whether the torrent is private is known only once its
                    // metadata arrives.
                    let PeerSource { peers, trackers } = make_peer_rx(false).context(
                        "no known way to resolve peers (no DHT, no trackers, no initial_peers)",
                    )?;
                    let resolved_magnet = self
                        .resolve_magnet(
                            info_hashes,
                            &identity,
                            peers,
                            &tracker_tiers,
                            opts.peer_opts,
                        )
                        .await?;

                    seen_peers = resolved_magnet.seen_peers.clone();
                    let peer_rx = if resolved_magnet.metadata.info.info().private {
                        // A private torrent takes peers only from its own
                        // trackers (BEP 27): drop the discovery used to fetch the
                        // metadata, and the peers it found.
                        drop(resolved_magnet.peer_rx);
                        make_peer_rx(true)
                    } else {
                        // Add back seen_peers into the peer stream, as we consumed some peers
                        // while resolving the magnet.
                        Some(PeerSource {
                            peers: merge_streams(
                                resolved_magnet.peer_rx,
                                futures::stream::iter(resolved_magnet.seen_peers),
                            )
                            .boxed(),
                            trackers,
                        })
                    };
                    (resolved_magnet.metadata, peer_rx)
                }
            }
        };

        trace!("Torrent metadata: {:#?}", &metadata.info.info());
        // A magnet link names some of the hashes; the metadata has them all.
        let info_hashes = metadata.info_hashes;
        let info_hash = info_hashes.best();

        let only_files = compute_only_files(
            &metadata.info,
            opts.only_files,
            opts.only_files_regex,
            opts.list_only,
        )?;

        let initial_have = opts
            .initial_have
            .take()
            .map(|have| normalize_initial_have(have, metadata.info.lengths()))
            .transpose()?;

        let piece_priorities = match opts.piece_priorities.take() {
            Some(priorities) => {
                let lengths = metadata.info.lengths();
                let mut all = vec![PiecePriority::Normal; lengths.total_pieces() as usize];
                for (index, priority) in validate_piece_priorities(lengths, &priorities)? {
                    all[index.get_usize()] = priority;
                }
                Some(all)
            }
            None => None,
        };

        let output_folder = match (opts.output_folder, opts.sub_folder) {
            (None, None) => self.output_folder.join(
                self.get_default_subfolder_for_torrent(&metadata.info, name.as_deref())?
                    .unwrap_or_default(),
            ),
            (Some(o), None) => PathBuf::from(o),
            (Some(_), Some(_)) => {
                bail!("you can't provide both output_folder and sub_folder")
            }
            (None, Some(s)) => self.output_folder.join(s),
        };

        if opts.list_only {
            return Ok(AddTorrentResponse::ListOnly(ListOnlyResponse {
                info_hash,
                torrent_bytes: metadata.torrent_bytes(),
                info: metadata.info,
                only_files,
                output_folder,
                seen_peers,
            }));
        }

        let storage_factory = opts
            .storage_factory
            .take()
            .or_else(|| self.default_storage_factory.as_ref().map(|f| f.clone_box()))
            .unwrap_or_else(|| FilesystemStorageFactory::default().boxed());

        let id = if let Some(id) = opts.preferred_id {
            id
        } else if let Some(p) = self.persistence.as_ref() {
            p.next_id().await?
        } else {
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        };

        let _permit = self.spawner.semaphore().acquire_owned().await?;

        let (managed_torrent, metadata) = {
            let mut g = self.db.write();
            if let Some((id, handle)) = g.torrents.iter().find_map(|(eid, t)| {
                if t.info_hashes().overlaps(&info_hashes) || *eid == id {
                    Some((*eid, t.clone()))
                } else {
                    None
                }
            }) {
                return Ok(AddTorrentResponse::AlreadyManaged(id, handle));
            }

            let span = debug_span!(parent: self.rs(), "torrent", id);
            let peer_opts = self.merge_peer_opts(opts.peer_opts);
            let metadata = Arc::new(metadata);
            let minfo = Arc::new(ManagedTorrentShared {
                id,
                span,
                info_hash,
                info_hashes,
                trackers: tracker_tiers.iter().flatten().cloned().collect(),
                tracker_tiers,
                spawner: self.spawner.clone(),
                peer_id: identity.peer_id,
                storage_factory,
                options: ManagedTorrentOptions {
                    force_tracker_interval: opts.force_tracker_interval,
                    peer_connect_timeout: peer_opts.connect_timeout,
                    peer_read_write_timeout: peer_opts.read_write_timeout,
                    peer_max_request_window: peer_opts.max_request_window,
                    allow_overwrite: opts.overwrite,
                    output_folder,
                    ratelimits: Limits::new(opts.ratelimits),
                    initial_peers: opts.initial_peers.clone().unwrap_or_default(),
                    peer_limit: opts.peer_limit.or(self.peer_limit),
                    disable_v1_hashes: opts.disable_v1_hashes,
                    super_seeding: opts.super_seeding,
                },
                connector: self.connector.clone(),
                session: Arc::downgrade(self),
                magnet_name: name,
                client_name_and_version: identity.version.clone(),
                user_agent: identity.user_agent.clone(),
                last_run: Default::default(),
                web_seeds: Arc::new(WebSeeds::of_torrent(&metadata, &opts.web_seeds)),
            });

            let initializing = Arc::new(TorrentStateInitializing::new(
                minfo.clone(),
                metadata.clone(),
                only_files.clone(),
                self.spawner
                    .block_in_place(|| minfo.storage_factory.create_and_init(&minfo, &metadata))?,
                false,
                initial_have,
                piece_priorities.clone(),
            ));
            let handle = Arc::new(ManagedTorrent {
                locked: RwLock::new(ManagedTorrentLocked {
                    paused: opts.paused,
                    state: ManagedTorrentState::Initializing(initializing),
                    only_files,
                    piece_priorities,
                    trackers: None,
                    auto_scrape: None,
                }),
                state_change_notify: Notify::new(),
                shared: minfo,
                metadata: ArcSwapOption::new(Some(metadata.clone())),
            });

            g.add_torrent(handle.clone(), id);
            (handle, metadata)
        };

        if let Some(p) = self.persistence.as_ref()
            && let Err(e) = p.store(id, &managed_torrent).await
        {
            self.db.write().torrents.remove(&id);
            return Err(e);
        }

        let _e = managed_torrent.shared.span.clone().entered();

        managed_torrent
            .start(peer_rx, opts.paused)
            .context("error starting torrent")?;

        if let Some(name) = metadata.info.name() {
            info!(?name, "added torrent");
        }

        Ok(AddTorrentResponse::Added(id, managed_torrent))
    }

    pub fn get(&self, id: TorrentIdOrHash) -> Option<ManagedTorrentHandle> {
        match id {
            TorrentIdOrHash::Id(id) => self.db.read().torrents.get(&id).cloned(),
            TorrentIdOrHash::Hash(id) => self.db.read().torrents.iter().find_map(|(_, v)| {
                if v.info_hashes().answers_to(id) {
                    Some(v.clone())
                } else {
                    None
                }
            }),
        }
    }

    pub async fn delete(&self, id: TorrentIdOrHash, delete_files: bool) -> anyhow::Result<()> {
        let id = match id {
            TorrentIdOrHash::Id(id) => id,
            TorrentIdOrHash::Hash(h) => self
                .db
                .read()
                .torrents
                .values()
                .find_map(|v| {
                    if v.info_hashes().answers_to(h) {
                        Some(v.id())
                    } else {
                        None
                    }
                })
                .context("no such torrent in db")?,
        };
        let removed = self
            .db
            .write()
            .torrents
            .remove(&id)
            .with_context(|| format!("torrent with id {id} did not exist"))?;

        if let Err(e) = removed.pause() {
            debug!("error pausing torrent before deletion: {e:#}")
        }

        let metadata = removed.metadata.load_full().expect("TODO");

        let storage = removed
            .with_state_mut(|s| match s.take() {
                ManagedTorrentState::Initializing(p) => p.files.take().ok(),
                ManagedTorrentState::Paused(p) => Some(p.files),
                ManagedTorrentState::Live(l) => l
                    .pause()
                    // inspect_err not available in 1.75
                    .map_err(|e| {
                        warn!(?id, "error pausing torrent: {e:#}");
                        e
                    })
                    .ok()
                    .map(|p| p.files),
                _ => None,
            })
            .map(Ok)
            .unwrap_or_else(|| {
                removed
                    .shared
                    .storage_factory
                    .create(removed.shared(), &metadata)
            });

        if let Some(p) = self.persistence.as_ref() {
            if let Err(e) = p.delete(id).await {
                error!(
                    ?id,
                    "error deleting torrent from persistence database: {e:#}"
                );
            } else {
                debug!(?id, "deleted torrent from persistence database")
            }
        }

        match (storage, delete_files) {
            (Err(e), true) => return Err(e).context("torrent deleted, but could not delete files"),
            (Ok(storage), true) => {
                debug!("will delete files");
                remove_files_and_dirs(&metadata.file_infos, &storage);
                if removed.shared().options.output_folder != self.output_folder
                    && let Err(e) = storage.remove_directory_if_empty(Path::new(""))
                {
                    warn!(
                        ?id,
                        "error removing {:?}: {e:#}",
                        removed.shared().options.output_folder
                    )
                }
            }
            (_, false) => {
                debug!("not deleting files")
            }
        };

        info!(id, "deleted torrent");
        Ok(())
    }

    pub fn make_peer_rx_managed_torrent(
        self: &Arc<Self>,
        t: &Arc<ManagedTorrent>,
        announce: bool,
    ) -> Option<PeerStream> {
        self.peer_source_managed_torrent(t, announce)
            .map(|s| s.peers)
    }

    fn peer_source_managed_torrent(
        self: &Arc<Self>,
        t: &ManagedTorrentHandle,
        announce: bool,
    ) -> Option<PeerSource> {
        let is_private = t.with_metadata(|m| m.info.info().private).unwrap_or(false);
        self.make_peer_rx(
            t.info_hashes(),
            &t.shared().identity(),
            t.shared().tracker_tiers.clone(),
            announce,
            t.shared().options.force_tracker_interval,
            t.shared().options.initial_peers.clone(),
            is_private,
        )
    }

    /// The tiers a torrent announces to: its own, then the session's trackers
    /// unless the torrent is private.
    fn announce_tiers(
        &self,
        tracker_tiers: Vec<Vec<url::Url>>,
        is_private: bool,
    ) -> Vec<Vec<url::Url>> {
        if self.disable_trackers {
            return Vec::new();
        }
        merge_tracker_tiers(tracker_tiers, self.trackers.iter().cloned(), is_private)
    }

    // Get a peer stream from both DHT and trackers, under every hash the
    // torrent goes by (libtorrent `torrent::dht_announce`, `lsd_announce`
    // and `announce_with_tracker` each loop over `info_hash_t::for_each`).
    fn make_peer_rx(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        identity: &Identity,
        tracker_tiers: Vec<Vec<url::Url>>,
        announce: bool,
        force_tracker_interval: Option<Duration>,
        initial_peers: Vec<SocketAddr>,
        is_private: bool,
    ) -> Option<PeerSource> {
        let dht_rx = if is_private {
            None
        } else {
            self.dht.as_ref().map(|dht| {
                futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                    dht_peers(
                        dht.clone(),
                        info_hash,
                        if announce {
                            self.announce_port.subscribe()
                        } else {
                            tokio::sync::watch::channel(None).1
                        },
                    )
                }))
            })
        };

        let lsd_rx = if is_private {
            None
        } else {
            self.lsd.as_ref().map(|lsd| {
                futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                    lsd.announce(
                        info_hash,
                        // Local peers reach the listen port itself.
                        if announce {
                            self.announce_port.configured
                        } else {
                            None
                        },
                    )
                    .boxed()
                }))
            })
        };

        let tracker_rx_stats = PeerRxTorrentInfo {
            info_hashes,
            session: self.clone(),
            torrent: Default::default(),
        };
        let (tracker_rx, trackers) = TrackerComms::start_with_tiers(
            info_hashes,
            identity.clone(),
            self.announce_tiers(tracker_tiers, is_private),
            Box::new(tracker_rx_stats),
            force_tracker_interval,
            self.announce_port.subscribe(),
            self.reqwest_client.clone(),
            self.udp_tracker_client.clone(),
            self.stopped_announces.clone(),
        )
        .map(|task| (task.peers, task.status))
        .unzip();

        let initial_peers_rx = if initial_peers.is_empty() {
            None
        } else {
            Some(futures::stream::iter(initial_peers))
        };
        merge_two_optional_streams(
            merge_two_optional_streams(
                merge_two_optional_streams(dht_rx, tracker_rx),
                initial_peers_rx,
            ),
            lsd_rx,
        )
        .map(|peers| PeerSource { peers, trackers })
    }

    async fn try_update_persistence_metadata(&self, handle: &ManagedTorrentHandle) {
        if let Some(p) = self.persistence.as_ref()
            && let Err(e) = p.update_metadata(handle.id(), handle).await
        {
            warn!(storage=?p, error=?e, "error updating metadata")
        }
    }

    pub async fn pause(&self, handle: &ManagedTorrentHandle) -> anyhow::Result<()> {
        handle.pause()?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    pub async fn unpause(self: &Arc<Self>, handle: &ManagedTorrentHandle) -> anyhow::Result<()> {
        let peer_rx = self.peer_source_managed_torrent(handle, true);
        handle.start(peer_rx, false)?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    pub async fn update_only_files(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
        only_files: &HashSet<usize>,
    ) -> anyhow::Result<()> {
        handle.update_only_files(only_files)?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    /// Change a torrent's upload and download caps while it runs (see
    /// `ManagedTorrent::set_ratelimits`) and persist them.
    pub async fn update_ratelimits(
        &self,
        handle: &ManagedTorrentHandle,
        limits: LimitsConfig,
    ) -> anyhow::Result<()> {
        handle.set_ratelimits(limits);
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.listen_addr
    }

    /// The port peers are told to connect to: the one a mapper got on the
    /// gateway, else the configured one.
    pub fn announce_port(&self) -> Option<u16> {
        self.announce_port.get()
    }

    async fn resolve_magnet(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        identity: &Identity,
        peer_rx: PeerStream,
        tracker_tiers: &[Vec<url::Url>],
        peer_opts: Option<PeerConnectionOptions>,
    ) -> anyhow::Result<ResolveMagnetResult> {
        match read_metainfo_from_peer_receiver(
            identity.peer_id,
            info_hashes,
            Default::default(),
            peer_rx,
            Some(self.merge_peer_opts(peer_opts)),
            self.connector.clone(),
            identity.version.clone(),
        )
        .await
        {
            ReadMetainfoResult::Found {
                info,
                info_bytes,
                rx,
                seen,
            } => {
                trace!(?info, "received result from DHT");
                let info = info.validate()?;
                Ok(ResolveMagnetResult {
                    metadata: TorrentMetadata::new(
                        info,
                        torrent_file_from_info_bytes(info_bytes.as_ref(), tracker_tiers)?,
                        info_bytes.0,
                        Vec::new(),
                    )?,
                    peer_rx: rx,
                    seen_peers: {
                        let seen = seen.into_iter().collect::<Vec<_>>();
                        for peer in &seen {
                            trace!(?peer, "seen")
                        }
                        seen
                    },
                })
            }
            ReadMetainfoResult::ChannelClosed { .. } => {
                bail!("input address stream exhausted, no way to discover torrent metainfo")
            }
        }
    }

    pub async fn create_and_serve_torrent(
        self: &Arc<Self>,
        path: &Path,
        opts: CreateTorrentOptions<'_>,
    ) -> Result<(CreateTorrentResult, ManagedTorrentHandle), ApiError> {
        if !path.exists() {
            return Err(ApiError::from((
                StatusCode::BAD_REQUEST,
                "path doesn't exist",
            )));
        }

        let torrent = create_torrent(path, opts, &self.spawner)
            .await
            .with_status(StatusCode::BAD_REQUEST)?;

        let bytes = torrent.as_bytes()?;

        let handle = self
            .add_torrent(
                AddTorrent::TorrentFileBytes(bytes.clone()),
                Some(AddTorrentOptions {
                    paused: false,
                    overwrite: true,
                    output_folder: Some(
                        torrent
                            .output_folder
                            .to_str()
                            .context("invalid utf-8")?
                            .to_owned(),
                    ),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("error adding to session")?;

        Ok((torrent, handle))
    }
}

pub(crate) struct ResolveMagnetResult {
    pub metadata: TorrentMetadata,
    pub peer_rx: PeerStream,
    pub seen_peers: Vec<SocketAddr>,
}

fn remove_files_and_dirs(infos: &FileInfos, files: &dyn TorrentStorage) {
    let mut all_dirs = HashSet::new();
    for (id, fi) in infos.iter().enumerate() {
        if fi.attrs.padding {
            continue;
        }
        let mut fname = &*fi.relative_filename;
        if let Err(e) = files.remove_file(id, fname) {
            warn!(?fi.relative_filename, error=?e, "could not delete file");
        } else {
            debug!(?fi.relative_filename, "deleted the file")
        }
        while let Some(parent) = fname.parent() {
            if parent != Path::new("") {
                all_dirs.insert(parent);
            }
            fname = parent;
        }
    }

    let all_dirs = {
        let mut v = all_dirs.into_iter().collect::<Vec<_>>();
        v.sort_unstable_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
        v
    };
    for dir in all_dirs {
        if let Err(e) = files.remove_directory_if_empty(dir) {
            warn!("error removing {dir:?}: {e:#}");
        } else {
            debug!("removed {dir:?}")
        }
    }
}

// Ad adapter for converting stats into the format that tracker_comms accepts.
struct PeerRxTorrentInfo {
    info_hashes: InfoHashes,
    session: Arc<Session>,
    // Kept once found, so the counters stay readable after the torrent is
    // removed from the session (for the `stopped` announce).
    torrent: std::sync::OnceLock<ManagedTorrentHandle>,
}

impl PeerRxTorrentInfo {
    fn torrent(&self) -> Option<&ManagedTorrentHandle> {
        if let Some(t) = self.torrent.get() {
            return Some(t);
        }
        let found = self.session.with_torrents(|torrents| {
            torrents
                .map(|(_, t)| t)
                .find(|t| t.info_hashes().overlaps(&self.info_hashes))
                .cloned()
        })?;
        Some(self.torrent.get_or_init(|| found))
    }
}

impl crate::tracker::TorrentStatsProvider for PeerRxTorrentInfo {
    fn get(&self) -> crate::tracker::TrackerCommsStats {
        let mt = match self.torrent() {
            Some(mt) => mt,
            None => {
                trace!(info_hashes=?self.info_hashes, "can't find torrent in the session, using default stats");
                return Default::default();
            }
        };
        let live = mt.live();

        use crate::torrent_state::stats::TorrentStatsState as TS;
        use crate::tracker::TrackerCommsStatsState as S;

        let torrent_state = match mt.stats().state {
            TS::Initializing { .. } => S::Initializing,
            TS::Live => S::Live,
            TS::Paused => S::Paused,
            TS::Error => S::None,
        };
        // The running run's counters; once it has ended (its pieces are gone
        // from the live state) the ones recorded as it ended.
        let last_run = *mt.shared().last_run.lock();
        let run = live
            .as_ref()
            .map(|l| (l.get_uploaded_bytes(), l.get_downloaded_bytes()))
            .or(last_run.map(|r| (r.uploaded_bytes, r.downloaded_bytes)));
        // What the whole torrent still lacks, whatever is selected, so a
        // torrent that has its selected files but not every piece never reads
        // as a seed (libtorrent `torrent::bytes_left`). Once the pieces are
        // gone with the torrent, what its run recorded as it ended.
        let left_bytes = mt
            .with_chunk_tracker(|c| c.get_lengths().total_length() - c.get_hns().have_bytes)
            .ok()
            .or(last_run.map(|r| r.left_bytes));
        crate::tracker::TrackerCommsStats {
            uploaded_bytes: run.map_or(0, |(uploaded, _)| uploaded),
            downloaded_bytes: run.map_or(0, |(_, downloaded)| downloaded),
            left_bytes,
            torrent_state,
            // `completed` is for becoming a seed, not for finishing a
            // selection (libtorrent `torrent::finished` calls `completed`
            // only when `is_seed`). A run that began finished had every
            // selected piece, so one that finished during it began short of
            // a seed too.
            finished_during_run: live.is_some_and(|l| l.finished_during_run())
                && left_bytes == Some(0),
        }
    }

    fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
        async move {
            loop {
                if let Some(mt) = self.torrent() {
                    // The torrent finishes its selection; it completes only if
                    // that makes it a seed. An error, or a selection that
                    // finished short of a seed, leaves nothing to wait on in
                    // this run: a later regular announce reads the state.
                    if mt.wait_until_completed().await.is_err() || !self.get().is_completed() {
                        futures::future::pending::<()>().await;
                    }
                    return;
                }
                // A magnet still resolving its metadata isn't in the session yet.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::{
        AddTorrent, AddTorrentOptions, AddTorrentResponse, Session, SessionOptions,
        TorrentIdOrHash, is_bare_info_hash, merge_tracker_tiers, metainfo_tracker_tiers,
        torrent_file_from_info_bytes, torrent_from_bytes,
    };

    fn url(s: &str) -> url::Url {
        url::Url::parse(s).unwrap()
    }

    fn tiers(t: &[&[&str]]) -> Vec<Vec<url::Url>> {
        t.iter()
            .map(|tier| tier.iter().map(|u| url(u)).collect())
            .collect()
    }

    fn bstr(s: &str) -> String {
        format!("{}:{s}", s.len())
    }

    #[test]
    fn the_session_announces_the_tcp_port_a_mapper_got_nat_pmp_before_upnp() {
        use super::AnnouncePort;
        use crate::upnp::{
            PortMapped,
            PortmapProtocol::{Tcp, Udp},
            PortmapTransport::{NatPmp, Upnp},
        };
        let event = |transport, protocol, external_port| PortMapped {
            transport,
            protocol,
            local_port: 6881,
            external_port,
            external_ip: None,
            error: None,
        };
        let port = AnnouncePort::new(Some(6881));
        assert_eq!(port.get(), Some(6881));
        port.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
        assert_eq!(port.get(), Some(40001));
        // Peers connect over TCP; a UDP mapping does not move the port.
        port.on_port_mapping(&event(Upnp, Udp, Some(40002)));
        assert_eq!(port.get(), Some(40001));
        port.on_port_mapping(&event(NatPmp, Tcp, Some(50000)));
        assert_eq!(port.get(), Some(50000));
        // A failed or removed mapping hands the port back.
        port.on_port_mapping(&event(NatPmp, Tcp, None));
        assert_eq!(port.get(), Some(40001));
        port.on_port_mapping(&event(Upnp, Tcp, None));
        assert_eq!(port.get(), Some(6881));
        // An announce hears of a change of the port, and only of a change.
        let mut announce = port.subscribe();
        port.on_port_mapping(&event(Upnp, Udp, Some(40002)));
        port.on_port_mapping(&event(NatPmp, Tcp, None));
        assert!(!announce.has_changed().unwrap());
        port.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
        assert!(announce.has_changed().unwrap());
        assert_eq!(*announce.borrow_and_update(), Some(40001));
        // Nothing to announce stays nothing.
        let none = AnnouncePort::new(None);
        none.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
        assert_eq!(none.get(), None);
    }

    #[tokio::test]
    async fn the_dht_announces_a_port_that_changes_while_it_searches() {
        // libtorrent torrent::dht_announce reads the port at each announce.
        // A node run by hand on 127.0.0.1 answers ping, find_node and
        // get_peers, giving a token, and reports each announce_peer port.
        use crate::bencode::{BencodeValue, ByteBufOwned, dyn_from_bytes};
        use crate::dht::{DhtConfig, DhtState, Id20};
        use futures::StreamExt;
        use std::net::Ipv4Addr;
        use tokio::net::UdpSocket;

        const NODE_ID: [u8; 20] = [0x42; 20];

        fn get<'a>(v: &'a BencodeValue<ByteBufOwned>, key: &str) -> &'a BencodeValue<ByteBufOwned> {
            match v {
                BencodeValue::Dict(d) => &d[&ByteBufOwned::from(key.as_bytes())],
                _ => panic!("not a dict: {v:?}"),
            }
        }
        fn bytes(v: &BencodeValue<ByteBufOwned>) -> &[u8] {
            match v {
                BencodeValue::Bytes(b) => b.as_ref(),
                _ => panic!("not bytes: {v:?}"),
            }
        }

        // The port of the next announce_peer the node hears, answering
        // every query before it.
        async fn next_announced(node: &UdpSocket) -> i64 {
            let mut buf = [0u8; 2048];
            loop {
                let (len, from) = node.recv_from(&mut buf).await.unwrap();
                let msg = dyn_from_bytes::<ByteBufOwned>(&buf[..len]).unwrap();
                if bytes(get(&msg, "y")) != b"q" {
                    continue;
                }
                let tid = bytes(get(&msg, "t"));
                let mut reply = b"d1:rd2:id20:".to_vec();
                reply.extend_from_slice(&NODE_ID);
                reply.extend_from_slice(b"5:token2:tke1:t");
                reply.extend_from_slice(format!("{}:", tid.len()).as_bytes());
                reply.extend_from_slice(tid);
                reply.extend_from_slice(b"1:y1:re");
                node.send_to(&reply, from).await.unwrap();
                if bytes(get(&msg, "q")) == b"announce_peer" {
                    match get(get(&msg, "a"), "port") {
                        BencodeValue::Integer(port) => return *port,
                        other => panic!("port is {other:?}"),
                    }
                }
            }
        }

        let node = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let node_addr = node.local_addr().unwrap();
        let dht = DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            bootstrap_addrs: Some(vec![node_addr.to_string()]),
            ..Default::default()
        })
        .await
        .unwrap();
        dht.add_node(node_addr);
        let (port, listen_port) = tokio::sync::watch::channel(Some(6881));
        // The node is the closest there is to the hash: its own id.
        let mut peers = super::dht_peers(dht, Id20::new(NODE_ID), listen_port);
        let first = tokio::select! {
            _ = peers.next() => panic!("the node gave no peers"),
            port = next_announced(&node) => port,
        };
        assert_eq!(first, 6881);
        port.send_replace(Some(40001));
        let second = tokio::select! {
            _ = peers.next() => panic!("the node gave no peers"),
            port = next_announced(&node) => port,
        };
        assert_eq!(second, 40001);
    }

    #[tokio::test]
    async fn the_session_maps_tcp_and_udp_only_for_the_sockets_it_listens_on() {
        use super::listening_protocols;
        use crate::{
            listen::{ListenerMode, ListenerOptions},
            upnp::PortmapProtocol::{Tcp, Udp},
        };
        for (mode, expected) in [
            (ListenerMode::TcpOnly, vec![Tcp]),
            (ListenerMode::UtpOnly, vec![Udp]),
            (ListenerMode::TcpAndUtp, vec![Tcp, Udp]),
        ] {
            let token = tokio_util::sync::CancellationToken::new();
            let listen = ListenerOptions {
                mode,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }
            .start(None, token.clone(), None, None)
            .await
            .unwrap();
            assert_eq!(listening_protocols(&listen), expected, "{mode:?}");
            token.cancel();
        }
    }

    /// A one-piece torrent whose announce-list is `[[a1, a2], [b1]]`.
    fn torrent_bytes(name: &str, private: bool) -> Bytes {
        let a1 = "http://a1.test/announce";
        let a2 = "udp://a2.test:6969/announce";
        let b1 = "http://b1.test/announce";
        let private = if private { "7:privatei1e" } else { "" };
        format!(
            "d8:announce{}13:announce-listll{}{}el{}ee4:infod6:lengthi1e4:name{}12:piece lengthi16384e6:pieces{}{private}ee",
            bstr(a1),
            bstr(a1),
            bstr(a2),
            bstr(b1),
            bstr(name),
            bstr(&"x".repeat(20)),
        )
        .into_bytes()
        .into()
    }

    fn own_tiers() -> Vec<Vec<url::Url>> {
        tiers(&[
            &["http://a1.test/announce", "udp://a2.test:6969/announce"],
            &["http://b1.test/announce"],
        ])
    }

    #[test]
    fn test_is_bare_info_hash() {
        assert!(is_bare_info_hash(
            "08ada5a7a6183aae1e09d831df6748d566095a10"
        ));
        assert!(is_bare_info_hash(
            "08ADA5A7A6183AAE1E09D831DF6748D566095A10"
        ));
        // 40 characters, but a URL.
        assert!(!is_bare_info_hash(
            "https://nyaa.si/download/2164312.torrent"
        ));
        assert!(!is_bare_info_hash(
            "08ada5a7a6183aae1e09d831df6748d566095a1"
        ));
    }

    #[test]
    fn test_torrent_file_from_info_and_bytes() {
        let orig_full_torrent =
            include_bytes!("../resources/ubuntu-21.04-desktop-amd64.iso.torrent");
        let parsed = torrent_from_bytes(Bytes::from_static(orig_full_torrent)).unwrap();
        let parsed_tiers = metainfo_tracker_tiers(&parsed.meta);

        let generated_torrent =
            torrent_file_from_info_bytes(parsed.meta.info.raw_bytes.as_ref(), &parsed_tiers)
                .unwrap();
        let generated_parsed = torrent_from_bytes(generated_torrent).unwrap();
        assert_eq!(parsed.meta.info_hash, generated_parsed.meta.info_hash);
        assert_eq!(parsed.meta.info, generated_parsed.meta.info);
        assert_eq!(parsed_tiers, metainfo_tracker_tiers(&generated_parsed.meta));
    }

    #[test]
    fn test_metainfo_tracker_tiers() {
        let parsed = torrent_from_bytes(torrent_bytes("x", false)).unwrap();
        assert_eq!(metainfo_tracker_tiers(&parsed.meta), own_tiers());
    }

    #[test]
    fn test_merge_tracker_tiers() {
        let extra = [
            url("http://extra.test/announce"),
            // Already one of the torrent's own trackers: keeps its own place.
            url("http://b1.test/announce"),
        ];

        assert_eq!(
            merge_tracker_tiers(own_tiers(), extra.clone(), false),
            tiers(&[
                &["http://a1.test/announce", "udp://a2.test:6969/announce"],
                &["http://b1.test/announce"],
                &["http://extra.test/announce"],
            ])
        );
        assert_eq!(merge_tracker_tiers(own_tiers(), extra, true), own_tiers());

        // A tracker repeated in a later tier keeps only its first place, and
        // a tier left empty is dropped.
        assert_eq!(
            merge_tracker_tiers(
                tiers(&[
                    &["http://a.test/"],
                    &["http://a.test/"],
                    &["http://b.test/"]
                ]),
                std::iter::empty(),
                false
            ),
            tiers(&[&["http://a.test/"], &["http://b.test/"]])
        );
    }

    #[tokio::test]
    async fn test_private_torrent_announces_only_to_its_own_tiers() {
        let dir = TempDir::new().unwrap();
        let session = Session::new_with_opts(
            dir.path().to_owned(),
            SessionOptions {
                dht: None,
                disable_local_service_discovery: true,
                trackers: HashSet::from([url("http://session.test/announce")]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let add = |name: &'static str, private: bool| {
            let session = session.clone();
            let output_folder = dir.path().join(name).to_str().unwrap().to_owned();
            async move {
                session
                    .add_torrent(
                        AddTorrent::from_bytes(torrent_bytes(name, private)),
                        Some(AddTorrentOptions {
                            paused: true,
                            output_folder: Some(output_folder),
                            trackers: Some(vec!["http://added.test/announce".to_owned()]),
                            ..Default::default()
                        }),
                    )
                    .await
                    .unwrap()
                    .into_handle()
                    .unwrap()
            }
        };

        let private = add("private", true).await;
        assert_eq!(private.shared().tracker_tiers, own_tiers());
        assert_eq!(
            session.announce_tiers(private.shared().tracker_tiers.clone(), true),
            own_tiers()
        );

        let public = add("public", false).await;
        let with_added = tiers(&[
            &["http://a1.test/announce", "udp://a2.test:6969/announce"],
            &["http://b1.test/announce"],
            &["http://added.test/announce"],
        ]);
        assert_eq!(public.shared().tracker_tiers, with_added);
        let mut with_session = with_added;
        with_session.push(vec![url("http://session.test/announce")]);
        assert_eq!(
            session.announce_tiers(public.shared().tracker_tiers.clone(), false),
            with_session
        );

        session.stop().await;
    }

    #[test]
    fn test_dht_identity_follows_the_peer_id() {
        use super::dht_identity;
        use crate::core::{hash_id::Id20, peer_id::generate_azereus_style};

        // No configured peer id: the DHT keeps its own defaults.
        assert_eq!(dht_identity(None), (None, None));

        // Azureus style: the fingerprint, and the client letters with major.minor.
        let id = generate_azereus_style(*b"XY", (1, 2, 3, 4));
        assert_eq!(
            dht_identity(Some(&id)),
            (Some(b"-XY1234-".to_vec()), Some(b"XY\x01\x02".to_vec()))
        );
        let id = generate_azereus_style(*b"XY", (10, 36, 0, 0));
        assert_eq!(dht_identity(Some(&id)).1, Some(b"XY\x0a\x24".to_vec()));

        // Any other shape: no fingerprint to carry over.
        let id = Id20::new([b'M'; 20]);
        assert_eq!(dht_identity(Some(&id)), (Some(Vec::new()), None));
    }

    // A session listening on loopback that seeds a finished one-piece
    // torrent whose own upload limit is `upload`.
    async fn seeding(
        connections_limit: usize,
        upload: crate::limits::LimitKind,
    ) -> (
        TempDir,
        std::sync::Arc<Session>,
        super::ManagedTorrentHandle,
    ) {
        let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
            1,
            16384,
            Some("connection_slots"),
        );
        let torrent = crate::create_torrent(
            dir.path(),
            crate::CreateTorrentOptions {
                piece_length: Some(16384),
                ..Default::default()
            },
            &crate::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                connections_limit,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ratelimits: crate::limits::LimitsConfig {
                        upload_bps: upload,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_completed().await.unwrap();
        (dir, session, handle)
    }

    // What a tracker announce of `handle`'s torrent reports.
    fn tracker_stats(
        session: &std::sync::Arc<Session>,
        handle: &super::ManagedTorrentHandle,
    ) -> (super::PeerRxTorrentInfo, crate::tracker::TrackerCommsStats) {
        use crate::tracker::TorrentStatsProvider;
        let info = super::PeerRxTorrentInfo {
            info_hashes: handle.info_hashes(),
            session: session.clone(),
            torrent: Default::default(),
        };
        let stats = info.get();
        (info, stats)
    }

    #[tokio::test]
    async fn a_seed_announces_nothing_left() {
        use crate::tracker::TorrentStatsProvider;
        use futures::FutureExt;
        let (_dir, session, handle) = seeding(8, Default::default()).await;
        let (info, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(0));
        // Seeding from the start: no `completed`, and nothing to wait for.
        assert!(!stats.finished_during_run);
        assert!(info.wait_until_completed().now_or_never().is_some());
        session.stop().await;
    }

    #[tokio::test]
    async fn a_finished_selection_short_of_a_seed_announces_what_the_torrent_lacks() {
        // libtorrent torrent::bytes_left counts the whole torrent, and
        // torrent::finished sends `completed` only when is_seed.
        use crate::tracker::TorrentStatsProvider;
        use futures::FutureExt;
        const PIECE: usize = 16384;
        let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
            2,
            PIECE,
            Some("partial_selection"),
        );
        let torrent = crate::create_torrent(
            dir.path(),
            crate::CreateTorrentOptions {
                piece_length: Some(PIECE as u32),
                ..Default::default()
            },
            &crate::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        std::fs::remove_file(dir.path().join("1.data")).unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        let (_, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(PIECE as u64));

        // Drop the missing file from the selection: the selection finishes
        // during this run, the torrent still lacks that file's piece.
        let had = handle
            .with_metadata(|m| {
                m.file_infos
                    .iter()
                    .position(|f| f.relative_filename.ends_with("0.data"))
                    .unwrap()
            })
            .unwrap();
        handle.update_only_files(&HashSet::from([had])).unwrap();
        assert!(handle.live().unwrap().finished_during_run());

        let (info, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(PIECE as u64));
        assert!(!stats.finished_during_run);
        assert!(info.wait_until_completed().now_or_never().is_none());

        // Paused, the pieces still say what the torrent lacks.
        session.pause(&handle).await.unwrap();
        let (_, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(PIECE as u64));
        session.stop().await;
    }

    // Connects to the session and sends a handshake for `handle`'s torrent.
    // Returns what the session answers before it closes, up to its own
    // 68-byte handshake: empty when it refused the connection.
    async fn answer(session: &Session, handle: &super::ManagedTorrentHandle) -> Vec<u8> {
        greet(session, handle.info_hash()).await
    }

    // Connects to the session and sends a handshake for `info_hash`; returns
    // what [`answer`] returns.
    async fn greet(session: &Session, info_hash: super::Id20) -> Vec<u8> {
        greet_with(session, info_hash, false).await.1
    }

    // Connects to the session and sends a handshake for `info_hash`,
    // offering v2 when `offer_v2` holds; returns the connection and what
    // [`answer`] returns.
    async fn greet_with(
        session: &Session,
        info_hash: super::Id20,
        offer_v2: bool,
    ) -> (tokio::net::TcpStream, Vec<u8>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(session.listen_addr().unwrap())
            .await
            .unwrap();
        let mut handshake = [0u8; 68];
        let len = crate::peer_protocol::Handshake::new(info_hash, super::Id20::new([7; 20]))
            .offering_v2(offer_v2)
            .serialize_unchecked_len(&mut handshake);
        // A refused connection may be closed before or while this is written,
        // and reset after: either way nothing comes back.
        let _ = stream.write_all(&handshake[..len]).await;
        let mut answer = Vec::new();
        let _ = (&mut stream).take(68).read_to_end(&mut answer).await;
        (stream, answer)
    }

    // Whether a 68-byte handshake answer offers v2.
    fn offers_v2(answer: &[u8]) -> bool {
        crate::peer_protocol::Handshake::deserialize(answer)
            .unwrap()
            .0
            .offers_v2()
    }

    fn is_handshake_for(answer: &[u8], handle: &super::ManagedTorrentHandle) -> bool {
        answer.len() == 68
            && &answer[1..20] == b"BitTorrent protocol"
            && answer[28..48] == handle.info_hash().0
    }

    #[tokio::test]
    async fn an_incoming_connection_past_the_session_cap_is_closed() {
        let (_dir, session, handle) = seeding(1, crate::limits::LimitKind::Unlimited).await;

        // The one place under the cap is taken: the listener closes the
        // connection it accepts without reading the handshake.
        let held = session.connection_slots.try_open().unwrap();
        assert!(answer(&session, &handle).await.is_empty());

        // Freed, the same connection is taken and answered.
        drop(held);
        assert!(is_handshake_for(&answer(&session, &handle).await, &handle));

        session.stop().await;
    }

    #[tokio::test]
    async fn an_incoming_connection_to_a_finished_torrent_with_upload_closed_is_refused() {
        let (_dir, session, handle) = seeding(200, crate::limits::LimitKind::Closed).await;
        assert!(answer(&session, &handle).await.is_empty());

        // With its upload open again, the finished torrent takes the peer.
        handle.set_ratelimits(crate::limits::LimitsConfig::default());
        assert!(is_handshake_for(&answer(&session, &handle).await, &handle));

        session.stop().await;
    }

    // A session listening on loopback that seeds `torrent` from its payload.
    async fn seeding_v2(
        torrent: &crate::core::v2_test_torrent::TestTorrent,
    ) -> (
        TempDir,
        std::sync::Arc<Session>,
        super::ManagedTorrentHandle,
    ) {
        let dir = TempDir::with_prefix("v2_identity").unwrap();
        for (name, data) in &torrent.files {
            std::fs::write(dir.path().join(name), data).unwrap();
        }
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent_bytes()),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_completed().await.unwrap();
        (dir, session, handle)
    }

    fn v2_torrent(hybrid: bool) -> crate::core::v2_test_torrent::TestTorrent {
        use crate::core::v2_test_torrent::{TestTorrent, payload};
        let mut torrent = TestTorrent::new(
            vec![("a", payload(40000, 1)), ("b", payload(9000, 2))],
            16384,
        );
        torrent.hybrid = hybrid;
        torrent
    }

    // The info hash of a 68-byte handshake answer.
    fn answered_hash(answer: &[u8]) -> Option<super::Id20> {
        (answer.len() == 68).then(|| super::Id20::new(answer[28..48].try_into().unwrap()))
    }

    #[tokio::test]
    async fn a_hybrid_answers_a_peer_with_the_hash_it_greeted_with() {
        // libtorrent session_impl::find_torrent keys a hybrid under v1 and the
        // truncated v2; bt_peer_connection::on_receive_handshake answers a
        // peer greeting with the truncated v2 as a v2 peer.
        let torrent = v2_torrent(true);
        let (_dir, session, handle) = seeding_v2(&torrent).await;
        let hashes = handle.info_hashes();
        let v1 = hashes.v1.unwrap();
        let v2 = hashes.v2.unwrap().truncate_for_dht();
        assert_eq!(handle.info_hash(), v1);

        assert_eq!(answered_hash(&greet(&session, v1).await), Some(v1));
        assert_eq!(answered_hash(&greet(&session, v2).await), Some(v2));
        assert!(greet(&session, super::Id20::new([9; 20])).await.is_empty());

        for id in [v1, v2] {
            assert_eq!(
                session.get(TorrentIdOrHash::Hash(id)).map(|t| t.id()),
                Some(handle.id())
            );
        }
        let again = session
            .add_torrent(AddTorrent::from_bytes(torrent.torrent_bytes()), None)
            .await
            .unwrap();
        assert!(matches!(again, AddTorrentResponse::AlreadyManaged(id, _) if id == handle.id()));

        session.stop().await;
    }

    #[tokio::test]
    async fn a_handle_gives_its_version_both_hashes_and_its_magnet() {
        // libtorrent `torrent_handle::info_hashes`, `torrent_info::v1`/`v2`
        // and `make_magnet_uri(torrent_handle)`: btih, then btmh, then dn.
        use crate::core::torrent_metainfo::TorrentVersion;
        for (hybrid, version) in [(true, TorrentVersion::Hybrid), (false, TorrentVersion::V2)] {
            let (_dir, session, handle) = seeding_v2(&v2_torrent(hybrid)).await;
            let hashes = handle.info_hashes();
            assert_eq!(hashes.v1.is_some(), hybrid);
            assert_eq!(handle.version(), Some(version));
            let v2 = format!("xt=urn:btmh:1220{}", hashes.v2.unwrap().as_string());
            let expected = match hashes.v1 {
                Some(v1) => format!("magnet:?xt=urn:btih:{}&{v2}&dn=test", v1.as_string()),
                None => format!("magnet:?{v2}&dn=test"),
            };
            let magnet = handle.magnet();
            assert_eq!(magnet.to_string(), expected);
            assert_eq!(magnet.info_hashes(), hashes);
            session.stop().await;
        }
    }

    #[tokio::test]
    async fn a_v2_torrent_goes_by_its_truncated_hash() {
        // libtorrent info_hash_t::get_best and peer_connection::associated_info_hash
        // for a torrent without v1.
        let (_dir, session, handle) = seeding_v2(&v2_torrent(false)).await;
        let hashes = handle.info_hashes();
        assert_eq!(hashes.v1, None);
        let v2 = hashes.v2.unwrap().truncate_for_dht();
        assert_eq!(handle.info_hash(), v2);
        assert_eq!(answered_hash(&greet(&session, v2).await), Some(v2));

        session.stop().await;
    }

    #[tokio::test]
    async fn a_hybrid_moves_a_peer_offering_v2_to_its_v2_hash() {
        // libtorrent bt_peer_connection::on_receive_handshake: an incoming
        // peer setting 0x10 of reserved byte 7 on a hybrid is v2 and is
        // answered with the truncated v2; write_handshake offers the move
        // only to a peer that is not v2.
        let (_dir, session, handle) = seeding_v2(&v2_torrent(true)).await;
        let hashes = handle.info_hashes();
        let v1 = hashes.v1.unwrap();
        let v2 = hashes.v2.unwrap().truncate_for_dht();

        let (_stream, moved) = greet_with(&session, v1, true).await;
        assert_eq!(answered_hash(&moved), Some(v2));
        assert!(!offers_v2(&moved));

        let (_stream, stayed) = greet_with(&session, v1, false).await;
        assert_eq!(answered_hash(&stayed), Some(v1));
        assert!(offers_v2(&stayed));

        session.stop().await;
    }

    #[tokio::test]
    async fn a_v1_torrent_refuses_a_peer_offering_v2() {
        // libtorrent bt_peer_connection::on_receive_handshake: the v2 bit on
        // a torrent without v2 disconnects with invalid_info_hash.
        let (_dir, session, handle) = seeding(200, crate::limits::LimitKind::Unlimited).await;
        let (_stream, refused) = greet_with(&session, handle.info_hash(), true).await;
        assert!(refused.is_empty());
        let (_stream, taken) = greet_with(&session, handle.info_hash(), false).await;
        assert!(is_handshake_for(&taken, &handle));
        assert!(!offers_v2(&taken));

        session.stop().await;
    }

    #[tokio::test]
    async fn a_peer_that_does_not_speak_v2_is_dropped_for_a_hash_request() {
        // libtorrent bt_peer_connection::on_hash_request: a hash message
        // from a peer that is not protocol_v2 disconnects it.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let torrent = v2_torrent(true);
        let (_dir, session, handle) = seeding_v2(&torrent).await;
        let (mut stream, answer) =
            greet_with(&session, handle.info_hashes().v1.unwrap(), false).await;
        assert_eq!(answered_hash(&answer), handle.info_hashes().v1);

        let request =
            crate::peer_protocol::Message::HashRequest(crate::peer_protocol::HashRequest {
                root: crate::core::v2_test_torrent::file_hashes(&torrent.files[0].1, 16384)
                    .0
                    .unwrap(),
                base: 0,
                index: 0,
                count: 2,
                proof_layers: 0,
            });
        let mut frame = [0u8; 53];
        let len = request.serialize(&mut frame, &Default::default).unwrap();
        stream.write_all(&frame[..len]).await.unwrap();

        // The session closes the connection: the read ends.
        let mut rest = Vec::new();
        let _ = stream.read_to_end(&mut rest).await;

        session.stop().await;
    }

    /// A magnet's `x.pe` peers are the torrent's initial peers: with no DHT,
    /// trackers or other peers, the link's peer alone supplies the metadata
    /// and the content.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_magnets_peer_supplies_its_metadata_and_content() -> anyhow::Result<()> {
        use crate::{
            core::{magnet::Magnet, torrent_metainfo},
            storage::StorageFactory,
            tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        };
        use anyhow::Context;

        let t = TestTorrent::new("test_magnet_x_pe", 3 * 16384 + 100, 16384).await?;
        let seeder =
            Session::new_with_opts(t.dir.path().into(), isolated_session_opts(true)).await?;
        seeder
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    output_folder: Some(t.dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?
            .wait_until_completed()
            .await?;
        let seeder_addr = seeder.listen_addr().context("seeder is not listening")?;

        let mut magnet = Magnet::of(
            torrent_metainfo::torrent_from_bytes(&t.torrent)?.info_hashes,
            None,
            Vec::new(),
        );
        magnet.peers.push(seeder_addr);
        let out = TempDir::with_prefix("test_magnet_x_pe_out")?;
        let downloader =
            Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
        let storage = MemoryStorageFactory::default();
        downloader
            .add_torrent(
                AddTorrent::from_url(magnet.to_string()),
                Some(AddTorrentOptions {
                    storage_factory: Some(storage.clone_box()),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?
            .wait_until_completed()
            .await?;
        assert!(storage.log.file_contents(0) == t.content, "content differs");
        Ok(())
    }

    // An incoming peer's encryption handshake and BitTorrent handshake end
    // at the handshake timeout, whatever the peer timeout (libtorrent
    // `peer_connection::second_tick`).
    #[tokio::test(start_paused = true)]
    async fn an_incoming_peer_that_sends_no_handshake_ends_at_the_handshake_timeout() {
        use crate::{
            mse::EncryptionPolicy,
            peer_connection::{DEFAULT_HANDSHAKE_TIMEOUT, PeerConnectionOptions},
            peer_info_reader::tests::halves,
            stream_connect::ConnectionKind,
        };
        for (encryption, step) in [
            (EncryptionPolicy::Disabled, "error reading handshake"),
            (
                EncryptionPolicy::Require,
                "error in the encryption handshake",
            ),
        ] {
            let dir = TempDir::with_prefix("handshake_timeout").unwrap();
            let session = Session::new_with_opts(
                dir.path().into(),
                SessionOptions {
                    dht: None,
                    persistence: None,
                    disable_trackers: true,
                    disable_local_service_discovery: true,
                    encryption,
                    connect: Some(super::ConnectionOptions {
                        enable_tcp: true,
                        peer_opts: Some(PeerConnectionOptions {
                            read_write_timeout: Some(std::time::Duration::from_secs(120)),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let (ours, _theirs) = tokio::io::duplex(1 << 16);
            let (reader, writer) = halves(ours);
            let start = tokio::time::Instant::now();
            let err = session
                .clone()
                .check_incoming_connection(
                    "127.0.0.1:6881".parse().unwrap(),
                    ConnectionKind::Tcp,
                    reader,
                    writer,
                )
                .await
                .err()
                .unwrap();
            assert!(format!("{err:#}").contains(step), "{err:#}");
            assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT, "{encryption:?}");
            session.stop().await;
        }
    }
}
