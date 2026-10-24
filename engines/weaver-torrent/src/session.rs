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
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
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
    net::{HttpClients, Resolver, SystemResolver},
    spawn_utils::spawn_with_cancel,
    torrent_metainfo::{TorrentMetaV1Owned, ValidatedTorrentMetaV1Info, WebSeed, WebSeedKind},
};
use crate::dht::{
    Dht, DhtBuilder, DhtConfig, DhtPersistenceConfig, Id20, PersistentDht, dht_listen_addr,
};
use crate::lsd::{LocalServiceDiscovery, LocalServiceDiscoveryOptions};
use crate::meter::Meter;
use crate::natpmp::{GatewayRoute, NatPmpMapper};
use crate::peer_protocol::Handshake;
use crate::ssl::{SslHttpClients, SslIdentity, SslTorrent};
use crate::torrent_state::live::{
    choker::{Choker, OPTIMISTIC_UNCHOKE_INTERVAL, UNCHOKE_INTERVAL},
    web_seed::WebSeeds,
};
use crate::tracker::{
    AutoScrape, ListenPort, Reachable, StoppedAnnounces, TrackerAnnounce, TrackerComms,
    TrackerHandover, TrackerStatus, TrackerTask, UdpTrackerClient, Unreachable,
};
use crate::upnp::{
    PortMapEvents, PortMapRequest, PortMapped, PortmapProtocol, PortmapTransport,
    UpnpPortForwarder, UpnpPortForwarderOptions,
};
use crate::{
    ApiError, CreateTorrentOptions, FileInfos, ManagedTorrent, ManagedTorrentShared,
    api::TorrentIdOrHash,
    api_error::WithStatus,
    bitv_factory::{BitVFactory, NonPersistentBitVFactory},
    connection_slots::{
        ConnectionSlots, DEFAULT_CONNECTION_SPEED, DEFAULT_CONNECTIONS_LIMIT,
        DEFAULT_CONNECTIONS_SLACK,
    },
    create_torrent,
    create_torrent_file::CreateTorrentResult,
    dht_utils::{ReadMetainfoResult, read_metainfo_from_peer_receiver},
    ip_ranges::IpRanges,
    limits::{Limits, LimitsConfig},
    listen::{Accept, ListenerOptions},
    merge_streams::merge_streams,
    network::SocketFactory,
    peer_connection::PeerConnectionOptions,
    piece_priority::{
        FilePriority, PiecePriority, file_priorities, file_to_piece_priorities,
        validate_piece_priorities, wanted_files,
    },
    read_buf::ReadBuf,
    session_persistence::{SessionPersistenceStore, json::JsonSessionPersistenceStore},
    session_stats::SessionStats,
    spawn_utils::BlockingSpawner,
    storage::{
        BoxStorageFactory, StorageFactoryExt, TorrentStorage, filesystem::FilesystemStorageFactory,
    },
    stream_connect::{ConnectionKind, ConnectionOptions, StreamConnector, StreamConnectorArgs},
    torrent_state::{
        AddIncomingPeerResult, ManagedTorrentHandle, ManagedTorrentLocked, ManagedTorrentOptions,
        ManagedTorrentState, TorrentMetadata, TorrentStateLive,
        initializing::{Checkpoint, TorrentStateInitializing, normalize_checkpoint},
        live::peers::PeerList,
    },
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite, PeerSource, PeerSourceFlag, PeerStream},
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

pub(crate) mod external_ip;

use external_ip::ExternalIps;

pub const SUPPORTED_SCHEMES: [&str; 3] = ["http:", "https:", "magnet:"];

pub type TorrentId = usize;

// How often the session turns peers over (libtorrent
// `peer_turnover_interval`), and when it first does after it starts
// (`session_impl::m_disconnect_time_scaler`).
const PEER_TURNOVER_INTERVAL: Duration = Duration::from_secs(300);
const FIRST_PEER_TURNOVER: Duration = Duration::from_secs(90);
// The share of a connection cap, in percent, at which peers are turned over
// (libtorrent `peer_turnover_cutoff`).
const PEER_TURNOVER_CUTOFF: usize = 90;

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
    // Rings as a torrent is added: a magnet's torrent enters the session
    // once its metadata is received.
    added: Arc<Notify>,
}

impl SessionDatabase {
    fn add_torrent(&mut self, torrent: ManagedTorrentHandle, id: TorrentId) {
        self.torrents.insert(id, torrent);
        self.added.notify_waiters();
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
            let mut peers = dht.get_peers(info_hash, *port.borrow_and_update());
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
    // The port SSL torrents announce: the SSL listener's, while the session
    // announces a port at all.
    ssl_port: tokio::sync::watch::Sender<Option<u16>>,
    ssl_listen_addr: Option<SocketAddr>,
    // Our address as the internet sees it, one for each family.
    pub(crate) external_ips: Arc<ExternalIps>,
    dht: Option<Dht>,
    pub(crate) connector: Arc<StreamConnector>,
    pub(crate) reqwest_client: reqwest::Client,
    // Builds the clients web seeds are asked through, each held to the
    // address its request was checked at.
    pub(crate) http_clients: Arc<dyn HttpClients>,
    // Resolves web seeds' host names, so their addresses are known before
    // they are asked: the socket factory's resolver, or the host's.
    pub(crate) resolver: Arc<dyn Resolver>,
    udp_tracker_client: UdpTrackerClient,
    disable_trackers: bool,
    stopped_announces: StoppedAnnounces,
    auto_scrape: AutoScrape,

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

    lsd: Option<LocalServiceDiscovery>,

    // Limits and throttling
    pub(crate) concurrent_initialize_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) connection_slots: Arc<ConnectionSlots>,
    pub(crate) allow_multiple_connections_per_ip: bool,
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

// Each file's level at the start: those given, one a file; or Normal for the
// files `only_files` names and Ignored for the rest; or every file Normal.
// Padding is Ignored whatever is given.
fn compute_file_priorities(
    file_infos: &FileInfos,
    given: Option<Vec<FilePriority>>,
    only_files: Option<&[usize]>,
) -> anyhow::Result<Vec<FilePriority>> {
    let levels = match (given, only_files) {
        (Some(_), Some(_)) => {
            bail!("file_priorities and only_files are mutually exclusive");
        }
        (Some(given), None) => {
            if given.len() != file_infos.len() {
                bail!(
                    "file_priorities has {} levels, but the torrent has {} files",
                    given.len(),
                    file_infos.len()
                );
            }
            given
        }
        (None, Some(only_files)) => (0..file_infos.len())
            .map(|index| {
                if only_files.contains(&index) {
                    FilePriority::Normal
                } else {
                    FilePriority::Ignored
                }
            })
            .collect(),
        (None, None) => vec![FilePriority::Normal; file_infos.len()],
    };
    Ok(file_priorities(file_infos, &levels))
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
    /// An explicit list of file IDs to download: they are Normal, every
    /// other file Ignored. To see the file indices, run with "list_only".
    pub only_files: Option<Vec<usize>>,
    /// Each file's level, one a file by index (qBittorrent's four; see
    /// [`FilePriority`] and [`ManagedTorrent::set_file_priorities`]). None
    /// is every file Normal. Exclusive with `only_files` and
    /// `only_files_regex`; a list of the wrong length is refused.
    pub file_priorities: Option<Vec<FilePriority>>,
    /// Pick the top-level pieces first, then every other wanted piece by
    /// index (see [`ManagedTorrent::set_sequential`]).
    #[serde(default)]
    pub sequential: bool,
    /// Put the first and last pieces of each wanted file at the top level
    /// (see [`ManagedTorrent::set_first_last_piece_first`]).
    #[serde(default)]
    pub first_last_piece_first: bool,
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

    /// Addresses banned from the start, as a restart saved them (libtorrent
    /// `add_torrent_params::banned_peers`): no peer at one joins the peer
    /// list. See [`ManagedTorrent::resume_peers`].
    #[serde(default)]
    pub banned_peers: Vec<IpAddr>,

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

    /// The torrent's trackers, in tiers, in place of those its metainfo or
    /// magnet link names (libtorrent `add_torrent_params::trackers` and
    /// `tracker_tiers` from resume data, which replace the .torrent's).
    /// [`Self::trackers`] still follow them.
    pub tracker_tiers: Option<Vec<Vec<String>>>,

    /// What the storage already holds, as a checkpoint of an earlier run
    /// recorded it (see [`Checkpoint`]). When set, the initial hash check
    /// does not run: its pieces are taken as have and its blocks as
    /// written, every other piece and block as missing, and none is read
    /// back from the storage but a piece every block of which was written
    /// and that is not had, whose hash is checked. It also takes precedence
    /// over any fastresume bitfield. A bitfield of the wrong length is
    /// refused.
    #[serde(skip)]
    pub checkpoint: Option<Checkpoint>,

    /// The initial fetch priority of the given pieces, by piece index, over
    /// what the files' levels give them (libtorrent applies
    /// `add_torrent_params::piece_priorities` after the file priorities).
    /// See [`PiecePriority`] and [`ManagedTorrent::set_piece_priorities`].
    /// An index out of range is refused.
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
    /// Take peers from the torrent's trackers alone, as a private torrent
    /// does (BEP 27) whatever its metainfo says: no DHT, local discovery or
    /// peer exchange, and none of the session's trackers beside its own.
    /// Off by default; a private torrent is held to it either way.
    #[serde(default)]
    pub trackers_only: bool,
    /// Our identity under an SSL torrent's CA, presented to its peers and
    /// its HTTPS trackers (see [`Session::set_ssl_identity`]). An SSL
    /// torrent added without one connects to no peer and announces nowhere
    /// until one is set. Not read for any other torrent.
    #[serde(skip)]
    pub ssl_identity: Option<SslIdentity>,
    /// For a magnet: receives its metadata fetch as the fetch starts, so the
    /// trackers' answers and where it asks for peers can be read while the
    /// metadata is fetched. Not sent when the metadata is given.
    #[serde(skip)]
    pub metadata_fetch: Option<tokio::sync::oneshot::Sender<MetadataFetch>>,
    /// The tracker task a metadata fetch of this torrent on this session
    /// announced from (see [`ListOnlyResponse::trackers`]): a run started
    /// by this add takes it over, so its trackers hear one torrent, and
    /// should go by the [`Self::identity`] the fetch announced under. A
    /// paused add, or one that fails, drops it, which sends `stopped`.
    #[serde(skip)]
    pub tracker_handover: Option<TrackerHandover>,
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
            file_priorities: None,
            sequential: false,
            first_last_piece_first: false,
            overwrite: false,
            list_only: false,
            output_folder: None,
            sub_folder: None,
            peer_opts: None,
            force_tracker_interval: None,
            disable_trackers: false,
            ratelimits: LimitsConfig::default(),
            initial_peers: None,
            banned_peers: Vec::new(),
            web_seeds: Vec::new(),
            peer_limit: None,
            preferred_id: None,
            storage_factory: None,
            trackers: None,
            tracker_tiers: None,
            checkpoint: None,
            piece_priorities: None,
            identity: None,
            disable_v1_hashes: v1_hashes_disabled_by_default(),
            super_seeding: false,
            trackers_only: false,
            ssl_identity: None,
            metadata_fetch: None,
            tracker_handover: None,
        }
    }
}

pub struct ListOnlyResponse {
    pub info_hash: Id20,
    pub info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
    /// Each file's level, as the options give it.
    pub file_priorities: Vec<FilePriority>,
    pub sequential: bool,
    pub first_last_piece_first: bool,
    pub output_folder: PathBuf,
    pub seen_peers: Vec<SocketAddr>,
    pub torrent_bytes: Bytes,
    /// The tracker task the metadata fetch announced from, held for the run
    /// that follows on this session (see
    /// [`AddTorrentOptions::tracker_handover`]). Dropping it first sends
    /// `stopped`. None when the metadata was given or trackers are off.
    pub trackers: Option<TrackerHandover>,
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

/// The limits a running session takes as they change (libtorrent
/// `session_impl::apply_settings` with `update_connections_limit`,
/// `update_unchoke_limit` and `update_connection_speed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionLimits {
    /// [`SessionOptions::connections_limit`].
    pub connections_limit: usize,
    /// [`SessionOptions::unchoke_slots_limit`].
    pub unchoke_slots_limit: usize,
    /// [`SessionOptions::connection_speed`].
    pub connection_speed: NonZeroU32,
}

/// What the session refuses to reach, taken as it changes (libtorrent
/// `session_impl::set_ip_filter`, with `no_connect_privileged_ports` and
/// `apply_ip_filter_to_trackers`).
#[derive(Clone)]
pub struct IpFilter {
    /// The address ranges no peer is taken from or dialed at, and no web
    /// seed is asked at (libtorrent's `ip_filter`).
    pub blocked: Arc<IpRanges>,
    /// No peer on a port below 1024 is dialed or kept (libtorrent
    /// `no_connect_privileged_ports`). Off by default.
    pub no_privileged_ports: bool,
    /// Trackers are held to `blocked` too (libtorrent
    /// `apply_ip_filter_to_trackers`). On by default.
    pub trackers: bool,
}

impl Default for IpFilter {
    fn default() -> Self {
        Self {
            blocked: Arc::default(),
            no_privileged_ports: false,
            trackers: true,
        }
    }
}

impl std::fmt::Debug for IpFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpFilter")
            .field("blocked", &self.blocked.len())
            .field("no_privileged_ports", &self.no_privileged_ports)
            .field("trackers", &self.trackers)
            .finish()
    }
}

// One list is one filter: two filters are the same when they share it.
impl PartialEq for IpFilter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.blocked, &other.blocked)
            && self.no_privileged_ports == other.no_privileged_ports
            && self.trackers == other.trackers
    }
}

impl IpFilter {
    /// Whether a peer on `port` is refused.
    fn refuses_port(&self, port: u16) -> bool {
        self.no_privileged_ports && port < 1024
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

    /// The IP filter the session opens with, its ranges loaded by whoever
    /// builds the session, so that one list serves every session that
    /// shares it; [`Session::set_ip_filter`] changes it.
    pub ip_filter: IpFilter,
    pub allowlist_url: Option<String>,

    // The list of tracker URLs to always use for each torrent.
    pub trackers: HashSet<url::Url>,

    /// Default peer limit per torrent.
    pub peer_limit: Option<usize>,

    /// Peers the session uploads to at once, over all its torrents, one in
    /// five of them and at least one on optimistic slots (libtorrent
    /// `unchoke_slots_limit`). Defaults to 8.
    pub unchoke_slots_limit: usize,

    /// Peer connections, incoming and outgoing, the session holds open at
    /// once across all its torrents. Outgoing connects wait for a free place;
    /// an incoming connection past it takes one in the slack. Defaults to
    /// 200.
    pub connections_limit: usize,

    /// Incoming connections past `connections_limit` the session takes, each
    /// to replace a connection of the torrent that holds the most when that
    /// is more than its own torrent holds; one past both is closed (libtorrent
    /// `connections_slack`). Defaults to 10.
    pub connections_slack: usize,

    /// Outgoing connects the session begins per second across all its
    /// torrents, handed to the torrents that want peers in turn. Defaults
    /// to 30.
    pub connection_speed: NonZeroU32,

    /// Disable LSD multicast
    pub disable_local_service_discovery: bool,

    /// A test's LSD socket in place of the multicast one.
    #[cfg(test)]
    pub(crate) lsd_socket: Option<Box<dyn crate::lsd::Socket>>,

    /// Leave peer exchange (ut_pex) out: it is not advertised to peers, and
    /// none is sent or read, as in libtorrent without the ut_pex plugin.
    pub disable_pex: bool,

    /// Force IPv4 only.
    pub ipv4_only: bool,

    /// Create peer sockets with this factory instead of binding kernel
    /// sockets. See [`SocketFactory`] for what it covers.
    pub socket_factory: Option<Arc<dyn SocketFactory>>,

    /// Build the HTTP clients with this instead of the session's own builder.
    /// Its client for no target downloads torrents and the block and allow
    /// lists; a tracker or a web seed is asked through a client for the
    /// addresses the session checked its host at. When set,
    /// `bind_device_name` and the identity's User-Agent do not apply to HTTP
    /// requests; configure them on the clients.
    ///
    /// The client for no target does its own DNS lookups:
    /// [`SocketFactory::resolver`] does not apply to it. If those lookups must be routed too, build it
    /// with its own resolver (`reqwest::ClientBuilder::dns_resolver`).
    pub http_client: Option<Arc<dyn HttpClients>>,

    /// Message stream encryption for peer connections, outgoing and incoming.
    /// Defaults to [`EncryptionPolicy::Disabled`](crate::EncryptionPolicy::Disabled).
    pub encryption: crate::mse::EncryptionPolicy,

    /// Whether peers at one address may each hold a connection to a torrent
    /// (libtorrent `allow_multiple_connections_per_ip`). Off by default: one
    /// connection per address.
    pub allow_multiple_connections_per_ip: bool,

    /// Where the session also sends each report its port mappers make, as
    /// they make it.
    pub port_map_events: Option<PortMapEvents>,
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
            ip_filter: IpFilter::default(),
            allowlist_url: None,
            trackers: HashSet::new(),
            peer_limit: None,
            unchoke_slots_limit: crate::torrent_state::live::choker::DEFAULT_UNCHOKE_SLOTS_LIMIT,
            connections_limit: DEFAULT_CONNECTIONS_LIMIT,
            connections_slack: DEFAULT_CONNECTIONS_SLACK,
            connection_speed: DEFAULT_CONNECTION_SPEED,
            disable_local_service_discovery: false,
            #[cfg(test)]
            lsd_socket: None,
            disable_pex: false,
            ipv4_only: false,
            socket_factory: None,
            http_client: None,
            encryption: crate::mse::EncryptionPolicy::Disabled,
            allow_multiple_connections_per_ip: false,
            port_map_events: None,
        }
    }
}

// Where a torrent stands when a connection past the session's cap must
// replace one of another torrent's; the lowest gives one up: a torrent with
// connections, then a finished one, so the downloading ones keep theirs,
// then the most connections (libtorrent `compare_disconnect_torrent`).
fn disconnect_candidate_rank(torrent: &TorrentStateLive) -> (bool, bool, std::cmp::Reverse<usize>) {
    let (held, _) = torrent.connections();
    (held == 0, !torrent.is_finished(), std::cmp::Reverse(held))
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
    /// How the peer's handshake went (libtorrent
    /// `bt_peer_connection::m_encrypted`, `m_rc4_encrypted`).
    pub encryption: crate::mse::Encryption,
    /// What has crossed the socket so far, the handshakes included.
    pub meter: Meter,
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

/// The tiers the caller gave in place of the torrent's own.
fn own_tracker_tiers(opts: &AddTorrentOptions) -> Option<Vec<Vec<url::Url>>> {
    opts.tracker_tiers.as_ref().map(|tiers| {
        tiers
            .iter()
            .map(|tier| {
                tier.iter()
                    .filter_map(|t| url::Url::parse(t).ok())
                    .collect()
            })
            .collect()
    })
}

/// Whether a torrent takes peers from its trackers alone: its metainfo
/// marks it `private` (BEP 27), or it was added
/// [`trackers_only`](AddTorrentOptions::trackers_only). Such a torrent
/// neither looks for peers on the DHT, the local network or by peer
/// exchange, nor announces to any tracker but its own.
pub(crate) const fn trackers_only(private: bool, added_trackers_only: bool) -> bool {
    private || added_trackers_only
}

/// Where a magnet's metadata fetch asks for peers. Until its metadata says
/// whether it is private, it asks its own trackers alone, and goes on to
/// the DHT and local discovery only once they named no peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchPhase {
    /// Its own trackers, until one names a peer, one answers, or every one
    /// fails.
    Trackers,
    /// Its trackers, the DHT and local discovery: no tracker named a peer.
    Discovery,
    /// No tracker named a peer, and there is no discovery to go on to.
    Waiting,
}

impl FetchPhase {
    /// The phase of a fetch whose trackers stand at `table`, with discovery
    /// to go on to or not. The trackers are done without a peer once one
    /// answered and none named a peer, or once every one failed; a fetch
    /// with no tracker is done at once.
    fn of(table: &[TrackerAnnounce], discovery: bool) -> Self {
        let results = || table.iter().map(|t| t.last.as_ref().map(|l| &l.result));
        let named_peer = results().any(|r| matches!(r, Some(Ok(ok)) if ok.peers > 0));
        let done = results().any(|r| matches!(r, Some(Ok(_))))
            || results().all(|r| matches!(r, Some(Err(_))));
        match (done && !named_peer, discovery) {
            (false, _) => Self::Trackers,
            (true, true) => Self::Discovery,
            (true, false) => Self::Waiting,
        }
    }
}

impl std::fmt::Display for FetchPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trackers => "trackers",
            Self::Discovery => "trackers, DHT, LSD",
            Self::Waiting => "no peers from trackers",
        })
    }
}

/// A magnet's metadata fetch, while it runs.
#[derive(Clone, Debug)]
pub struct MetadataFetch {
    /// The announce table it announces from, unless trackers are off.
    pub trackers: Option<TrackerStatus>,
    /// Where it asks for peers now.
    pub phase: tokio::sync::watch::Receiver<FetchPhase>,
}

/// Follows a fetch's announce table into `phase`: returns once the fetch
/// goes on to discovery, and never while it does not.
async fn follow_fetch(
    mut table: Option<tokio::sync::watch::Receiver<Vec<TrackerAnnounce>>>,
    discovery: bool,
    phase: tokio::sync::watch::Sender<FetchPhase>,
) {
    loop {
        let now = match &mut table {
            Some(table) => FetchPhase::of(&table.borrow_and_update(), discovery),
            None => FetchPhase::of(&[], discovery),
        };
        phase.send_if_modified(|was| std::mem::replace(was, now) != now);
        if now == FetchPhase::Discovery {
            return;
        }
        let moved = match &mut table {
            Some(table) => table.changed().await.is_ok(),
            None => false,
        };
        if !moved {
            std::future::pending::<()>().await;
        }
    }
}

/// The tiers a torrent announces to: its own tiers in order, then each of
/// `extra` as a tier of its own, unless the torrent takes peers from its
/// own trackers only. A tracker listed more than once keeps its first
/// place, and empty tiers are dropped.
fn merge_tracker_tiers(
    own: Vec<Vec<url::Url>>,
    extra: impl IntoIterator<Item = url::Url>,
    trackers_only: bool,
) -> Vec<Vec<url::Url>> {
    let extra = (!trackers_only).then(|| extra.into_iter().map(|t| vec![t]));
    crate::tracker::tidy_tiers(own.into_iter().chain(extra.into_iter().flatten()))
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
            let swarm = token.child_token();
            let swarm_tasks = TaskTracker::new();
            // Every name the session looks up, on whichever socket.
            let resolver: Arc<dyn Resolver> = opts
                .socket_factory
                .as_ref()
                .and_then(|factory| factory.resolver())
                .unwrap_or_else(|| Arc::new(SystemResolver));

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
                            swarm.child_token(),
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
                let dht = if let Some(persistence_config) = dht_config.persistence {
                    PersistentDht::create_from_config(
                        persistence_config,
                        dht_config.port,
                        opts.ipv4_only,
                        DhtConfig {
                            bootstrap_addrs: dht_config.bootstrap_addrs,
                            cancellation_token: Some(swarm.clone()),
                            bind_device: bind_device.as_ref(),
                            socket: dht_socket,
                            resolver: Some(resolver.clone()),
                            node_id_prefix: dht_node_id_prefix,
                            client_version: dht_client_version,
                            tasks: Some(swarm_tasks.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                    .context("error initializing persistent DHT")?
                } else {
                    let listen_addr = dht_listen_addr(dht_config.port, None, opts.ipv4_only);
                    DhtBuilder::with_config(DhtConfig {
                        bootstrap_addrs: dht_config.bootstrap_addrs,
                        cancellation_token: Some(swarm.child_token()),
                        bind_device: bind_device.as_ref(),
                        listen_addr: Some(listen_addr),
                        socket: dht_socket,
                        resolver: Some(resolver.clone()),
                        node_id_prefix: dht_node_id_prefix,
                        client_version: dht_client_version,
                        tasks: Some(swarm_tasks.clone()),
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

            let http_clients: Arc<dyn HttpClients> = match opts.http_client.take() {
                Some(clients) => clients,
                None => {
                    let user_agent = identity.user_agent.clone();
                    let bind_device = opts.bind_device_name.clone();
                    Arc::new(move || {
                        #[allow(unused_mut)]
                        let mut builder = reqwest::Client::builder().user_agent(&user_agent);
                        #[cfg(not(windows))]
                        if let Some(bd) = bind_device.as_ref() {
                            builder = builder.interface(bd);
                        }
                        builder
                    })
                }
            };
            let reqwest_client = http_clients
                .client(None)
                .context("error building HTTP(S) client")?;

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
                    UdpTrackerClient::new_with_socket(token.clone(), socket, Some(resolver.clone()))
                }
                None => {
                    UdpTrackerClient::new(token.clone(), bind_device.as_ref(), resolver.clone())
                        .await
                        .context("error creating UDP tracker client")?
                }
            };

            let lsd = {
                if opts.disable_local_service_discovery {
                    None
                } else {
                    LocalServiceDiscovery::new(LocalServiceDiscoveryOptions {
                        cancel_token: swarm.clone(),
                        bind_device: bind_device.as_ref(),
                        #[cfg(test)]
                        socket: opts.lsd_socket.take(),
                        ..Default::default()
                    })
                    .await
                    .inspect_err(|e| warn!("error starting local service discovery: {e:#}"))
                    .ok()
                }
            };

            let external_ips = Arc::new(ExternalIps::new(tokio::time::Instant::now()));
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
                swarm,
                swarm_tasks,
                announce_port: Arc::new(AnnouncePort::new(
                    listen_result.as_ref().and_then(|l| l.announce_port),
                )),
                listen_addr: listen_result.as_ref().map(|l| l.addr),
                ssl_port: tokio::sync::watch::channel(
                    listen_result
                        .as_ref()
                        .and_then(|l| l.ssl.as_ref().zip(l.announce_port))
                        .map(|(ssl, _)| ssl.addr.port()),
                )
                .0,
                ssl_listen_addr: listen_result
                    .as_ref()
                    .and_then(|l| l.ssl.as_ref())
                    .map(|ssl| ssl.addr),
                external_ips,
                utp: listen_result
                    .as_ref()
                    .is_some_and(|l| l.utp_socket.is_some()),
                default_storage_factory: opts.default_storage_factory,
                reqwest_client,
                http_clients,
                resolver,
                connector: stream_connector,
                root_span: opts.root_span,
                stats: Arc::new(SessionStats::new()),
                concurrent_initialize_semaphore: Arc::new(tokio::sync::Semaphore::new(
                    opts.concurrent_init_limit.unwrap_or(3),
                )),
                udp_tracker_client,
                connection_slots: Arc::new(ConnectionSlots::new(
                    opts.connections_limit,
                    opts.connections_slack,
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
                choker: Arc::new(Choker::new(opts.unchoke_slots_limit)),
                disable_pex: opts.disable_pex,
                allow_multiple_connections_per_ip: opts.allow_multiple_connections_per_ip,
                ip_filter: RwLock::new(opts.ip_filter),
                allowlist,
                lsd,
            });

            if let Some(mut listen) = listen_result {
                let protocols = listening_protocols(&listen);
                let max = listen.max_pending_incoming_handshake_checks;
                let plain = crate::listen::Listening {
                    tcp_socket: listen.tcp_socket.take(),
                    custom_tcp_socket: listen.custom_tcp_socket.take(),
                    utp_socket: listen.utp_socket.take(),
                    addr: listen.addr,
                };
                session.spawn_listeners(plain, false, max);
                if let Some(ssl) = listen.ssl.take() {
                    session.spawn_listeners(ssl, true, max);
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
                        opts.bind_device_name.as_deref(),
                        opts.port_map_events.take(),
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
            session.start_peer_turnover();
            session.start_choker();

            Ok(session)
        }
        .boxed()
    }

    async fn check_incoming_connection(
        self: Arc<Self>,
        addr: SocketAddr,
        kind: ConnectionKind,
        tls: bool,
        reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> anyhow::Result<(Arc<TorrentStateLive>, CheckedIncomingConnection)> {
        let handshake_timeout = self.peer_opts.handshake_timeout();

        // The session's one address rule, whichever direction the connection
        // goes (libtorrent `session_impl::incoming_connection` refuses a
        // filtered endpoint with `peer_blocked_alert::ip_filter`; it never
        // sees one of a family it has no listen socket for).
        if let Err(why) = self.may_reach(addr.ip()) {
            if why == Unreachable::Filtered {
                self.stats
                    .counters
                    .blocked_incoming
                    .fetch_add(1, Ordering::Relaxed);
            }
            bail!("incoming connection refused: {} is {why}", addr.ip());
        }
        // No peer on a privileged port is kept, whichever way it connects
        // (libtorrent `torrent::privileged_port_updated` disconnects one).
        if self.refuses_port(addr.port()) {
            bail!("incoming connection refused: {addr} is on a privileged port");
        }

        // Every byte from here on crossed the peer's socket for the torrent
        // it greets, and is charged to it once the connection is its own.
        let (meter, mut reader, mut writer) = Meter::wrap(reader, writer);

        // A connection on the SSL listener is TLS from its first byte, under
        // the torrent its SNI names: that torrent's CA checks the peer's
        // chain and our identity under it answers. No encryption handshake
        // runs inside it.
        let (encryption, named) = if tls {
            let (r, w, named) = tokio::time::timeout(handshake_timeout, async {
                let incoming = crate::ssl::Incoming::start(reader, writer)
                    .await
                    .context("not a TLS connection")?;
                let sni = incoming
                    .server_name()
                    .context("the TLS connection names no torrent")?;
                let (id, config) = self
                    .db
                    .read()
                    .torrents
                    .iter()
                    .find_map(|(id, t)| {
                        let ssl = t.shared().ssl.as_ref()?;
                        ssl.answers_to(&sni).then(|| (*id, ssl.server_config()))
                    })
                    .with_context(|| format!("no SSL torrent is named {sni}"))?;
                let config =
                    config.with_context(|| format!("torrent {id} has no identity to present"))?;
                let (r, w) = incoming.accept(config).await.context("TLS handshake")?;
                anyhow::Ok((r, w, id))
            })
            .await
            .context("TLS handshake timed out")??;
            reader = r;
            writer = w;
            (crate::mse::Encryption::None, Some(named))
        } else {
            // With encryption enabled, an encrypted connection is unwrapped here
            // and a plaintext one passed through (or refused); with it disabled
            // the halves are left as they are. A method is settled only when the
            // encryption handshake ran.
            // Each connection is kept under its torrent's encryption: an
            // encrypted one names its torrent in the encryption handshake, a
            // plaintext one only in its BitTorrent handshake.
            let session_policy = self.connector.encryption();
            let policies: Vec<(Id20, crate::mse::EncryptionPolicy)> = self
                .db
                .read()
                .torrents
                .values()
                .flat_map(|t| {
                    let policy = t
                        .shared()
                        .options
                        .peer_opts
                        .encryption_under(session_policy);
                    t.info_hashes().ids20().map(move |hash| (hash, policy))
                })
                .collect();
            let encryption = crate::mse::upgrade_incoming(
                session_policy,
                &mut reader,
                &mut writer,
                handshake_timeout,
                &policies,
            )
            .await
            .context("error in the encryption handshake")?;
            (encryption, None)
        };

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
        // An SSL torrent takes a connection only through TLS under its own
        // CA, and a TLS connection only greets the torrent it named.
        match named {
            Some(named) if named != id => {
                bail!("the TLS connection named torrent {named} and greets torrent {id}")
            }
            Some(_) => {}
            None if torrent.is_ssl() => {
                bail!("torrent {id} is an SSL torrent and takes no plain connection")
            }
            None => {
                let policy = torrent
                    .shared()
                    .options
                    .peer_opts
                    .encryption_under(self.connector.encryption());
                if !policy.allows(encryption) {
                    bail!("torrent {id} keeps no {encryption:?} connection under {policy:?}");
                }
            }
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
                encryption,
                meter,
            },
        ))
    }

    /// Accepts on a listener's sockets; `tls` says it is the SSL listener.
    fn spawn_listeners(
        self: &Arc<Self>,
        listening: crate::listen::Listening,
        tls: bool,
        max_pending_incoming_handshake_checks: usize,
    ) {
        let addr = listening.addr;
        if let Some(tcp) = listening.tcp_socket {
            let this = self.clone();
            self.spawn_on_swarm(
                debug_span!(parent: self.rs(), "tcp_listen", ?addr, tls),
                "tcp_listen",
                async move {
                    this.task_listener(tcp, tls, max_pending_incoming_handshake_checks)
                        .await
                },
            );
        }
        if let Some(tcp) = listening.custom_tcp_socket {
            let this = self.clone();
            self.spawn_on_swarm(
                debug_span!(parent: self.rs(), "tcp_listen", ?addr, tls),
                "tcp_listen",
                async move {
                    this.task_listener(tcp, tls, max_pending_incoming_handshake_checks)
                        .await
                },
            );
        }
        if let Some(utp) = listening.utp_socket {
            let this = self.clone();
            self.spawn_on_swarm(
                debug_span!(parent: self.rs(), "utp_listen", ?addr, tls),
                "utp_listen",
                async move {
                    this.task_listener(utp, tls, max_pending_incoming_handshake_checks)
                        .await
                },
            );
        }
    }

    async fn task_listener<A: Accept>(
        self: Arc<Self>,
        l: A,
        tls: bool,
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
                            // Dropping the halves closes a connection past the
                            // session's cap and its slack.
                            let Some(slot) = session.connection_slots.try_accept() else {
                                debug!(%addr, "at the session's connection limit, closing incoming connection");
                                continue;
                            };
                            let span = debug_span!(parent: session.rs(), "incoming", addr=%addr);
                            futs.push(
                                session.check_incoming_connection(addr, A::KIND, tls, Box::new(read), Box::new(write))
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
                    match live.add_incoming_peer(checked, slot) {
                        Ok(AddIncomingPeerResult::Added) => {
                            if let Some(session) = session.upgrade() {
                                session.settle_over_limit(&live, addr);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(?addr, ?kind, "error handing over incoming connection: {e:#}");
                        }
                    }
                },
                else => continue,
            }
        }
    }

    /// Keep the listen port mapped on the gateway over UPnP and over PCP /
    /// NAT-PMP at once (libtorrent `session_impl::start_upnp` and
    /// `start_natpmp`), announce the TCP port a mapper got, count the
    /// address the gateway gave as the router's vote (`on_port_mapping`),
    /// and pass each report on to `forward`. Both mappers work on the
    /// interface the session's traffic leaves through: the bound device, or
    /// the one the host routes public traffic to.
    fn start_port_mapping(
        &self,
        requests: Vec<PortMapRequest>,
        bind_device_name: Option<&str>,
        forward: Option<PortMapEvents>,
    ) {
        let interface =
            crate::route_interface::route_interface(bind_device_name).map_err(|e| format!("{e:#}"));
        info!(
            ?requests,
            ?interface,
            "starting UPnP and NAT-PMP / PCP port mapping"
        );
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let upnp_opts = UpnpPortForwarderOptions {
            description: self.identity.user_agent.clone(),
            ..Default::default()
        };
        match UpnpPortForwarder::new(requests.clone(), Some(upnp_opts), interface.clone()) {
            Ok(upnp) => self.spawn_port_mapper(
                "upnp_forward",
                upnp.run(self.cancellation_token.clone(), events_tx.clone()),
            ),
            Err(e) => warn!("error starting UPnP port mapping: {e:#}"),
        }
        let route = interface.and_then(|i| GatewayRoute::of(&i).map_err(|e| format!("{e:#}")));
        self.spawn_port_mapper(
            "natpmp_forward",
            NatPmpMapper::new(route, requests, None)
                .run(self.cancellation_token.clone(), events_tx),
        );
        let announce_port = self.announce_port.clone();
        let external_ips = self.external_ips.clone();
        self.spawn_port_mapper("port_mapping", async move {
            while let Some(event) = events.recv().await {
                match &event.error {
                    None => info!(?event, "port mapping"),
                    Some(error) => {
                        warn!(?event.transport, ?event.protocol, "port mapping failed: {error}")
                    }
                }
                announce_port.on_port_mapping(&event);
                external_ips.on_port_mapping(&event, tokio::time::Instant::now());
                if let Some(forward) = &forward {
                    let _ = forward.send(event);
                }
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
        self.swarm.cancel();
        self.swarm_tasks.close();
        self.swarm_tasks.wait().await;
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
            timeout = ?stopped.timeout(),
            "stopping the session: waiting on its trackers' stopped announces"
        );
        if tokio::time::timeout(stopped.timeout(), stopped.wait_idle())
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

    fn start_peer_turnover(self: &Arc<Self>) {
        let session = Arc::downgrade(self);
        self.spawn(
            debug_span!(parent: self.rs(), "peer_turnover"),
            "peer_turnover",
            async move {
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + FIRST_PEER_TURNOVER,
                    PEER_TURNOVER_INTERVAL,
                );
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    session
                        .upgrade()
                        .context("session is dead")?
                        .turn_peers_over();
                }
            },
        )
    }

    // Hands the upload slots out every UNCHOKE_INTERVAL and passes the
    // optimistic ones on every OPTIMISTIC_UNCHOKE_INTERVAL, each at once when
    // the choker asks for it sooner, over every live torrent's peers at once
    // (libtorrent `session_impl::second_tick`). When both fall due the
    // optimistic slots pass first, so the round sees their new holders.
    fn start_choker(self: &Arc<Self>) {
        let session = Arc::downgrade(self);
        let choker = self.choker.clone();
        self.spawn(
            debug_span!(parent: self.rs(), "choker"),
            "choker",
            async move {
                let first = tokio::time::Instant::now() + UNCHOKE_INTERVAL;
                let mut rounds = tokio::time::interval_at(first, UNCHOKE_INTERVAL);
                let mut rotations = tokio::time::interval_at(first, OPTIMISTIC_UNCHOKE_INTERVAL);
                loop {
                    let rotate = tokio::select! {
                        biased;
                        _ = rotations.tick() => true,
                        _ = choker.rotation_requested() => {
                            rotations.reset();
                            true
                        }
                        _ = choker.reshuffle_requested() => {
                            rounds.reset();
                            false
                        }
                        _ = rounds.tick() => false,
                    };
                    let torrents = session
                        .upgrade()
                        .context("session is dead")?
                        .live_torrents();
                    let now = tokio::time::Instant::now();
                    if rotate {
                        choker.rotate_optimistic(&torrents, now);
                    } else {
                        choker.recalculate(&torrents, now);
                    }
                }
            },
        )
    }

    // The peer at `addr` of `torrent` said it is interested: it takes one of
    // the session's upload slots when one is free.
    pub(crate) fn peer_interested(&self, torrent: &TorrentStateLive, addr: SocketAddr) {
        self.choker.peer_interested(torrent, addr);
    }

    // The torrents that are live, so whose peers the session uploads to and
    // turns over.
    fn live_torrents(&self) -> Vec<Arc<TorrentStateLive>> {
        self.with_torrents(|torrents| torrents.filter_map(|(_, t)| t.live()).collect())
    }

    // Makes room for connect candidates when connections run short
    // (libtorrent `session_impl::second_tick`). When the session holds
    // PEER_TURNOVER_CUTOFF percent of its cap, the torrent with the most
    // connections turns some over; otherwise each torrent holding that share
    // of its own cap does. Under a cap of 6 it would be too disruptive, and
    // nothing is turned over.
    pub(crate) fn turn_peers_over(&self) {
        let limit = self.connection_slots.limit();
        if limit < 6 {
            return;
        }
        let torrents = self.live_torrents();
        if self.connection_slots.in_use() >= limit * PEER_TURNOVER_CUTOFF / 100 {
            if let Some(most) = torrents.iter().max_by_key(|t| t.connections().0) {
                most.turn_peers_over();
            }
            return;
        }
        for torrent in &torrents {
            let (held, max) = torrent.connections();
            if max >= 6 && held >= max * PEER_TURNOVER_CUTOFF / 100 {
                torrent.turn_peers_over();
            }
        }
    }

    // An incoming connection at `addr` of `live` that the slack let in past
    // the session's cap takes a place under it: a free one, or that of the
    // lowest-ranked connection of the torrent that holds the most, when it
    // holds more than `live` does; else it is closed (libtorrent
    // `session_impl::incoming_connection` marks it
    // `peer_connection::peer_exceeds_limit`, and attaching it settles that:
    // `peer_connection::attach_to_torrent`, `find_disconnect_candidate_torrent`,
    // `torrent::find_lowest_ranking_peer`). One that displaced a connection
    // of its own torrent took that one's place, and is under the cap.
    pub(crate) fn settle_over_limit(&self, live: &TorrentStateLive, addr: SocketAddr) {
        if !live.exceeds_limit(addr, &self.connection_slots) {
            return;
        }
        let slot = self.connection_slots.try_open().or_else(|| {
            let fullest = self
                .live_torrents()
                .into_iter()
                .min_by_key(|t| disconnect_candidate_rank(t))?;
            (fullest.connections().0 > live.connections().0)
                .then(|| fullest.give_up_lowest())
                .flatten()
        });
        live.settle_slot(addr, slot);
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
                        // the files
                        if opts.only_files.is_none() && opts.file_priorities.is_none() {
                            opts.only_files = Some(so);
                        }
                    }

                    InternalAddResult {
                        info_hashes: magnet.info_hashes(),
                        // Each tracker of a magnet link is a tier of its own.
                        tracker_tiers: merge_tracker_tiers(
                            own_tracker_tiers(&opts).unwrap_or_else(|| {
                                magnet
                                    .trackers
                                    .into_iter()
                                    .filter_map(|t| url::Url::parse(&t).ok())
                                    .map(|t| vec![t])
                                    .collect()
                            }),
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
                        own_tracker_tiers(&opts)
                            .unwrap_or_else(|| metainfo_tracker_tiers(&torrent.meta)),
                        opts.trackers
                            .iter()
                            .flatten()
                            .filter_map(|t| url::Url::parse(t).ok()),
                        trackers_only(torrent.meta.info.data.private, opts.trackers_only),
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
        let dht_announce = tokio::sync::watch::channel(()).0;

        let make_peer_rx = |info_hashes: InfoHashes,
                            identity: &Identity,
                            private: bool,
                            trackers: Option<TrackerTask>| {
            self.make_peer_rx(
                info_hashes,
                identity,
                tracker_tiers.clone(),
                !opts.paused && !opts.list_only,
                opts.force_tracker_interval,
                opts.initial_peers.clone().unwrap_or_default(),
                trackers_only(private, opts.trackers_only),
                dht_announce.subscribe(),
                None,
                trackers,
            )
        };

        let mut seen_peers = Vec::new();

        // The torrent's one peer list, from its first metadata connection to
        // its last connection (libtorrent `torrent::m_peer_list`, which
        // `torrent::set_metadata` keeps).
        let peer_list = PeerList::resumed(
            opts.initial_peers.iter().flatten().copied(),
            opts.banned_peers.iter().copied(),
        );

        // The tracker task of this torrent's metadata fetch, for the run
        // that follows: the caller's, or this magnet's own.
        let mut handover = opts.tracker_handover.take();
        let (metadata, peer_list) = match metadata {
            Some(metadata) => (metadata, peer_list),
            None => {
                // The DHT and local discovery, when the session runs either
                // and the torrent was not added to take peers from its
                // trackers alone.
                let discovery = !opts.trackers_only && (self.dht.is_some() || self.lsd.is_some());
                if self.disable_trackers
                    && !discovery
                    && opts.initial_peers.as_ref().is_none_or(Vec::is_empty)
                {
                    bail!("no known way to resolve peers (no DHT, no trackers, no initial_peers)");
                }
                // Whether the torrent is private is known only once its
                // metadata arrives: until then it is taken to be, and asks
                // its own trackers alone.
                handover = handover.or_else(|| {
                    self.start_trackers(
                        info_hashes,
                        &identity,
                        tracker_tiers.clone(),
                        opts.force_tracker_interval,
                        true,
                        None,
                    )
                    .map(TrackerHandover::new)
                });
                let fetched = handover.as_ref().map(TrackerHandover::task);
                let table = fetched.as_ref().map(|task| task.status.clone());
                let (phase_tx, phase) = tokio::sync::watch::channel(FetchPhase::of(
                    &table
                        .as_ref()
                        .map(TrackerStatus::announces)
                        .unwrap_or_default(),
                    discovery,
                ));
                if let Some(tx) = opts.metadata_fetch.take() {
                    let _ = tx.send(MetadataFetch {
                        trackers: table.clone(),
                        phase,
                    });
                }
                let asked_first =
                    make_peer_rx(info_hashes, &identity, true, fetched).map(|source| source.peers);
                // The DHT and local discovery, once the trackers named no
                // peer.
                let then = {
                    let session = self.clone();
                    let dht_announce = dht_announce.subscribe();
                    async move {
                        follow_fetch(table.map(|t| t.subscribe()), discovery, phase_tx).await;
                        session
                            .discovery_peers(info_hashes, false, dht_announce, None)
                            .unwrap_or_else(|| futures::stream::empty().boxed())
                    }
                    .flatten_stream()
                    .boxed()
                };
                let peers = match asked_first {
                    Some(first) => merge_streams(first, then).boxed(),
                    None => then,
                };
                let resolved_magnet = self
                    .resolve_magnet(
                        info_hashes,
                        &identity,
                        peers,
                        peer_list,
                        &tracker_tiers,
                        opts.peer_opts,
                    )
                    .await?;

                seen_peers = resolved_magnet.seen_peers;
                // The fetch's own discovery ends with it; its trackers carry
                // on into the run.
                drop(resolved_magnet.peer_rx);
                // The peers the fetch found stay on the list it wrote
                // into, with what their connections left there.
                (resolved_magnet.metadata, resolved_magnet.peers)
            }
        };

        // A running torrent finds its peers from the start; an SSL torrent's
        // are found as it goes live, under its identity. Its trackers hear
        // it as the torrent its metadata fetch announced: that fetch's task
        // carries on, starting from the peers it found.
        let mut peer_rx = None;
        if !opts.paused && !opts.list_only && metadata.info.info().ssl_cert.is_none() {
            let private = metadata.info.info().private;
            let trackers_only = trackers_only(private, opts.trackers_only);
            let trackers = handover
                .take()
                .and_then(TrackerHandover::take)
                .and_then(|task| {
                    task.take_over(
                        metadata.info_hashes,
                        self.announce_tiers(tracker_tiers.clone(), trackers_only),
                    )
                });
            peer_rx = make_peer_rx(metadata.info_hashes, &identity, private, trackers);
        }

        trace!("Torrent metadata: {:#?}", &metadata.info.info());
        // A magnet link names some of the hashes; the metadata has them all.
        let info_hashes = metadata.info_hashes;
        let info_hash = info_hashes.best();

        let ssl = match metadata.info.info().ssl_cert.as_ref() {
            Some(ca) => {
                let ssl = SslTorrent::new(ca.as_ref(), info_hashes)
                    .context("the torrent's SSL certificate")?;
                ssl.set_identity(opts.ssl_identity.take())
                    .context("the torrent's SSL identity")?;
                Some(Arc::new(ssl))
            }
            None => None,
        };

        let only_files = compute_only_files(
            &metadata.info,
            opts.only_files,
            opts.only_files_regex,
            opts.list_only,
        )?;

        let checkpoint = opts
            .checkpoint
            .take()
            .map(|checkpoint| normalize_checkpoint(checkpoint, metadata.info.lengths()))
            .transpose()?;

        let file_priorities = compute_file_priorities(
            &metadata.file_infos,
            opts.file_priorities.take(),
            only_files.as_deref(),
        )?;
        // The files' rule first, then the pieces set one by one over it
        // (libtorrent `torrent::init`).
        let lengths = metadata.info.lengths();
        let mut piece_priorities = file_to_piece_priorities(
            lengths,
            &metadata.file_infos,
            &file_priorities,
            opts.first_last_piece_first,
        );
        for (index, priority) in
            validate_piece_priorities(lengths, &opts.piece_priorities.take().unwrap_or_default())?
        {
            piece_priorities[index.get_usize()] = priority;
        }
        let only_files = Some(wanted_files(&file_priorities));

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
                file_priorities,
                sequential: opts.sequential,
                first_last_piece_first: opts.first_last_piece_first,
                output_folder,
                seen_peers,
                trackers: handover,
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
                tracker_tiers: parking_lot::RwLock::new(tracker_tiers),
                dht_announce,
                spawner: self.spawner.clone(),
                peer_id: identity.peer_id,
                storage_factory,
                options: ManagedTorrentOptions {
                    force_tracker_interval: opts.force_tracker_interval,
                    peer_opts,
                    allow_overwrite: opts.overwrite,
                    output_folder,
                    ratelimits: Limits::new(opts.ratelimits),
                    initial_peers: opts.initial_peers.clone().unwrap_or_default(),
                    peer_limit: opts.peer_limit.or(self.peer_limit),
                    disable_v1_hashes: opts.disable_v1_hashes,
                    super_seeding: opts.super_seeding,
                    sequential: opts.sequential.into(),
                    trackers_only: opts.trackers_only,
                },
                connector: self.connector.clone(),
                session: Arc::downgrade(self),
                magnet_name: name,
                client_name_and_version: identity.version.clone(),
                user_agent: identity.user_agent.clone(),
                last_run: Default::default(),
                web_seeds: Arc::new(WebSeeds::of_torrent(&metadata, &opts.web_seeds)),
                ssl,
            });

            let initializing = Arc::new(TorrentStateInitializing::new(
                minfo.clone(),
                metadata.clone(),
                only_files,
                self.spawner
                    .block_in_place(|| minfo.storage_factory.create_and_init(&minfo, &metadata))?,
                false,
                checkpoint,
                Some(piece_priorities.clone()),
                peer_list,
            ));
            let handle = Arc::new(ManagedTorrent {
                locked: RwLock::new(ManagedTorrentLocked {
                    paused: opts.paused,
                    state: ManagedTorrentState::Initializing(initializing),
                    files: file_priorities,
                    first_last: opts.first_last_piece_first,
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

    pub(crate) fn peer_source_managed_torrent(
        self: &Arc<Self>,
        t: &ManagedTorrentHandle,
        announce: bool,
    ) -> Option<PeerSource> {
        self.make_peer_rx(
            t.info_hashes(),
            &t.shared().identity(),
            t.shared().tracker_tiers(),
            announce,
            t.shared().options.force_tracker_interval,
            t.shared().options.initial_peers.clone(),
            t.takes_peers_from_trackers_only(),
            t.shared().dht_announce.subscribe(),
            t.shared().ssl.as_ref(),
            None,
        )
    }

    /// The tiers a torrent announces to: its own, then the session's trackers
    /// unless it takes peers from its own trackers only.
    pub(crate) fn announce_tiers(
        &self,
        tracker_tiers: Vec<Vec<url::Url>>,
        trackers_only: bool,
    ) -> Vec<Vec<url::Url>> {
        if self.disable_trackers {
            return Vec::new();
        }
        merge_tracker_tiers(tracker_tiers, self.trackers.iter().cloned(), trackers_only)
    }

    /// Whether the session may reach `ip`, whoever is there: a peer, a web
    /// seed, or a tracker at any hop. It must be of a family the session
    /// connects over, as an IPv4-only session reaches no IPv6 address
    /// (libtorrent `listen_socket_t::can_route`, which
    /// `http_tracker_connection::on_filter` and
    /// `udp_tracker_connection::name_lookup` apply first), and the IP filter,
    /// the blocklist and the allowlist, must let it through (libtorrent's one
    /// `ip_filter`: `torrent::add_peer` applies it to a peer,
    /// `torrent::connect_web_seed` to a web seed, and the two tracker
    /// functions to a tracker while [`IpFilter::trackers`] holds).
    pub(crate) fn may_reach(&self, ip: IpAddr) -> Result<(), Unreachable> {
        self.may_reach_under(ip, true)
    }

    // `may_reach`, the IP filter applied only when `filtered`.
    fn may_reach_under(&self, ip: IpAddr, filtered: bool) -> Result<(), Unreachable> {
        if self.ipv4_only && ip.is_ipv6() {
            return Err(Unreachable::Family);
        }
        if filtered
            && (self.ip_filter.read().blocked.has(ip)
                || self.allowlist.as_ref().is_some_and(|l| !l.has(ip)))
        {
            return Err(Unreachable::Filtered);
        }
        Ok(())
    }

    /// The addresses the tracker task may ask a tracker at: those of a
    /// family the session connects over, and while the filter applies to
    /// trackers those it lets through (libtorrent
    /// `torrent::build_tracker_request` and `scrape_tracker_impl` hand the
    /// request the filter only when `apply_ip_filter_to_trackers` is set),
    /// read at each lookup.
    // Weak, as the tracker task outlives no session: holding it strongly
    // would keep a session whose torrents announce nowhere alive.
    fn reachable(self: &Arc<Self>) -> Reachable {
        let session = Arc::downgrade(self);
        Arc::new(move |ip| {
            session.upgrade().map_or(Ok(()), |s| {
                let filtered = s.ip_filter.read().trackers;
                s.may_reach_under(ip, filtered)
            })
        })
    }

    // A torrent's tracker task, under every hash it goes by. The task runs
    // with no tracker too, so trackers added later are announced to; a
    // session without trackers starts none.
    fn start_trackers(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        identity: &Identity,
        tracker_tiers: Vec<Vec<url::Url>>,
        force_tracker_interval: Option<Duration>,
        trackers_only: bool,
        ssl: Option<&Arc<SslTorrent>>,
    ) -> Option<TrackerTask> {
        if self.disable_trackers {
            return None;
        }
        let stats = PeerRxTorrentInfo {
            info_hashes,
            session: Arc::downgrade(self),
            torrent: Default::default(),
        };
        Some(TrackerComms::start_with_tiers(
            info_hashes,
            identity.clone(),
            self.announce_tiers(tracker_tiers, trackers_only),
            Box::new(stats),
            force_tracker_interval,
            match ssl {
                Some(_) => self.ssl_port.subscribe(),
                None => self.announce_port.subscribe(),
            },
            self.external_ips.tracker_vote(),
            match ssl {
                Some(ssl) => Arc::new(SslHttpClients {
                    inner: self.http_clients.clone(),
                    ssl: ssl.clone(),
                }),
                None => self.http_clients.clone(),
            },
            self.reachable(),
            self.udp_tracker_client.clone(),
            self.stopped_announces.clone(),
        ))
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
        trackers_only: bool,
        dht_announce: tokio::sync::watch::Receiver<()>,
        ssl: Option<&Arc<SslTorrent>>,
        trackers: Option<TrackerTask>,
    ) -> Option<PeerSource> {
        // An SSL torrent with no identity asks no one for peers.
        if ssl.is_some_and(|ssl| !ssl.has_identity()) {
            return None;
        }
        let discovery_rx = if trackers_only {
            None
        } else {
            self.discovery_peers(info_hashes, announce, dht_announce, ssl)
        };

        let (tracker_rx, trackers) = trackers
            .or_else(|| {
                self.start_trackers(
                    info_hashes,
                    identity,
                    tracker_tiers,
                    force_tracker_interval,
                    trackers_only,
                    ssl,
                )
            })
            .map(|task| (task.peers, task.status))
            .unzip();

        let initial_peers_rx = if initial_peers.is_empty() {
            None
        } else {
            Some(futures::stream::iter(initial_peers))
        };
        // Each peer goes with who named it (libtorrent `torrent::add_peer`'s
        // `peer_source_flags_t`; a torrent's own peers are `resume_data`).
        let named = |source| move |addr| (addr, source);
        merge_two_optional_streams(
            merge_two_optional_streams(
                tracker_rx.map(|s| s.map(named(PeerSourceFlag::Tracker))),
                initial_peers_rx.map(|s| s.map(named(PeerSourceFlag::ResumeData))),
            ),
            discovery_rx,
        )
        .map(|peers| PeerSource { peers, trackers })
    }

    // The peers the DHT and local discovery name, under every hash the
    // torrent goes by; None when the session runs neither.
    fn discovery_peers(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        announce: bool,
        dht_announce: tokio::sync::watch::Receiver<()>,
        ssl: Option<&Arc<SslTorrent>>,
    ) -> Option<PeerStream> {
        // The port the torrent's peers are told to reach it at: an SSL
        // torrent's is the SSL listener's.
        let port = || match ssl {
            Some(_) => self.ssl_port.subscribe(),
            None => self.announce_port.subscribe(),
        };
        let dht_rx = self.dht.as_ref().map(|dht| {
            futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                dht_peers(
                    dht.clone(),
                    info_hash,
                    if announce {
                        port()
                    } else {
                        tokio::sync::watch::channel(None).1
                    },
                    dht_announce.clone(),
                )
            }))
        });

        // Each source starts when the torrent first asks it for peers, which
        // is once it is checked and running: trackers and the DHT by their
        // streams, the LSD by registering here (libtorrent
        // `torrent::start_announcing` runs only after `files_checked`, never
        // while paused).
        let lsd_rx = self.lsd.as_ref().map(|lsd| {
            // Local peers reach the listen port itself.
            let port = if !announce {
                None
            } else if ssl.is_some() {
                *self.ssl_port.borrow()
            } else {
                self.announce_port.configured
            };
            futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                let lsd = lsd.clone();
                futures::stream::once(async move { lsd.announce(info_hash, port) })
                    .flatten()
                    .boxed()
            }))
        });

        let named = |source| move |addr| (addr, source);
        merge_two_optional_streams(
            dht_rx.map(|s| s.map(named(PeerSourceFlag::Dht))),
            lsd_rx.map(|s| s.map(named(PeerSourceFlag::Lsd))),
        )
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

    /// Forget every piece of the torrent and check them all again, trusting
    /// no checkpoint or saved state (libtorrent `torrent::force_recheck`).
    /// A running torrent runs again once checked; a paused one stays paused.
    /// Nothing happens while it is being checked already.
    pub async fn force_recheck(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
    ) -> anyhow::Result<()> {
        handle.recheck()?;
        let paused = handle.is_paused();
        let peer_rx = if paused {
            None
        } else {
            self.peer_source_managed_torrent(handle, true)
        };
        handle.start(peer_rx, paused)
    }

    /// [`ManagedTorrent::set_file_priorities`], and the session's saved
    /// state follows.
    pub async fn set_file_priorities(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
        priorities: &[(usize, FilePriority)],
    ) -> anyhow::Result<()> {
        handle.set_file_priorities(priorities)?;
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

    /// Whether the session listens on uTP as well, on the listen address.
    pub fn listens_utp(&self) -> bool {
        self.utp
    }

    /// Our address as the internet sees it, IPv4 first, of each family the
    /// vote has settled on.
    pub fn external_ips(&self) -> Vec<IpAddr> {
        self.external_ips.get()
    }

    /// The port peers are told to connect to: the one a mapper got on the
    /// gateway, else the configured one.
    pub fn announce_port(&self) -> Option<u16> {
        self.announce_port.get()
    }

    /// The address the SSL listener took, when the session listens for SSL
    /// torrents' peers.
    pub fn ssl_listen_addr(&self) -> Option<SocketAddr> {
        self.ssl_listen_addr
    }

    /// The port a torrent's peers are told to reach it at: the SSL
    /// listener's for an SSL torrent, else [`Self::announce_port`].
    pub(crate) fn announce_port_of(&self, ssl: bool) -> Option<u16> {
        if ssl {
            *self.ssl_port.borrow()
        } else {
            self.announce_port()
        }
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

    async fn resolve_magnet(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        identity: &Identity,
        peer_rx: PeerStream,
        peer_list: PeerList,
        tracker_tiers: &[Vec<url::Url>],
        peer_opts: Option<PeerConnectionOptions>,
    ) -> anyhow::Result<ResolveMagnetResult> {
        match read_metainfo_from_peer_receiver(
            identity.peer_id,
            info_hashes,
            peer_list,
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
                peers,
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
                    peers,
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
    /// The torrent's peer list, as the metadata connections left it.
    pub peers: PeerList,
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
    // Weak for the reason `Session::reachable` gives.
    session: std::sync::Weak<Session>,
    // Kept once found, so the counters stay readable after the torrent is
    // removed from the session (for the `stopped` announce).
    torrent: std::sync::OnceLock<ManagedTorrentHandle>,
}

impl PeerRxTorrentInfo {
    fn torrent(&self) -> Option<&ManagedTorrentHandle> {
        if let Some(t) = self.torrent.get() {
            return Some(t);
        }
        let found = self.session.upgrade()?.with_torrents(|torrents| {
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
            TS::Paused | TS::NeedsCertificate => S::Paused,
            TS::Error => S::None,
        };
        // The running run's counters; once it has ended (its pieces are gone
        // from the live state) the ones recorded as it ended.
        let last_run = *mt.shared().last_run.lock();
        let run = live
            .as_ref()
            .map(|l| {
                (
                    l.get_uploaded_bytes(),
                    l.get_downloaded_bytes(),
                    l.get_failed_bytes(),
                    l.get_redundant_bytes(),
                )
            })
            .or(last_run.map(|r| {
                (
                    r.uploaded_bytes,
                    r.downloaded_bytes,
                    r.corrupt_bytes,
                    r.redundant_bytes,
                )
            }));
        let (uploaded, downloaded, corrupt, redundant) = run.unwrap_or_default();
        // What the whole torrent still lacks, whatever is selected, so a
        // torrent that has its selected files but not every piece never reads
        // as a seed (libtorrent `torrent::bytes_left`). Once the pieces are
        // gone with the torrent, what its run recorded as it ended.
        let left_bytes = mt
            .with_chunk_tracker(|c| c.bytes_left())
            .ok()
            .or(last_run.map(|r| r.left_bytes));
        crate::tracker::TrackerCommsStats {
            uploaded_bytes: uploaded,
            downloaded_bytes: downloaded,
            corrupt_bytes: corrupt,
            redundant_bytes: redundant,
            left_bytes,
            torrent_state,
            // `completed` is for becoming a seed, not for finishing a
            // selection (libtorrent `torrent::finished` calls `completed`
            // only when `is_seed`), and only for a run that began short of
            // one, whatever was wanted when it began: a torrent added as a
            // seed sends none (`torrent::files_checked` presets `m_complete_sent`).
            completed_during_run: left_bytes == Some(0) && live.is_some_and(|l| !l.seed_at_start()),
        }
    }

    fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
        async move {
            // A magnet still receiving its metadata isn't in the session
            // yet: it is looked for again as each torrent is added
            // (libtorrent waits on `metadata_received_alert`).
            let Some(added) = self.session.upgrade().map(|s| s.db.read().added.clone()) else {
                return futures::future::pending().await;
            };
            loop {
                let mut next_added = std::pin::pin!(added.notified());
                next_added.as_mut().enable();
                if let Some(mt) = self.torrent() {
                    // The torrent finishes its selection; it completes only if
                    // that makes it a seed. A selection that finishes short of
                    // a seed keeps the wait, so a later wider selection that
                    // makes it one still announces at once. An error leaves
                    // nothing to wait on in this run: a later regular
                    // announce reads the state.
                    if mt.wait_until_seed().await.is_err() {
                        futures::future::pending::<()>().await;
                    }
                    return;
                }
                next_added.await;
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::{
        AddTorrent, AddTorrentOptions, AddTorrentResponse, IpFilter, Session, SessionOptions,
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
        let (announce, announce_rx) = tokio::sync::watch::channel(());
        let mut peers = super::dht_peers(dht, Id20::new(NODE_ID), listen_port, announce_rx);
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

        // A forced DHT announce (libtorrent torrent_handle::force_dht_announce)
        // starts a new search, which announces again.
        announce.send_replace(());
        let third = tokio::select! {
            _ = peers.next() => panic!("the node gave no peers"),
            port = next_announced(&node) => port,
        };
        assert_eq!(third, 40001);
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

        let add = |name: &'static str, private: bool, trackers_only: bool| {
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
                            trackers_only,
                            ..Default::default()
                        }),
                    )
                    .await
                    .unwrap()
                    .into_handle()
                    .unwrap()
            }
        };

        let private = add("private", true, false).await;
        assert!(private.takes_peers_from_trackers_only());
        assert_eq!(private.shared().tracker_tiers(), own_tiers());
        assert_eq!(
            session.announce_tiers(private.shared().tracker_tiers(), true),
            own_tiers()
        );

        // A torrent its metainfo does not mark private, added to take peers
        // from its trackers alone, is held to the same.
        let trackers_only = add("trackers_only", false, true).await;
        assert!(trackers_only.takes_peers_from_trackers_only());
        assert_eq!(trackers_only.shared().tracker_tiers(), own_tiers());
        assert_eq!(
            session.announce_tiers(
                trackers_only.shared().tracker_tiers(),
                trackers_only.takes_peers_from_trackers_only()
            ),
            own_tiers()
        );

        let public = add("public", false, false).await;
        assert!(!public.takes_peers_from_trackers_only());
        let with_added = tiers(&[
            &["http://a1.test/announce", "udp://a2.test:6969/announce"],
            &["http://b1.test/announce"],
            &["http://added.test/announce"],
        ]);
        assert_eq!(public.shared().tracker_tiers(), with_added);
        let mut with_session = with_added;
        with_session.push(vec![url("http://session.test/announce")]);
        assert_eq!(
            session.announce_tiers(public.shared().tracker_tiers(), false),
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
        seeding_with(connections_limit, super::DEFAULT_CONNECTIONS_SLACK, upload).await
    }

    // As [`seeding`], the session taking `connections_slack` incoming
    // connections past its cap.
    async fn seeding_with(
        connections_limit: usize,
        connections_slack: usize,
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
                connections_slack,
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
            session: std::sync::Arc::downgrade(session),
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
        assert!(!stats.completed_during_run);
        assert!(info.wait_until_completed().now_or_never().is_some());
        session.stop().await;
    }

    // A one-piece torrent of a file in a new directory, and a session
    // reaching no network, the torrent not added to it.
    async fn unadded(name: &str) -> (TempDir, Bytes, std::sync::Arc<Session>) {
        let dir =
            crate::tests::test_util::create_default_random_dir_with_torrents(1, 16384, Some(name));
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
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (dir, torrent, session)
    }

    // libtorrent waits for a magnet's metadata on `metadata_received_alert`:
    // the announce's wait for completion finds the torrent as it enters the
    // session, not on a later look.
    #[tokio::test]
    async fn a_magnets_completion_wait_finds_its_torrent_as_it_is_added() {
        use crate::tracker::TorrentStatsProvider;
        use futures::FutureExt;
        let (dir, torrent, session) = unadded("magnet_wait").await;
        let info_hashes = crate::core::torrent_metainfo::torrent_from_bytes(&torrent)
            .unwrap()
            .info_hashes;
        let info = super::PeerRxTorrentInfo {
            info_hashes,
            session: std::sync::Arc::downgrade(&session),
            torrent: Default::default(),
        };
        let mut wait = info.wait_until_completed();
        assert!((&mut wait).now_or_never().is_none());

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
        handle.wait_until_completed().await.unwrap();
        assert!(wait.now_or_never().is_some());
        session.stop().await;
    }

    // A torrent's wait for completion follows its state: a torrent removed
    // while it downloads ends the wait.
    #[tokio::test]
    async fn a_completion_wait_ends_when_its_torrent_is_deleted() {
        use futures::FutureExt;
        let (_dir, torrent, session) = unadded("deleted_wait").await;
        let empty = TempDir::with_prefix("deleted_wait_out").unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(empty.path().to_str().unwrap().to_owned()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        assert!(handle.live().is_some());

        let mut wait = handle.wait_until_completed();
        assert!((&mut wait).now_or_never().is_none());
        session.delete(handle.id().into(), false).await.unwrap();
        assert!(wait.await.is_err());
        session.stop().await;
    }

    // The file levels, sequential and first-and-last given at add are the
    // torrent's, and each change rewrites the pieces from the levels
    // (libtorrent `torrent::prioritize_files`, qBittorrent
    // `TorrentImpl::setFirstLastPiecePriority`).
    #[tokio::test(start_paused = true)]
    async fn file_levels_and_the_picking_toggles_are_set_at_add_and_changed_after() {
        use crate::{FilePriority::*, PiecePriority};
        const PIECE: usize = 16384;
        let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
            2,
            PIECE,
            Some("levels"),
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
        let both = AddTorrentOptions {
            output_folder: Some(dir.path().to_str().unwrap().to_owned()),
            overwrite: true,
            only_files: Some(vec![0]),
            file_priorities: Some(vec![Normal, Normal]),
            ..Default::default()
        };
        assert!(
            session
                .add_torrent(AddTorrent::from_bytes(torrent.clone()), Some(both))
                .await
                .is_err()
        );

        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    file_priorities: Some(vec![Ignored, High]),
                    sequential: true,
                    first_last_piece_first: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        let level = |file: usize| {
            let start = handle
                .with_metadata(|m| m.file_infos[file].piece_range.start)
                .unwrap();
            handle.piece_priority(start).unwrap()
        };
        assert_eq!(handle.file_priorities(), [Ignored, High]);
        assert_eq!(handle.only_files(), Some(vec![1]));
        assert!(handle.sequential());
        assert!(handle.first_last_piece_first());
        // A one-piece file is all head and tail.
        assert_eq!(level(0), PiecePriority::Skip);
        assert_eq!(level(1), PiecePriority::Critical);

        handle.set_first_last_piece_first(false).unwrap();
        assert_eq!(level(1), PiecePriority::High);
        handle
            .set_file_priorities(&[(0, Maximum), (1, Ignored)])
            .unwrap();
        assert_eq!(handle.file_priorities(), [Maximum, Ignored]);
        assert_eq!(handle.only_files(), Some(vec![0]));
        assert_eq!(level(0), PiecePriority::Critical);
        assert_eq!(level(1), PiecePriority::Skip);
        assert!(handle.set_file_priorities(&[(2, Normal)]).is_err());
        handle.set_sequential(false);
        assert!(!handle.sequential());
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
        let ignored: Vec<_> = (0..handle.file_priorities().len())
            .filter(|file| *file != had)
            .map(|file| (file, crate::FilePriority::Ignored))
            .collect();
        handle.set_file_priorities(&ignored).unwrap();
        assert!(handle.live().unwrap().is_finished());
        assert!(!handle.live().unwrap().is_seed());

        let (info, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(PIECE as u64));
        assert!(!stats.completed_during_run);
        assert!(info.wait_until_completed().now_or_never().is_none());

        // Paused, the pieces still say what the torrent lacks, and so does
        // what the run recorded as it ended.
        session.pause(&handle).await.unwrap();
        let (_, stats) = tracker_stats(&session, &handle);
        assert_eq!(stats.left_bytes, Some(PIECE as u64));
        assert_eq!(
            handle.shared().last_run.lock().map(|run| run.left_bytes),
            Some(PIECE as u64)
        );
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

    // Each incoming connection is kept under its torrent's encryption, the
    // session's when the torrent names none: a plaintext one reaches a
    // torrent that prefers encryption in a session that requires it, and is
    // refused by a torrent that requires it in a session that prefers it.
    #[tokio::test]
    async fn an_incoming_plaintext_connection_is_judged_by_its_torrents_encryption() {
        use crate::{mse::EncryptionPolicy, peer_connection::PeerConnectionOptions};
        for (session_policy, torrent_policy, kept) in [
            (EncryptionPolicy::Prefer, None, true),
            (
                EncryptionPolicy::Prefer,
                Some(EncryptionPolicy::Require),
                false,
            ),
            (EncryptionPolicy::Require, None, false),
            (
                EncryptionPolicy::Require,
                Some(EncryptionPolicy::Prefer),
                true,
            ),
        ] {
            let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
                1,
                16384,
                Some("torrent_encryption"),
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
                    encryption: session_policy,
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
                    AddTorrent::from_bytes(torrent),
                    Some(AddTorrentOptions {
                        output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                        overwrite: true,
                        peer_opts: Some(PeerConnectionOptions {
                            encryption: torrent_policy,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap();
            handle.wait_until_completed().await.unwrap();
            let answer = answer(&session, &handle).await;
            assert_eq!(
                is_handshake_for(&answer, &handle),
                kept,
                "{session_policy:?} {torrent_policy:?}"
            );
            session.stop().await;
        }
    }

    #[tokio::test]
    async fn an_incoming_connection_past_the_session_cap_and_slack_is_closed() {
        let (_dir, session, handle) = seeding_with(1, 0, crate::limits::LimitKind::Unlimited).await;

        // The one place under the cap is taken and there is no slack: the
        // listener closes the connection it accepts without reading the
        // handshake (libtorrent `session_impl::incoming_connection`).
        let held = session.connection_slots.try_open().unwrap();
        assert!(answer(&session, &handle).await.is_empty());

        // Freed, the same connection is taken and answered.
        drop(held);
        assert!(is_handshake_for(&answer(&session, &handle).await, &handle));

        session.stop().await;
    }

    // A connection from `addr` the listener took and checked, for the
    // torrent of `info_hash`; and our end of it.
    fn incoming(
        addr: &str,
        info_hash: super::Id20,
    ) -> (super::CheckedIncomingConnection, tokio::io::DuplexStream) {
        let (ours, theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = crate::peer_info_reader::tests::halves(ours);
        let checked = super::CheckedIncomingConnection {
            kind: crate::stream_connect::ConnectionKind::Tcp,
            addr: addr.parse().unwrap(),
            reader,
            writer,
            read_buf: crate::read_buf::ReadBuf::new(),
            handshake: crate::peer_protocol::Handshake::new(info_hash, super::Id20::new([7; 20])),
            encryption: crate::mse::Encryption::None,
            meter: Default::default(),
        };
        (checked, theirs)
    }

    // libtorrent `session_impl::incoming_connection`: past `connections_limit`
    // the session takes up to `connections_slack` more incoming connections.
    // Attaching one closes the lowest-ranked connection of the torrent that
    // holds the most, when it holds more than the newcomer's own, and the
    // newcomer is under the cap in its place (`peer_connection::
    // attach_to_torrent`, `session_impl::find_disconnect_candidate_torrent`,
    // `torrent::find_lowest_ranking_peer`).
    #[tokio::test]
    async fn an_incoming_connection_in_the_slack_replaces_one_of_the_fullest_torrent() {
        use crate::torrent_state::AddIncomingPeerResult;
        use tokio::io::AsyncReadExt;

        let (dir, session, fullest) = seeding_with(2, 1, crate::limits::LimitKind::Unlimited).await;
        // Another torrent of the same file, in pieces of another length.
        let other = crate::create_torrent(
            dir.path(),
            crate::CreateTorrentOptions {
                piece_length: Some(32768),
                ..Default::default()
            },
            &crate::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        let other = session
            .add_torrent(
                AddTorrent::from_bytes(other),
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
        other.wait_until_completed().await.unwrap();
        tokio::time::pause();
        let (fullest_live, other_live) = (fullest.live().unwrap(), other.live().unwrap());
        let slots = &session.connection_slots;

        // The fullest torrent takes both places under the cap.
        let mut ends = Vec::new();
        for addr in ["127.0.0.1:6881", "127.0.0.2:6881"] {
            let slot = slots.try_accept().unwrap();
            assert!(!slots.exceeds_limit(&slot));
            let (checked, theirs) = incoming(addr, fullest.info_hash());
            let added = fullest_live.add_incoming_peer(checked, slot).unwrap();
            assert!(matches!(added, AddIncomingPeerResult::Added));
            ends.push(theirs);
        }

        // The next connection takes the slack's one place.
        let slot = slots.try_accept().unwrap();
        assert!(slots.exceeds_limit(&slot));
        assert!(slots.try_accept().is_none());
        let (checked, _newcomer_end) = incoming("127.0.0.3:6881", other.info_hash());
        let newcomer = checked.addr;
        let added = other_live.add_incoming_peer(checked, slot).unwrap();
        assert!(matches!(added, AddIncomingPeerResult::Added));
        session.settle_over_limit(&other_live, newcomer);

        // It holds a place under the cap, the fullest torrent one connection
        // fewer, and the slack's place is free again.
        assert!(!other_live.exceeds_limit(newcomer, slots));
        assert_eq!(fullest_live.connections().0, 1);
        assert_eq!(slots.in_use(), 2);
        assert!(slots.exceeds_limit(&slots.try_accept().unwrap()));
        // Of its two connections, alike in rank, the first by address closed.
        let mut rest = Vec::new();
        ends[0].read_to_end(&mut rest).await.unwrap();

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
                // A test greets the seed from this host more than once.
                allow_multiple_connections_per_ip: true,
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
                    false,
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

    // libtorrent `torrent::set_metadata` keeps `m_peer_list`: the list a
    // magnet's metadata connections write into is the one the torrent
    // connects from for its whole life. Which handshake a peer last completed
    // (`torrent_peer::pe_support`), learnt over a metadata connection, is the
    // one the torrent's first connection to it sends
    // (`bt_peer_connection::on_connected`).
    #[tokio::test(start_paused = true)]
    async fn memory_a_metadata_connection_leaves_is_read_by_the_torrents_connector_afterwards() {
        use crate::{
            mse::EncryptionPolicy,
            peer_info_reader::tests::{Dialed, MockPeer, halves, metadata},
            read_buf::ReadBuf,
            storage::StorageFactory,
            tests::embedder_util::{MemoryStorageFactory, isolated_session_opts},
        };
        use std::time::Duration;

        let (info, hashes) = metadata(10);
        let (tx, mut dials) = tokio::sync::mpsc::unbounded_channel();
        let dir = TempDir::with_prefix("metadata_memory").unwrap();
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                socket_factory: Some(Arc::new(Dialed(tx))),
                encryption: EncryptionPolicy::Prefer,
                connect: Some(super::ConnectionOptions {
                    enable_tcp: true,
                    peer_opts: None,
                }),
                ..isolated_session_opts(false)
            },
        )
        .await
        .unwrap();
        let add = session.add_torrent(
            AddTorrent::from_url(format!(
                "magnet:?xt=urn:btih:{}",
                hashes.v1.unwrap().as_string()
            )),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                initial_peers: Some(vec!["127.0.0.1:6881".parse().unwrap()]),
                ..Default::default()
            }),
        );
        let peer = async {
            // The peer speaks only plaintext: the first metadata
            // connection's encrypted handshake is no BitTorrent one.
            let (_, stream) = dials.recv().await.unwrap();
            let (mut read, write) = halves(stream);
            let refused = ReadBuf::new()
                .read_handshake(&mut read, Duration::MAX)
                .await;
            assert!(refused.is_err());
            drop((read, write));
            // The next sends the plaintext one and gets the metadata.
            let (_, stream) = dials.recv().await.unwrap();
            let mut fetched = MockPeer::greet(stream, hashes).await;
            fetched.send_extended_handshake(info.len() as u32).await;
            let piece = fetched.next_request().await.unwrap();
            fetched.send_piece(&info, piece).await;
            // Then the torrent connects.
            let (_, stream) = dials.recv().await.unwrap();
            let (mut read, _write) = halves(stream);
            let greeting = ReadBuf::new()
                .read_handshake(&mut read, Duration::MAX)
                .await;
            (greeting.map(|h| h.info_hash), fetched)
        };

        let (added, (greeting, _fetched)) = tokio::join!(add, peer);
        added.unwrap();
        assert_eq!(greeting.unwrap(), hashes.v1.unwrap());
        let stats = session.connector.stats().encryption.snapshot();
        assert_eq!(stats.failures, 1, "{stats:?}");
        session.stop().await;
    }

    // A session for the incoming-connection rule tests, with nothing that
    // reaches out.
    async fn quiet_session(dir: &TempDir, opts: SessionOptions) -> std::sync::Arc<Session> {
        Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                ..opts
            },
        )
        .await
        .unwrap()
    }

    // Writes an IP list holding `range` into `dir`; returns its path.
    fn ip_list(dir: &TempDir, name: &str, range: &str) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, format!("test:{range}\n")).unwrap();
        path
    }

    // Hands the session an incoming connection from `addr` whose peer stays
    // open and sends nothing, and polls the check once: the refusal when it
    // refuses without reading, `None` while it waits on the peer's handshake.
    fn check_once(session: &std::sync::Arc<Session>, addr: &str) -> Option<String> {
        use crate::{peer_info_reader::tests::halves, stream_connect::ConnectionKind};
        use futures::FutureExt;
        let (ours, _theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        session
            .clone()
            .check_incoming_connection(
                addr.parse().unwrap(),
                ConnectionKind::Tcp,
                false,
                reader,
                writer,
            )
            .now_or_never()
            .map(|checked| format!("{:#}", checked.err().unwrap()))
    }

    // libtorrent `torrent::start_announcing` runs from `files_checked`: a
    // torrent announces to the local network only once its check is done,
    // as it does to its trackers and the DHT.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_announces_on_the_local_network_only_once_checked() -> anyhow::Result<()> {
        use crate::{
            lsd::RecordingSocket,
            storage::StorageFactory,
            tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        };
        use anyhow::Context;

        let t = TestTorrent::new("test_lsd_after_check", 2 * 16384, 16384).await?;
        let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
        let out = TempDir::with_prefix("test_lsd_after_check_out")?;
        let session = Session::new_with_opts(
            out.path().into(),
            SessionOptions {
                disable_local_service_discovery: false,
                lsd_socket: Some(Box::new(RecordingSocket(sent_tx))),
                // A loopback listener announces no port unless told one.
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    announce_port: Some(6881),
                    ..Default::default()
                }),
                ..isolated_session_opts(false)
            },
        )
        .await?;
        let lsd = session.lsd.clone().context("no LSD")?;
        let checks = session.concurrent_initialize_semaphore.clone();
        let held = checks
            .clone()
            .acquire_many_owned(checks.available_permits() as u32)
            .await?;

        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?;
        let info_hash = handle.info_hash();
        assert!(!lsd.registered(info_hash), "announced before its check");

        drop(held);
        let announce = sent.recv().await.context("no announce")?;
        assert!(announce.contains(&info_hash.as_string()), "{announce}");
        assert!(lsd.registered(info_hash));

        // A paused torrent is checked, and announces nothing.
        let paused = TestTorrent::new("test_lsd_paused", 2 * 16384, 16384).await?;
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(paused.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    paused: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?;
        handle.wait_until_initialized().await?;
        assert!(handle.is_paused());
        assert!(!lsd.registered(handle.info_hash()));
        // Nor once rechecked: it stays paused.
        session.force_recheck(&handle).await?;
        handle.wait_until_initialized().await?;
        assert!(handle.is_paused());
        assert!(!lsd.registered(handle.info_hash()));
        session.stop().await;
        Ok(())
    }

    fn blocked_incoming(session: &Session) -> u64 {
        session
            .stats
            .counters
            .blocked_incoming
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // An incoming connection passes the rule an outgoing one does: libtorrent
    // `session_impl::incoming_connection` refuses an endpoint its one
    // `ip_filter` blocks (`peer_blocked_alert::ip_filter`) before reading a
    // byte, and the blocklist and the allowlist are that one filter.
    #[tokio::test(start_paused = true)]
    async fn an_incoming_connection_from_a_filtered_address_is_refused_unread() {
        let dir = TempDir::with_prefix("incoming_filtered").unwrap();
        let session = quiet_session(
            &dir,
            SessionOptions {
                ip_filter: IpFilter {
                    blocked: Arc::new(
                        crate::IpRanges::load_from_file(ip_list(
                            &dir,
                            "block.txt",
                            "10.0.0.1-10.0.0.1",
                        ))
                        .await
                        .unwrap(),
                    ),
                    ..IpFilter::default()
                },
                allowlist_url: Some(
                    url::Url::from_file_path(ip_list(&dir, "allow.txt", "10.0.0.0-10.0.0.255"))
                        .unwrap()
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .await;

        // On the blocklist, then off the allowlist: each refused unread, and
        // counted.
        for (counted, addr) in [(1, "10.0.0.1:6881"), (2, "192.0.2.1:6881")] {
            let refusal = check_once(&session, addr).expect("refused unread");
            assert!(refusal.contains("banned by the IP filter"), "{refusal}");
            assert_eq!(blocked_incoming(&session), counted);
        }

        // Let through, the check waits on the peer's handshake.
        assert_eq!(check_once(&session, "10.0.0.2:6881"), None);
        assert_eq!(blocked_incoming(&session), 2);
        session.stop().await;
    }

    // An IPv4-only session refuses an IPv6 peer whichever way the connection
    // goes. libtorrent's session has no listen socket of a family it does not
    // connect over, so `incoming_connection` never sees such a peer and posts
    // no `peer_blocked_alert` for it: the refusal is not counted as blocked.
    #[tokio::test(start_paused = true)]
    async fn an_ipv6_incoming_connection_on_an_ipv4_only_session_is_refused() {
        let dir = TempDir::with_prefix("incoming_family").unwrap();
        let session = quiet_session(
            &dir,
            SessionOptions {
                ipv4_only: true,
                ..Default::default()
            },
        )
        .await;

        let refusal = check_once(&session, "[2001:db8::1]:6881").expect("refused unread");
        assert!(
            refusal.contains("of a family the session does not connect over"),
            "{refusal}"
        );
        assert_eq!(blocked_incoming(&session), 0);

        assert_eq!(check_once(&session, "192.0.2.1:6881"), None);
        session.stop().await;
    }

    // libtorrent `session_impl::set_ip_filter` and `update_privileged_ports`
    // take hold in a running session: an incoming connection is judged by
    // the filter as it is when it arrives, its address against the ranges
    // and its port against `no_connect_privileged_ports`.
    #[tokio::test(start_paused = true)]
    async fn a_running_session_judges_incoming_connections_by_its_filter_as_it_changes() {
        let dir = TempDir::with_prefix("incoming_refiltered").unwrap();
        let session = quiet_session(&dir, SessionOptions::default()).await;
        assert_eq!(check_once(&session, "10.0.0.1:6881"), None);
        assert_eq!(check_once(&session, "10.0.0.2:80"), None);

        let blocked = Arc::new(crate::IpRanges::new(
            ["10.0.0.1".parse().unwrap().."10.0.0.2".parse().unwrap()],
            [],
        ));
        session.set_ip_filter(IpFilter {
            blocked: Arc::clone(&blocked),
            no_privileged_ports: true,
            trackers: true,
        });
        let refusal = check_once(&session, "10.0.0.1:6881").expect("refused unread");
        assert!(refusal.contains("banned by the IP filter"), "{refusal}");
        let refusal = check_once(&session, "10.0.0.2:80").expect("refused unread");
        assert!(refusal.contains("privileged port"), "{refusal}");
        assert_eq!(check_once(&session, "10.0.0.2:1024"), None);
        assert_eq!(blocked_incoming(&session), 1);

        session.set_ip_filter(IpFilter::default());
        assert_eq!(check_once(&session, "10.0.0.1:6881"), None);
        assert_eq!(check_once(&session, "10.0.0.2:80"), None);
        session.stop().await;
    }

    // libtorrent `torrent::build_tracker_request` hands an announce the IP
    // filter only under `apply_ip_filter_to_trackers`: off, a tracker is
    // asked at an address the filter blocks; the family rule holds either
    // way.
    #[tokio::test(start_paused = true)]
    async fn trackers_are_held_to_the_ip_filter_only_while_it_applies_to_them() {
        use crate::tracker::Unreachable;
        let dir = TempDir::with_prefix("tracker_filter").unwrap();
        let session = quiet_session(
            &dir,
            SessionOptions {
                ipv4_only: true,
                ..Default::default()
            },
        )
        .await;
        let reachable = session.reachable();
        let blocked: std::net::IpAddr = "10.0.0.1".parse().unwrap();
        let filter = |trackers| IpFilter {
            blocked: Arc::new(crate::IpRanges::new(
                ["10.0.0.1".parse().unwrap().."10.0.0.2".parse().unwrap()],
                [],
            )),
            no_privileged_ports: false,
            trackers,
        };

        session.set_ip_filter(filter(true));
        assert_eq!(reachable(blocked), Err(Unreachable::Filtered));
        session.set_ip_filter(filter(false));
        assert_eq!(reachable(blocked), Ok(()));
        assert_eq!(
            reachable("2001:db8::1".parse().unwrap()),
            Err(Unreachable::Family)
        );
        // Peers stay held to it.
        assert_eq!(session.may_reach(blocked), Err(Unreachable::Filtered));
        session.stop().await;
    }

    #[test]
    fn a_fetch_goes_on_from_its_trackers_once_they_named_no_peer() {
        use super::FetchPhase::{self, Discovery, Trackers, Waiting};
        use crate::{AnnounceError, AnnounceErrorKind, AnnounceOk, LastAnnounce, TrackerAnnounce};

        let answered = |peers| TrackerAnnounce {
            last: Some(LastAnnounce {
                at: std::time::SystemTime::UNIX_EPOCH,
                result: Ok(AnnounceOk {
                    interval: std::time::Duration::from_secs(1800),
                    min_interval: std::time::Duration::from_secs(60),
                    seeders: None,
                    leechers: None,
                    downloaded: None,
                    peers,
                    warning: None,
                }),
            }),
            ..Default::default()
        };
        let failed = || TrackerAnnounce {
            last: Some(LastAnnounce {
                at: std::time::SystemTime::UNIX_EPOCH,
                result: Err(AnnounceError {
                    kind: AnnounceErrorKind::Failed,
                    message: "refused".to_owned(),
                }),
            }),
            ..Default::default()
        };
        let unasked = TrackerAnnounce::default;

        for (table, discovery, phase) in [
            (vec![], true, Discovery),
            (vec![], false, Waiting),
            (vec![unasked()], true, Trackers),
            (vec![failed(), unasked()], true, Trackers),
            (vec![failed(), failed()], true, Discovery),
            (vec![failed(), failed()], false, Waiting),
            (vec![answered(0)], true, Discovery),
            (vec![answered(0), unasked()], false, Waiting),
            (vec![answered(1)], true, Trackers),
            (vec![failed(), answered(2)], false, Trackers),
        ] {
            assert_eq!(
                FetchPhase::of(&table, discovery),
                phase,
                "{table:?}, discovery {discovery}"
            );
        }
    }
}
