// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::peer_sources::MetadataFetch;
use std::{
    borrow::Cow,
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::Identity;
use crate::buffers::ByteBufOwned;
use crate::core::{
    magnet::Magnet,
    net::{HttpClients, Resolver},
    torrent_metainfo::{ValidatedTorrentMetaV1Info, WebSeed},
};
use crate::dht::{DhtPersistenceConfig, Id20};
use crate::ssl::SslIdentity;
use crate::tracker::{HttpAnnounces, Tracker, TrackerHandover};
use crate::upnp::PortMapEvents;
use crate::{
    connection_slots::{
        ConnectionSlots, DEFAULT_CONNECTION_SPEED, DEFAULT_CONNECTIONS_LIMIT,
        DEFAULT_CONNECTIONS_SLACK,
    },
    ip_ranges::IpRanges,
    limits::LimitsConfig,
    listen::ListenerOptions,
    network::SocketFactory,
    peer_connection::PeerConnectionOptions,
    piece_priority::{FilePriority, PiecePriority},
    storage::BoxStorageFactory,
    stream_connect::ConnectionOptions,
    torrent_state::{ManagedTorrentHandle, TorrentTotals, initializing::Checkpoint},
    type_aliases::BF,
};
use anyhow::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{SUPPORTED_SCHEMES, TorrentId, read_local_file_including_stdin};

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

    /// Of `initial_peers`, those a save found elsewhere than the torrent's
    /// trackers and the operator (see
    /// [`ResumePeers::found`](crate::ResumePeers::found)). They come back
    /// marked so: a private torrent keeps them only until a tracker
    /// answers, as before the restart.
    #[serde(default)]
    pub found_peers: Vec<SocketAddr>,

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

    /// The torrent's trackers, in tiers, each with who named it, in place of
    /// those its metainfo or magnet link names, as a torrent resumed with
    /// its edited list announces to that list. [`Self::trackers`] still
    /// follow them.
    pub tracker_tiers: Option<Vec<Vec<Tracker>>>,

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
    /// What the torrent moved before this session took it, as another
    /// session's [`ManagedTorrent::totals`] last read: its counters, and
    /// what its trackers hear, go on from there.
    #[serde(skip)]
    pub totals: TorrentTotals,
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
    /// Neither announce the torrent on the DHT nor take peers from it,
    /// while the session runs it for others (libtorrent
    /// `torrent_flags::disable_dht`). Off by default.
    #[serde(default)]
    pub disable_dht: bool,
    /// Neither announce the torrent on the local network nor take peers
    /// found there (libtorrent `torrent_flags::disable_lsd`). Off by
    /// default.
    #[serde(default)]
    pub disable_lsd: bool,
    /// Neither advertise peer exchange to the torrent's peers, nor send
    /// them its messages, nor read theirs (libtorrent
    /// `torrent_flags::disable_pex`). Off by default.
    #[serde(default)]
    pub disable_pex: bool,
    /// Seed the files where the storage has them without checking them
    /// first: every piece is taken as had, whatever [`Self::checkpoint`]
    /// says, and each is checked the first time a peer asks for it. A piece
    /// that fails ends seed mode and every piece is checked again; seed
    /// mode also ends once every piece passed, or at any check of every
    /// piece (libtorrent `torrent_flags::seed_mode`). Off by default.
    #[serde(default)]
    pub seed_mode: bool,
    /// Under [`Self::seed_mode`], the pieces that passed their check before,
    /// as [`ManagedTorrent::seed_verified`] gave them: a peer's request for
    /// one is served without a second check (libtorrent's `verified_pieces`
    /// in resume data). Not read without seed mode.
    #[serde(skip)]
    pub seed_verified: Option<BF>,
    /// A refused write puts the torrent in upload mode until its owner
    /// calls the live run's `leave_upload_mode`, with no timed retry: the
    /// owner hears every write its storage refuses, watches the disk for
    /// one refused for want of room, and stops the torrent for any other.
    /// Off by default, which retries every refused write after ten minutes
    /// (libtorrent `optimistic_disk_retry`).
    #[serde(default)]
    pub owner_wakes_full_disk: bool,
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
            found_peers: Vec::new(),
            web_seeds: Vec::new(),
            peer_limit: None,
            preferred_id: None,
            storage_factory: None,
            trackers: None,
            tracker_tiers: None,
            checkpoint: None,
            piece_priorities: None,
            identity: None,
            totals: TorrentTotals::default(),
            disable_v1_hashes: v1_hashes_disabled_by_default(),
            super_seeding: false,
            disable_dht: false,
            disable_lsd: false,
            disable_pex: false,
            seed_mode: false,
            seed_verified: None,
            owner_wakes_full_disk: false,
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
    /// The web seeds the add named beside the metainfo:
    /// [`AddTorrentOptions::web_seeds`], then a magnet link's `ws`. They are not in
    /// `torrent_bytes`; the run that follows takes them as its
    /// [`AddTorrentOptions::web_seeds`] (libtorrent keeps a magnet's
    /// `add_torrent_params::url_seeds` across `torrent::set_metadata`).
    pub web_seeds: Vec<WebSeed>,
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
#[derive(Default)]
pub struct DhtSessionConfig {
    /// Bootstrap nodes (host:port or ip:port). Uses built-in defaults if None.
    pub bootstrap_addrs: Option<Vec<String>>,
    /// The DHT listen port. Priority: this explicit port -> persisted port
    /// (when persistence is enabled) -> random. The bind IP is derived from
    /// `SessionOptions::ipv4_only` (`0.0.0.0` if true, `[::]` otherwise).
    /// The socket is bound through `SessionOptions::socket_factory`.
    pub port: Option<u16>,
    /// Persistence behavior. If None, persistence is disabled.
    pub persistence: Option<DhtPersistenceConfig>,
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

/// The limits the sessions of one process share: one connection budget
/// within the process's open files, and one turn at a time to hash a
/// torrent's files (libtorrent's `connections_limit`, `connection_speed`
/// and `active_checking`, which its one session holds for the process).
/// Setting a sharing session's limits sets them for all.
#[derive(Clone)]
pub struct SharedLimits {
    pub(super) slots: Arc<ConnectionSlots>,
    pub(super) checks: Arc<tokio::sync::Semaphore>,
    pub(super) http_announces: HttpAnnounces,
}

impl SharedLimits {
    /// Connections within `limits`, the slack past them as
    /// [`SessionOptions::connections_slack`], and `concurrent_checks`
    /// torrents hashing their files at once (at least one).
    pub fn new(limits: SessionLimits, connections_slack: usize, concurrent_checks: usize) -> Self {
        Self {
            slots: Arc::new(ConnectionSlots::new(
                limits.connections_limit,
                connections_slack,
                limits.connection_speed,
            )),
            checks: Arc::new(tokio::sync::Semaphore::new(concurrent_checks.max(1))),
            http_announces: HttpAnnounces::default(),
        }
    }
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
    pub(super) fn refuses_port(&self, port: u16) -> bool {
        self.no_privileged_ports && port < 1024
    }
}

pub struct SessionOptions {
    /// DHT configuration. Set to None to disable DHT entirely.
    /// Defaults to DHT enabled with persistence.
    pub dht: Option<DhtSessionConfig>,

    /// The network device LSD binds to, and port mapping maps on. The
    /// session's other sockets are `socket_factory`'s.
    /// On OSX will use IP(V6)_BOUND_IF, on Linux will use SO_BINDTODEVICE.
    pub bind_device_name: Option<String>,

    /// Disable tracker communication
    pub disable_trackers: bool,

    /// How long the `stopped` announce a torrent sends to its trackers when it
    /// is paused, removed or the session stops may take, once the torrent's
    /// connections have closed. Session shutdown waits at most this long for
    /// them, and [`StoppedAnnounces::RUN_END_TIMEOUT`] for the connections.
    /// Defaults to [`StoppedAnnounces::DEFAULT_TIMEOUT`].
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

    /// How many torrents hash their files at once (libtorrent
    /// `settings_pack::active_checking`, qBittorrent
    /// `MaxActiveCheckingTorrents`); 1 when unset, both their default. A
    /// torrent its resume data vouches for does not wait for it.
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

    /// The connection budget and check turns this session shares with the
    /// other sessions of its process, in place of its own: the process
    /// has one descriptor limit and one disk, as libtorrent's one session
    /// does. Its `connections_limit`, `connections_slack`,
    /// `connection_speed` and `concurrent_init_limit` are then the shared
    /// ones'.
    pub shared_limits: Option<SharedLimits>,

    /// Disable LSD multicast
    pub disable_local_service_discovery: bool,

    /// A test's LSD socket in place of the multicast one.
    #[cfg(test)]
    pub(crate) lsd_socket: Option<Box<dyn crate::lsd::Socket>>,

    /// A test's way to join the LSD's groups in place of the multicast
    /// one; before `lsd_socket`.
    #[cfg(test)]
    pub(crate) lsd_join: Option<crate::lsd::Join>,

    /// Leave peer exchange (ut_pex) out: it is not advertised to peers, and
    /// none is sent or read, as in libtorrent without the ut_pex plugin.
    pub disable_pex: bool,

    /// Force IPv4 only.
    pub ipv4_only: bool,

    /// Every socket the session opens toward the internet: its peers' TCP
    /// dials and listeners, its uTP socket, its DHT socket, and its UDP
    /// trackers' socket. See [`SocketFactory`] for what it covers;
    /// [`KernelSockets`](crate::network::KernelSockets) opens the kernel's.
    pub socket_factory: Arc<dyn SocketFactory>,

    /// The resolver for every name the session looks up: DHT bootstrap
    /// nodes, UDP and HTTP trackers, and web seeds, whose addresses are
    /// checked before they are asked. A session given none looks no name up
    /// ([`NoNames`]).
    pub resolver: Option<Arc<dyn Resolver>>,

    /// The HTTP clients the session asks through: it builds none itself.
    /// Their client for no target downloads torrents and the allow list; a
    /// tracker or a web seed is asked through a client for the addresses
    /// the session checked its host at; a port mapper asks the gateway
    /// through their gateway client and socket. `bind_device_name` and the
    /// identity's User-Agent do not apply to them; configure them on the
    /// clients. A session given none makes no HTTP request ([`NoClients`]).
    ///
    /// The client for no target does its own DNS lookups:
    /// [`SessionOptions::resolver`] does not apply to it. If those lookups must be routed too, build it
    /// with its own resolver (`reqwest::ClientBuilder::dns_resolver`).
    pub http_client: Option<Arc<dyn HttpClients>>,

    /// Message stream encryption for peer connections, outgoing and incoming.
    /// Defaults to [`EncryptionPolicy::Disabled`](crate::EncryptionPolicy::Disabled).
    pub encryption: crate::mse::EncryptionPolicy,

    /// Whether peers at one address may each hold a connection to a torrent
    /// (libtorrent `allow_multiple_connections_per_ip`). Off by default: one
    /// connection per address.
    pub allow_multiple_connections_per_ip: bool,

    /// Whether a tracker or a web seed whose host has an internationalized
    /// (IDNA, `xn--`) label may be asked, on its first hop and on every
    /// redirect (libtorrent `allow_idna`, off by default, which checks HTTP
    /// trackers and web seeds; UDP trackers are not checked). Off by
    /// default: such a host is refused before its name is looked up.
    pub allow_idna: bool,

    /// Where the session also sends each report its port mappers make, as
    /// they make it.
    pub port_map_events: Option<PortMapEvents>,

    /// Rung as any announce table of the session changes, and as a magnet's
    /// metadata fetch moves to another phase.
    pub trackers_changed: Option<Arc<Notify>>,
}

impl SessionOptions {
    /// The default options of a session whose sockets `socket_factory`
    /// opens.
    pub fn new(socket_factory: Arc<dyn SocketFactory>) -> Self {
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
            shared_limits: None,
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
            #[cfg(test)]
            lsd_join: None,
            disable_pex: false,
            ipv4_only: false,
            socket_factory,
            resolver: None,
            http_client: None,
            encryption: crate::mse::EncryptionPolicy::Disabled,
            allow_multiple_connections_per_ip: false,
            allow_idna: false,
            port_map_events: None,
            trackers_changed: None,
        }
    }
}
