// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub mod initializing;
pub mod live;
pub mod paused;
pub(crate) mod seed_mode;
pub mod stats;
mod streaming;
pub mod utils;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::buffers::ByteBufOwned;
use crate::core::hash_id::{Id20, Id32, InfoHashes};
use crate::core::lengths::{Lengths, ValidPieceIndex};
use crate::core::magnet::Magnet;
use crate::core::torrent_metainfo::{TorrentMetaV1Owned, TorrentVersion, WebSeed, WebSeedKind};
use crate::peer_protocol::HashRequest;
use crate::v2_hashes::V2Hashes;
use anyhow::Context;
use anyhow::bail;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;

use crate::core::spawn_utils::spawn_with_cancel;
use crate::core::torrent_metainfo::ValidatedTorrentMetaV1Info;
pub use live::*;
use parking_lot::RwLock;

use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::warn;

use crate::Identity;
use crate::Session;
use crate::chunk_tracker::ChunkTracker;
use crate::file_info::FileInfo;
use crate::limits::{Limits, LimitsConfig};
use crate::piece_priority::{
    FilePriority, PiecePriority, file_priorities, file_to_piece_priorities,
    validate_piece_priorities, wanted_files,
};
use crate::session::TorrentId;
use crate::spawn_utils::BlockingSpawner;
use crate::storage::BoxStorageFactory;
use crate::stream_connect::StreamConnector;
use crate::torrent_state::stats::LiveStats;
use crate::type_aliases::BF;
use crate::type_aliases::FileInfos;
use crate::type_aliases::PeerSource;
use crate::type_aliases::PeerStream;

use initializing::TorrentStateInitializing;

use self::paused::TorrentStatePaused;
pub use self::stats::{TorrentStats, TorrentStatsState, TorrentTotals};
pub use self::streaming::FileStream;

// State machine transitions.
//
// - error -> initializing
// - initializing -> paused
// - paused -> live
// - live -> paused
//
// - initializing -> error
// - live -> error
pub enum ManagedTorrentState {
    Initializing(Arc<TorrentStateInitializing>),
    Paused(Box<TorrentStatePaused>),
    Live(Arc<TorrentStateLive>),
    Error(anyhow::Error),

    // This is used when swapping between states, outside world should never see it.
    None,
}

impl ManagedTorrentState {
    pub fn name(&self) -> &'static str {
        match self {
            ManagedTorrentState::Initializing(_) => "initializing",
            ManagedTorrentState::Paused(_) => "paused",
            ManagedTorrentState::Live(_) => "live",
            ManagedTorrentState::Error(_) => "error",
            ManagedTorrentState::None => "<invalid: none>",
        }
    }

    fn assert_paused(self) -> TorrentStatePaused {
        match self {
            Self::Paused(paused) => *paused,
            _ => panic!("Expected paused state"),
        }
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::None)
    }
}

pub(crate) struct ManagedTorrentLocked {
    // The torrent might not be in "paused" state technically,
    // but the intention might be for it to stay paused.
    //
    // This should change only on "unpause".
    pub(crate) paused: bool,
    pub(crate) state: ManagedTorrentState,
    // Each file's level, padding at Ignored, and whether the first and last
    // pieces of each wanted file go first: what the pieces' priorities are
    // derived from (qBittorrent `TorrentImpl::m_filePriorities` and
    // `m_hasFirstLastPiecePriority`).
    pub(crate) files: Vec<FilePriority>,
    pub(crate) first_last: bool,
    // The fetch priority of every piece: the files' rule at each change to
    // the files, with any piece set since over it (libtorrent keeps one
    // priority a piece, which a change to the file priorities rewrites
    // whole, `torrent::update_piece_priorities`).
    pub(crate) piece_priorities: Vec<PiecePriority>,
    // The announce table of the trackers of the latest live run. It outlives
    // the run, so the last outcomes stay readable while paused.
    pub(crate) trackers: Option<crate::tracker::TrackerStatus>,
}

#[derive(Default)]
pub(crate) struct ManagedTorrentOptions {
    pub force_tracker_interval: Option<Duration>,
    // The session's peer connection options with the torrent's own over
    // them; every connection of the torrent takes them whole.
    pub peer_opts: crate::peer_connection::PeerConnectionOptions,
    pub allow_overwrite: bool,
    pub output_folder: PathBuf,
    // The torrent's own rate limits. Every live run acquires through this one
    // object, so a change reaches the running peers and outlives pause/unpause.
    pub ratelimits: Limits,
    pub initial_peers: Vec<SocketAddr>,
    pub peer_limit: Option<usize>,
    /// [`AddTorrentOptions::disable_v1_hashes`](crate::AddTorrentOptions::disable_v1_hashes).
    pub disable_v1_hashes: bool,
    /// [`AddTorrentOptions::super_seeding`](crate::AddTorrentOptions::super_seeding).
    pub super_seeding: bool,
    /// [`AddTorrentOptions::sequential`](crate::AddTorrentOptions::sequential).
    /// Every live run picks through this one flag, so a change reaches the
    /// running peers and outlives pause/unpause.
    pub sequential: AtomicBool,
    /// [`AddTorrentOptions::trackers_only`](crate::AddTorrentOptions::trackers_only).
    pub trackers_only: bool,
    /// [`AddTorrentOptions::disable_dht`](crate::AddTorrentOptions::disable_dht).
    pub disable_dht: bool,
    /// [`AddTorrentOptions::disable_lsd`](crate::AddTorrentOptions::disable_lsd).
    pub disable_lsd: bool,
    /// [`AddTorrentOptions::disable_pex`](crate::AddTorrentOptions::disable_pex).
    pub disable_pex: bool,
    /// [`AddTorrentOptions::seed_mode`](crate::AddTorrentOptions::seed_mode),
    /// while it lasts. Every run checks through this one record, so a piece
    /// checked once is not checked again after a pause.
    pub seed: seed_mode::SeedMode,
}

impl ManagedTorrentOptions {
    pub const fn disable_upload(&self) -> bool {
        false
    }
}

// Torrent bencodee "info" + some precomputed fields based on it for frequent access.
pub struct TorrentMetadata {
    pub info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
    /// The torrent file as added, or as built from a magnet link.
    torrent_file: Bytes,
    /// `torrent_file` with the piece layers fetched from peers, once every
    /// one is held.
    torrent_file_with_layers: OnceLock<Bytes>,
    pub info_bytes: Bytes,
    /// The hashes of `info_bytes`: v1, v2 or both.
    pub info_hashes: InfoHashes,
    pub file_infos: FileInfos,
    /// The v2 hashes of a v2 or hybrid torrent.
    pub(crate) v2: Option<V2Hashes>,
    /// The web seeds the torrent file names; none for a magnet link, whose
    /// metadata carries none.
    pub web_seeds: Vec<WebSeed>,
}

impl TorrentMetadata {
    /// `layers` are the piece layers the torrent carries, one entry per
    /// file ([`ValidatedTorrentMetaV1Info::piece_layers`]); none for a
    /// magnet link.
    pub(crate) fn new(
        info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
        torrent_bytes: Bytes,
        info_bytes: Bytes,
        layers: Vec<Option<Vec<Id32>>>,
    ) -> anyhow::Result<Self> {
        let file_infos = info
            .iter_file_details_ext()
            .map(|fd| {
                Ok::<_, anyhow::Error>(FileInfo {
                    relative_filename: fd.details.filename.to_pathbuf(),
                    offset_in_torrent: fd.offset,
                    piece_range: fd.pieces,
                    len: fd.details.len,
                    attrs: fd.details.attrs(),
                })
            })
            .collect::<anyhow::Result<Vec<FileInfo>>>()?;

        Ok(Self {
            info_hashes: info.info().check_info_bytes(&info_bytes)?,
            v2: V2Hashes::new(&info, layers),
            info,
            torrent_file: torrent_bytes,
            torrent_file_with_layers: OnceLock::new(),
            info_bytes,
            file_infos,
            web_seeds: Vec::new(),
        })
    }

    /// The torrent file: as added, and with the piece layers fetched from
    /// peers once every one is held (libtorrent `write_torrent_file`, which
    /// writes `piece layers` from the trees it holds).
    pub fn torrent_bytes(&self) -> Bytes {
        self.torrent_file_with_layers
            .get()
            .unwrap_or(&self.torrent_file)
            .clone()
    }

    /// Which hashes verify the torrent's pieces.
    pub fn version(&self) -> TorrentVersion {
        self.info.version()
    }

    /// Takes a peer's answer to the request for a run of a piece layer ([`V2Hashes::add_chunk`]); once every layer is held they go
    /// into the torrent file.
    pub(crate) fn add_layer_chunk(
        &self,
        request: &HashRequest,
        hashes: &[Id32],
    ) -> anyhow::Result<()> {
        let v2 = self
            .v2
            .as_ref()
            .context("a v1 torrent has no piece layers")?;
        if v2.add_chunk(request, hashes)?
            && let Some(layers) = v2.layers()
        {
            let _ = self
                .torrent_file_with_layers
                .set(with_piece_layers(&self.torrent_file, &layers)?.into());
        }
        Ok(())
    }

    /// The metadata of a torrent file, with the piece layers it carries.
    pub(crate) fn from_torrent(
        meta: TorrentMetaV1Owned,
        torrent_bytes: Bytes,
    ) -> anyhow::Result<Self> {
        let info = meta.info.data.validate()?;
        let layers = match &meta.piece_layers {
            Some(layers) => info.piece_layers(layers)?,
            None => Vec::new(),
        };
        let multi_file = info.iter_file_details().nth(1).is_some();
        let web_seeds = crate::core::torrent_metainfo::web_seeds(
            meta.url_list.as_ref(),
            meta.httpseeds.as_ref(),
            multi_file,
        );
        Ok(Self {
            web_seeds,
            ..Self::new(info, torrent_bytes, meta.info.raw_bytes.0, layers)?
        })
    }

    pub fn lengths(&self) -> &Lengths {
        self.info.lengths()
    }

    /// Whether a piece fetched now could be checked: always with v1
    /// hashes, and with v2 alone once its file's piece layer is held.
    /// libtorrent fetches such a piece anyway and holds it unverified until
    /// `hash_picker::pick_hashes` brings the layer; Weaver fetches the
    /// layer first, so a piece is never held without a verdict.
    pub(crate) fn can_verify(&self, piece: ValidPieceIndex) -> bool {
        self.info.info().has_v1_listing()
            || self
                .v2
                .as_ref()
                .is_some_and(|v2| v2.expected(piece.get()).is_some())
    }
}

/// `torrent`, a torrent file, with `layers`, each file's root and piece
/// layer, as its `piece layers`, in place of any it had.
fn with_piece_layers(torrent: &[u8], layers: &[(Id32, &[Id32])]) -> anyhow::Result<Vec<u8>> {
    use crate::bencode::{
        ByteBuf, WithRawBytes, bencode_serialize_to_writer, from_bytes, raw_value::RawValue,
    };
    use std::collections::BTreeMap;

    let top: BTreeMap<ByteBuf<'_>, WithRawBytes<serde::de::IgnoredAny, ByteBuf<'_>>> =
        from_bytes(torrent).map_err(|e| anyhow::anyhow!("{e}"))?;
    let concatenated: Vec<Vec<u8>> = layers
        .iter()
        .map(|(_, layer)| layer.iter().flat_map(|h| h.0).collect())
        .collect();
    let by_root: BTreeMap<ByteBuf<'_>, ByteBuf<'_>> = layers
        .iter()
        .zip(&concatenated)
        .map(|((root, _), layer)| (ByteBuf(&root.0), ByteBuf(layer)))
        .collect();
    let mut piece_layers = Vec::new();
    bencode_serialize_to_writer(&by_root, &mut piece_layers)?;

    let mut out: BTreeMap<ByteBuf<'_>, RawValue<&[u8]>> = top
        .iter()
        .map(|(key, value)| (*key, RawValue(value.raw_bytes.0)))
        .collect();
    out.insert(ByteBuf(b"piece layers"), RawValue(&piece_layers));
    let mut file = Vec::new();
    bencode_serialize_to_writer(&out, &mut file)?;
    Ok(file)
}

/// Common information about torrent shared among all possible states.
///
// The reason it's not inlined into ManagedTorrent is to break the Arc cycle:
// ManagedTorrent contains the current torrent state, which in turn needs access to a bunch
// of stuff, but it shouldn't access the state.
pub struct ManagedTorrentShared {
    pub id: TorrentId,
    /// The hash the torrent is known by, `info_hashes.best()`.
    pub info_hash: Id20,
    /// Every hash the torrent answers to and announces under.
    pub info_hashes: InfoHashes,
    pub(crate) spawner: BlockingSpawner,
    /// The torrent's trackers grouped into tiers (BEP 12), in announce order,
    /// each with who named it; see [`ManagedTorrent::replace_trackers`].
    pub(crate) tracker_tiers: parking_lot::RwLock<Vec<Vec<crate::tracker::Tracker>>>,
    /// Each change starts a new DHT announce of the running torrent
    /// (libtorrent `torrent::dht_announce`).
    pub(crate) dht_announce: tokio::sync::watch::Sender<()>,
    pub peer_id: Id20,
    /// The `key` of its announces, with `peer_id` the torrent's for as long
    /// as it is kept.
    pub(crate) tracker_key: u32,
    pub span: tracing::Span,
    pub(crate) options: ManagedTorrentOptions,
    pub(crate) connector: Arc<StreamConnector>,
    pub(crate) storage_factory: BoxStorageFactory,
    pub(crate) session: Weak<Session>,

    // "dn" from magnet link
    pub(crate) magnet_name: Option<String>,

    // With `peer_id`, how the torrent names itself: the `v` of its extended
    // handshake and the User-Agent of its HTTP announces.
    pub(crate) client_name_and_version: String,
    pub(crate) user_agent: String,
    /// The `tracker id` each tracker gave, kept with the identity.
    pub(crate) tracker_ids: crate::TrackerIds,
    /// Whether a tracker was told the torrent completed, kept with the
    /// identity.
    pub(crate) completion: crate::Completion,
    /// Where its announces stood when a route move took it from its last
    /// session, kept with the identity.
    pub(crate) announcing: crate::Announcing,
    /// How the torrent's announces are written.
    pub(crate) announce_shape: &'static crate::tracker::AnnounceShape,

    /// What the most recent live run left, recorded as the run ends, so a
    /// `stopped` tracker announce made after it ended reports it.
    pub(crate) last_run: parking_lot::Mutex<Option<RunCounters>>,

    /// The tasks of the most recent live run to end, named as it ends: a
    /// `stopped` announce of that run waits for them, so what its closing
    /// connections dropped is in the counters it reports.
    pub(crate) ending_run: parking_lot::Mutex<Option<tokio_util::task::TaskTracker>>,

    /// What the torrent moved before its running run: the totals it was
    /// added with, and every run of it that ended since.
    pub(crate) moved: parking_lot::Mutex<TorrentTotals>,

    /// What the torrent had moved when its running run, or its last, began
    /// announcing. Its announces count from there (libtorrent
    /// `torrent::start_announcing` clears `m_stat`, failed and redundant
    /// bytes, torrent.cpp:10343-10353, v2.0.15). Before its first run, the
    /// totals it was added with; or, when a route move handed its announces
    /// over, where they counted from in the session it left.
    pub(crate) announced_from: parking_lot::Mutex<TorrentTotals>,
    /// A route move handed the torrent's announces over and no run of it has
    /// begun since: its first run goes on from `announced_from`, as a move is
    /// not a start.
    pub(crate) handed_over: std::sync::atomic::AtomicBool,

    /// The torrent's web seeds, kept across its runs.
    pub(crate) web_seeds: Arc<live::web_seed::WebSeeds>,

    /// An SSL torrent's CA and our identity under it; `None` for any other
    /// torrent.
    pub(crate) ssl: Option<Arc<crate::ssl::SslTorrent>>,

    /// The gauges of the torrent's running run; none between runs.
    pub(crate) run: ArcSwapOption<RunGauges>,
}

/// What one live run of a torrent left as it ended, as its tracker
/// announces report it (see `TrackerCommsStats`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RunCounters {
    /// What the whole torrent still lacked as the run ended
    /// (`ChunkTracker::bytes_left`).
    pub left_bytes: u64,
}

impl ManagedTorrentShared {
    pub(crate) fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }

    /// The torrent's trackers grouped into tiers (BEP 12), in announce order.
    pub fn tracker_tiers(&self) -> Vec<Vec<crate::tracker::Tracker>> {
        self.tracker_tiers.read().clone()
    }

    pub fn identity(&self) -> Identity {
        Identity {
            peer_id: self.peer_id,
            tracker_key: self.tracker_key,
            version: self.client_name_and_version.clone(),
            user_agent: self.user_agent.clone(),
            tracker_ids: self.tracker_ids.clone(),
            completion: self.completion.clone(),
            announcing: self.announcing.clone(),
            shape: self.announce_shape,
        }
    }
}

pub struct ManagedTorrent {
    // Static torrent configuration that doesn't change.
    pub shared: Arc<ManagedTorrentShared>,
    // Torrent metadata. Maybe be None when the magnet is resolving (not implemented yet)
    pub metadata: ArcSwapOption<TorrentMetadata>,
    pub(crate) state_change_notify: Notify,
    pub(crate) locked: RwLock<ManagedTorrentLocked>,
}

impl ManagedTorrent {
    pub fn id(&self) -> TorrentId {
        self.shared.id
    }

    pub fn name(&self) -> Option<String> {
        if let Some(m) = &*self.metadata.load() {
            return m
                .info
                .name()
                .map(|n| n.into_owned())
                .or_else(|| self.shared.magnet_name.clone());
        }
        self.shared.magnet_name.clone()
    }

    pub fn shared(&self) -> &ManagedTorrentShared {
        &self.shared
    }

    /// The resolved on-disk folder this torrent's files are written under.
    pub fn output_folder(&self) -> &Path {
        &self.shared.options.output_folder
    }

    /// Whether the torrent takes peers from its trackers alone (see
    /// [`AddTorrentOptions::trackers_only`](crate::AddTorrentOptions::trackers_only)):
    /// its metainfo marks it private, or it was added so.
    pub fn takes_peers_from_trackers_only(&self) -> bool {
        crate::session::trackers_only(
            self.with_metadata(|m| m.info.info().private)
                .unwrap_or(false),
            self.shared.options.trackers_only,
        )
    }

    /// Where the torrent looks for peers beside its trackers: none when it
    /// takes them from its trackers alone, else the DHT and the local
    /// network unless it was added without either.
    pub fn discovery(&self) -> crate::session::Discovery {
        crate::session::Discovery::of(
            self.takes_peers_from_trackers_only(),
            self.shared.options.disable_dht,
            self.shared.options.disable_lsd,
        )
    }

    /// Whether the torrent speaks peer exchange: its session runs it, it
    /// was not added without it, and it takes peers from more than its
    /// trackers.
    pub fn speaks_pex(&self) -> bool {
        crate::session::speaks_pex(
            self.shared
                .session
                .upgrade()
                .is_some_and(|session| !session.disable_pex),
            self.takes_peers_from_trackers_only(),
            self.shared.options.disable_pex,
        )
    }

    /// Whether the torrent is an SSL torrent: its metainfo names the CA its
    /// peers are issued under, and every connection it makes or takes is TLS
    /// under it.
    pub fn is_ssl(&self) -> bool {
        self.shared.ssl.is_some()
    }

    /// An SSL torrent with no identity of ours under its CA: it connects to
    /// no peer and announces nowhere until one is given.
    pub fn needs_certificate(&self) -> bool {
        self.shared
            .ssl
            .as_ref()
            .is_some_and(|ssl| !ssl.has_identity())
    }

    /// The encryption the torrent's connections are made under, over the
    /// session's; `None` is the session's.
    pub fn encryption(&self) -> Option<crate::EncryptionPolicy> {
        self.shared.options.peer_opts.encryption
    }

    pub fn with_metadata<R>(
        &self,
        mut f: impl FnMut(&Arc<TorrentMetadata>) -> R,
    ) -> anyhow::Result<R> {
        let r = self.metadata.load();
        let r = r.as_ref().context("torrent is not resolved")?;
        Ok(f(r))
    }

    pub fn info_hash(&self) -> Id20 {
        self.shared.info_hash
    }

    /// The torrent's v1 and v2 hashes.
    pub fn info_hashes(&self) -> InfoHashes {
        self.shared.info_hashes
    }

    /// Which hashes verify the torrent's pieces; `None` until its metadata
    /// is known.
    pub fn version(&self) -> Option<TorrentVersion> {
        self.with_metadata(|m| m.version()).ok()
    }

    /// The link to the torrent: both its hashes, its name and its trackers
    /// in tier order (libtorrent `make_magnet_uri(torrent_handle)`).
    pub fn magnet(&self) -> Magnet {
        let trackers = self
            .shared
            .tracker_tiers()
            .iter()
            .flatten()
            .map(|t| t.url.to_string())
            .collect();
        let mut magnet = Magnet::of(self.info_hashes(), self.name(), trackers);
        magnet.web_seeds = self
            .web_seeds()
            .into_iter()
            .filter(|s| s.kind == WebSeedKind::Url)
            .map(|s| s.url)
            .collect();
        magnet
    }

    /// The torrent's web seeds, but those banned for sending bad data
    /// (libtorrent `torrent_handle::url_seeds`).
    pub fn web_seeds(&self) -> Vec<WebSeed> {
        self.shared.web_seeds.list()
    }

    /// Adds a web seed, as given; false when the torrent has it (libtorrent
    /// `torrent_handle::add_url_seed`, `add_http_seed`).
    pub fn add_web_seed(&self, seed: WebSeed) -> bool {
        self.shared.web_seeds.add(seed)
    }

    /// Removes a web seed and closes its connection; false when the torrent
    /// has no such seed (libtorrent `torrent_handle::remove_url_seed`,
    /// `remove_http_seed`).
    pub fn remove_web_seed(&self, seed: &WebSeed) -> bool {
        self.shared.web_seeds.remove(seed)
    }

    /// The files fetched at all, by index: those above `Ignored`.
    pub fn only_files(&self) -> Option<Vec<usize>> {
        Some(wanted_files(&self.locked.read().files))
    }

    /// Each file's level, by index; padding is `Ignored`.
    pub fn file_priorities(&self) -> Vec<FilePriority> {
        self.locked.read().files.clone()
    }

    /// Set the level of the given files; files not named keep theirs, and
    /// padding stays `Ignored`. Every piece's priority is derived anew from
    /// the levels, which undoes any set piece by piece (libtorrent
    /// `torrent::prioritize_files`, `update_piece_priorities`). Refused,
    /// with nothing changed, if an index names no file, or while the
    /// initial check runs.
    pub fn set_file_priorities(&self, priorities: &[(usize, FilePriority)]) -> anyhow::Result<()> {
        self.prioritize_files(|files, _| {
            let count = files.len();
            for (index, priority) in priorities.iter().copied() {
                let slot = files.get_mut(index).with_context(|| {
                    format!("file {index} is out of range: the torrent has {count} files")
                })?;
                *slot = priority;
            }
            Ok(())
        })
    }

    /// Whether the first and last pieces of each wanted file go first.
    pub fn first_last_piece_first(&self) -> bool {
        self.locked.read().first_last
    }

    /// Put the first and last ceil(1% of the file / piece length) pieces of
    /// each wanted file at the top level, or, off, back at the file's
    /// (qBittorrent `TorrentImpl::setFirstLastPiecePriority`). As
    /// [`Self::set_file_priorities`], it derives every piece anew.
    pub fn set_first_last_piece_first(&self, on: bool) -> anyhow::Result<()> {
        self.prioritize_files(|_, first_last| {
            *first_last = on;
            Ok(())
        })
    }

    /// Whether pieces are picked in order; see [`Self::set_sequential`].
    pub fn sequential(&self) -> bool {
        self.shared.options.sequential.load(Ordering::Relaxed)
    }

    /// Pick the top-level pieces first, then every other wanted piece by
    /// index, in place of rarest first (libtorrent
    /// `torrent::set_sequential_download`). The running peers take it at
    /// their next pick.
    pub fn set_sequential(&self, on: bool) {
        self.shared.options.sequential.store(on, Ordering::Relaxed);
    }

    // The one path a change to the files takes: `change` edits the levels
    // and the first-and-last switch, every piece's priority is derived from
    // them, and the torrent's pieces take it.
    fn prioritize_files(
        &self,
        change: impl FnOnce(&mut Vec<FilePriority>, &mut bool) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let metadata = metadata.as_ref().context("torrent is not resolved")?;
        let mut g = self.locked.write();
        let mut files = g.files.clone();
        let mut first_last = g.first_last;
        change(&mut files, &mut first_last)?;
        let files = file_priorities(&metadata.file_infos, &files);
        let pieces =
            file_to_piece_priorities(metadata.lengths(), &metadata.file_infos, &files, first_last);
        match &mut g.state {
            ManagedTorrentState::Initializing(_) => bail!("can't update initializing torrent"),
            ManagedTorrentState::Error(_) => {}
            ManagedTorrentState::None => {}
            ManagedTorrentState::Paused(p) => p.prioritize(&files, &pieces),
            ManagedTorrentState::Live(l) => l.prioritize(&files, &pieces)?,
        };
        g.files = files;
        g.first_last = first_last;
        g.piece_priorities = pieces;
        Ok(())
    }

    pub fn with_state<R>(&self, f: impl FnOnce(&ManagedTorrentState) -> R) -> R {
        f(&self.locked.read().state)
    }

    /// The fetch priority of a piece, or None if the index is out of range or
    /// the torrent is not resolved yet.
    pub fn piece_priority(&self, piece: u32) -> Option<PiecePriority> {
        let metadata = self.metadata.load();
        metadata.as_ref()?.lengths().validate_piece_index(piece)?;
        self.locked
            .read()
            .piece_priorities
            .get(piece as usize)
            .copied()
    }

    /// Set the fetch priority of the given pieces; see [`PiecePriority`]. Pieces not
    /// named keep theirs. Refused, with nothing changed, if any index is out of range,
    /// or while the initial check runs.
    pub fn set_piece_priorities(&self, priorities: &[(u32, PiecePriority)]) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let metadata = metadata.as_ref().context("torrent is not resolved")?;
        let priorities = validate_piece_priorities(metadata.lengths(), priorities)?;

        let mut g = self.locked.write();
        match &mut g.state {
            ManagedTorrentState::Initializing(_) => bail!("can't update initializing torrent"),
            ManagedTorrentState::Error(_) => {}
            ManagedTorrentState::None => {}
            ManagedTorrentState::Paused(p) => {
                p.set_piece_priorities(priorities.iter().copied());
            }
            ManagedTorrentState::Live(l) => {
                l.set_piece_priorities(priorities.iter().copied())?;
            }
        };

        for (index, priority) in priorities {
            g.piece_priorities[index.get_usize()] = priority;
        }
        Ok(())
    }

    /// The torrent's upload and download caps; None is unlimited.
    pub fn ratelimits(&self) -> LimitsConfig {
        self.shared.options.ratelimits.get_config()
    }

    /// Change the torrent's upload and download caps (None is unlimited). Takes
    /// effect on the next acquire of every connected peer, without reconnecting,
    /// and is kept across pause/unpause. Use `Session::update_ratelimits` to also
    /// persist it.
    pub fn set_ratelimits(&self, limits: LimitsConfig) {
        self.shared.options.ratelimits.set_config(limits);
    }

    pub(crate) fn with_state_mut<R>(&self, f: impl FnOnce(&mut ManagedTorrentState) -> R) -> R {
        let mut g = self.locked.write();
        let r = f(&mut g.state);
        self.state_change_notify.notify_waiters();
        r
    }

    // The torrent's next change of state from now on. The wait is enabled
    // here, so a change between this call and its await is not missed.
    pub(crate) fn next_state_change(&self) -> Pin<Box<Notified<'_>>> {
        let mut changed = Box::pin(self.state_change_notify.notified());
        changed.as_mut().enable();
        changed
    }

    pub(crate) fn with_chunk_tracker<R>(
        &self,
        f: impl FnOnce(&ChunkTracker) -> R,
    ) -> anyhow::Result<R> {
        let g = self.locked.read();
        match &g.state {
            ManagedTorrentState::Paused(p) => Ok(f(&p.chunk_tracker)),
            ManagedTorrentState::Live(l) => Ok(f(l
                .lock_read("chunk_tracker")
                .get_chunks()
                .context("error getting chunks")?)),
            _ => bail!("no chunk tracker, torrent neither paused nor live"),
        }
    }

    /// What the torrent has written of its data, for a later run to start
    /// from (libtorrent `torrent::write_resume_data`); None while it is
    /// checked or stopped by an error.
    pub fn checkpoint(&self) -> Option<initializing::Checkpoint> {
        match &self.locked.read().state {
            ManagedTorrentState::Paused(p) => Some(
                p.chunk_tracker
                    .checkpoint(&*p.files, p.metadata.file_infos.len()),
            ),
            ManagedTorrentState::Live(l) => l.checkpoint().ok(),
            _ => None,
        }
    }

    /// The peers and bans a restart is to begin with (libtorrent
    /// `torrent::write_resume_data`); None while it is stopped by an error.
    pub fn resume_peers(&self) -> Option<live::peers::ResumePeers> {
        self.with_state(|s| match s {
            ManagedTorrentState::Initializing(init) => Some(init.resume_peers()),
            ManagedTorrentState::Paused(paused) => Some(paused.peers.resume_peers()),
            ManagedTorrentState::Live(live) => Some(live.resume_peers()),
            ManagedTorrentState::Error(_) | ManagedTorrentState::None => None,
        })
    }

    /// The torrent's connections to peers, by address (libtorrent
    /// `torrent_handle::get_peer_info`); none while it is not live.
    pub fn peers(&self) -> Vec<live::peer::info::PeerInfo> {
        self.live()
            .map(|live| live.peer_infos())
            .unwrap_or_default()
    }

    /// Bans each of `ips` on the torrent's peer list: a connection to a
    /// peer at one closes now, and none is made or taken from then on,
    /// across a pause and a restart (libtorrent `torrent::ban_peer`).
    /// Returns the peers and bans its next start begins with, to be saved;
    /// None while the torrent is stopped by an error and holds no list.
    pub fn ban_peers(&self, ips: &[std::net::IpAddr]) -> Option<live::peers::ResumePeers> {
        // Under the state lock, so a pause or start moves the list with the
        // bans in it.
        let mut g = self.locked.write();
        match &mut g.state {
            ManagedTorrentState::Initializing(init) => {
                ips.iter().for_each(|ip| init.ban_peer(*ip));
                Some(init.resume_peers())
            }
            ManagedTorrentState::Paused(paused) => {
                ips.iter().for_each(|ip| paused.peers.ban(*ip));
                Some(paused.peers.resume_peers())
            }
            ManagedTorrentState::Live(live) => {
                ips.iter().for_each(|ip| live.ban_peer(*ip));
                Some(live.resume_peers())
            }
            ManagedTorrentState::Error(_) | ManagedTorrentState::None => None,
        }
    }

    /// Get the live state if the torrent is live.
    /// The gauges of the torrent's running run, read without its lock; none
    /// while it does not run.
    pub fn gauges(&self) -> Option<Arc<RunGauges>> {
        self.shared.run.load_full()
    }

    pub fn live(&self) -> Option<Arc<TorrentStateLive>> {
        let g = self.locked.read();
        match &g.state {
            ManagedTorrentState::Live(live) => Some(live.clone()),
            _ => None,
        }
    }

    // Get live torrent but wait a bit until it's initialized if it is
    pub(crate) async fn live_wait_initializing(
        &self,
        duration: Duration,
    ) -> Option<Arc<TorrentStateLive>> {
        timeout(duration, self.wait_until_initialized())
            .await
            .ok()?
            .ok()?;
        self.live()
    }

    fn stop_with_error(&self, error: anyhow::Error) {
        let mut g = self.locked.write();

        match g.state.take() {
            ManagedTorrentState::Live(live) => {
                if let Err(err) = live.pause() {
                    warn!(
                        id = self.shared.id,
                        info_hash = ?self.shared.info_hash,
                        "error pausing live torrent during fatal error handling: {err:#}",
                    );
                }
            }
            ManagedTorrentState::Error(e) => {
                warn!(
                    id = self.shared.id,
                    info_hash = ?self.shared.info_hash,
                    "bug: torrent already was in error state when trying to stop it. Previous error was: {e:#}",
                );
            }
            ManagedTorrentState::None => {
                warn!(
                    id = self.shared.id,
                    info_hash = ?self.shared.info_hash,
                    "bug: torrent encountered in None state during fatal error handling"
                )
            }
            _ => {}
        };

        self.state_change_notify.notify_waiters();

        g.state = ManagedTorrentState::Error(error)
    }

    /// peer_rx: the peer stream. If start_paused=false, must be set.
    /// start_paused: if set, the torrent will initialize (check file integrity), but will not start
    pub(crate) fn start(
        self: &Arc<Self>,
        peer_rx: Option<PeerSource>,
        start_paused: bool,
    ) -> anyhow::Result<()> {
        fn _start<'a>(
            t: &'a Arc<ManagedTorrent>,
            peer_rx: Option<PeerSource>,
            start_paused: bool,
            session: Arc<Session>,
            g: Option<parking_lot::RwLockWriteGuard<'a, ManagedTorrentLocked>>,
            token: CancellationToken,
        ) -> anyhow::Result<()> {
            let mut g = g.unwrap_or_else(|| t.locked.write());

            match &g.state {
                ManagedTorrentState::Live(_) => {
                    bail!("torrent is already live");
                }
                ManagedTorrentState::Initializing(init) => {
                    let init = init.clone();
                    init.clear_pause_request();
                    if !init.try_start_check() {
                        return Ok(());
                    }

                    let t = t.clone();
                    let span = t.shared().span.clone();
                    let token = token.clone();

                    spawn_with_cancel(
                        debug_span!(parent: span.clone(), "initialize_and_start"),
                        "initialize_and_start",
                        token.clone(),
                        async move {
                            // Only hashing the files waits its turn
                            // (`TorrentStateInitializing::resume_or_check`):
                            // a torrent its resume data vouches for starts
                            // beside the others.
                            // Readers of the state see it move to checking
                            // the files as the check waits its turn.
                            let check_result =
                                init.check(&|| t.state_change_notify.notify_waiters()).await;

                            match check_result {
                                Ok(mut paused) => {
                                    let mut g = t.locked.write();
                                    let ManagedTorrentState::Initializing(checked) = &g.state
                                    else {
                                        debug!(
                                            "no need to start torrent anymore, as it switched state from initializing"
                                        );
                                        return Ok(());
                                    };

                                    paused.peers = checked.take_peers();
                                    t.shared.completion.checked(
                                        paused.chunk_tracker.bytes_left() == 0,
                                        checked.is_full_check(),
                                    );
                                    g.state = ManagedTorrentState::Paused(Box::new(paused));
                                    t.state_change_notify.notify_waiters();
                                    // The checked torrent goes live only if it
                                    // is still asked to run: a pause or start
                                    // made during the check overrides the one
                                    // the check began with (libtorrent
                                    // torrent.cpp:9739-9746 and :2371, v2.0.15,
                                    // gate every step on `m_paused` as it is
                                    // now).
                                    let start_paused = g.paused;
                                    _start(&t, peer_rx, start_paused, session, Some(g), token)
                                }
                                Err(err) => {
                                    if init.is_pause_requested() {
                                        // The torrent stays initializing
                                        // and checks again once started.
                                        // A check that ends otherwise
                                        // leaves the state with the
                                        // torrent it read for, so no
                                        // reader sees it end early.
                                        init.finish_check();
                                        debug!("initial check paused");
                                        t.state_change_notify.notify_waiters();
                                        return Ok(());
                                    }

                                    let result = anyhow::anyhow!("{:?}", err);
                                    t.locked.write().state = ManagedTorrentState::Error(err);
                                    t.state_change_notify.notify_waiters();
                                    Err(result)
                                }
                            }
                        },
                    );
                    Ok(())
                }
                ManagedTorrentState::Paused(_) => {
                    if start_paused {
                        return Ok(());
                    }
                    // An SSL torrent goes live only with an identity to
                    // present; until it has one it waits here, asking no
                    // peer and no tracker.
                    if t.needs_certificate() {
                        return Ok(());
                    }
                    // A run handed no peer source takes the one the session
                    // makes for it now, as it starts.
                    let peer_rx = peer_rx.or_else(|| session.peer_source_managed_torrent(t, true));
                    let paused = g.state.take().assert_paused();
                    // The run's announces count from here, unless it is the
                    // first after a route move handed them over: those go on.
                    if !t
                        .shared
                        .handed_over
                        .swap(false, std::sync::atomic::Ordering::Relaxed)
                    {
                        *t.shared.announced_from.lock() = *t.shared.moved.lock();
                        t.shared.announcing.forget();
                    }
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let live = TorrentStateLive::new(paused, tx, token.clone())?;
                    g.state = ManagedTorrentState::Live(live.clone());
                    t.state_change_notify.notify_waiters();

                    spawn_fatal_errors_receiver(t, rx, token);
                    if let Some(peer_rx) = peer_rx {
                        if let (Some(before), Some(now)) = (&g.trackers, &peer_rx.trackers) {
                            now.follow(before);
                        }
                        spawn_peer_adder(&live, peer_rx.peers);
                        g.trackers = peer_rx.trackers;
                    }
                    Ok(())
                }
                ManagedTorrentState::Error(_) => {
                    // After an error the data is no longer vouched for: check it.
                    let initializing = t.checking_everything(&g, Default::default())?;
                    g.state = ManagedTorrentState::Initializing(initializing);
                    t.state_change_notify.notify_waiters();

                    // Recurse.
                    _start(t, peer_rx, start_paused, session, Some(g), token)
                }
                ManagedTorrentState::None => bail!("bug: torrent is in empty state"),
            }
        }

        let session = self
            .shared
            .session
            .upgrade()
            .context("session is dead, cannot start torrent")?;
        let mut g = self.locked.write();
        g.paused = start_paused;
        let cancellation_token = session.cancellation_token().child_token();

        _start(
            self,
            peer_rx,
            start_paused,
            session,
            Some(g),
            cancellation_token,
        )
    }

    pub fn is_paused(&self) -> bool {
        self.locked.read().paused
    }

    /// Every tracker of the latest live run and how announcing to it went.
    /// Empty when the torrent has not run with trackers yet. While paused, the
    /// last outcomes stay readable and nothing is in flight.
    pub fn tracker_announces(&self) -> Vec<crate::tracker::TrackerAnnounce> {
        self.tracker_status()
            .map(|t| t.announces())
            .unwrap_or_default()
    }

    /// The torrent's trackers become `tiers`, each kept where, and as
    /// whoever, first named it: its next run announces to them, and a running torrent does from now on (see
    /// [`crate::tracker::TrackerStatus::replace`]). The table of its latest
    /// run lists them at once.
    pub fn replace_trackers(&self, tiers: Vec<Vec<crate::tracker::Tracker>>) {
        let tiers = crate::tracker::tidy_tiers(tiers);
        *self.shared.tracker_tiers.write() = tiers.clone();
        let (Some(status), Some(session)) = (self.tracker_status(), self.shared.session.upgrade())
        else {
            return;
        };
        status.replace(session.announce_tiers(tiers, self.takes_peers_from_trackers_only()));
    }

    /// Adds `tiers` after the torrent's last tier, a tracker it has already
    /// left where it is, as whoever named it.
    pub fn add_trackers(&self, tiers: Vec<Vec<crate::tracker::Tracker>>) {
        let current = self.shared.tracker_tiers();
        self.replace_trackers(crate::tracker::added_tiers(&current, tiers));
    }

    /// Removes `urls` from the torrent's trackers, through
    /// `replace_trackers`.
    pub fn remove_trackers(&self, urls: &[url::Url]) {
        let current = self.shared.tracker_tiers();
        self.replace_trackers(crate::tracker::without_trackers(&current, urls));
    }

    /// Brings the next announce forward to `after` from now, to every
    /// tracker or to `tracker` alone (libtorrent
    /// `torrent_handle::force_reannounce`); see
    /// [`crate::tracker::TrackerStatus::reannounce`]. Nothing happens unless
    /// the torrent runs. False when `tracker` is not one of the torrent's.
    pub fn force_reannounce(
        &self,
        after: Duration,
        tracker: Option<&url::Url>,
        ignore_min_interval: bool,
    ) -> bool {
        let known = tracker.is_none_or(|t| {
            self.shared
                .tracker_tiers
                .read()
                .iter()
                .flatten()
                .any(|u| &u.url == t)
        });
        if known && let Some(status) = self.tracker_status() {
            status.reannounce(after, tracker, ignore_min_interval);
        }
        known
    }

    /// Starts a new DHT announce of the running torrent under each of its
    /// hashes (libtorrent `torrent_handle::force_dht_announce`).
    pub fn force_dht_announce(&self) {
        self.shared.dht_announce.send_replace(());
    }

    /// The announce table behind [`Self::tracker_announces`], to wait for
    /// changes with [`crate::tracker::TrackerStatus::subscribe`]. A new live run
    /// brings a new table.
    pub fn tracker_status(&self) -> Option<crate::tracker::TrackerStatus> {
        self.locked.read().trackers.clone()
    }

    /// A route move is about to take the torrent out of this session to add
    /// it to another under its identity. A move is not a start: its
    /// announcing trackers are sent no `stopped` as it leaves, and the
    /// session it is added to goes on from where its announces stand, its
    /// counters running on and no `started` sent again to a tracker that
    /// answered one. A torrent not announcing hands nothing over: its next
    /// run is a start. Call it just before removing the torrent from this
    /// session.
    pub fn hand_over(&self) {
        let started = self
            .locked
            .read()
            .trackers
            .as_ref()
            .and_then(|trackers| trackers.hand_over());
        match started {
            Some(started) => {
                let from = *self.shared.announced_from.lock();
                self.shared.announcing.hand_over(from, started);
            }
            None => self.shared.announcing.forget(),
        }
    }

    /// Asks a tracker of the latest live run for the swarm's counts now; see
    /// [`crate::tracker::TrackerStatus::scrape`]. Fails when the torrent has
    /// not run with trackers yet.
    pub async fn scrape(&self) -> anyhow::Result<crate::tracker::ScrapeOk> {
        let trackers = self
            .tracker_status()
            .context("the torrent has not run with trackers")?;
        trackers.scrape().await
    }

    // A run that checks every piece and trusts nothing saved, on files
    // opened again, founded on `peers`: the restart after an error, and a
    // forced recheck.
    fn checking_everything(
        &self,
        g: &ManagedTorrentLocked,
        peers: live::peers::PeerList,
    ) -> anyhow::Result<Arc<TorrentStateInitializing>> {
        let metadata = self
            .metadata
            .load_full()
            .context("the torrent has no metadata to check")?;
        let files = self
            .shared
            .storage_factory
            .create_and_init(self.shared(), &metadata)?;
        // A check of every piece ends seed mode: from here a piece is had
        // when the check finds it (libtorrent `torrent::force_recheck` and
        // `torrent::leave_seed_mode`).
        self.shared.options.seed.leave();
        // Every piece is forgotten before anything is checked, so nothing
        // the storage saves from here vouches for one the check has not
        // found (libtorrent `torrent::force_recheck`: `set_have_all(false)`,
        // `m_add_torrent_params.reset()`).
        files
            .on_check_everything()
            .context("the storage did not forget its pieces")?;
        Ok(Arc::new(TorrentStateInitializing::new(
            self.shared.clone(),
            metadata,
            Some(wanted_files(&g.files)),
            files,
            initializing::InitialCheck::Full,
            Some(g.piece_priorities.clone()),
            peers,
        )))
    }

    /// Forget every piece and check them all again, trusting no checkpoint
    /// or saved state (libtorrent `torrent::force_recheck`): the connections
    /// close, the announces stop, the peer list stays, and the files are
    /// opened again. Nothing happens while the torrent is being checked
    /// already. The check runs on the next [`Self::start`].
    pub(crate) fn recheck(&self) -> anyhow::Result<()> {
        let mut g = self.locked.write();
        let peers = match &g.state {
            ManagedTorrentState::Initializing(_) => return Ok(()),
            ManagedTorrentState::Live(live) => live.pause()?.peers,
            ManagedTorrentState::Paused(_) => g.state.take().assert_paused().peers,
            ManagedTorrentState::Error(_) => Default::default(),
            ManagedTorrentState::None => bail!("bug: torrent is in empty state"),
        };
        g.state = ManagedTorrentState::Initializing(self.checking_everything(&g, peers)?);
        self.state_change_notify.notify_waiters();
        Ok(())
    }

    /// Whether the torrent is in seed mode (see
    /// [`crate::AddTorrentOptions::seed_mode`]).
    pub fn seed_mode(&self) -> bool {
        self.shared.options.seed.on()
    }

    /// The pieces that passed their check while the torrent is in seed mode,
    /// to add it with again (see [`crate::AddTorrentOptions::seed_verified`];
    /// libtorrent `torrent::verified`, saved as `verified_pieces`). None once
    /// seed mode ended.
    pub fn seed_verified(&self) -> Option<BF> {
        self.shared.options.seed.verified()
    }

    /// `f` while the torrent stays in seed mode, which it stays in until `f`
    /// returns; None once it left it. What the storage is given under it
    /// cannot cross a check of every piece. Peers' requests wait on `f`, so
    /// it reads nothing from disk.
    pub fn while_seeding<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
        self.shared.options.seed.while_on(f)
    }

    /// A piece of a seed-mode torrent checked now, as a peer's request
    /// checks it before the piece is served (libtorrent
    /// `peer_connection::fill_send_buffer`), whether the torrent runs or is
    /// paused: true when it passed, now or before, and the storage heard
    /// [`crate::storage::TorrentStorage::on_piece_checked`] when it passed
    /// now. One that fails ends seed mode and every piece is checked again
    /// (libtorrent `torrent::leave_seed_mode` with `check_files`). None
    /// while the torrent neither runs nor is paused, or for no piece of it.
    /// Reads the piece from disk: called off the async runtime.
    pub fn check_seeded(&self, piece: u32) -> Option<anyhow::Result<bool>> {
        let metadata = self.metadata.load();
        let piece = metadata.as_ref()?.lengths().validate_piece_index(piece)?;
        if let Some(live) = self.live() {
            return Some(live.check_seeded(piece));
        }
        let left = {
            let g = self.locked.read();
            let ManagedTorrentState::Paused(paused) = &g.state else {
                return None;
            };
            let seed = &self.shared.options.seed;
            if !seed.unchecked(piece) {
                return Some(Ok(true));
            }
            let ops = crate::file_ops::FileOps::new(
                &paused.metadata,
                &*paused.files,
                self.shared.options.disable_v1_hashes,
            );
            if matches!(ops.check_piece(piece), Ok(crate::file_ops::Verdict::Passed)) {
                let checked = paused.files.on_piece_checked(piece);
                seed.checked(piece);
                return Some(checked.map(|()| true));
            }
            seed.leave()
        };
        if left {
            warn!(
                piece = piece.get(),
                "a piece failed its check in seed mode: checking every piece"
            );
            let session = self.shared.session.upgrade()?;
            let id = self.shared.id;
            tokio::spawn(async move {
                let Some(handle) = session.get(crate::api::TorrentIdOrHash::Id(id)) else {
                    return;
                };
                if let Err(error) = session.force_recheck(&handle).await {
                    warn!(error = %error, "could not check a torrent that left seed mode");
                }
            });
        }
        Some(Ok(false))
    }

    /// A running SSL torrent left without an identity stops its run and
    /// waits for one, as it was asked to run: its connections close and its
    /// announces stop.
    pub(crate) fn wait_for_certificate(&self) -> anyhow::Result<()> {
        let mut g = self.locked.write();
        if let ManagedTorrentState::Live(live) = &g.state {
            let paused = live.pause()?;
            g.state = ManagedTorrentState::Paused(Box::new(paused));
            self.state_change_notify.notify_waiters();
        }
        Ok(())
    }

    /// Whether the torrent waits for an identity to start: an SSL torrent
    /// without one, checked and asked to run.
    pub(crate) fn waits_for_certificate(&self) -> bool {
        let g = self.locked.read();
        self.is_ssl() && !g.paused && matches!(g.state, ManagedTorrentState::Paused(_))
    }

    /// Pause the torrent if it's live.
    pub(crate) fn pause(&self) -> anyhow::Result<()> {
        let mut g = self.locked.write();
        match &g.state {
            ManagedTorrentState::Live(live) => {
                let paused = live.pause()?;
                g.state = ManagedTorrentState::Paused(Box::new(paused));
                g.paused = true;
                self.state_change_notify.notify_waiters();
                Ok(())
            }
            ManagedTorrentState::Initializing(init) => {
                let init = init.clone();
                g.paused = true;
                init.request_pause();
                self.state_change_notify.notify_waiters();
                Ok(())
            }
            ManagedTorrentState::Paused(_) => {
                bail!("torrent is already paused");
            }
            ManagedTorrentState::Error(_) => {
                bail!("can't pause torrent in error state")
            }
            ManagedTorrentState::None => bail!("bug: torrent is in empty state"),
        }
    }

    /// What the torrent has moved since it was added: its runs that ended
    /// and the one running.
    pub fn totals(&self) -> TorrentTotals {
        let g = self.locked.read();
        let moved = *self.shared.moved.lock();
        match &g.state {
            ManagedTorrentState::Live(live) => moved + TorrentTotals::from(&live.stats_snapshot()),
            _ => moved,
        }
    }

    /// Get stats.
    pub fn stats(&self) -> TorrentStats {
        use stats::TorrentStatsState as S;
        let mut resp = TorrentStats {
            total_bytes: self
                .metadata
                .load()
                .as_ref()
                .map(|r| r.info.lengths().total_length())
                .unwrap_or_default(),
            file_progress: Vec::new(),
            state: S::Error,
            error: None,
            progress_bytes: 0,
            uploaded_bytes: 0,
            finished: false,
            live: None,
        };

        {
            let g = self.locked.read();
            match &g.state {
                // A paused torrent whose check a pause stopped, or that was
                // paused before its check began, checks nothing: it is
                // paused, and checks again once it is resumed. libtorrent
                // holds a paused torrent in `checking_files` with no check
                // running (`torrent::should_check_files`, torrent.cpp:9740-9747,
                // v2.0.15), and qBittorrent shows it stopped
                // (`TorrentImpl::updateState`, torrentimpl.cpp:1316, which
                // shows a check only for a torrent not stopped).
                ManagedTorrentState::Initializing(i) if g.paused && !i.is_checking() => {
                    resp.state = S::Paused;
                }
                ManagedTorrentState::Initializing(i) => {
                    resp.state = S::Initializing {
                        paused: g.paused,
                        checking: i.is_reading_pieces(),
                    };
                    resp.progress_bytes = i.checked_bytes.load(Ordering::Relaxed);
                }
                ManagedTorrentState::Paused(p) => {
                    resp.state = if !g.paused && self.needs_certificate() {
                        S::NeedsCertificate
                    } else {
                        S::Paused
                    };
                    let hns = p.hns();
                    resp.total_bytes = hns.total();
                    resp.progress_bytes = hns.progress();
                    resp.finished = hns.finished();
                    resp.file_progress = p.chunk_tracker.per_file_have_bytes().to_owned();
                }
                ManagedTorrentState::Live(l) => {
                    resp.state = S::Live;
                    let live_stats = LiveStats::from(l.as_ref());
                    let hns = l.get_hns().unwrap_or_default();
                    resp.total_bytes = hns.total();
                    resp.progress_bytes = hns.progress();
                    resp.finished = hns.finished();
                    resp.uploaded_bytes = l.get_uploaded_bytes();
                    resp.file_progress = l
                        .lock_read("file_progress")
                        .get_chunks()
                        .ok()
                        .map(|c| c.per_file_have_bytes().to_owned())
                        .unwrap_or_default();
                    resp.live = Some(live_stats);
                }
                ManagedTorrentState::Error(e) => {
                    resp.state = S::Error;
                    resp.error = Some(format!("{e:?}"))
                }
                ManagedTorrentState::None => {
                    resp.state = S::Error;
                    resp.error = Some("bug: torrent in broken \"None\" state".to_string());
                }
            }
        }

        resp
    }

    #[inline(never)]
    pub fn wait_until_initialized(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        async move {
            loop {
                let changed = self.next_state_change();
                let done = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) => Ok(false),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                    _ => Ok(true),
                })?;
                if done {
                    return Ok(());
                }
                changed.await;
            }
        }
        .boxed()
    }

    /// Resolves once a run has every wanted piece.
    #[inline(never)]
    pub fn wait_until_completed(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        self.wait_live(|live| async move { live.wait_until_completed().await }.boxed())
    }

    /// Resolves once a run has every piece, whichever are wanted: the
    /// torrent is a seed (libtorrent `torrent::is_seed`).
    pub(crate) fn wait_until_seed(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        self.wait_live(|live| async move { live.wait_until_seed().await }.boxed())
    }

    /// Resolves once `wait` resolves on a run of the torrent.
    fn wait_live(
        &self,
        wait: impl Fn(Arc<TorrentStateLive>) -> BoxFuture<'static, ()> + Send + 'static,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        async move {
            // A run reaches the point waited for, or the state changes and
            // is read again: a paused torrent waits for the next run, a
            // failed or removed one ends the wait.
            loop {
                let changed = self.next_state_change();
                let live = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) | ManagedTorrentState::Paused(_) => {
                        Ok(None)
                    }
                    ManagedTorrentState::Live(l) => Ok(Some(l.clone())),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                })?;
                match live {
                    Some(live) => tokio::select! {
                        _ = wait(live) => return Ok(()),
                        _ = changed => {}
                    },
                    None => changed.await,
                }
            }
        }
        .boxed()
    }
}

pub type ManagedTorrentHandle = Arc<ManagedTorrent>;

fn spawn_fatal_errors_receiver(
    state: &Arc<ManagedTorrent>,
    rx: tokio::sync::oneshot::Receiver<anyhow::Error>,
    token: CancellationToken,
) {
    let span = state.shared.span.clone();
    let id = state.shared.id;
    let info_hash = state.shared.info_hash;
    let state = Arc::downgrade(state);
    spawn_with_cancel::<&'static str>(
        debug_span!(parent: span, "fatal_errors_receiver"),
        "fatal_errors_receiver",
        token,
        async move {
            let e = match rx.await {
                Ok(e) => e,
                Err(_) => return Ok(()),
            };
            if let Some(state) = state.upgrade() {
                state.stop_with_error(e);
            } else {
                warn!(
                    ?id,
                    ?info_hash,
                    "tried to stop the torrent with error, but couldn't upgrade the arc"
                );
            }
            Ok(())
        },
    );
}

/// Hands each peer the trackers, the DHT, local service discovery and the
/// initial list find to the live run, as each comes, with who found it
/// (libtorrent `torrent::add_peer`, called from `tracker_response` and
/// `on_dht_announce_response` with their `peer_source_flags_t`). The run's
/// cancellation token ends it (`TorrentStateLive::spawn`), as does the
/// stream closing; it holds the run weakly, as libtorrent's DHT callback
/// holds the torrent (`on_dht_announce_response_disp`).
fn spawn_peer_adder(live: &Arc<TorrentStateLive>, mut peer_rx: PeerStream) {
    live.spawn(
        debug_span!(parent: live.torrent().span.clone(), "external_peer_adder"),
        format!("[{}]external_peer_adder", live.shared.id),
        {
            let live = Arc::downgrade(live);
            async move {
                while let Some((peer, source)) = peer_rx.next().await {
                    trace!(?peer, ?source, "received peer");
                    let Some(live) = live.upgrade() else {
                        return Ok(());
                    };
                    live.add_peer(peer, source);
                }
                debug!("peer_rx closed, closing peer adder");
                Ok(())
            }
        },
    );
}
