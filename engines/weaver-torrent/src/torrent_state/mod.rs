// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub mod initializing;
pub mod live;
pub mod paused;
pub mod stats;
mod streaming;
pub mod utils;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::buffers::ByteBufOwned;
use crate::core::hash_id::{Id20, Id32, InfoHashes};
use crate::core::lengths::{Lengths, ValidPieceIndex};
use crate::core::magnet::Magnet;
use crate::core::torrent_metainfo::{TorrentMetaV1Owned, TorrentVersion, WebSeed};
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
use crate::piece_priority::{PiecePriority, validate_piece_priorities};
use crate::session::TorrentId;
use crate::spawn_utils::BlockingSpawner;
use crate::storage::BoxStorageFactory;
use crate::stream_connect::StreamConnector;
use crate::torrent_state::stats::LiveStats;
use crate::type_aliases::FileInfos;
use crate::type_aliases::PeerSource;
use crate::type_aliases::PeerStream;

use initializing::TorrentStateInitializing;

use self::paused::TorrentStatePaused;
pub use self::stats::{TorrentStats, TorrentStatsState};
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
    Paused(TorrentStatePaused),
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
            Self::Paused(paused) => paused,
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
    pub(crate) only_files: Option<Vec<usize>>,
    // The fetch priority of every piece; None while all are Normal.
    pub(crate) piece_priorities: Option<Vec<PiecePriority>>,
    // The announce table of the trackers of the latest live run. It outlives
    // the run, so the last outcomes stay readable while paused.
    pub(crate) trackers: Option<crate::tracker::TrackerStatus>,
}

#[derive(Default)]
pub(crate) struct ManagedTorrentOptions {
    pub force_tracker_interval: Option<Duration>,
    pub peer_connect_timeout: Option<Duration>,
    pub peer_read_write_timeout: Option<Duration>,
    pub peer_max_request_window: Option<usize>,
    pub allow_overwrite: bool,
    pub output_folder: PathBuf,
    // The torrent's own rate limits. Every live run acquires through this one
    // object, so a change reaches the running peers and outlives pause/unpause.
    pub ratelimits: Limits,
    pub initial_peers: Vec<SocketAddr>,
    pub peer_limit: Option<usize>,
    /// [`AddTorrentOptions::disable_v1_hashes`](crate::AddTorrentOptions::disable_v1_hashes).
    pub disable_v1_hashes: bool,
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
    /// Every tracker in `tracker_tiers`.
    pub trackers: HashSet<url::Url>,
    /// The torrent's trackers grouped into tiers (BEP 12), in announce order.
    pub tracker_tiers: Vec<Vec<url::Url>>,
    pub peer_id: Id20,
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

    /// Counters of the most recent live run, recorded as the run ends, so a
    /// `stopped` tracker announce made after it ended reports them.
    pub(crate) last_run: parking_lot::Mutex<Option<RunCounters>>,
}

/// Transfer counters of one live run of a torrent, as its tracker announces
/// report them (see `TrackerCommsStats`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RunCounters {
    /// Payload bytes uploaded during the run.
    pub uploaded_bytes: u64,
    /// Bytes of the pieces downloaded during the run that passed their check.
    pub downloaded_bytes: u64,
    /// Bytes of the selected pieces still missing.
    pub left_bytes: u64,
}

impl ManagedTorrentShared {
    pub(crate) fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }

    pub fn identity(&self) -> Identity {
        Identity {
            peer_id: self.peer_id,
            version: self.client_name_and_version.clone(),
            user_agent: self.user_agent.clone(),
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
            .tracker_tiers
            .iter()
            .flatten()
            .map(|t| t.to_string())
            .collect();
        Magnet::of(self.info_hashes(), self.name(), trackers)
    }

    pub fn only_files(&self) -> Option<Vec<usize>> {
        self.locked.read().only_files.clone()
    }

    pub fn with_state<R>(&self, f: impl FnOnce(&ManagedTorrentState) -> R) -> R {
        f(&self.locked.read().state)
    }

    /// The fetch priority of a piece, or None if the index is out of range or
    /// the torrent is not resolved yet.
    pub fn piece_priority(&self, piece: u32) -> Option<PiecePriority> {
        let metadata = self.metadata.load();
        metadata.as_ref()?.lengths().validate_piece_index(piece)?;
        Some(
            self.locked
                .read()
                .piece_priorities
                .as_ref()
                .map(|p| p[piece as usize])
                .unwrap_or_default(),
        )
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

        let all = g.piece_priorities.get_or_insert_with(|| {
            vec![PiecePriority::Normal; metadata.lengths().total_pieces() as usize]
        });
        for (index, priority) in priorities {
            all[index.get_usize()] = priority;
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
        f(&mut self.locked.write().state)
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

    /// Get the live state if the torrent is live.
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
                            let concurrent_init_semaphore =
                                session.concurrent_initialize_semaphore.clone();
                            let _permit = concurrent_init_semaphore
                                .acquire()
                                .await
                                .context("bug: concurrent init semaphore was closed")?;

                            let check_result = init.check().await;
                            init.finish_check();

                            match check_result {
                                Ok(paused) => {
                                    let mut g = t.locked.write();
                                    if let ManagedTorrentState::Initializing(_) = &g.state {
                                    } else {
                                        debug!(
                                            "no need to start torrent anymore, as it switched state from initializing"
                                        );
                                        return Ok(());
                                    }

                                    g.state = ManagedTorrentState::Paused(paused);
                                    t.state_change_notify.notify_waiters();
                                    _start(&t, peer_rx, start_paused, session, Some(g), token)
                                }
                                Err(err) => {
                                    if init.is_pause_requested() {
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
                    let paused = g.state.take().assert_paused();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let live = TorrentStateLive::new(paused, tx, token.clone())?;
                    g.state = ManagedTorrentState::Live(live.clone());
                    t.state_change_notify.notify_waiters();

                    spawn_fatal_errors_receiver(t, rx, token);
                    if let Some(peer_rx) = peer_rx {
                        spawn_peer_adder(&live, peer_rx.peers);
                        g.trackers = peer_rx.trackers;
                    }
                    Ok(())
                }
                ManagedTorrentState::Error(_) => {
                    let metadata = t.metadata.load_full().expect("TODO");
                    let initializing = Arc::new(TorrentStateInitializing::new(
                        t.shared.clone(),
                        metadata.clone(),
                        g.only_files.clone(),
                        t.shared
                            .storage_factory
                            .create_and_init(t.shared(), &metadata)?,
                        true,
                        // After an error the data is no longer vouched for: check it.
                        None,
                        g.piece_priorities.clone(),
                    ));
                    g.state = ManagedTorrentState::Initializing(initializing.clone());
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

    /// The announce table behind [`Self::tracker_announces`], to wait for
    /// changes with [`crate::tracker::TrackerStatus::subscribe`]. A new live run
    /// brings a new table.
    pub fn tracker_status(&self) -> Option<crate::tracker::TrackerStatus> {
        self.locked.read().trackers.clone()
    }

    /// Pause the torrent if it's live.
    pub(crate) fn pause(&self) -> anyhow::Result<()> {
        let mut g = self.locked.write();
        match &g.state {
            ManagedTorrentState::Live(live) => {
                let paused = live.pause()?;
                g.state = ManagedTorrentState::Paused(paused);
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
                ManagedTorrentState::Initializing(i) => {
                    resp.state = S::Initializing { paused: g.paused };
                    resp.progress_bytes = i.checked_bytes.load(Ordering::Relaxed);
                }
                ManagedTorrentState::Paused(p) => {
                    resp.state = S::Paused;
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
            // TODO: rewrite, this polling is horrible
            loop {
                let done = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) => Ok(false),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                    _ => Ok(true),
                })?;
                if done {
                    return Ok(());
                }
                let _ = timeout(
                    Duration::from_millis(100),
                    self.state_change_notify.notified(),
                )
                .await;
            }
        }
        .boxed()
    }

    #[inline(never)]
    pub fn wait_until_completed(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        async move {
            // TODO: rewrite, this polling is horrible
            let live = loop {
                let live = self.with_state(|s| match s {
                    ManagedTorrentState::Initializing(_) | ManagedTorrentState::Paused(_) => {
                        Ok(None)
                    }
                    ManagedTorrentState::Live(l) => Ok(Some(l.clone())),
                    ManagedTorrentState::Error(e) => bail!("{:?}", e),
                    ManagedTorrentState::None => bail!("bug: torrent state is None"),
                })?;
                if let Some(live) = live {
                    break live;
                }
                let _ = timeout(Duration::from_secs(1), self.state_change_notify.notified()).await;
            };

            live.wait_until_completed().await;
            Ok(())
        }
        .boxed()
    }

    // Returns true if needed to unpause torrent.
    // This is just implementation detail - it's easier to pause/unpause than to tinker with internals.
    pub(crate) fn update_only_files(&self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let metadata = metadata.as_ref().context("torrent is not resolved")?;
        let file_count = metadata.file_infos.len();
        for f in only_files.iter().copied() {
            if f >= file_count {
                anyhow::bail!("only_files contains invalid value {f}")
            }
        }

        // if live, need to update chunk tracker
        // - if already finished: need to pause, then unpause (to reopen files etc)
        // if paused, need to update chunk tracker

        let mut g = self.locked.write();
        match &mut g.state {
            ManagedTorrentState::Initializing(_) => bail!("can't update initializing torrent"),
            ManagedTorrentState::Error(_) => {}
            ManagedTorrentState::None => {}
            ManagedTorrentState::Paused(p) => {
                p.update_only_files(only_files)?;
            }
            ManagedTorrentState::Live(l) => {
                l.update_only_files(only_files)?;
            }
        };

        g.only_files = Some(only_files.iter().copied().collect());
        Ok(())
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

fn spawn_peer_adder(live: &Arc<TorrentStateLive>, mut peer_rx: PeerStream) {
    live.spawn(
        debug_span!(parent: live.torrent().span.clone(), "external_peer_adder"),
        format!("[{}]external_peer_adder", live.shared.id),
        {
            let live = live.clone();
            async move {
                let live = {
                    let weak = Arc::downgrade(&live);
                    drop(live);
                    weak
                };

                loop {
                    match timeout(Duration::from_secs(5), peer_rx.next()).await {
                        Ok(Some(peer)) => {
                            trace!(?peer, "received peer");
                            let live = match live.upgrade() {
                                Some(live) => live,
                                None => return Ok(()),
                            };
                            live.add_peer(peer);
                        }
                        Ok(None) => {
                            debug!("peer_rx closed, closing peer adder");
                            return Ok(());
                        }
                        // If timeout, check if the torrent is live.
                        Err(_) if live.strong_count() == 0 => {
                            debug!("timed out waiting for peers, torrent isn't live, closing peer adder");
                            return Ok(());
                        }
                        Err(_) => continue,
                    }
                }
            }
        },
    );
}
