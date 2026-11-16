// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::options::AddTorrent;
use super::options::AddTorrentOptions;
use super::options::AddTorrentResponse;
use super::options::ListOnlyResponse;
use super::peer_finder::{self, PeerAsk, TrackerAsk};
use super::peer_sources::Discovery;
use super::peer_sources::FetchPhase;
use super::peer_sources::MetadataFetch;
use super::peer_sources::a_tracker_answered;
use super::peer_sources::follow_fetch;
use super::peer_sources::merge_tracker_tiers;
use super::peer_sources::metainfo_tracker_tiers;
use super::peer_sources::own_tracker_tiers;
use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use crate::Identity;
use crate::bencode::bencode_serialize_to_writer;
use crate::buffers::ByteBufOwned;
use crate::core::{
    hash_id::InfoHashes,
    magnet::Magnet,
    net::NamedBy,
    torrent_metainfo::{ValidatedTorrentMetaV1Info, WebSeed, WebSeedKind},
    v2_tree::torrent_name,
};
use crate::dht::Id20;
use crate::ssl::SslTorrent;
use crate::torrent_state::live::web_seed::WebSeeds;
use crate::tracker::{Tracker, TrackerHandover, TrackerStatus, TrackerTask};
use crate::{
    ApiError, CreateTorrentOptions, FileInfos, ManagedTorrent, ManagedTorrentShared,
    api_error::WithStatus,
    create_torrent,
    create_torrent_file::CreateTorrentResult,
    dht_utils::{MetadataPeers, ReadMetainfoResult, read_metainfo_from_peer_receiver},
    limits::Limits,
    merge_streams::merge_streams,
    peer_connection::PeerConnectionOptions,
    peer_info_reader::{self, FetchDht},
    piece_priority::{
        FilePriority, file_priorities, file_to_piece_priorities, validate_piece_priorities,
        wanted_files,
    },
    storage::{StorageFactoryExt, filesystem::FilesystemStorageFactory},
    torrent_state::{
        ManagedTorrentHandle, ManagedTorrentLocked, ManagedTorrentOptions, ManagedTorrentState,
        TorrentMetadata,
        initializing::{Checkpoint, InitialCheck, TorrentStateInitializing, normalize_checkpoint},
        live::peers::PeerList,
        seed_mode::SeedMode,
    },
    type_aliases::{BF, PeerStream},
};
use anyhow::{Context, bail};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::BoxStream};
use http::StatusCode;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::Notify;
use tracing::{Instrument, debug_span, info, trace};

use super::{
    ResolveMagnetResult, Session, is_bare_info_hash, torrent_from_bytes, torrent_from_url,
};

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
            let selected = selected_files(&only_files, info.iter_file_lengths().count());
            for (idx, fd) in info.iter_file_details().enumerate() {
                if !selected[idx] {
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

// Whether each of a torrent's `files` files is one `only_files` names, one
// flag a file, built in one pass so a pass over the files does not search
// the list for each. An index past the files names none.
fn selected_files(only_files: &[usize], files: usize) -> Vec<bool> {
    let mut selected = vec![false; files];
    for &index in only_files {
        if let Some(flag) = selected.get_mut(index) {
            *flag = true;
        }
    }
    selected
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
        (None, Some(only_files)) => selected_files(only_files, file_infos.len())
            .into_iter()
            .map(|selected| {
                if selected {
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

pub(super) fn merge_two_optional_streams<T>(
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

pub(super) fn torrent_file_from_info_bytes(
    info_bytes: &[u8],
    tracker_tiers: &[Vec<Tracker>],
) -> anyhow::Result<Bytes> {
    #[derive(Serialize)]
    struct Tmp<'a> {
        announce: &'a str,
        #[serde(rename = "announce-list")]
        announce_list: Vec<Vec<&'a str>>,
        info: crate::bencode::raw_value::RawValue<&'a [u8]>,
    }

    let mut w = Vec::new();
    let v = Tmp {
        info: crate::bencode::raw_value::RawValue(info_bytes),
        announce: tracker_tiers
            .iter()
            .flatten()
            .next()
            .map(|t| t.url.as_str())
            .unwrap_or(""),
        announce_list: tracker_tiers
            .iter()
            .map(|tier| tier.iter().map(|t| t.url.as_str()).collect())
            .collect(),
    };
    bencode_serialize_to_writer(&v, &mut w)?;
    Ok(w.into())
}

struct InternalAddResult {
    info_hashes: InfoHashes,
    metadata: Option<TorrentMetadata>,
    tracker_tiers: Vec<Vec<Tracker>>,
    name: Option<String>,
}

impl Session {
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
                            named_by: NamedBy::Torrent,
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
                                    .map(|t| vec![Tracker::torrent(t)])
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
                            torrent_from_url(self.http_clients.as_ref(), &url).await?
                        }
                        AddTorrent::Url(url) => {
                            bail!(
                                "unsupported URL {}. Supporting magnet:, http:, and https",
                                crate::tracker::redacted_tracker_url(&url)
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
        let dht_announce = tokio::sync::watch::channel(()).0;

        let make_peer_rx = |info_hashes: InfoHashes,
                            identity: &Identity,
                            private: bool,
                            running: Option<TrackerTask>| {
            self.make_peer_rx(
                PeerAsk {
                    info_hashes,
                    ssl: None,
                    port: self.told_port(None),
                    announce: !opts.paused && !opts.list_only,
                    dht_announce: dht_announce.subscribe(),
                    initial_peers: own_peers(&opts.initial_peers, &opts.found_peers, private),
                    trackers: TrackerAsk {
                        identity,
                        tiers: tracker_tiers.clone(),
                        force_interval: opts.force_tracker_interval,
                        private,
                        running,
                    },
                },
                Discovery::of(private, opts.disable_dht, opts.disable_lsd),
            )
        };

        let mut seen_peers = Vec::new();

        // The torrent's one peer list, from its first metadata connection to
        // its last connection (libtorrent `torrent::m_peer_list`, which
        // `torrent::set_metadata` keeps).
        // The peers a save found elsewhere come back marked so.
        let peer_list = PeerList::resumed(
            opts.initial_peers.iter().flatten().copied(),
            &opts.found_peers,
            opts.banned_peers.iter().copied(),
        );

        // The tracker task of this torrent's metadata fetch, for the run
        // that follows: the caller's, or this magnet's own.
        let mut handover = opts.tracker_handover.take();
        let (metadata, peer_list) = match metadata {
            // A private torrent saved while its list still owed its
            // untracked peers (a magnet's whose trackers had not answered)
            // owes them again: they stay until a tracker answers.
            Some(metadata) if metadata.info.info().private => (metadata, peer_list.owing_found()),
            Some(metadata) => (metadata, peer_list),
            None => {
                // The DHT and local discovery, when the session runs either
                // and the torrent was not added without it.
                let sources = Discovery::added(false, &opts);
                let discovery =
                    (sources.dht && self.dht.is_some()) || (sources.lsd && self.lsd.is_some());
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
                    let mut trackers = TrackerAsk {
                        identity: &identity,
                        tiers: tracker_tiers.clone(),
                        force_interval: opts.force_tracker_interval,
                        private: true,
                        running: None,
                    };
                    self.start_trackers(info_hashes, &mut trackers, None, &self.announce_port)
                        .map(TrackerHandover::new)
                });
                let fetched = handover.as_ref().map(TrackerHandover::task);
                let table = fetched.as_ref().map(|task| task.status.clone());
                let (unable_tx, unable) = tokio::sync::watch::channel(HashSet::new());
                let (phase_tx, phase) = tokio::sync::watch::channel(FetchPhase::of(
                    &table
                        .as_ref()
                        .map(TrackerStatus::announces)
                        .unwrap_or_default(),
                    &unable.borrow(),
                    discovery,
                ));
                // The nodes the fetch's peers name go to the DHT once the
                // fetch looks for peers there.
                let fetch_dht = self
                    .dht
                    .clone()
                    .filter(|_| sources.dht)
                    .map(|dht| FetchDht::new(dht, phase.clone()));
                if let Some(tx) = opts.metadata_fetch.take() {
                    let _ = tx.send(MetadataFetch {
                        trackers: table.clone(),
                        phase,
                    });
                }
                let asked_first =
                    make_peer_rx(info_hashes, &identity, true, fetched).map(|source| source.peers);
                // The DHT and local discovery, once the trackers named no
                // peer that may still serve the metadata, each where the
                // torrent's options let it look.
                let then = {
                    let session = self.clone();
                    let dht_announce = dht_announce.subscribe();
                    let identity = identity.clone();
                    async move {
                        follow_fetch(
                            table.map(|t| t.subscribe()),
                            unable,
                            discovery,
                            phase_tx,
                            session.trackers_changed.clone(),
                        )
                        .await;
                        peer_finder::after_trackers(
                            &session,
                            info_hashes,
                            &identity,
                            dht_announce,
                            sources,
                        )
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
                        peer_info_reader::MetadataFetch::counted_in(
                            info_hashes,
                            fetch_dht,
                            self.stats.swarm.clone(),
                        ),
                        &identity,
                        MetadataPeers {
                            list: peer_list,
                            stream: peers,
                            order: self.connect_order(),
                        },
                        &unable_tx,
                        &tracker_tiers,
                        opts.peer_opts,
                    )
                    .await?;

                seen_peers = resolved_magnet.seen_peers;
                // The fetch's own discovery ends with it; its trackers carry
                // on into the run.
                drop(resolved_magnet.peer_rx);
                // The peers the fetch found stay on the list it wrote
                // into, with what their connections left there. A private
                // torrent's keeps those a tracker or the operator named;
                // the others (the DHT's, local discovery's) leave once a
                // tracker has answered, now when one already has, else at
                // the first answer. While none has, they stay, so a torrent
                // whose trackers never answer still downloads.
                let peers = if resolved_magnet.metadata.info.info().private {
                    let answered = handover
                        .as_ref()
                        .is_some_and(|h| a_tracker_answered(&h.status().announces()));
                    resolved_magnet.peers.taking_trackers_alone(answered)
                } else {
                    resolved_magnet.peers
                };
                (resolved_magnet.metadata, peers)
            }
        };
        // A running torrent finds its peers from the start; an SSL torrent's
        // are found as it goes live, under its identity. Its trackers hear
        // it as the torrent its metadata fetch announced: that fetch's task
        // carries on, starting from the peers it found.
        let mut peer_rx = None;
        if !opts.paused && !opts.list_only && metadata.info.info().ssl_cert.is_none() {
            let private = metadata.info.info().private;
            let trackers = handover
                .take()
                .and_then(TrackerHandover::take)
                .and_then(|task| {
                    task.take_over(
                        metadata.info_hashes,
                        self.announce_tiers(tracker_tiers.clone(), private),
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
                let info = metadata.info.info();
                let name = torrent_name(
                    info.name_utf8
                        .as_ref()
                        .or(info.name.as_ref())
                        .map(|name| name.as_ref()),
                    // libtorrent's: SHA-1 of the info dict, v2 or not.
                    || {
                        use crate::sha1w::ISha1;
                        let mut sha1 = crate::sha1w::Sha1::new();
                        sha1.update(&metadata.info_bytes);
                        Id20::new(sha1.finish()).as_string()
                    },
                );
                let ssl = SslTorrent::new(ca.as_ref(), info_hashes, &name)
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

        // Seed mode takes every piece as had (libtorrent `torrent::init`
        // under `seed_mode` sets every piece had without reading one).
        if opts.seed_mode {
            let pieces = metadata.info.lengths().total_pieces() as usize;
            let mut have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
            have[..pieces].fill(true);
            opts.checkpoint = Some(Checkpoint::pieces(have));
        }
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

        let _permit = self.spawner.owned_permit().await?;

        let (managed_torrent, metadata) = {
            let mut g = self.db.write();
            if let Some((id, handle)) = info_hashes
                .ids20()
                .find_map(|hash| g.by_hash(hash))
                .or_else(|| g.torrents.get(&id).map(|t| (id, t)))
            {
                return Ok(AddTorrentResponse::AlreadyManaged(id, handle.clone()));
            }

            let span = debug_span!(parent: self.rs(), "torrent", id);
            let peer_opts = self.merge_peer_opts(opts.peer_opts);
            let metadata = Arc::new(metadata);
            // Where the announces of the session a route move took the
            // torrent from stood: its first run goes on from there. One
            // that joins paused starts when it is resumed.
            let handed_from = if opts.paused || opts.list_only {
                identity.announcing.forget();
                None
            } else {
                identity.announcing.take_from()
            };
            let minfo = Arc::new(ManagedTorrentShared {
                id,
                span,
                info_hash,
                info_hashes,
                tracker_tiers: parking_lot::RwLock::new(tracker_tiers),
                dht_announce,
                spawner: self.spawner.clone(),
                peer_id: identity.peer_id,
                tracker_key: identity.tracker_key,
                storage_factory,
                options: ManagedTorrentOptions {
                    force_tracker_interval: opts.force_tracker_interval,
                    peer_opts,
                    allow_overwrite: opts.overwrite,
                    output_folder,
                    ratelimits: Limits::new(opts.ratelimits),
                    initial_peers: own_peers(
                        &opts.initial_peers,
                        &opts.found_peers,
                        metadata.info.info().private,
                    ),
                    peer_limit: opts.peer_limit.or(self.peer_limit),
                    disable_v1_hashes: opts.disable_v1_hashes,
                    super_seeding: opts.super_seeding,
                    sequential: opts.sequential.into(),
                    disable_dht: opts.disable_dht,
                    disable_lsd: opts.disable_lsd,
                    disable_pex: opts.disable_pex,
                    owner_wakes_full_disk: opts.owner_wakes_full_disk,
                    seed: if opts.seed_mode {
                        SeedMode::over(
                            metadata.info.lengths().total_pieces(),
                            opts.seed_verified.as_ref(),
                        )
                    } else {
                        SeedMode::default()
                    },
                },
                connector: self.connector.clone(),
                session: Arc::downgrade(self),
                magnet_name: name,
                client_name_and_version: identity.version.clone(),
                user_agent: identity.user_agent.clone(),
                tracker_ids: identity.tracker_ids.clone(),
                completion: identity.completion.clone(),
                announcing: identity.announcing.clone(),
                announce_shape: identity.shape,
                last_run: Default::default(),
                ending_run: Default::default(),
                moved: parking_lot::Mutex::new(opts.totals),
                announced_from: parking_lot::Mutex::new(handed_from.unwrap_or(opts.totals)),
                handed_over: handed_from.is_some().into(),
                web_seeds: Arc::new(WebSeeds::of_torrent(&metadata, &opts.web_seeds)),
                ssl,
                run: Default::default(),
            });

            let initializing = Arc::new(TorrentStateInitializing::new(
                minfo.clone(),
                metadata.clone(),
                only_files,
                self.spawner
                    .block_in_place(|| minfo.storage_factory.create_and_init(&minfo, &metadata))?,
                InitialCheck::From(checkpoint),
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
            self.db.write().remove_torrent(id);
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

    async fn resolve_magnet(
        self: &Arc<Self>,
        fetch: Arc<peer_info_reader::MetadataFetch>,
        identity: &Identity,
        peers: MetadataPeers<PeerStream>,
        unable: &tokio::sync::watch::Sender<HashSet<SocketAddr>>,
        tracker_tiers: &[Vec<Tracker>],
        peer_opts: Option<PeerConnectionOptions>,
    ) -> anyhow::Result<ResolveMagnetResult> {
        match read_metainfo_from_peer_receiver(
            identity.peer_id,
            fetch,
            peers,
            unable,
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

/// The peers a torrent's own list hands each of its runs: those it was
/// added with, but for a private torrent, not those a save found elsewhere
/// than its trackers. Its peer list holds those while it owes them, and
/// once they leave, handing them out again would bring them back as the
/// operator's. A public torrent's list keeps every peer it was added
/// with.
fn own_peers(
    initial: &Option<Vec<SocketAddr>>,
    found: &[SocketAddr],
    private: bool,
) -> Vec<SocketAddr> {
    let peers = initial.iter().flatten().copied();
    if private && !found.is_empty() {
        let found: std::collections::HashSet<_> = found.iter().collect();
        peers.filter(|addr| !found.contains(addr)).collect()
    } else {
        peers.collect()
    }
}

#[cfg(test)]
mod tests {
    use super::selected_files;

    // The flags built once say what searching the list for each file said,
    // over seeded lists with repeats, gaps and indexes past the files.
    #[test]
    fn selected_files_say_what_searching_the_list_says() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for _ in 0..500 {
            let files = usize::try_from(next() % 64).unwrap();
            let named = usize::try_from(next() % 80).unwrap();
            let only: Vec<usize> = (0..named)
                .map(|_| usize::try_from(next() % 80).unwrap())
                .collect();
            let searched: Vec<bool> = (0..files).map(|i| only.contains(&i)).collect();
            assert_eq!(
                selected_files(&only, files),
                searched,
                "{only:?} of {files}"
            );
        }
    }
}
