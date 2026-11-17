// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_room::disk_warning;
use super::listing::{CategoryPick, FileTotals, Pick, Shown};
use super::live::Live;
use super::settings::{Presence, SeedingLevels, TorrentSettings};
use super::{Reading, TorrentService, files_of, hashes_of, on_disk, report, wanted};
use crate::categories::model::resolve_category_config;
use crate::jobs::JobId;
use crate::jobs::handle::{HoldOrigin, JobInfo};
use crate::jobs::model::JobStatus;
use crate::proxies::{ResolvedBy, TorrentScope};
use crate::settings::model::TrackerRule;
use crate::settings::{Seeding, SeedingPolicy};
use crate::torrents::layout::PieceLayout;
use crate::torrents::metrics::{
    Published, SeedingSeries, SessionSeries, TorrentMetrics, TorrentSeries,
};
use crate::torrents::sessions::TorrentRouteReason;
use crate::torrents::{
    FilePriority, TorrentCounters, TorrentFile, TorrentSource, TorrentState, TorrentTracker,
    TrackerEntry, TrackerStatus, status,
};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};
use weaver_torrent::{Magnet, TorrentVersion, TrackerAnnounce};

/// A torrent as consumers read it.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentView {
    pub job_id: JobId,
    pub name: String,
    /// The hash it goes by: its v1 hash, or its v2 hash cut to 20 bytes
    /// when it has no v1 hash.
    pub infohash: String,
    pub infohash_v1: Option<String>,
    pub infohash_v2: Option<String>,
    /// Which hashes verify its pieces, once its metainfo is in hand.
    pub version: Option<TorrentVersion>,
    /// The link to it: both its hashes and its name.
    pub magnet: String,
    pub source: TorrentSource,
    pub state: TorrentState,
    pub detail: Option<String>,
    /// The reason the service states itself, when `detail` is its words.
    pub stated: Option<status::Stated>,
    /// Why its route holds it, while the state comes from that hold.
    pub route_reason: Option<TorrentRouteReason>,
    /// Who holds it stopped, while a hold does.
    pub paused_by: Option<HoldOrigin>,
    pub category: Option<String>,
    /// The scope selected by its tracker rule, category, or the default.
    pub scope: TorrentScope,
    pub private: bool,
    pub forced: bool,
    /// The wanted bytes in hand, over the wanted bytes: the pieces it
    /// counts, a check under way included.
    pub progress: f64,
    /// While its files are checked, the share of them the check has read.
    pub check_progress: Option<f64>,
    pub total_bytes: u64,
    pub have_bytes: u64,
    /// The bytes of the files it wants, and how many of them are on disk;
    /// a file turned off counts in neither.
    pub wanted_bytes: u64,
    pub wanted_have_bytes: u64,
    /// Payload sent and received over every run, kept through pauses and
    /// restarts (libtorrent `torrent_status::all_time_upload` and
    /// `all_time_download`).
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    /// Payload sent and received in the current run alone, from nothing
    /// at each start (libtorrent `torrent_status::total_payload_upload` and
    /// `total_payload_download`); zero while it does not run.
    pub run_uploaded_bytes: u64,
    pub run_downloaded_bytes: u64,
    pub ratio: f64,
    pub download_bps: u64,
    pub upload_bps: u64,
    /// The torrent's own upload and download limits in bytes per second;
    /// None is unlimited and zero holds that direction closed.
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
    pub eta_seconds: Option<u64>,
    pub peers: u32,
    pub seeds: u32,
    pub leechers: u32,
    /// The most peers a torrent connects to.
    pub max_peers: u32,
    /// The swarm's seeds and leechers as its trackers last counted them.
    pub swarm_seeds: Option<u32>,
    pub swarm_leechers: Option<u32>,
    /// Seconds until the next tracker announce.
    pub reannounce_seconds: Option<u64>,
    /// Seconds spent running in a session.
    pub active_seconds: u64,
    /// Seconds spent complete and seeding, in all.
    pub seeding_seconds: u64,
    /// Seconds of the current unbroken seeding run.
    pub consecutive_seeding_seconds: u64,
    /// The bytes moved over the time active.
    pub download_avg_bps: u64,
    pub upload_avg_bps: u64,
    /// Bytes received and thrown away this run: pieces that failed their
    /// check, and blocks that arrived when already in hand. Counts from zero
    /// each time the torrent starts.
    pub wasted_bytes: u64,
    /// The last time a complete copy was in reach.
    pub last_seen_complete_ms: Option<i64>,
    /// The last time it sent or received payload, in milliseconds since the
    /// epoch; None while it never has.
    pub last_activity_ms: Option<i64>,
    /// The torrent's pieces, once its metainfo is in hand.
    pub pieces: Option<TorrentPieces>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub comment: Option<String>,
    pub added_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    /// Where its files sit when they were placed outside its storage
    /// directory, with no folder of their own.
    pub files_dir: Option<String>,
    /// Its place among the torrents still downloading, from 1; None while
    /// it seeds.
    pub queue_position: Option<u32>,
    pub seeding_left: Option<String>,
    /// The torrent's own seeding policy, when it sets a leaf.
    pub seeding_override: Option<SeedingPolicy>,
    /// The seeding terms in force, and the level each comes from.
    pub seeding_policy: Seeding,
    pub seeding_levels: SeedingLevels,
    /// The host pattern of the tracker rule its trackers match.
    pub tracker_rule: Option<String>,
    /// The upload cap its levels resolve to, held beside its own limit.
    pub upload_cap: Option<u64>,
    /// Whether its archives are extracted: as its job was built, or as the
    /// levels say until then.
    pub unpack: bool,
    /// Whether it was asked to extract its archives; `None` inherits.
    pub unpack_override: Option<bool>,
    pub route: Option<String>,
    pub files: Vec<TorrentFile>,
    /// Whether its pieces are fetched in order.
    pub sequential: bool,
    /// Whether each file's first and last pieces are fetched first.
    pub first_last_piece_first: bool,
    /// Each file's bytes in hand, by its index in the torrent.
    pub file_have: Vec<u64>,
    pub trackers: Vec<TorrentTracker>,
    /// The attributes its job was added with.
    pub metadata: Vec<(String, String)>,
    /// Its job's tags, in tag order.
    pub tags: Vec<String>,
}

/// What a support report reads of a torrent beyond its view.
#[derive(Debug, Clone)]
pub struct TorrentSupport {
    pub view: TorrentView,
    /// The one copy of its metainfo, once it is in hand.
    pub metainfo: Option<Bytes>,
    /// The ledger as of this reading, with the bytes it wasted.
    pub counters: TorrentCounters,
    /// Its established peers, the seeders among them, and the peers still
    /// in their handshake.
    pub peers: (u32, u32, u32),
    pub peer_kinds: PeerKinds,
    /// Where its metadata fetch asks for peers, while one runs.
    pub fetch: Option<weaver_torrent::FetchPhase>,
    pub in_session: bool,
    pub leg: Option<TorrentLeg>,
    /// Whether its job is built; until it is, the pieces its checkpoint
    /// has in hand.
    pub has_job: bool,
    pub kept_pieces: u32,
    /// The bytes of the files it wants, and those of them in hand.
    pub wanted_bytes: u64,
    pub wanted_have_bytes: u64,
    /// The snapshot the exporter renders, as last published.
    pub metrics: TorrentMetrics,
}

/// The leg of its route a torrent's session runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentLeg {
    pub position: usize,
    pub egress_id: u32,
    pub own_sockets: bool,
}

/// A torrent's connections to peers, counted by how they were made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerKinds {
    pub tcp: u32,
    pub utp: u32,
    pub outgoing: u32,
    pub incoming: u32,
    pub encrypted: u32,
}

impl PeerKinds {
    pub fn of(peers: &[weaver_torrent::PeerInfo]) -> Self {
        let mut kinds = Self::default();
        for peer in peers {
            match peer.connection {
                weaver_torrent::ConnectionKind::Tcp => kinds.tcp += 1,
                weaver_torrent::ConnectionKind::Utp => kinds.utp += 1,
            }
            if peer.outgoing {
                kinds.outgoing += 1;
            } else {
                kinds.incoming += 1;
            }
            if peer.encryption != weaver_torrent::Encryption::None {
                kinds.encrypted += 1;
            }
        }
        kinds
    }
}

/// A torrent's pieces: how many, how long each is, and how many verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TorrentPieces {
    pub count: u32,
    pub size: u64,
    pub have: u32,
}

impl TorrentView {
    /// Where the torrent's output is: the folder that holds it, and the
    /// output itself, its file when it is one file named as the torrent is,
    /// else its folder. Both are empty until weaver has placed the torrent.
    pub fn paths(&self) -> (String, String) {
        self.paths_as(self.is_one_file())
    }

    /// Whether it is one file named as the torrent is.
    pub fn is_one_file(&self) -> bool {
        matches!(self.files.as_slice(), [file] if file.path == self.name)
    }

    /// Its `paths`, given whether it is one file named as the torrent is:
    /// for a view read without its files.
    pub fn paths_as(&self, one_file: bool) -> (String, String) {
        let Some(dir) = self.files_dir.as_ref().or(self.storage_dir.as_ref()) else {
            return (String::new(), String::new());
        };
        let dir = PathBuf::from(dir);
        let single = if one_file {
            Path::new(&self.name).file_name()
        } else {
            None
        };
        let content = match single {
            Some(name) => dir.join(name),
            None => dir.clone(),
        };
        // Files with no folder of their own sit in the directory they went
        // to, which is their save path (qBittorrent `TorrentImpl::contentPath`
        // is the save path for files with no root folder).
        let save = match &self.files_dir {
            Some(_) => dir,
            None => content
                .parent()
                .map_or_else(|| content.clone(), Path::to_path_buf),
        };
        (
            save.to_string_lossy().into_owned(),
            content.to_string_lossy().into_owned(),
        )
    }

    /// The tracker it goes by (qBittorrent `TorrentImpl::currentTracker`:
    /// libtorrent's `current_tracker`, the tracker that last answered, else
    /// the first one listed). Weaver keeps each tracker's latest outcome
    /// only, so the one that last answered is the working one announced to
    /// most recently.
    pub fn current_tracker(&self) -> Option<&TorrentTracker> {
        self.trackers
            .iter()
            .filter(|tracker| tracker.status == TrackerStatus::Working)
            .max_by_key(|tracker| tracker.last_announce_ms)
            .or_else(|| self.trackers.first())
    }

    /// How much of `file` is in hand, from 0 to 1.
    pub fn file_progress(&self, file: &TorrentFile) -> f64 {
        let have = self
            .file_have
            .get(file.index as usize)
            .copied()
            .unwrap_or(0);
        if file.length == 0 {
            1.0
        } else {
            (have as f64 / file.length as f64).min(1.0)
        }
    }
}

// What the exporter reads of a torrent beside its view.
struct Unviewed {
    wanted_bytes: u64,
    hash_failed_bytes: u64,
    redundant_bytes: u64,
    seeding_seconds: u64,
    consecutive_seeding_seconds: u64,
    peers_connecting: u32,
    leg: Option<(String, usize)>,
    fetch: Option<weaver_torrent::FetchPhase>,
    seeding: SeedingSeries,
}

impl Unviewed {
    // The torrent's figures: its view's, with these and its trackers.
    fn series(self, view: &TorrentView, announces: &[TrackerAnnounce]) -> TorrentSeries {
        TorrentSeries {
            job_id: view.job_id,
            category: view.category.clone().unwrap_or_default(),
            route: view.route.clone().unwrap_or_default(),
            state: view.state,
            have_bytes: view.have_bytes,
            total_bytes: view.total_bytes,
            wanted_bytes: self.wanted_bytes,
            pieces: view.pieces,
            downloaded_bytes: view.downloaded_bytes,
            uploaded_bytes: view.uploaded_bytes,
            hash_failed_bytes: self.hash_failed_bytes,
            redundant_bytes: self.redundant_bytes,
            active_seconds: view.active_seconds,
            seeding_seconds: self.seeding_seconds,
            consecutive_seeding_seconds: self.consecutive_seeding_seconds,
            download_bps: view.download_bps,
            upload_bps: view.upload_bps,
            ratio: view.ratio,
            peers_established: view.peers,
            peers_connecting: self.peers_connecting,
            peer_seeders: view.seeds,
            leg: self.leg,
            fetch: self.fetch,
            seeding: self.seeding,
            trackers: crate::torrents::metrics::trackers(&view.trackers, announces),
        }
    }
}

impl TorrentService {
    // The torrents, as queue items, into the published queue, and with
    // their sessions as the tick last read them, as the exporter reads
    // them: one reading of each torrent.
    pub(super) async fn publish(&self) {
        let now = self.inner.clock.unix_ms();
        // Presence is counted in the walk that picks the torrents.
        let mut presence = Presence::default();
        let listed = self
            .listed_noting(None, false, now, |torrent| presence.note(torrent))
            .await;
        let sessions = self
            .inner
            .sessions_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let (views, torrents): (Vec<TorrentView>, Vec<TorrentSeries>) = listed.into_iter().unzip();
        self.inner.presence.send_if_modified(|was| {
            let changed = *was != presence;
            *was = presence;
            changed
        });
        let sessions = with_routes_of(sessions, &torrents);
        self.inner
            .published
            .send_replace(Arc::new(Published { torrents, sessions }));
        if let Some(queue) = self.queue() {
            queue.publish_owned_jobs(
                crate::jobs::Protocol::Torrent,
                self.with_retiring(job_infos(views, now)),
            );
        }
    }

    // The job of `job_id`, which changed, into the published queue beside
    // the others as they stand: a settle reads the torrents that spoke, not
    // every one (libtorrent posts a state update for the torrents that
    // changed). The exporter's reading takes it at the next publish of
    // every torrent.
    pub(super) async fn publish_job(&self, job_id: JobId) {
        let Some(queue) = self.queue() else {
            return;
        };
        let now = self.inner.clock.unix_ms();
        if let Some(view) = self.torrent(job_id).await {
            let status = job_status(&view);
            queue.publish_owned_job(
                crate::jobs::Protocol::Torrent,
                report(view, status, now).job_info(),
            );
        }
    }

    /// Warnings that stand: the disk filling up under waiting torrents.
    pub async fn warnings(&self) -> Vec<String> {
        let live = self.inner.live.lock().await;
        let waiting: Vec<&str> = live
            .values()
            .filter(|torrent| torrent.upload_mode())
            .map(|torrent| torrent.record.name.as_str())
            .collect();
        if waiting.is_empty() {
            return Vec::new();
        }
        let volume = self.settings().complete_dir;
        vec![disk_warning(&volume, &waiting)]
    }

    pub(super) fn view(&self, settings: &TorrentSettings, torrent: &Live, now: i64) -> TorrentView {
        // The session as it is now, not as the last tick found it: the
        // state and every figure from one reading of it.
        self.view_of(settings, torrent, torrent.reading(now), now)
    }

    pub(super) fn view_of(
        &self,
        settings: &TorrentSettings,
        torrent: &Live,
        reading: Reading,
        now: i64,
    ) -> TorrentView {
        let (state, why) = status::project(&torrent.observed(reading.engine));
        let status::WhyParts {
            detail,
            stated,
            route_reason,
        } = status::Why::split(why);
        let counters = &reading.counters;
        let record = &torrent.record;
        let total = torrent.total;
        let have = on_disk(torrent.layout.as_deref(), &reading.file_have);
        let (wanted, wanted_have) = wanted(
            torrent.layout.as_deref(),
            &torrent.levels,
            &reading.file_have,
        );
        let rules = self.rules();
        let rule = torrent.rule(&rules);
        let (policy, seeding_levels) = settings.seeding(record, rule);
        let swarm = status::swarm(&torrent.announces(), now);
        // The time the torrent has run to this reading, the part of a
        // second included, and its averages over that same time.
        let active_seconds =
            status::active_seconds(counters.active_duration_s, reading.timed.active_ms);
        TorrentView {
            job_id: record.job_id,
            name: record.name.clone(),
            infohash: hashes_of(record).best().as_string(),
            infohash_v1: record.infohash_v1.map(hex::encode),
            infohash_v2: record.infohash_v2.map(hex::encode),
            version: torrent.about.version,
            magnet: Magnet::of(hashes_of(record), Some(record.name.clone()), Vec::new())
                .to_string(),
            source: record.source,
            state,
            detail,
            stated,
            route_reason,
            paused_by: torrent.paused_by,
            category: settings
                .category(record.category_id)
                .map(|category| category.name.clone()),
            private: torrent.private,
            forced: record.forced,
            // Over the bytes it wants, all of them had when it wants none
            // (libtorrent `torrent_status::progress`).
            progress: if total == 0 {
                0.0
            } else if wanted == 0 {
                1.0
            } else {
                (wanted_have as f64 / wanted as f64).min(1.0)
            },
            check_progress: reading.checked.filter(|_| {
                matches!(
                    state,
                    TorrentState::CheckingFiles | TorrentState::CheckingResumeData
                )
            }),
            total_bytes: total,
            have_bytes: have,
            wanted_bytes: wanted,
            wanted_have_bytes: wanted_have,
            uploaded_bytes: counters.uploaded_bytes,
            downloaded_bytes: counters.downloaded_bytes,
            run_uploaded_bytes: reading.run.1,
            run_downloaded_bytes: reading.run.0,
            ratio: status::ratio(counters, have),
            download_bps: reading.speeds.0,
            upload_bps: reading.speeds.1,
            upload_limit: record.upload_limit,
            download_limit: record.download_limit,
            // The wanted bytes still to come: an Ignored file never comes
            // (qBittorrent `TorrentImpl::eta`, wanted less completed).
            eta_seconds: status::eta_seconds(wanted.saturating_sub(wanted_have), reading.speeds.0),
            peers: reading.peers.0,
            seeds: reading.peers.1,
            leechers: reading.peers.0.saturating_sub(reading.peers.1),
            max_peers: settings.torrent.max_peers_total,
            swarm_seeds: swarm.seeds,
            swarm_leechers: swarm.leechers,
            reannounce_seconds: swarm.reannounce_seconds,
            active_seconds,
            seeding_seconds: counters.finished_duration_s,
            consecutive_seeding_seconds: counters.consecutive_seeding_s,
            download_avg_bps: status::average_bps(counters.downloaded_bytes, active_seconds),
            upload_avg_bps: status::average_bps(counters.uploaded_bytes, active_seconds),
            wasted_bytes: counters.wasted_bytes(),
            last_seen_complete_ms: counters.last_seen_complete_ms,
            last_activity_ms: counters.last_activity_ms,
            pieces: torrent.pieces(),
            created_by: torrent.about.created_by.clone(),
            created_at_ms: torrent.about.created_at_ms,
            comment: torrent.about.comment.clone(),
            added_at_ms: record.added_at_ms,
            completed_at_ms: record.completed_at_ms,
            storage_dir: record.storage_dir.clone(),
            files_dir: record.files_dir.clone(),
            queue_position: torrent.queue_rank,
            seeding_left: status::goal_left(&policy, counters, have, now),
            seeding_override: record.policy_override,
            seeding_policy: policy,
            seeding_levels,
            tracker_rule: rule.map(|rule| rule.host_pattern.clone()),
            upload_cap: settings.upload_cap(record, rule),
            unpack: record
                .unpack_in_force
                .unwrap_or_else(|| settings.unpack(record, rule)),
            unpack_override: record.unpack,
            scope: settings.scope(record.category_id, rule),
            route: torrent.route.as_ref().map(|key| key.as_str().to_owned()),
            files: Vec::new(),
            sequential: record.sequential,
            first_last_piece_first: record.first_last_piece_first,
            file_have: reading.file_have,
            trackers: Vec::new(),
            metadata: record.metadata.clone(),
            tags: Vec::new(),
        }
    }

    // Each view's tags, from the tags as last read. A view whose tags cannot
    // be read shows none.
    fn tag(&self, views: &mut [&mut TorrentView]) {
        match self.inner.db.tag_snapshot() {
            Ok(tags) => {
                for view in views {
                    view.tags = tags.of(view.job_id).to_vec();
                }
            }
            Err(error) => tracing::warn!(error = %error, "could not read the tags"),
        }
    }

    /// One torrent, with its files and trackers.
    pub async fn torrent(&self, job_id: JobId) -> Option<TorrentView> {
        self.ensure(&[job_id]).await;
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let (mut view, announces, pinned, listed) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let mut view = self.view(&settings, torrent, now);
            view.files = torrent.files();
            (
                view,
                torrent.announces(),
                torrent.record.pool_member,
                torrent.trackers.clone(),
            )
        };
        self.track(&mut view, pinned, &listed, &announces, &mut HashMap::new());
        self.tag(&mut [&mut view]);
        Some(view)
    }

    /// What a support report reads of one torrent: its view, its metainfo,
    /// and the held figures the view folds together. One pass over the
    /// torrent under the lock its view takes; its peers are read after,
    /// from its session.
    pub async fn support(&self, job_id: JobId) -> Option<TorrentSupport> {
        self.ensure(&[job_id]).await;
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let (mut support, announces, handle, pinned, listed) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let reading = torrent.reading(now);
            let (wanted_bytes, wanted_have_bytes) = wanted(
                torrent.layout.as_deref(),
                &torrent.levels,
                &reading.file_have,
            );
            let attached = torrent.attached.as_ref();
            let support = TorrentSupport {
                view: self.view(&settings, torrent, now),
                metainfo: torrent.record.metainfo.clone(),
                counters: reading.counters,
                peers: reading.peers,
                peer_kinds: PeerKinds::default(),
                fetch: torrent.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
                in_session: attached.is_some(),
                leg: attached
                    .and_then(|attached| attached.leg.as_ref())
                    .map(|leg| TorrentLeg {
                        position: leg.tenancy.leg().1,
                        egress_id: leg.egress_id,
                        own_sockets: leg.own_sockets,
                    }),
                has_job: torrent.store.is_some(),
                kept_pieces: torrent.kept_pieces,
                wanted_bytes,
                wanted_have_bytes,
                metrics: TorrentMetrics::default(),
            };
            (
                support,
                torrent.announces(),
                attached.map(|attached| Arc::clone(&attached.handle)),
                torrent.record.pool_member,
                torrent.trackers.clone(),
            )
        };
        self.track(
            &mut support.view,
            pinned,
            &listed,
            &announces,
            &mut HashMap::new(),
        );
        self.tag(&mut [&mut support.view]);
        if let Some(handle) = handle {
            support.peer_kinds = PeerKinds::of(&handle.peers());
        }
        support.metrics = self.metrics();
        Some(support)
    }

    /// Every torrent, or a category's, in the order they were added, each
    /// with its trackers.
    pub async fn torrents(&self, category: Option<&str>) -> Vec<TorrentView> {
        let now = self.inner.clock.unix_ms();
        self.listed(category, false, now)
            .await
            .into_iter()
            .map(|(view, _)| view)
            .collect()
    }

    /// Every torrent with its files, from one pass over them: what
    /// `torrent` gives one at a time.
    pub async fn torrents_with_files(&self) -> Vec<TorrentView> {
        let now = self.inner.clock.unix_ms();
        self.listed(None, true, now)
            .await
            .into_iter()
            .map(|(view, _)| view)
            .collect()
    }

    // Every torrent, or a category's, in the order they were added, each
    // with its trackers, and its figures as the exporter reads them: both
    // from one reading of its session.
    async fn listed(
        &self,
        category: Option<&str>,
        files: bool,
        now: i64,
    ) -> Vec<(TorrentView, TorrentSeries)> {
        self.listed_noting(category, files, now, |_| {}).await
    }

    // As `listed`, with `note` shown every live torrent in the walk that
    // picks them.
    async fn listed_noting(
        &self,
        category: Option<&str>,
        files: bool,
        now: i64,
        note: impl FnMut(&Live) + Send,
    ) -> Vec<(TorrentView, TorrentSeries)> {
        let shown = if files { Shown::WHOLE } else { Shown::LISTED };
        let pick = Pick {
            category: category.map(CategoryPick::Named),
            ..Pick::default()
        };
        self.listed_by(shown, true, false, now, |settings, live| {
            pick.ids_noting(settings, live, note)
        })
        .await
        .into_iter()
        .map(|(view, series, _)| (view, series.expect("asked for")))
        .collect()
    }

    // The torrents `pick` names, in the order it names them, each with what
    // `shown` asks for, with its figures as the exporter reads them when
    // `series` asks for those, both from one reading of its session, and
    // with the totals of its files when `totals` asks for those. Only the
    // torrents picked are read.
    pub(super) async fn listed_by(
        &self,
        shown: Shown,
        series: bool,
        totals: bool,
        now: i64,
        pick: impl FnOnce(&TorrentSettings, &BTreeMap<JobId, Live>) -> Vec<JobId>,
    ) -> Vec<(TorrentView, Option<TorrentSeries>, Option<FileTotals>)> {
        // A torrent's figures name its trackers.
        let shown = Shown {
            trackers: shown.trackers || series,
            ..shown
        };
        let settings = self.settings();
        // Under the lock, each torrent's files are only taken by reference
        // with its levels: the list is built after the lock is let go.
        type Files = Option<(Option<Arc<PieceLayout>>, Vec<FilePriority>)>;
        // With its pool member, which names its trackers' lookups, and the
        // trackers it lists.
        type Listed = (
            TorrentView,
            Vec<TrackerAnnounce>,
            Option<Unviewed>,
            Files,
            Option<u32>,
            Option<Arc<[TrackerEntry]>>,
        );
        let mut views: Vec<Listed> = {
            let live = self.inner.live.lock().await;
            pick(&settings, &live)
                .iter()
                .filter_map(|job_id| live.get(job_id))
                .map(|torrent| {
                    let reading = torrent.reading(now);
                    let unviewed = series.then(|| self.unviewed(&settings, torrent, &reading, now));
                    let view = self.view_of(&settings, torrent, reading, now);
                    let files = (shown.files || totals)
                        .then(|| (torrent.layout.clone(), torrent.levels.clone()));
                    (
                        view,
                        if shown.trackers {
                            torrent.announces()
                        } else {
                            Vec::new()
                        },
                        unviewed,
                        files,
                        torrent.record.pool_member,
                        shown.trackers.then(|| torrent.trackers.clone()),
                    )
                })
                .collect()
        };
        let mut names = HashMap::new();
        let mut file_totals = Vec::with_capacity(if totals { views.len() } else { 0 });
        for (view, announces, _, files, pinned, listed) in &mut views {
            if let Some((layout, levels)) = files.take() {
                if totals {
                    file_totals.push(FileTotals::of_layout(
                        &view.name,
                        layout.as_deref(),
                        &levels,
                        &view.file_have,
                    ));
                }
                if shown.files {
                    view.files = files_of(layout.as_deref(), &levels);
                }
            }
            if let Some(listed) = listed {
                self.track(view, *pinned, listed, announces, &mut names);
            }
        }
        if shown.tags {
            self.tag(&mut views.iter_mut().map(|(view, ..)| view).collect::<Vec<_>>());
        }
        let mut file_totals = file_totals.into_iter();
        views
            .into_iter()
            .map(|(view, announces, unviewed, ..)| {
                let series = unviewed.map(|unviewed| unviewed.series(&view, &announces));
                (view, series, file_totals.next())
            })
            .collect()
    }

    // What the exporter reads of `torrent` that its view does not carry,
    // from `reading`.
    fn unviewed(
        &self,
        settings: &TorrentSettings,
        torrent: &Live,
        reading: &Reading,
        now: i64,
    ) -> Unviewed {
        let rules = self.rules();
        let policy = settings.policy(&torrent.record, torrent.rule(&rules));
        let (wanted_bytes, _) = wanted(
            torrent.layout.as_deref(),
            &torrent.levels,
            &reading.file_have,
        );
        Unviewed {
            wanted_bytes,
            hash_failed_bytes: reading.counters.failed_bytes,
            redundant_bytes: reading.counters.redundant_bytes,
            seeding_seconds: reading.counters.finished_duration_s,
            consecutive_seeding_seconds: reading.counters.consecutive_seeding_s,
            peers_connecting: reading.peers.2,
            leg: torrent
                .attached
                .as_ref()
                .and_then(|attached| attached.leg.as_ref())
                .map(|leg| leg.tenancy.leg().clone()),
            fetch: torrent.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            seeding: SeedingSeries::of(&policy, &reading.counters, torrent.have_bytes, now),
        }
    }

    // A view's trackers: those listed for it, as its run announces to
    // them, with who resolves their names on its route, on a pool route its
    // member `pinned`. `names` holds each scope's and member's planned once.
    pub(super) fn track(
        &self,
        view: &mut TorrentView,
        pinned: Option<u32>,
        listed: &[TrackerEntry],
        announces: &[TrackerAnnounce],
        names: &mut HashMap<(TorrentScope, Option<u32>), Option<ResolvedBy>>,
    ) {
        let names = *names.entry((view.scope, pinned)).or_insert_with(|| {
            self.inner
                .sessions
                .routes()
                .plan(view.scope)
                .names_for(pinned)
        });
        view.trackers = status::trackers(listed.to_vec(), announces, names);
    }

    /// The torrents as queue items.
    pub async fn job_infos(&self) -> Vec<JobInfo> {
        let now = self.inner.clock.unix_ms();
        job_infos(self.torrents(None).await, now)
    }

    /// The scope whose route a torrent added in `category` under `rule` takes.
    pub fn scope_of(&self, category: Option<&str>, rule: Option<&TrackerRule>) -> TorrentScope {
        let settings = self.settings();
        let id = category
            .and_then(|name| resolve_category_config(&settings.categories, name))
            .map(|category| category.id);
        settings.scope(id, rule)
    }
}

// The sessions as read, with every route a torrent names and no session was
// read on held: a route's series say what runs on it now, and with none
// open, nothing runs.
fn with_routes_of(
    mut sessions: Vec<SessionSeries>,
    torrents: &[TorrentSeries],
) -> Vec<SessionSeries> {
    for torrent in torrents {
        let route = torrent.route.as_str();
        if !route.is_empty() && !sessions.iter().any(|session| session.route == route) {
            sessions.push(SessionSeries::held(route));
        }
    }
    sessions.sort_by(|a, b| a.route.cmp(&b.route));
    sessions
}

// `views` as queue items at `now_ms`.
fn job_infos(views: Vec<TorrentView>, now_ms: i64) -> Vec<JobInfo> {
    views
        .into_iter()
        .map(|view| {
            let status = job_status(&view);
            report(view, status, now_ms).job_info()
        })
        .collect()
}

pub(super) fn job_status(view: &TorrentView) -> JobStatus {
    match view.state {
        TorrentState::Queued => JobStatus::Queued,
        TorrentState::FetchingMetadata => JobStatus::FetchingMetadata,
        TorrentState::CheckingFiles => JobStatus::CheckingFiles,
        // Read back from what was saved: a check, as qBittorrent's filter
        // counts it (torrentfilter.cpp:190).
        TorrentState::CheckingResumeData => JobStatus::CheckingFiles,
        TorrentState::Downloading => JobStatus::Downloading,
        TorrentState::Stalled => JobStatus::Stalled,
        TorrentState::Seeding => JobStatus::Seeding,
        TorrentState::Paused => JobStatus::Paused,
        TorrentState::Moving => JobStatus::Moving,
        // It waits for a certificate, held where it is.
        TorrentState::NeedsCertificate => JobStatus::Stalled,
        // Held stopped until the operator starts it again.
        TorrentState::FilesMissing => JobStatus::Paused,
        // A stop with any reason but its met goal is a failure.
        TorrentState::Stopped => match view.detail.as_deref() {
            _ if view.stated == Some(status::Stated::GoalMet) => JobStatus::Complete,
            None if view.completed_at_ms.is_some() => JobStatus::Complete,
            detail => JobStatus::Failed {
                error: detail.unwrap_or_default().to_owned(),
            },
        },
    }
}
