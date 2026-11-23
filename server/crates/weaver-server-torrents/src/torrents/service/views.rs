// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_room::disk_warning;
use super::listing::{CategoryPick, FileTotals, Pick, Shown};
use super::live::Live;
use super::settings::{Presence, SeedingLevels, TorrentSettings};
use super::{
    ActionError, NotReady, Reading, TorrentService, files_of, hashes_of, on_disk, report, wanted,
};
use crate::jobs::JobId;
use crate::jobs::handle::{HoldOrigin, JobInfo};
use crate::jobs::model::JobStatus;
use crate::proxies::{ResolvedBy, TorrentScope};
use crate::settings::model::TrackerRule;
use crate::settings::{Seeding, SeedingPolicy};
use crate::tags::TagsQueries;
use crate::torrents::layout::PieceLayout;
use crate::torrents::metrics::{
    Published, SeedingSeries, SessionSeries, TorrentMetrics, TorrentSeries,
};
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::sessions::TorrentRouteReason;
use crate::torrents::status::Engine;
use crate::torrents::{
    FilePriority, TorrentCounters, TorrentFile, TorrentSource, TorrentState, TorrentTracker,
    TrackerEntry, TrackerStatus, UnpackEnd, UnpackedFile, status,
};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};
use weaver_server_settings::categories::model::resolve_category_config;
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
    /// While its start waits for room: what it needs on its disk, and what
    /// the disk leaves, or the most it ever holds, as a usenet job shows.
    pub disk_need_bytes: Option<u64>,
    pub disk_free_bytes: Option<u64>,
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
    /// How its unpacking came out, once over: the unpacking's own line,
    /// which nothing of the torrent's reads.
    pub unpack_end: Option<UnpackEnd>,
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
    /// The reading its clock fields were taken at.
    pub clock: ViewClock,
}

/// The reading a view's clock fields were taken at: a reader that holds the
/// view runs them on to the time it answers, as a reading then gives them
/// while nothing else about the torrent changes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ViewClock {
    /// When it was read, in milliseconds since the epoch.
    pub at_ms: i64,
    /// When its soonest tracker announce is, in milliseconds since the epoch.
    pub next_announce_ms: Option<i64>,
    /// The milliseconds the time active, the time seeding and the current
    /// seeding run had run past their whole seconds.
    pub active_ms: u16,
    pub finished_ms: u16,
    pub consecutive_ms: u16,
    /// Whether its session ran it: its time active runs on.
    pub active: bool,
}

/// A view's clock fields at a given time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockFields {
    pub active_seconds: u64,
    pub seeding_seconds: u64,
    pub consecutive_seeding_seconds: u64,
    pub reannounce_seconds: Option<u64>,
}

/// What a list's row carries beside its own fields when asked for it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowParts {
    pub files: Vec<TorrentFile>,
    /// Each file's bytes in hand, by its index in the torrent.
    pub file_have: Vec<u64>,
    pub trackers: Vec<TorrentTracker>,
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

pub use weaver_server_torrent_store::torrents::metrics::TorrentPieces;

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

    /// Its clock fields run on to `now_ms` from the reading it was taken
    /// at, as a reading at `now_ms` gives them while nothing else about the
    /// torrent changed: the time active while its session runs it, the time
    /// seeding and the current seeding run while it seeds (the durations of
    /// `Timed::advanced`), and the countdown to its next announce. A time
    /// before the reading runs nothing on.
    pub fn clock_at(&self, now_ms: i64) -> ClockFields {
        let clock = &self.clock;
        let elapsed = u64::try_from(now_ms - clock.at_ms).unwrap_or(0);
        // Whole seconds and the part past them, run on while `running`.
        let run = |whole: u64, part: u16, running: bool| {
            if running {
                let ms = u64::from(part).saturating_add(elapsed);
                (whole.saturating_add(ms / 1000), ms % 1000)
            } else {
                (whole, u64::from(part))
            }
        };
        // The time active counts a part of a second as a whole one.
        let active_whole = self
            .active_seconds
            .saturating_sub(u64::from(clock.active_ms > 0));
        let (active_whole, active_part) = run(active_whole, clock.active_ms, clock.active);
        let seeding = self.state == TorrentState::Seeding;
        ClockFields {
            active_seconds: status::active_seconds(active_whole, active_part),
            seeding_seconds: run(self.seeding_seconds, clock.finished_ms, seeding).0,
            consecutive_seeding_seconds: run(
                self.consecutive_seeding_seconds,
                clock.consecutive_ms,
                seeding,
            )
            .0,
            reannounce_seconds: match clock.next_announce_ms {
                Some(next) => Some(status::reannounce_seconds(next, now_ms)),
                None => self.reannounce_seconds,
            },
        }
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
            tracker_rule: view.tracker_rule.clone(),
        }
    }
}

impl TorrentService {
    // The torrents, as queue items, into the published queue, as rows into
    // the snapshot, and with their sessions as the tick last read them, as
    // the exporter reads them: one reading of each torrent.
    pub async fn publish(&self) {
        let now = self.inner.clock.unix_ms();
        // Whether every stored torrent is here, taken before they are read.
        let every = self.whole().is_ok();
        let stamp = self.inner.snapshot.stamp();
        // Presence, and whether any torrent downloads, are counted in the
        // walk that picks the torrents.
        let mut presence = Presence::default();
        let mut downloading = false;
        let listed = self
            .listed_noting(None, false, true, now, |torrent| {
                presence.note(torrent);
                downloading |= torrent.downloading();
            })
            .await;
        self.note_downloading(downloading);
        let sessions = self
            .inner
            .sessions_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut torrents = Vec::with_capacity(listed.len());
        let mut rows = Vec::with_capacity(listed.len());
        for (view, series, totals) in listed {
            torrents.push(series);
            rows.push((view, totals));
        }
        self.inner.snapshot.replace(&rows, every, stamp);
        self.inner.presence.send_if_modified(|was| {
            let changed = *was != presence;
            *was = presence;
            changed
        });
        let sessions = with_routes_of(sessions, &torrents);
        {
            let mut at = super::locked(&self.inner.published_at);
            *at = torrents
                .iter()
                .enumerate()
                .map(|(place, torrent)| (torrent.job_id, place))
                .collect();
            self.inner
                .published
                .send_replace(Arc::new(Published { torrents, sessions }));
        }
        if let Some(queue) = self.queue() {
            let views = rows.into_iter().map(|(view, _)| view).collect();
            queue.publish_owned_jobs(
                crate::jobs::Protocol::Torrent,
                self.with_retiring(job_infos(views, now)),
            );
        }
    }

    // The torrents of `hashes`, whose trackers or metadata fetch changed,
    // published as `publish` publishes each: their rows, their figures for
    // the exporter and their jobs in the queue, beside the others as they
    // stand. Only they are read (libtorrent posts a state update for the
    // torrents that changed); one not yet in the published figures waits
    // for the next publish of every torrent there.
    pub async fn publish_hashes(
        &self,
        hashes: &std::collections::HashSet<weaver_torrent::core::Id20>,
    ) {
        self.publish_picked(|live| {
            live.values()
                .filter(|torrent| hashes.contains(&hashes_of(&torrent.record).best()))
                .map(|torrent| torrent.record.job_id)
                .collect()
        })
        .await;
    }

    // The torrent of `job_id`, whose metadata fetch began or ended,
    // published as a tracker change publishes its torrents: its row, its
    // figures for the exporter and its job in the queue.
    pub(super) async fn publish_fetch(&self, job_id: JobId) {
        self.publish_picked(|live| {
            live.contains_key(&job_id)
                .then_some(job_id)
                .into_iter()
                .collect()
        })
        .await;
    }

    // The torrents `pick` takes from the live ones, published as
    // `publish_hashes` says.
    async fn publish_picked(&self, pick: impl FnOnce(&BTreeMap<JobId, Live>) -> Vec<JobId>) {
        let now = self.inner.clock.unix_ms();
        let stamp = self.inner.snapshot.stamp();
        let listed = self
            .listed_by(Shown::LISTED, true, true, now, |_, live| pick(live))
            .await;
        if listed.is_empty() {
            return;
        }
        let mut series = Vec::with_capacity(listed.len());
        let mut rows = Vec::with_capacity(listed.len());
        for (view, figures, totals) in listed {
            series.extend(figures);
            rows.push((view, totals.unwrap_or_default()));
        }
        self.inner.snapshot.put_some(&rows, stamp);
        {
            let at = super::locked(&self.inner.published_at);
            self.inner.published.send_if_modified(|published| {
                let mut moved = false;
                for figures in series {
                    let Some(&place) = at.get(&figures.job_id) else {
                        continue;
                    };
                    if published.torrents.get(place) != Some(&figures) {
                        Arc::make_mut(published).torrents[place] = figures;
                        moved = true;
                    }
                }
                moved
            });
        }
        if let Some(queue) = self.queue() {
            for (view, _) in rows {
                let status = job_status(&view);
                queue.publish_owned_job(
                    crate::jobs::Protocol::Torrent,
                    report(view, status, now).job_info(),
                );
            }
        }
    }

    // The job of `job_id`, which changed, into the published queue beside
    // the others as they stand, and its row into the snapshot: a settle
    // reads the torrents that spoke, not every one (libtorrent posts a
    // state update for the torrents that changed). The exporter's reading
    // takes it at the next publish of every torrent.
    pub async fn publish_job(&self, job_id: JobId) {
        self.publish_job_as(job_id, false).await;
    }

    // `publish_job` for the torrent an add just put in place: the add is
    // what creates its job, once. A start publishes it again and says
    // nothing of it.
    pub(super) async fn publish_added(&self, job_id: JobId) {
        self.publish_job_as(job_id, true).await;
    }

    async fn publish_job_as(&self, job_id: JobId, added: bool) {
        let now = self.inner.clock.unix_ms();
        let stamp = self.inner.snapshot.stamp();
        let view = self.torrent(job_id).await;
        self.inner.snapshot.put_one(job_id, view.as_ref(), stamp);
        let Some(queue) = self.queue() else {
            return;
        };
        if let Some(view) = view {
            let status = job_status(&view);
            let job = report(view, status, now).job_info();
            // Created once it is in the queue, so a reader of the event
            // finds its row.
            let created = added.then(|| job.clone());
            queue.publish_owned_job(crate::jobs::Protocol::Torrent, job);
            if let Some(job) = created {
                queue.created_owned_job(&job);
            }
        }
    }

    /// Each row's tags as the tags now stand: the tags move a row with no
    /// publish of the torrents. A change to the tags does this on its own
    /// soon after; a caller that changed them and answers from the rows
    /// does it at once. It reads the database: call it off the async
    /// runtime.
    pub fn retag(&self) {
        let stamp = self.inner.snapshot.stamp();
        match self.inner.db.tag_snapshot() {
            Ok(tags) => self
                .inner
                .snapshot
                .retag(|job_id| tags.of(job_id).to_vec(), stamp),
            Err(error) => tracing::warn!(error = %error, "could not read the tags"),
        }
    }

    // The rows follow the tags as they change.
    pub(super) fn spawn_follow_tags(&self) {
        let service = self.clone();
        let mut changes = self.inner.db.subscribe_tag_changes();
        self.keep(tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                let service = service.clone();
                if let Err(error) = tokio::task::spawn_blocking(move || service.retag()).await {
                    tracing::warn!(%error, "could not follow the tags");
                }
            }
        }));
    }

    /// Warnings that stand: the stored torrents not all loaded, and the
    /// disk filling up under waiting torrents.
    pub async fn warnings(&self) -> Vec<String> {
        let mut warnings: Vec<String> = self
            .load_fault()
            .map(|why| NotReady::Faulted(why).to_string())
            .into_iter()
            .collect();
        let live = self.inner.live.lock().await;
        let waiting: Vec<&str> = live
            .values()
            .filter(|torrent| torrent.upload_mode())
            .map(|torrent| torrent.record.name.as_str())
            .collect();
        if !waiting.is_empty() {
            let volume = self.settings().complete_dir;
            warnings.push(disk_warning(&volume, &waiting));
        }
        warnings
    }

    pub(super) fn view(&self, settings: &TorrentSettings, torrent: &Live, now: i64) -> TorrentView {
        // The session as it is now, not as the last tick found it: the
        // state and every figure from one reading of it.
        self.view_of(settings, torrent, torrent.reading(now), now)
    }

    pub fn view_of(
        &self,
        settings: &TorrentSettings,
        torrent: &Live,
        reading: Reading,
        now: i64,
    ) -> TorrentView {
        #[cfg(any(test, feature = "test-support"))]
        self.inner
            .views_built
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let [active_ms, finished_ms, consecutive_ms] = reading.timed.parts();
        let clock_runs = matches!(reading.engine, Engine::Live { .. });
        let counters = &reading.counters;
        let record = &torrent.record;
        let total = torrent.total;
        let have = on_disk(torrent.layout.as_deref(), &reading.file_have);
        let rules = self.rules();
        let rule = torrent.rule(&rules);
        let (policy, seeding_levels) = settings.seeding(record, rule);
        let mut observed = torrent.observed(reading.engine.clone());
        observed.goal_met =
            status::stopped_goal_met(&policy, counters, have, now, record.wound_down());
        let (state, why) = status::project(&observed);
        let status::WhyParts {
            detail,
            stated,
            route_reason,
        } = status::Why::split(why);
        let (wanted, wanted_have) = wanted(
            torrent.layout.as_deref(),
            &torrent.levels,
            &reading.file_have,
        );
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
            disk_need_bytes: torrent
                .start_refused
                .and_then(|refused| refused.figures().0),
            disk_free_bytes: torrent
                .start_refused
                .and_then(|refused| refused.figures().1),
            route_reason,
            paused_by: torrent.paused_by,
            category: settings
                .category(record.category_id)
                .map(|category| category.name.clone()),
            private: torrent.record.private,
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
            // Over the time spent downloading, as qBittorrent's properties
            // read it, so seeding does not wear the average down.
            download_avg_bps: status::average_bps(
                counters.downloaded_bytes,
                active_seconds.saturating_sub(counters.finished_duration_s),
            ),
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
            tracker_rule: rule.map(|rule| rule.name.clone()),
            upload_cap: settings.upload_cap(record, rule),
            unpack: record
                .unpack_in_force
                .unwrap_or_else(|| settings.unpack(record, rule)),
            unpack_override: record.unpack,
            unpack_end: record.unpack_end.clone(),
            scope: settings.scope(record.category_id, rule),
            route: torrent.route.key().map(|key| key.as_str().to_owned()),
            files: Vec::new(),
            sequential: record.sequential,
            first_last_piece_first: record.first_last_piece_first,
            file_have: reading.file_have,
            trackers: Vec::new(),
            metadata: record.metadata.clone(),
            tags: Vec::new(),
            clock: ViewClock {
                at_ms: now,
                next_announce_ms: swarm.next_announce_ms,
                // Each under a second.
                active_ms: u16::try_from(active_ms).unwrap_or_default(),
                finished_ms: u16::try_from(finished_ms).unwrap_or_default(),
                consecutive_ms: u16::try_from(consecutive_ms).unwrap_or_default(),
                active: clock_runs,
            },
        }
    }

    // Each view's tags, from the tags as last read; a cold read runs off the
    // async runtime. A view whose tags cannot be read shows none.
    async fn tag(&self, views: &mut [&mut TorrentView]) {
        let tags = match self.inner.db.cached_tag_snapshot() {
            Some(tags) => Ok(tags),
            None => {
                let db = self.inner.db.clone();
                tokio::task::spawn_blocking(move || db.tag_snapshot())
                    .await
                    .map_err(|error| crate::StateError::Database(error.to_string()))
                    .and_then(|result| result)
            }
        };
        match tags {
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
        self.torrent_shown(job_id, Shown::WHOLE).await
    }

    /// One torrent, carrying beside its own fields only what `shown` names:
    /// its files, its trackers and its tags are read only when asked for,
    /// and are empty otherwise.
    pub async fn torrent_shown(&self, job_id: JobId, shown: Shown) -> Option<TorrentView> {
        self.ensure(&[job_id]).await;
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let (mut view, tracked) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let mut view = self.view(&settings, torrent, now);
            if shown.files {
                view.files = torrent.files();
            }
            let tracked = shown.trackers.then(|| {
                (
                    torrent.announces(),
                    torrent.record.pool_member,
                    torrent.trackers.clone(),
                )
            });
            (view, tracked)
        };
        if let Some((announces, pinned, listed)) = tracked {
            self.track(&mut view, pinned, &listed, &announces, &mut HashMap::new());
        }
        if shown.tags {
            self.tag(&mut [&mut view]).await;
        }
        Some(view)
    }

    /// The files the torrent's unpacking placed beside its own, which its
    /// file list shows after them: each by its name under the directory its
    /// files are in, as its row keeps them, with its length on disk. One no
    /// longer there is not listed. They are no piece, peer or progress of
    /// the torrent's. One read of its row and one look at each file, off the
    /// runtime.
    pub async fn unpacked_files(&self, job_id: JobId) -> Result<Vec<UnpackedFile>, ActionError> {
        self.ensure(&[job_id]).await;
        let dir = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id).ok_or(ActionError::NotFound(job_id))?;
            if torrent.record.unpack_end.is_none() {
                return Ok(Vec::new());
            }
            super::files_dir(torrent)
        };
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || {
            let names = db
                .torrent_outputs(job_id)
                .map_err(|error| ActionError::State(error.to_string()))?;
            Ok(names
                .into_iter()
                .filter_map(|name| {
                    let meta = std::fs::metadata(dir.join(&name)).ok()?;
                    meta.is_file().then_some(UnpackedFile {
                        name,
                        length: meta.len(),
                    })
                })
                .collect())
        })
        .await
        .map_err(|error| ActionError::State(error.to_string()))?
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
        self.tag(&mut [&mut support.view]).await;
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
        self.listed_noting(category, files, false, now, |_| {})
            .await
            .into_iter()
            .map(|(view, series, _)| (view, series))
            .collect()
    }

    // As `listed`, with `note` shown every live torrent in the walk that
    // picks them, and with the totals of their files when `totals` asks for
    // them (default ones otherwise).
    async fn listed_noting(
        &self,
        category: Option<&str>,
        files: bool,
        totals: bool,
        now: i64,
        note: impl FnMut(&Live) + Send,
    ) -> Vec<(TorrentView, TorrentSeries, FileTotals)> {
        let shown = if files { Shown::WHOLE } else { Shown::LISTED };
        let pick = Pick {
            category: category.map(CategoryPick::Named),
            ..Pick::default()
        };
        self.listed_by(shown, true, totals, now, |settings, live| {
            pick.ids_noting(settings, live, note)
        })
        .await
        .into_iter()
        .map(|(view, series, totals)| {
            (view, series.expect("asked for"), totals.unwrap_or_default())
        })
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
            self.tag(&mut views.iter_mut().map(|(view, ..)| view).collect::<Vec<_>>())
                .await;
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
        view.trackers = self.trackers_of(view.scope, pinned, listed, announces, names);
    }

    // `track`'s trackers of a torrent whose route is `scope`'s.
    fn trackers_of(
        &self,
        scope: TorrentScope,
        pinned: Option<u32>,
        listed: &[TrackerEntry],
        announces: &[TrackerAnnounce],
        names: &mut HashMap<(TorrentScope, Option<u32>), Option<ResolvedBy>>,
    ) -> Vec<TorrentTracker> {
        let names = *names
            .entry((scope, pinned))
            .or_insert_with(|| self.inner.sessions.routes().plan(scope).names_for(pinned));
        status::trackers(listed.to_vec(), announces, names)
    }

    /// What a list's rows, read from the snapshot, carry beside their own
    /// fields when asked: each torrent of `ids`' files with the bytes in
    /// hand of each when `files` asks, and its trackers when `trackers`
    /// does, read as [`TorrentService::torrent`] reads them. No view is
    /// built: only these parts of the torrents named are read.
    pub async fn row_parts(
        &self,
        ids: &[JobId],
        files: bool,
        trackers: bool,
    ) -> HashMap<JobId, RowParts> {
        let settings = self.settings();
        let rules = self.rules();
        // Under the lock, each torrent's files are only taken by reference
        // with its levels, and its trackers' entries with its announces: the
        // lists are built after the lock is let go.
        type Read = (
            JobId,
            Option<(Option<Arc<PieceLayout>>, Vec<FilePriority>, Vec<u64>)>,
            Option<(
                TorrentScope,
                Option<u32>,
                Arc<[TrackerEntry]>,
                Vec<TrackerAnnounce>,
            )>,
        );
        let read: Vec<Read> = {
            let live = self.inner.live.lock().await;
            ids.iter()
                .filter_map(|job_id| live.get(job_id))
                .map(|torrent| {
                    let record = &torrent.record;
                    (
                        record.job_id,
                        files.then(|| {
                            (
                                torrent.layout.clone(),
                                torrent.levels.clone(),
                                torrent.file_have_now(),
                            )
                        }),
                        trackers.then(|| {
                            (
                                settings.scope(record.category_id, torrent.rule(&rules)),
                                record.pool_member,
                                torrent.trackers.clone(),
                                torrent.announces(),
                            )
                        }),
                    )
                })
                .collect()
        };
        let mut names = HashMap::new();
        read.into_iter()
            .map(|(job_id, files, trackers)| {
                let (files, file_have) = files
                    .map_or_else(Default::default, |(layout, levels, have)| {
                        (files_of(layout.as_deref(), &levels), have)
                    });
                let trackers =
                    trackers.map_or_else(Vec::new, |(scope, pinned, listed, announces)| {
                        self.trackers_of(scope, pinned, &listed, &announces, &mut names)
                    });
                (
                    job_id,
                    RowParts {
                        files,
                        file_have,
                        trackers,
                    },
                )
            })
            .collect()
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
    let mut read: std::collections::HashSet<String> = sessions
        .iter()
        .map(|session| session.route.to_string())
        .collect();
    for torrent in torrents {
        let route = torrent.route.as_str();
        if !route.is_empty() && read.insert(route.to_owned()) {
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

pub fn job_status(view: &TorrentView) -> JobStatus {
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
