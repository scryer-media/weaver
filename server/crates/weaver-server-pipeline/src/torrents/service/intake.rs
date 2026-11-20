// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::live::Live;
use super::retire::{held_at, held_tops};
use super::settings::TorrentSettings;
use super::{
    Asked, NEEDS_DHT, PeerTerms, TorrentService, give_back, hashes_of, held_by, identity_of,
    in_table_order, locked, once_a_tier, queued, state_error, take_turn, tidy_trackers,
    tracker_tiers,
};
use crate::auth::CallerScope;
use crate::categories::CategoryConfig;
use crate::categories::model::{
    completion_parent, may_place, resolve_category_config, validate_category_path_component,
};
use crate::jobs::JobId;
use crate::persistence::Database;
use crate::pipeline::claim_complete_destination_path;
use crate::post_processing::model::QueueEvent;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::proxies::TorrentScope;
use crate::settings::model::{TrackerRule, tracker_rule_for};
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::{Client, SeedingPolicy, TorrentConfig};
use crate::tags::TagsQueries;
use crate::torrents::admission::{Manifest, Refusal};
use crate::torrents::identity::ClientIdentity;
use crate::torrents::metrics::{FetchOutcome, TorrentEvent};
use crate::torrents::model::Adopted;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::persistence::TorrentsPersistenceMarkTorrentRetiringInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceTorrentHistoryListsInternalQueries;
use crate::torrents::queries::TorrentsInternalQueries;
use crate::torrents::queries::TorrentsQueries;
use crate::torrents::sessions::{RoutePlan, SessionError};
use crate::torrents::{
    ContentLayout, Infohash, KeptIntent, TorrentCounters, TorrentInsert, TorrentIntent,
    TorrentRecord, TorrentSource, TrackerEntry, admission, files, status,
};
use bytes::Bytes;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use tokio::sync::oneshot;
use weaver_server_torrents::torrents::metainfo::{self, tracker};
pub(super) use weaver_server_torrents::torrents::metainfo::{About, Parsed, with_web_seeds};
use weaver_torrent::core::InfoHashes;
use weaver_torrent::tracker::Tracker;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, FilePriority, Identity, Magnet, ResumePeers,
    Session,
};

// The client a torrent under `rule` claims to be on every surface that
// names one: the client its tracker rule names, else Weaver.
pub(super) fn client_of(rule: Option<&TrackerRule>) -> ClientIdentity {
    rule.and_then(|rule| rule.client)
        .unwrap_or(Client::Weaver)
        .name()
}

/// Where a torrent comes from.
#[derive(Debug, Clone)]
pub enum AddSource {
    Metainfo(Vec<u8>),
    Magnet(String),
}

impl AddSource {
    /// The torrent `bytes` hold, as `kind` classified them, when they are
    /// one; the bytes back otherwise.
    pub fn of(kind: crate::ingest::Kind, bytes: Vec<u8>) -> Result<Self, Vec<u8>> {
        use crate::ingest::Kind;
        match kind {
            Kind::Metainfo => Ok(Self::Metainfo(bytes)),
            Kind::Magnet(uri) => Ok(Self::Magnet(uri)),
            Kind::Nzb | Kind::Url(_) => Err(bytes),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AddRequest {
    pub source: AddSource,
    pub category: Option<String>,
    pub policy_override: Option<SeedingPolicy>,
    pub paused: bool,
    pub forced: bool,
    // Whether its archives are extracted; `None` inherits.
    pub unpack: Option<bool>,
    // The attributes its job carries.
    pub metadata: Vec<(String, String)>,
    // Its own rate limits in bytes a second, none being no limit.
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
    // Where its files go.
    pub placement: Placement,
    // The tags its job is added with.
    pub tags: Vec<String>,
}

impl AddRequest {
    /// The add an intake's class-neutral submission asks for: the torrent
    /// it holds, with its own placement and no limits of its own. The one
    /// place a submission becomes a torrent's add.
    pub fn of(submission: crate::jobs::class::Submission) -> Result<Self, AddError> {
        let source = AddSource::of(submission.kind, submission.bytes)
            .map_err(|_| AddError::Invalid("not a torrent".into()))?;
        Ok(Self {
            source,
            category: submission.category,
            policy_override: submission.policy_override,
            paused: submission.paused,
            forced: submission.forced,
            unpack: submission.unpack,
            metadata: submission.metadata,
            upload_limit: None,
            download_limit: None,
            placement: Placement::default(),
            tags: submission.tags,
        })
    }
}

/// Where a torrent's files go, as a client adding it asks (qBittorrent
/// `AddTorrentParams`). The default is weaver's own: a folder of the
/// torrent's under its category's directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placement {
    /// The directory its files go under; None for its category's.
    pub save_path: Option<String>,
    /// How they are laid out there; None for a folder of its own, or with
    /// a save path, as the metainfo lists them.
    pub layout: Option<ContentLayout>,
    /// The directory it downloads under, moving under the save path once
    /// complete; None to download where it stays.
    pub download_path: Option<String>,
    /// Its files are complete where they go already: it seeds them at once
    /// unchecked, and checks each piece as a peer first asks for it
    /// (libtorrent `torrent_flags::seed_mode`).
    pub seed_mode: bool,
}

// `placement` with its paths absolute, as qBittorrent
// `SessionImpl::initLoadTorrentParams` makes them as a torrent is added: a
// relative save path is under the default save path, here `default_save`,
// the torrent's category directory, and a download path with no save path
// moves to the default save path once complete. A relative download path is
// under the default download path, which weaver has none of, so under the
// save path.
pub(super) fn absolute_placement(placement: &Placement, default_save: &Path) -> Placement {
    let under = |base: &Path, path: &str| {
        if Path::new(path).is_absolute() {
            path.to_owned()
        } else {
            base.join(path).to_string_lossy().into_owned()
        }
    };
    let save_path = match (&placement.save_path, &placement.download_path) {
        (Some(path), _) => Some(under(default_save, path)),
        (None, Some(_)) => Some(default_save.to_string_lossy().into_owned()),
        (None, None) => None,
    };
    let download_base = save_path.as_deref().map_or(default_save, Path::new);
    let download_path = placement
        .download_path
        .as_deref()
        .map(|path| under(download_base, path));
    Placement {
        save_path,
        download_path,
        ..placement.clone()
    }
}

// The category an add settled on before its source is read.
enum Admitted {
    // None named, or a name that cannot be a category.
    None,
    // One that is there.
    There(CategoryConfig),
    // One to make, at default settings, once the add goes ahead.
    ToMake(CategoryConfig),
}

impl Admitted {
    fn config(&self) -> Option<&CategoryConfig> {
        match self {
            Self::None => None,
            Self::There(category) | Self::ToMake(category) => Some(category),
        }
    }
}

// Where `placement` puts a torrent of `category` added by `caller`: a path
// that is not absolute resolved under the folder the torrent would save in
// (`absolute_placement`), and every path one the caller may save at
// (`may_place`). The one owner of that answer, for the add and for a
// caller that asks before it fetches anything for one.
fn placement_as(
    caller: CallerScope,
    settings: &TorrentSettings,
    category: Option<&CategoryConfig>,
    placement: &Placement,
) -> Result<Placement, AddError> {
    let relative = |path: &Option<String>| {
        path.as_deref()
            .is_some_and(|path| !Path::new(path).is_absolute())
    };
    let placement = if relative(&placement.save_path)
        || relative(&placement.download_path)
        || (placement.download_path.is_some() && placement.save_path.is_none())
    {
        let parent = completion_parent(
            &settings.complete_dir,
            &settings.categories,
            category.map(|category| category.name.as_str()),
        )
        .map_err(AddError::Invalid)?;
        absolute_placement(placement, &parent)
    } else {
        placement.clone()
    };
    if [&placement.save_path, &placement.download_path]
        .into_iter()
        .flatten()
        .any(|path| {
            !may_place(
                caller,
                Path::new(path.trim()),
                &settings.complete_dir,
                &settings.categories,
            )
        })
    {
        return Err(AddError::Forbidden(OUTSIDE_SAVE_FOLDERS.to_owned()));
    }
    Ok(placement)
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AddError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Refused(#[from] Refusal),
    #[error("already added as job {}", .0.0)]
    Duplicate(JobId),
    #[error("no network for this torrent: {0}")]
    NoNetwork(String),
    #[error("{}", status::TORRENTS_OFF)]
    Off,
    #[error("{0}")]
    State(String),
    // The caller may not save there (`may_place`).
    #[error("{0}")]
    Forbidden(String),
}

// Why a caller that only controls is refused a save location.
pub(super) const OUTSIDE_SAVE_FOLDERS: &str =
    "this key may save only under the default save folder or a category's folder";

// The hashes a duplicate of a torrent going by `hashes` would share.
fn infohashes(hashes: InfoHashes) -> Vec<Infohash> {
    hashes
        .v1
        .map(|hash| Infohash::V1(hash.0))
        .into_iter()
        .chain(hashes.v2.map(|hash| Infohash::V2(hash.0)))
        .collect()
}

pub(super) fn parse(bytes: &[u8]) -> Result<Parsed, AddError> {
    metainfo::parse(bytes).map_err(AddError::Invalid)
}

// A magnet's metadata fetch on its route's session, for the run that
// follows it: the session, the identity its trackers heard `started` under,
// and its tracker task.
pub(super) struct Fetched {
    pub(super) session: Arc<Session>,
    pub(super) identity: Identity,
    pub(super) trackers: weaver_torrent::tracker::TrackerHandover,
}

// A magnet's metadata fetch: running, or ended by a pause, which a resume
// starts again. Removing or pausing the torrent aborts a running fetch,
// which lets its trackers go with one `stopped`.
pub(super) enum Resolving {
    Fetching(tokio::task::AbortHandle),
    Paused,
}

impl TorrentService {
    // Add a torrent. A metainfo becomes a job at once; a magnet needs its
    // route's session to fetch the metainfo, and becomes one when it has.
    /// Whether the route a torrent in `scope` would take carries DHT, so a
    /// magnet with no trackers can find its peers there.
    pub async fn carries_dht(&self, scope: TorrentScope) -> bool {
        let plan = self.inner.sessions.routes().plan(scope);
        let (plan, _) = self.placed(&plan, None).await;
        self.inner.sessions.carries_dht(&plan).await
    }

    /// The same loaded IP filter used when a route opens its session.
    pub async fn ip_filter(
        &self,
        torrent: &TorrentConfig,
    ) -> Result<weaver_torrent::IpFilter, String> {
        self.inner.sessions.ip_filter(torrent).await
    }

    /// Add the torrent `request` names, as the server itself adds it: its
    /// save location is not checked against a caller's. An entry point
    /// that takes a save location from a caller adds through `add_as`.
    pub async fn add(&self, request: AddRequest) -> Result<JobId, AddError> {
        self.add_as(CallerScope::Local, request).await
    }

    /// Whether `caller` may add a torrent of `category` at `placement`:
    /// the answer [`Self::add_as`] gives before it looks at the torrent
    /// itself, from the same owner (`admitted`). Asked before anything is
    /// fetched for the add, so an add that would be refused for torrents
    /// being off, for a category that is not there, for a folder that does
    /// not resolve or for a place the caller may not save at reaches no
    /// host.
    pub fn may_add_at(
        &self,
        caller: CallerScope,
        category: Option<&str>,
        placement: &Placement,
    ) -> Result<(), AddError> {
        self.admitted(caller, category, placement).map(|_| ())
    }

    // What every add settles before its source is read: torrents take new
    // ones, its paths, made absolute, are ones `caller` may save at
    // (`placement_as`), and its category is there or is one to make. The
    // one owner of those answers, for the add and for `may_add_at`.
    //
    // A name no category matches (by name in any letter case, then by
    // alias, `resolve_category_config`) is one to make, as qBittorrent's
    // add makes it (`SessionImpl::addTorrent_impl`, sessionimpl.cpp:2853-
    // 2857); a name that cannot be a category adds the torrent with none,
    // as there. A category to make has no folder of its own: it saves
    // under the default save folder, and so grants a key held to the save
    // folders no place it did not have. Nothing is made here.
    fn admitted(
        &self,
        caller: CallerScope,
        category: Option<&str>,
        placement: &Placement,
    ) -> Result<(TorrentSettings, Admitted, Placement), AddError> {
        // Off, a wind-down's sessions open included: no torrent is new.
        let settings = self.settings();
        if !self.takes_new() {
            return Err(AddError::Off);
        }
        let category = match category.filter(|name| !name.trim().is_empty()) {
            Some(name) => match resolve_category_config(&settings.categories, name) {
                Some(category) => Admitted::There(category.clone()),
                None => match validate_category_path_component(name) {
                    Ok(name) if self.inner.categories.get().is_some() => {
                        Admitted::ToMake(CategoryConfig::named(name))
                    }
                    Ok(_) => {
                        // A service with nowhere to make one in refuses
                        // it, a place the caller may not save at first.
                        if let Err(refused @ AddError::Forbidden(_)) =
                            placement_as(caller, &settings, None, placement)
                        {
                            return Err(refused);
                        }
                        return Err(AddError::Invalid(format!("no category {name}")));
                    }
                    Err(_) => Admitted::None,
                },
            },
            None => Admitted::None,
        };
        let placement = placement_as(caller, &settings, category.config(), placement)?;
        Ok((settings, category, placement))
    }

    // The category `admitted` found for an add, made now when it is one to
    // make, and the settings that hold it. Two adds that name one new
    // category make one (`categories::make_category`).
    async fn made(
        &self,
        settings: TorrentSettings,
        category: Admitted,
    ) -> Result<(TorrentSettings, Option<CategoryConfig>), AddError> {
        let category = match category {
            Admitted::None => return Ok((settings, None)),
            Admitted::There(category) => return Ok((settings, Some(category))),
            Admitted::ToMake(category) => category,
        };
        let Some(config) = self.inner.categories.get() else {
            return Err(AddError::Invalid(format!("no category {}", category.name)));
        };
        let held = crate::categories::category_writes().await;
        let made = crate::categories::make_category(&held, config, &self.inner.db, category)
            .await
            .map_err(|error| AddError::State(state_error(error)))?;
        let category = made.category().clone();
        drop(held);
        tracing::info!(id = category.id, name = %category.name, "category made for a torrent added in it");
        // The torrents' settings hold it from now on, as they would after
        // the settings were handed on again.
        let settings = {
            let mut held = self
                .inner
                .settings
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !held.categories.iter().any(|there| there.id == category.id) {
                held.categories.push(category.clone());
            }
            TorrentSettings {
                categories: held.categories.clone(),
                ..settings
            }
        };
        Ok((settings, Some(category)))
    }

    /// Add the torrent `request` names for `caller`, who must be allowed
    /// its save and download paths once they are absolute (`may_place`).
    pub async fn add_as(
        &self,
        caller: CallerScope,
        request: AddRequest,
    ) -> Result<JobId, AddError> {
        let mut request = request;
        let (settings, category, placement) =
            self.admitted(caller, request.category.as_deref(), &request.placement)?;
        request.placement = placement;
        // A category to make is made once the add is past every refusal of
        // the caller's and of the torrent's, as qBittorrent's add makes it
        // past its own (`SessionImpl::addTorrent_impl`).
        match request.source.clone() {
            AddSource::Metainfo(bytes) => {
                let bytes = Bytes::from(bytes);
                let parsed = parse(&bytes)?;
                let left_out = self.admit(&parsed.manifest)?;
                self.duplicate(parsed.hashes)?;
                let (settings, category) = self.made(settings, category).await?;
                let job_id = self.reserve()?;
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    parsed.hashes,
                    parsed.manifest.name.clone(),
                    TorrentSource::Metainfo,
                    Some(bytes),
                );
                let (dir, files_dir, adopted) = self
                    .place(
                        &settings,
                        category.as_ref(),
                        &record,
                        &parsed.manifest,
                        job_id,
                    )
                    .await?;
                record.storage_dir = Some(dir.clone());
                record.files_dir = files_dir;
                record.seed_mode = request.placement.seed_mode;
                record.private = parsed.private;
                let held = held_at(
                    &record,
                    held_tops(parsed.manifest.paths.iter().map(String::as_str)),
                )
                .await;
                let mut torrent = match self.insert(record, &parsed.trackers, &adopted, Some(held))
                {
                    Ok(torrent) => torrent,
                    Err(error) => {
                        give_back(&dir, job_id).await;
                        return Err(error);
                    }
                };
                self.tag_added(job_id, &request.tags);
                let made = self.give_metainfo(&mut torrent, &parsed, &left_out);
                // Raised for an add, never for a restore (qBittorrent emits
                // `torrentAdded` from `addTorrent_impl` alone).
                self.raise_script_event(&torrent, QueueEvent::TorrentAdded, &[]);
                self.enqueue(torrent).await;
                // Answered without waiting on its route: it reads as opening
                // until its route's session is open, then goes on by itself.
                match made {
                    Ok(whole) => {
                        if whole {
                            self.inner.signal.look(job_id);
                        }
                        self.spawn_attach(job_id).await;
                    }
                    Err(error) => self.fail(job_id, error).await,
                }
                self.inner.counters.event(TorrentEvent::Added);
                self.publish_job(job_id).await;
                Ok(job_id)
            }
            AddSource::Magnet(uri) => {
                let magnet = Magnet::parse(&uri)
                    .map_err(|error| AddError::Invalid(format!("not a magnet: {error}")))?;
                let hashes = magnet.info_hashes();
                self.duplicate(hashes)?;
                let (settings, category) = self.made(settings, category).await?;
                let trackers: Vec<TrackerEntry> = magnet
                    .trackers
                    .iter()
                    .enumerate()
                    .filter_map(|(tier, url)| tracker(tier as u32, url.as_bytes()))
                    .collect();
                let rules = self.rules();
                let rule =
                    tracker_rule_for(&rules, trackers.iter().map(|tracker| tracker.url.as_str()));
                let scope = settings.scope(category.as_ref().map(|category| category.id), rule);
                let plan = self.inner.sessions.routes().plan(scope);
                let (plan, pin) = self.placed(&plan, None).await;
                // Answered without waiting on its route (libtorrent
                // `session_impl::add_torrent` starts a magnet at once): the
                // fetch opens the route's session by itself. A magnet with
                // no trackers and no peers is refused here when the route's
                // session runs and carries no DHT; one whose route is not
                // open yet is told so by its fetch, once the route is.
                if magnet.trackers.is_empty()
                    && magnet.peers.is_empty()
                    && let Some(session) = self.inner.sessions.running_session(&plan).await
                    && session.get_dht().is_none()
                {
                    return Err(AddError::NoNetwork(NEEDS_DHT.into()));
                }
                let job_id = self.reserve()?;
                let name = magnet
                    .name
                    .clone()
                    .unwrap_or_else(|| hashes.best().as_string());
                // A magnet has no files to seed until its metainfo is in
                // hand (libtorrent `torrent_flags::seed_mode` needs the
                // metainfo at add): it downloads.
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    hashes,
                    name,
                    TorrentSource::Magnet,
                    None,
                );
                record.pool_member = pin;
                // Nothing placed, nothing held, until its metainfo is.
                let mut torrent = self.insert(record, &trackers, &Adopted::default(), None)?;
                self.tag_added(job_id, &request.tags);
                let identity = identity_of(torrent.rule(&rules), &torrent.identity);
                let terms = PeerTerms::of(&self.settings(), torrent.rule(&rules));
                torrent.route = Some(plan.key.clone());
                self.raise_script_event(&torrent, QueueEvent::TorrentAdded, &[]);
                self.enqueue(torrent).await;
                self.spawn_resolve(job_id, plan, uri, None, identity, terms)
                    .await;
                self.inner.counters.event(TorrentEvent::Added);
                self.publish_job(job_id).await;
                Ok(job_id)
            }
        }
    }

    // A torrent just added takes the tags it was added with. It is in the
    // store already, so a tag that cannot be written leaves it untagged
    // rather than undoing the add.
    fn tag_added(&self, job_id: JobId, tags: &[String]) {
        if let Err(error) = self
            .inner
            .db
            .tag_new_job(job_id, tags.iter().map(String::as_str))
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not tag a torrent");
        }
    }

    // A torrent just added joins the live ones last in the queue. No
    // earlier torrent's turn or rank moves for it, so its own follow from
    // the queue's end as the last pass left it, which it then extends: an
    // add reads no other torrent. Whatever changed since that pass is
    // settled at the next. One that took its place before another added
    // since, and arrives after it, asks for that pass now: the turns given
    // since are out of order.
    async fn enqueue(&self, mut torrent: Live) {
        let mut live = self.inner.live.lock().await;
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let now = self.inner.clock.unix_ms();
        let queued = queued(&settings, &rules, &torrent, now);
        let place = (torrent.record.queue_position, torrent.record.job_id);
        {
            let mut end = self
                .inner
                .queue_end
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut turn = end.slots.take(&queued, &holds);
            // Added while the stored torrents are still being read, it
            // waits for its turn among all of them: the pass at the load's
            // end gives it, and its place past every one of them.
            // Asked under the lock the load ends under, so its place is
            // given by the load's end or is its own already. A load whose
            // page failed lets the torrents here run meanwhile.
            let partial = {
                let mut marks = locked(&self.inner.load_marks);
                let loading = self.loading();
                if loading {
                    marks.asked.push(Asked::Added(torrent.record.job_id));
                }
                loading && marks.fault.is_none()
            };
            if partial {
                turn.active &= queued.forced || queued.seeding;
            }
            if end.last.is_some_and(|last| place < last) {
                self.inner.signal.want_pass();
            } else {
                end.last = Some(place);
            }
            take_turn(&mut torrent, &queued, turn, &holds, now);
        }
        // There is a torrent from here, not from the next publish.
        self.inner.presence.send_if_modified(|presence| {
            let was = *presence;
            presence.note(&torrent);
            *presence != was
        });
        live.insert(torrent.record.job_id, torrent);
    }

    // The files of `manifest.paths`, by index, that admission leaves out.
    fn admit(&self, manifest: &Manifest) -> Result<Vec<usize>, AddError> {
        let policy = self
            .inner
            .db
            .post_processing_settings()
            .map_err(|error| AddError::State(state_error(error)))?;
        Ok(admission::admit(&policy, manifest)?)
    }

    fn duplicate(&self, hashes: InfoHashes) -> Result<(), AddError> {
        match self.inner.db.get_torrent_by_infohash(&infohashes(hashes)) {
            Ok(Some(existing)) => Err(self.already(existing.job_id)),
            Ok(None) => Ok(()),
            Err(error) => Err(AddError::State(state_error(error))),
        }
    }

    // Why an add of a torrent that is already `job_id` is refused: still
    // being removed when a removal has marked it, a duplicate otherwise.
    // The add never waits on that removal.
    fn already(&self, job_id: JobId) -> AddError {
        match self.inner.db.torrent_is_retiring(job_id) {
            Ok(true) => AddError::Refused(Refusal::Removing(job_id)),
            Ok(false) => AddError::Duplicate(job_id),
            Err(error) => AddError::State(state_error(error)),
        }
    }

    fn reserve(&self) -> Result<JobId, AddError> {
        self.inner
            .db
            .reserve_next_job_id()
            .map_err(|error| AddError::State(state_error(error)))
    }

    // The torrent's directory under its category's parent, taken on disk
    // now: its output name, suffixed with the job's id when that is taken.
    async fn claim_dir(
        &self,
        settings: &TorrentSettings,
        category: Option<&CategoryConfig>,
        manifest: &Manifest,
        job_id: JobId,
    ) -> Result<String, AddError> {
        let parent = completion_parent(
            &settings.complete_dir,
            &settings.categories,
            category.map(|category| category.name.as_str()),
        )
        .map_err(AddError::Invalid)?;
        let dir = claim_complete_destination_path(
            &parent,
            &admission::output_name(manifest),
            job_id,
            &HashSet::new(),
        )
        .await
        .map_err(|error| {
            AddError::State(format!("could not make the torrent's directory: {error}"))
        })?;
        let db = self.inner.db.clone();
        let marked = dir.clone();
        tokio::task::spawn_blocking(move || mark_storage(&db, &marked, job_id, false))
            .await
            .map_err(|error| AddError::State(error.to_string()))?;
        Ok(dir.to_string_lossy().into_owned())
    }

    // Where a torrent's job writes, and where its files go when that is not
    // there, as `record` asks. Without a save path or a layout that strips
    // the folder, its files go in a folder of its own claimed under its
    // category's directory, as weaver lays every torrent out. With a save
    // path they go under it as qBittorrent lays them out
    // (`SessionImpl::addTorrent_impl`), the metainfo's layout unless asked
    // otherwise, into whatever is there already. Files with no folder of
    // their own sit straight in the directory they go to, and the job
    // writes in a folder of its own claimed as weaver claims one. With it,
    // what was already in the directory the files go to before anything is
    // made there: nothing in a folder Weaver claims.
    async fn place(
        &self,
        settings: &TorrentSettings,
        category: Option<&CategoryConfig>,
        record: &TorrentRecord,
        manifest: &Manifest,
        job_id: JobId,
    ) -> Result<(String, Option<String>, Adopted), AddError> {
        let found = |dir: PathBuf| {
            let names = manifest.paths.clone();
            async move {
                tokio::task::spawn_blocking(move || files::adopted(&dir, &names))
                    .await
                    .map_err(|error| AddError::State(error.to_string()))
            }
        };
        let layout = record.content_layout;
        let category_parent = || {
            completion_parent(
                &settings.complete_dir,
                &settings.categories,
                category.map(|category| category.name.as_str()),
            )
            .map_err(AddError::Invalid)
        };
        // Absolute: `add` resolved a relative one.
        let parent = record.save_path.as_deref().map(PathBuf::from);
        let foldered = layout
            .unwrap_or(ContentLayout::Original)
            .foldered(manifest.single_file);
        // Made off the async runtime: on the blocking pool where the folder
        // is marked with it, through `tokio::fs` where it is made alone.
        let unmade = |error: std::io::Error| {
            AddError::State(format!("could not make the torrent's directory: {error}"))
        };
        match parent {
            Some(parent) if foldered => {
                let dir = parent.join(admission::output_name(manifest));
                let adopted = found(dir.clone()).await?;
                let db = self.inner.db.clone();
                let marked = dir.clone();
                tokio::task::spawn_blocking(move || {
                    let existed = std::fs::symlink_metadata(&marked).is_ok();
                    std::fs::create_dir_all(&marked).map_err(unmade)?;
                    mark_storage(&db, &marked, job_id, existed);
                    Ok::<_, AddError>(())
                })
                .await
                .map_err(|error| AddError::State(error.to_string()))??;
                Ok((dir.to_string_lossy().into_owned(), None, adopted))
            }
            Some(parent) => {
                let adopted = found(parent.clone()).await?;
                tokio::fs::create_dir_all(&parent).await.map_err(unmade)?;
                let dir = self.claim_dir(settings, category, manifest, job_id).await?;
                Ok((dir, Some(parent.to_string_lossy().into_owned()), adopted))
            }
            None if layout.is_none_or(|layout| layout.foldered(manifest.single_file)) => Ok((
                self.claim_dir(settings, category, manifest, job_id).await?,
                None,
                Adopted::default(),
            )),
            None => {
                let parent = category_parent()?;
                let adopted = found(parent.clone()).await?;
                tokio::fs::create_dir_all(&parent).await.map_err(unmade)?;
                let dir = self.claim_dir(settings, category, manifest, job_id).await?;
                Ok((dir, Some(parent.to_string_lossy().into_owned()), adopted))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        job_id: JobId,
        request: &AddRequest,
        category: Option<&CategoryConfig>,
        hashes: InfoHashes,
        name: String,
        source: TorrentSource,
        metainfo: Option<Bytes>,
    ) -> TorrentRecord {
        // qBittorrent `TorrentImpl`: a torrent with a download path in use
        // downloads there and moves under its save path once complete.
        let placement = &request.placement;
        let (save_path, move_to) = match &placement.download_path {
            Some(download) if !placement.seed_mode => {
                (Some(download.clone()), placement.save_path.clone())
            }
            _ => (placement.save_path.clone(), None),
        };
        TorrentRecord {
            job_id,
            infohash_v1: hashes.v1.map(|hash| hash.0),
            infohash_v2: hashes.v2.map(|hash| hash.0),
            name,
            source,
            metainfo,
            category_id: category.map(|category| category.id),
            added_at_ms: self.inner.clock.unix_ms(),
            kept: KeptIntent::Intent(if request.paused {
                TorrentIntent::Paused
            } else {
                TorrentIntent::Resumed
            }),
            forced: request.forced,
            completed_at_ms: None,
            storage_dir: None,
            policy_override: SeedingPolicy::kept(request.policy_override),
            unpack: request.unpack,
            unpack_in_force: None,
            unpack_over: false,
            unpack_end: None,
            download_ends: 0,
            metadata: request.metadata.clone(),
            upload_limit: request.upload_limit,
            download_limit: request.download_limit,
            sequential: false,
            first_last_piece_first: false,
            save_path,
            content_layout: placement.layout,
            files_dir: None,
            seed_mode: false,
            move_to,
            queue_position: self
                .inner
                .next_queue_position
                .fetch_add(1, Ordering::Relaxed),
            pool_member: None,
            private: false,
        }
    }

    // Keep `record` with its trackers: a torrent new to the database, under
    // the rule its trackers match, and forced when that rule admits its
    // torrents so.
    fn insert(
        &self,
        mut record: TorrentRecord,
        trackers: &[TrackerEntry],
        adopted: &Adopted,
        // What it holds where its files go; None for a magnet, which holds
        // its whole folder until its metainfo arrives.
        held: Option<Vec<String>>,
    ) -> Result<Live, AddError> {
        // The table keeps a URL once in a tier, the first that names it:
        // the same list is written and held, so the two never differ.
        let trackers = &once_a_tier(trackers);
        let rules = self.rules();
        let rule = tracker_rule_for(&rules, trackers.iter().map(|tracker| tracker.url.as_str()));
        record.forced |= rule.is_some_and(|rule| rule.forced);
        let rule_id = rule.map(|rule| rule.id);
        match self
            .inner
            .db
            .insert_placed_torrent(&record, trackers, adopted, held.as_deref())
        {
            Ok(TorrentInsert::Inserted) => {
                let now = self.inner.clock.unix_ms();
                let mut torrent = Live::new(
                    record,
                    TorrentCounters::default(),
                    ResumePeers::default(),
                    now,
                );
                torrent.rule_id = rule_id;
                torrent.trackers = in_table_order(trackers.to_vec());
                Ok(torrent)
            }
            Ok(TorrentInsert::Duplicate(existing)) => Err(self.already(existing)),
            Err(error) => Err(AddError::State(state_error(error))),
        }
    }

    // Fetch a magnet's metainfo on its route's session, once the route has
    // one, then admit it as any other. A route that gives no session holds
    // the magnet, which resolves again at a later tick. Until the fetch
    // ends, removing or pausing the torrent ends it (see `Resolving`). A
    // magnet already fetching goes on with that fetch; a paused one waits
    // for its resume to fetch.
    async fn spawn_resolve(
        &self,
        job_id: JobId,
        plan: RoutePlan,
        uri: String,
        tracker_tiers: Option<Vec<Vec<Tracker>>>,
        identity: Identity,
        terms: PeerTerms,
    ) {
        let service = self.clone();
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return;
        };
        match torrent.resolving {
            Some(Resolving::Fetching(_)) => return,
            _ if !torrent.resumed() => {
                torrent.resolving = Some(Resolving::Paused);
                return;
            }
            _ => {}
        }
        let task = tokio::spawn(async move {
            service
                .resolve_on(job_id, plan, uri, tracker_tiers, identity, terms)
                .await;
            // The fetch's announce table is the torrent's until it is
            // attached, which reads the same table, or the fetch's
            // trackers are let go.
            service.fetching(job_id, None).await;
        });
        torrent.resolving = Some(Resolving::Fetching(task.abort_handle()));
        drop(live);
        self.keep(task);
    }

    async fn resolve_on(
        &self,
        job_id: JobId,
        plan: RoutePlan,
        uri: String,
        tracker_tiers: Option<Vec<Vec<Tracker>>>,
        identity: Identity,
        terms: PeerTerms,
    ) {
        let session = match self.open_session(&plan, &identity).await {
            Ok(session) => session,
            Err(error) => {
                self.fetch_ended(job_id).await;
                self.inner.counters.fetch(FetchOutcome::NoSession);
                return self.held(job_id, Some(plan.key), held_by(error)).await;
            }
        };
        // A magnet with no trackers and no peers of its own finds its peers
        // on the DHT alone: on a route that carries none it can never fetch.
        let trackerless = tracker_tiers
            .as_ref()
            .is_none_or(|tiers| tiers.iter().all(Vec::is_empty))
            && Magnet::parse(&uri)
                .is_ok_and(|magnet| magnet.trackers.is_empty() && magnet.peers.is_empty());
        if trackerless && session.get_dht().is_none() {
            if self.fetch_ended(job_id).await {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, NEEDS_DHT.into()).await;
            }
            return;
        }
        // Its trackers name by its route's profile, on whichever session.
        let naming = self.inner.sessions.torrent_names(&plan, &session).await;
        let mut options = AddTorrentOptions {
            list_only: true,
            tracker_tiers,
            identity: Some(identity.clone()),
            naming: Some(naming.naming()),
            ..Default::default()
        };
        terms.apply(&mut options);
        let (fetch_tx, fetch_rx) = oneshot::channel();
        options.metadata_fetch = Some(fetch_tx);
        let fetch = session.add_torrent(AddTorrent::from_url(uri), Some(options));
        let fetching = async {
            if let Ok(fetch) = fetch_rx.await {
                self.fetching(job_id, Some(fetch)).await;
            }
        };
        let (listed, ()) = tokio::join!(fetch, fetching);
        // A fetch ended first was counted where it was ended.
        if !self.fetch_ended(job_id).await {
            return;
        }
        let listed = match listed {
            Ok(AddTorrentResponse::ListOnly(listed)) => listed,
            Ok(_) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, "the session did not return the metainfo".into())
                    .await;
                return;
            }
            Err(error) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, format!("{error:#}")).await;
                return;
            }
        };
        // The fetch's trackers go to the attach, which takes them over or
        // lets them go.
        let fetched = listed.trackers.map(|trackers| Fetched {
            session,
            identity,
            trackers,
        });
        // The link's web seeds are stored in the metainfo, and the levels
        // its `so=` gave its files go with it.
        let bytes = with_web_seeds(listed.torrent_bytes, &listed.web_seeds);
        match self
            .resolved(job_id, bytes, &listed.file_priorities, fetched)
            .await
        {
            Ok(()) => self.inner.counters.fetch(FetchOutcome::Resolved),
            Err(error) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, error.to_string()).await;
            }
        }
    }

    // A magnet's metadata fetch has ended: false when the torrent is gone,
    // or a pause ended the fetch first.
    async fn fetch_ended(&self, job_id: JobId) -> bool {
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return false;
        };
        match torrent.resolving.take() {
            Some(Resolving::Fetching(_)) => true,
            paused => {
                torrent.resolving = paused;
                false
            }
        }
    }

    // A magnet's metadata fetch becomes `fetch`. A fetch is the torrent's
    // only while it runs: one the pause ended still gives is not shown.
    // Published either way: a fetch begun shows its phase, and one ended
    // what it ended in, the metainfo it gave among it. Only this torrent
    // changed, so only its row is published, as a settle publishes the
    // torrents that spoke: every magnet a start resumes ends a fetch, and a
    // publish of every torrent for each would cost the square of them.
    pub(super) async fn fetching(
        &self,
        job_id: JobId,
        fetch: Option<weaver_torrent::MetadataFetch>,
    ) {
        #[cfg(any(test, feature = "test-support"))]
        let ended = fetch.is_none();
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id)
            && (fetch.is_none() || matches!(torrent.resolving, Some(Resolving::Fetching(_))))
        {
            torrent.fetching = fetch;
        }
        self.publish_job(job_id).await;
        #[cfg(any(test, feature = "test-support"))]
        if ended {
            self.inner.fetches_ended.send_modify(|ended| *ended += 1);
        }
    }

    // A magnet's metainfo is in hand: `selected` is each file's level as
    // the link's `so=` gave it, by the metainfo's file index (libtorrent
    // `parse_magnet_uri` keeps it as the add's file priorities).
    pub(super) async fn resolved(
        &self,
        job_id: JobId,
        bytes: Bytes,
        selected: &[FilePriority],
        fetched: Option<Fetched>,
    ) -> Result<(), AddError> {
        let parsed = parse(&bytes)?;
        let left_out = self.admit(&parsed.manifest)?;
        let settings = self.settings();
        let Some(record) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| torrent.record.clone())
        else {
            return Ok(());
        };
        let known = hashes_of(&record);
        // A hash only the metainfo names may be another torrent's: a hybrid
        // added once by each of its hashes.
        self.duplicate(InfoHashes {
            v1: parsed.hashes.v1.filter(|_| known.v1.is_none()),
            v2: parsed.hashes.v2.filter(|_| known.v2.is_none()),
        })?;
        let category = settings.category(record.category_id).cloned();
        let (dir, files_dir, adopted) = self
            .place(
                &settings,
                category.as_ref(),
                &record,
                &parsed.manifest,
                job_id,
            )
            .await?;
        let held = {
            let mut placed = record.clone();
            placed.storage_dir = Some(dir.clone());
            placed.files_dir = files_dir.clone();
            held_at(
                &placed,
                held_tops(parsed.manifest.paths.iter().map(String::as_str)),
            )
            .await
        };
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            drop(live);
            give_back(&dir, job_id).await;
            return Ok(());
        };
        torrent.record.infohash_v1 = parsed.hashes.v1.map(|hash| hash.0);
        torrent.record.infohash_v2 = parsed.hashes.v2.map(|hash| hash.0);
        torrent.record.name = parsed.manifest.name.clone();
        torrent.record.storage_dir = Some(dir.clone());
        torrent.record.files_dir = files_dir.clone();
        torrent.record.metainfo = Some(bytes.clone());
        torrent.record.private = parsed.private;
        // The trackers the magnet listed stay, edits included, each as
        // whoever named it; the metainfo's own join after them as the
        // torrent's.
        let mut trackers = torrent.trackers.to_vec();
        trackers.extend(parsed.trackers.iter().cloned());
        let trackers = tidy_trackers(trackers);
        self.inner
            .db
            .set_torrent_metainfo(
                job_id,
                torrent.record.infohash_v1,
                torrent.record.infohash_v2,
                &parsed.manifest.name,
                &bytes,
                parsed.private,
                &dir,
                files_dir.as_deref(),
                &trackers,
                &held,
                &adopted,
            )
            .map_err(|error| AddError::State(state_error(error)))?;
        // The table holds the merged list now, whatever the next write does.
        torrent.rule_id = self.rule_id(&trackers);
        torrent.trackers = in_table_order(trackers);
        // The files the link left out are Ignored before the store is made
        // over them, as the operator's own levels would be. Padding is no
        // one's choice: it is stored as Normal.
        let files = parsed.layout.files().map(|file| file.padding);
        let levels: Vec<FilePriority> = files
            .zip(
                selected
                    .iter()
                    .copied()
                    .chain(std::iter::repeat(FilePriority::Normal)),
            )
            .map(|(padding, level)| if padding { FilePriority::Normal } else { level })
            .collect();
        if levels.iter().any(|level| *level != FilePriority::Normal) {
            self.inner
                .db
                .set_torrent_file_priorities(job_id, &levels)
                .map_err(|error| AddError::State(state_error(error)))?;
        }
        let made = self.give_metainfo(torrent, &parsed, &left_out);
        drop(live);
        match made {
            Ok(true) => self.inner.signal.look(job_id),
            Ok(false) => {}
            Err(error) => {
                self.fail(job_id, error).await;
                return Ok(());
            }
        }
        // An SSL torrent's trackers hear it only as it goes live, under its
        // own identity: the fetch's trackers are let go here.
        let fetched = fetched.filter(|_| !parsed.ssl);
        self.attach(job_id, fetched).await;
        Ok(())
    }

    // Fetch a stored magnet's metainfo again, on its route's session once
    // the route is up.
    pub(super) async fn resolve(&self, job_id: JobId) {
        let rules = self.rules();
        // One whose stored secrets did not decrypt is held: nothing of it is
        // fetched.
        let Some((record, rule, own)) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .filter(|torrent| torrent.sealed.is_none())
            .map(|torrent| {
                (
                    torrent.record.clone(),
                    torrent.rule(&rules),
                    torrent.identity.clone(),
                )
            })
        else {
            return;
        };
        let scope = self.settings().scope(record.category_id, rule);
        let plan = self
            .pinned(job_id, self.inner.sessions.routes().plan(scope))
            .await;
        // The trackers it lists, in their tiers: a magnet's fetch announces
        // to tiers, not to a bare list.
        let mut tiers = Vec::new();
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.route = Some(plan.key.clone());
            torrent.held = None;
            tiers = tracker_tiers(&torrent.trackers);
        }
        let uri = Magnet::of(hashes_of(&record), None, Vec::new()).to_string();
        self.spawn_resolve(
            job_id,
            plan,
            uri,
            Some(tiers),
            identity_of(rule, &own),
            PeerTerms::of(&self.settings(), rule),
        )
        .await;
    }

    /// The bytes at `url` for a torrent added in `category`, fetched over
    /// the route of its matched tracker rule, else its category's, else the
    /// default (see [`TorrentSettings::scope`]), as that rule's client. A leg
    /// that is down fetches nothing: the add fails for the route being down.
    pub async fn fetch_url(
        &self,
        proxies: &crate::proxies::ProxyRuntime,
        category: Option<&str>,
        url: &reqwest::Url,
        cookie: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        let rules = self.rules();
        let rule = tracker_rule_for(&rules, [url.as_str()]);
        let scope = self.scope_of(category, rule);
        let plan = self.inner.sessions.routes().plan(scope);
        if let Some(reason) = plan.down {
            return Err(SessionError::RouteDown(reason).to_string());
        }
        let client = client_of(rule);
        crate::ingest::fetch_torrent_url(proxies, scope, url, cookie, client).await
    }
}

// The marker of the folder a torrent's files go in, `dir`, naming its job:
// the one job that may remove from it once the torrent is in history. A
// folder Weaver made for the torrent (`existed` false) takes it; one that
// was there takes it only from a torrent's job that is in history and no
// longer a torrent, as when a torrent removed with its files kept is added
// again over them. An unmarked folder that was there, an NZB job's, or one
// a live torrent's marker holds, stays as it is: nothing in the new
// torrent's history removes from it. A read that fails takes nothing.
fn mark_storage(db: &Database, dir: &Path, job_id: JobId, existed: bool) {
    use crate::jobs::working_dir::{OutputDirOwner, output_dir_owner};
    let take = !existed
        || match output_dir_owner(dir) {
            Some(OutputDirOwner::Job(owner)) if owner != job_id => {
                let in_history = db.history_job_is_torrent(owner).unwrap_or(false);
                let live = db
                    .torrent_job_ids_among(&[owner])
                    .map_or(true, |live| !live.is_empty());
                in_history && !live
            }
            _ => false,
        };
    if take && let Err(error) = crate::jobs::working_dir::mark_weaver_owned_output_dir(dir, job_id)
    {
        tracing::warn!(job_id = job_id.0, dir = %dir.display(), error = %error, "could not mark a torrent's folder; history leaves its files");
    }
}
