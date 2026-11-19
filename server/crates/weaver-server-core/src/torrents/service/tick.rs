// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::completion::Placing;
use super::intake::Resolving;
use super::{ActionError, PeerTerms, TICK, TorrentService, ignored_of, load_of, state_error};
use crate::categories::model::resolve_category_config;
use crate::jobs::JobId;
use crate::proxies::{ResolvedBy, TorrentScope};
use crate::settings::SeedingPolicy;
use crate::torrents::FilePriority;
use crate::torrents::metrics::FetchOutcome;
use crate::torrents::sessions::{
    OPENING, RouteFailure, RouteKey, RoutePlan, SessionError, SessionView,
};
use crate::torrents::status::{Engine, Held};
use crate::torrents::store::TorrentStore;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, PoisonError};
use tokio::sync::watch;
use weaver_torrent::{Identity, Session};

/// A torrent route as it stands: the scopes that run on it, with their
/// names, and its session, open or held, once one was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentRoute {
    pub key: RouteKey,
    pub scopes: Vec<(TorrentScope, Option<String>)>,
    pub session: Option<SessionView>,
    /// Why its leg is down, when it is.
    pub down: Option<RouteFailure>,
    /// Who resolves the names its sessions look up.
    pub names: Option<ResolvedBy>,
}

impl TorrentService {
    // Each route's session as it stands, for the exporter: read by the tick,
    // wherever sessions open or close outside it, and as a torrent is given
    // its session.
    pub(super) async fn read_sessions(&self) {
        self.inner
            .sessions
            .read_into(&self.inner.sessions_read)
            .await;
    }

    // `plan`'s route's open session, opened for `opener` when it is not, and
    // read before a torrent runs in it: no publish shows a torrent running
    // on a route whose session it reads as held.
    pub(super) async fn open_session(
        &self,
        plan: &RoutePlan,
        opener: &Identity,
    ) -> Result<Arc<Session>, SessionError> {
        let session = self.inner.sessions.session(plan, opener).await?;
        self.read_sessions().await;
        Ok(session)
    }

    pub(super) fn spawn_tick(&self) {
        let service = self.clone();
        let tick = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut reloads = service.inner.sessions.routes().reloads();
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    () = service.woken(&mut reloads) => {}
                }
                service.tick().await;
            }
        });
        if let Some(before) = self
            .inner
            .tick
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(tick)
        {
            before.abort();
        }
    }

    // Resolves when the tick runs before its interval: the settings changed,
    // a session's leg was revoked, so it opens again at once, or the routes
    // took a saved configuration, so a route held down or failing opens
    // again on it at once.
    pub(crate) async fn woken(&self, reloads: &mut Option<watch::Receiver<u64>>) {
        tokio::select! {
            () = self.inner.settings_changed.notified() => {}
            () = self.inner.sessions.revoked() => {}
            () = reloaded(reloads) => {}
        }
    }

    // A session's local service discovery joins its groups on the
    // interfaces up as it opens; each time those change, it joins again on
    // the ones up then.
    pub(super) fn spawn_follow_interfaces(&self) {
        let Some(changes) = self.inner.sessions.routes().interface_changes() else {
            return;
        };
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            service.inner.sessions.follow_interfaces(changes).await;
        }));
    }

    // Each egress's caps and quota state reach the open sessions as they
    // change: a quota crossing its soft or hard cap, a reset, a new period
    // or a saved egress. A session is never closed for it; its uploads
    // trickle or move nothing, and it keeps announcing.
    pub(super) fn spawn_follow_egress_limits(&self) {
        let Some(mut changes) = self.inner.sessions.routes().egress_limit_changes() else {
            return;
        };
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.sessions.apply_egress_limits();
                if !changes.changed().await {
                    break;
                }
            }
        }));
    }

    // A tracker's answer, and a fetch moving on, are published as the
    // sessions ring them: the exporter reads them when the views do, not
    // at the next tick.
    pub(super) fn spawn_follow_trackers(&self) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.sessions.trackers_changed().await;
                service.publish().await;
            }
        }));
    }

    pub(super) fn spawn_settle(&self) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.signal.wake.notified().await;
                service.settle().await;
            }
        }));
    }

    // What the torrents that spoke did: a write the store could not make
    // fails its torrent, a refusal found while unpacking stops it, and one
    // with every file it wants whole, unpacked when it has a tee, completes.
    // A tee whose moves wait on a checkpoint has it written, and a torrent
    // whose pieces fell [`CHECKPOINT_PIECES`] behind has them written.
    pub(super) async fn settle(&self) {
        #[cfg(test)]
        self.inner
            .settling
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let dirty = self.inner.signal.take();
        for &job_id in &dirty {
            let made_whole = self.inner.signal.take_whole(job_id);
            let found = {
                let live = self.inner.live.lock().await;
                live.get(&job_id).and_then(|torrent| {
                    let store = Arc::clone(torrent.store.as_ref()?);
                    let levels = torrent.levels.clone();
                    Some((store, levels, torrent.complete()))
                })
            };
            let Some((store, levels, complete)) = found else {
                self.inner.signal.forget_writes(job_id);
                continue;
            };
            if let Some(error) = self.inner.signal.take_refused(job_id) {
                self.inner.signal.forget_writes(job_id);
                self.fail(job_id, error).await;
                continue;
            }
            if self.inner.signal.take_landed(job_id) {
                self.end_room_promise(job_id);
            }
            if let Some(no_room) = self.inner.signal.take_no_room(job_id) {
                self.hold_for_room(job_id, no_room).await;
            }
            if complete {
                continue;
            }
            self.files_downloaded(job_id, made_whole).await;
            let wanted = |file: usize| levels.get(file).copied().unwrap_or_default().wanted();
            let whole = store.has_files(wanted);
            let Some(tee) = store.tee() else {
                if whole {
                    self.completed(job_id).await;
                }
                continue;
            };
            let stepped = tee.lend_handed().and_then(|()| {
                // Shares of a file the torrent does not want never come.
                if whole {
                    tee.lose_unwanted(wanted)
                } else {
                    Ok(())
                }
            });
            if let Err(error) = stepped {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking step failed");
            }
            if let Some(error) = tee.refusal() {
                // A checkpoint the tee let go for it ends, on the torrent's
                // lane, before this: its failure goes with it.
                drop(
                    self.queue_save(job_id, "torrent_checkpoint_let_go", move |db| {
                        db.forget_checkpoint_failure(job_id);
                        Ok(())
                    }),
                );
                // With every file it wants, the torrent is done downloading
                // whatever its unpacking came to.
                if whole {
                    self.short(job_id, &store, error, &[]).await;
                } else {
                    self.refused(job_id, error).await;
                }
                continue;
            }
            if tee.with(|job| job.awaits_commit() || job.moves_waiting()) {
                self.checkpoint_tee(job_id, Arc::clone(tee), false).await;
            }
            if whole && tee.done() {
                match self.unpacked(job_id, &store, tee).await {
                    Placing::Waits => {}
                    Placing::Placed => self.completed(job_id).await,
                    Placing::Short { why, placed } => {
                        self.short(job_id, &store, why, &placed).await;
                    }
                }
            }
        }
        let due = self.inner.signal.take_due();
        self.save_checkpoints(due.jobs().clone()).await;
        due.queued();
        // A pass can move any torrent, so every one is picked and shown
        // again; otherwise only the torrents that spoke changed, and only
        // they are (libtorrent posts a state update for the torrents that
        // changed, not every one). The tick picks and publishes every one.
        if self.inner.signal.take_pass() {
            self.apply_queue_and_picking().await;
            self.publish().await;
        } else {
            self.pick(Some(&dirty)).await;
            for &job_id in &dirty {
                self.publish_job(job_id).await;
            }
        }
        #[cfg(test)]
        {
            self.inner
                .settling
                .store(false, std::sync::atomic::Ordering::SeqCst);
            self.inner.settled.send_modify(|settled| *settled += 1);
        }
    }

    /// The torrent's own upload and download limits in bytes per second
    /// (libtorrent `torrent::set_upload_limit`, `set_download_limit`):
    /// `None` keeps that limit as it is, `Some(None)` is unlimited and zero
    /// holds that direction closed. A limit is kept on the torrent's row
    /// before the torrent takes it, then reaches its peers at once; its
    /// seeding policy's upload cap holds it too where that is tighter.
    pub async fn set_rate_limits(
        &self,
        job_id: JobId,
        upload: Option<Option<u64>>,
        download: Option<Option<u64>>,
    ) -> Result<(), ActionError> {
        if let Some(limit) = [upload, download]
            .into_iter()
            .flatten()
            .flatten()
            .find(|limit| i64::try_from(*limit).is_err())
        {
            return Err(ActionError::Invalid(format!(
                "a limit of {limit} bytes a second is past the largest, {}",
                i64::MAX
            )));
        }
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_rate_limits(job_id, upload, download)
                .map_err(|error| ActionError::State(state_error(error)))?;
            let record = &mut torrent.record;
            if let Some(upload) = upload {
                record.upload_limit = upload;
            }
            if let Some(download) = download {
                record.download_limit = download;
            }
            Ok(())
        })
        .await?;
        self.apply_limits().await;
        self.publish().await;
        Ok(())
    }

    pub async fn set_policy_override(
        &self,
        job_id: JobId,
        policy: Option<SeedingPolicy>,
    ) -> Result<(), ActionError> {
        if let Some(policy) = &policy {
            policy.validate().map_err(ActionError::Invalid)?;
        }
        let policy = SeedingPolicy::kept(policy);
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_policy_override(job_id, policy)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.policy_override = policy;
            Ok(())
        })
        .await?;
        // New goals move what the holds and the queue make of the torrent.
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Move a torrent to another category; when that category routes
    /// elsewhere the torrent moves to that route's session.
    pub async fn set_category(
        &self,
        job_id: JobId,
        category: Option<String>,
    ) -> Result<(), ActionError> {
        let settings = self.settings();
        let category_id = match category.as_deref().filter(|name| !name.trim().is_empty()) {
            Some(name) => Some(
                resolve_category_config(&settings.categories, name)
                    .map(|category| category.id)
                    .ok_or_else(|| ActionError::Invalid(format!("no category {name}")))?,
            ),
            None => None,
        };
        let rules = self.rules();
        self.ensure(&[job_id]).await;
        let moved = {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let rule = torrent.rule(&rules);
            let before = settings.scope(torrent.record.category_id, rule);
            self.inner
                .db
                .set_torrent_category(job_id, category_id)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.category_id = category_id;
            let moved = before != settings.scope(category_id, rule);
            if moved {
                self.move_out(torrent).await;
                torrent.route = None;
                torrent.held = None;
            }
            moved
        };
        // Answered without waiting on the new route, as an add is.
        if moved {
            self.spawn_attach(job_id).await;
        }
        self.publish().await;
        Ok(())
    }

    /// Read every torrent, keep their ledgers, follow their routes, and
    /// apply what that changes.
    pub async fn tick(&self) {
        // A load of the stored torrents whose page failed reads it again.
        self.resume_load();
        // The jobs still to restore are restored off the tick, which goes on
        // at once: a torrent reads as kept until its job is back.
        self.start_restores();
        self.follow_routes().await;
        // Taken after the routes are followed, whose readings take their
        // own: no reading in the tick runs from before one already kept.
        let now = self.inner.clock.unix_ms();
        // Each torrent read, its checkpoint written when it is behind, its
        // goals applied, then its queue turn, picking and limits.
        self.policy_pass(now).await;
        self.drop_idle_sessions().await;
        self.read_sessions().await;
        self.publish().await;
        // A wind-down with nothing left running ends, off the tick, which
        // turning torrents off ends.
        if self.wind_down().is_some()
            && !self.settings().torrent.enabled
            && !self.presence().running
        {
            let service = self.clone();
            self.keep(tokio::spawn(async move { service.end_wind_down().await }));
        }
        self.inner.ticks.send_modify(|ticks| *ticks += 1);
    }

    // A torrent whose route changed moves to the new route's session, and
    // one whose peer terms changed goes through its session again; one
    // whose route went down is held, and comes back when it is up. A
    // session the settings would now open otherwise, or whose leg was
    // revoked under it, opens again, its torrents with it; one left on a
    // session its route no longer runs moves to the one it does. A magnet
    // fetching on a session its route no longer runs, or on a route gone
    // down, ends that fetch and fetches again on its route as it stands.
    pub(super) async fn follow_routes(&self) {
        let now = self.inner.clock.unix_ms();
        let settings = self.settings();
        let rules = self.rules();
        let outdated = self.inner.sessions.outdated().await;
        let running = self.inner.sessions.running().await;
        let mut reattach = Vec::new();
        let mut down = BTreeMap::new();
        let mut unresolved = Vec::new();
        // Torrents left on their session whose scope changed: they name as
        // their new scope from here.
        let mut renamed = Vec::new();
        // One plan per scope a pass: the torrents of a scope share it.
        let mut plans: HashMap<TorrentScope, RoutePlan> = HashMap::new();
        {
            let mut live = self.inner.live.lock().await;
            // A torrent placed on a pool member this pass counts there for
            // the next one placed.
            let mut load = load_of(live.values());
            for torrent in live.values_mut() {
                let rule = torrent.rule(&rules);
                let scope = settings.scope(torrent.record.category_id, rule);
                let (plan, pin) = plans
                    .entry(scope)
                    .or_insert_with(|| self.inner.sessions.routes().plan(scope))
                    .place(torrent.record.pool_member, |key| {
                        load.get(key).copied().unwrap_or(0)
                    });
                self.pin(torrent, pin);
                if torrent.route.as_ref() != Some(&plan.key) {
                    if let Some(count) = torrent.route.as_ref().and_then(|key| load.get_mut(key)) {
                        *count = count.saturating_sub(1);
                    }
                    *load.entry(plan.key.clone()).or_default() += 1;
                }
                if torrent.layout.is_none() {
                    let moved = matches!(torrent.resolving, Some(Resolving::Fetching(_)))
                        && (plan.down.is_some()
                            || torrent.route.as_ref() != Some(&plan.key)
                            || outdated.contains(&plan.key));
                    if moved && let Some(Resolving::Fetching(fetch)) = torrent.resolving.take() {
                        fetch.abort();
                        self.inner.counters.fetch(FetchOutcome::Cancelled);
                        torrent.fetching = None;
                    }
                    if moved || torrent.held.is_some() {
                        unresolved.push(torrent.record.job_id);
                    }
                    continue;
                }
                // One whose job is still to come back is the restore pass's:
                // it joins its route as its job is back, held there if the
                // route is down.
                if torrent.awaits_restore() {
                    continue;
                }
                let terms = PeerTerms::of(&settings, rule);
                if let Some(reason) = &plan.down {
                    down.insert(plan.key.clone(), reason.clone());
                    torrent.settle(now);
                    torrent.attached = None;
                    torrent.engine = Engine::Absent;
                    // Its run ended with its leg: so does a wait for room.
                    self.end_disk_room(torrent);
                    torrent.route = Some(plan.key);
                    torrent.held = Some(Held::RouteDown(reason.clone()));
                } else if torrent.attaching.is_some() {
                    // Its route's session is being opened for it already:
                    // the pass after that wait ends looks at it again.
                } else if torrent.route.as_ref() != Some(&plan.key)
                    || outdated.contains(&plan.key)
                    || torrent.attached.as_ref().is_none_or(|attached| {
                        attached.terms != terms
                            || !running
                                .get(&plan.key)
                                .is_some_and(|open| Arc::ptr_eq(open, &attached.session))
                    })
                {
                    self.move_out(torrent).await;
                    // A torrent placed on another route reads as that route
                    // from here, opening, while the torrents before it take
                    // their turn. One whose route's session could not be
                    // built reads as that cause while the route opens
                    // again; any other hold is over.
                    if torrent.route.as_ref() != Some(&plan.key) {
                        torrent.route = Some(plan.key.clone());
                        torrent.held = Some(Held::Session(OPENING));
                    } else if !matches!(torrent.held, Some(Held::Session(_))) {
                        torrent.held = None;
                    }
                    reattach.push(torrent.record.job_id);
                } else if let Some(attached) = torrent
                    .attached
                    .as_ref()
                    .filter(|attached| attached.scope != scope)
                {
                    renamed.push((torrent.record.job_id, plan, Arc::clone(&attached.session)));
                }
            }
        }
        for (job_id, plan, session) in renamed {
            let naming = self.inner.sessions.scope_naming(&plan, &session).await;
            let mut live = self.inner.live.lock().await;
            if let Some(attached) = live
                .get_mut(&job_id)
                .and_then(|torrent| torrent.attached.as_mut())
                .filter(|attached| Arc::ptr_eq(&attached.session, &session))
            {
                attached.names.follow(naming);
                attached.scope = plan.scope;
            }
        }
        for key in &outdated {
            self.inner.sessions.drop_route(key).await;
        }
        for (key, reason) in down {
            self.inner.sessions.hold(&key, reason).await;
        }
        // A route whose session did not open in this pass is not opened
        // again in it for each torrent on it: they are held for the same,
        // and the next pass opens it once more.
        let mut failed: HashMap<RouteKey, Held> = HashMap::new();
        for job_id in reattach {
            if !failed.is_empty() {
                let mut live = self.inner.live.lock().await;
                if let Some(torrent) = live.get_mut(&job_id)
                    && let Some(held) = torrent.route.as_ref().and_then(|key| failed.get(key))
                {
                    torrent.settle(self.inner.clock.unix_ms());
                    torrent.held = Some(held.clone());
                    continue;
                }
            }
            if let Some((key, held)) = self.attach(job_id, None).await {
                failed.insert(key, held);
            }
        }
        for job_id in unresolved {
            self.resolve(job_id).await;
        }
    }

    /// Set the operator's level for each given file, by its index in the
    /// torrent, and push the levels to the torrent's session. A file
    /// downloaded again whose bytes the side store keeps has them copied
    /// out into it first, as a job of the torrent's own, and the call does
    /// not wait for it: the levels are stored and reach the session once it
    /// is done, and a change asked meanwhile follows it (libtorrent
    /// `torrent::prioritize_files`, torrent.cpp:5749-5829 at v2.0.15).
    pub async fn set_file_priorities(
        &self,
        job_id: JobId,
        priorities: &[(u32, FilePriority)],
    ) -> Result<(), ActionError> {
        let copy = self
            .with_torrent(job_id, |torrent| {
                // A change asked while a copy is out builds on the one
                // already waiting for it, or on the one under way.
                let mut levels = torrent
                    .leveling
                    .as_ref()
                    .map(|leveling| leveling.next.as_ref().unwrap_or(&leveling.asked).clone())
                    .unwrap_or_else(|| torrent.levels.clone());
                let count = levels.len();
                for (index, priority) in priorities {
                    let Some(level) = levels.get_mut(*index as usize) else {
                        return Err(ActionError::Invalid(format!(
                            "file {index} is out of range: the torrent has {count} files"
                        )));
                    };
                    *level = *priority;
                }
                let padding: Vec<bool> = torrent
                    .layout
                    .iter()
                    .flat_map(|layout| layout.files().map(|file| file.padding))
                    .collect();
                // Padding holds Ignored whatever is asked, and is stored as
                // nothing asked: the metainfo says which files are padding.
                let asked: Vec<FilePriority> = levels
                    .iter()
                    .zip(&padding)
                    .map(|(level, padding)| {
                        if *padding {
                            FilePriority::Normal
                        } else {
                            *level
                        }
                    })
                    .collect();
                if let Some(leveling) = &mut torrent.leveling {
                    leveling.next = Some(asked);
                    return Ok(None);
                }
                let held: Vec<FilePriority> = asked
                    .iter()
                    .zip(&padding)
                    .map(|(level, padding)| level.held_by(*padding))
                    .collect();
                let ignored = ignored_of(&held);
                if let Some(store) = torrent.store.clone()
                    && store.needs_copy(&ignored)
                {
                    // Stored before the copy, so one that cannot be stored
                    // changes nothing and is the caller's error. A store
                    // opened with levels its copy did not reach copies the
                    // bytes out first (`TorrentStore::with_side`).
                    let reopened = self
                        .save_levels(torrent, &asked, &held)
                        .map_err(|error| ActionError::State(state_error(error)))?;
                    torrent.leveling = Some(Leveling {
                        task: None,
                        asked: asked.clone(),
                        next: None,
                        reopened,
                    });
                    return Ok(Some((store, asked)));
                }
                // No file's bytes to copy: only which files are set aside
                // changes, in memory. The levels are stored first, so no
                // file is written in place before a level that wants it is
                // stored; one the store cannot take is stored back as memory
                // still holds it, so a change that fails changes nothing.
                // Should that write fail too, the levels stored want what
                // memory still sets aside: nothing is written for it in this
                // run, and the next run downloads what was asked.
                let reopened = self
                    .save_levels(torrent, &asked, &held)
                    .map_err(|error| ActionError::State(state_error(error)))?;
                if let Some(store) = &torrent.store
                    && let Err(error) = store.relevel(&ignored)
                {
                    // A place stored back out of reach of the database stays
                    // stored there, so the counter does not hand it out again.
                    if self.store_back(torrent, reopened.is_some())
                        && let Some(place) = reopened
                    {
                        self.give_back_place(place);
                    }
                    return Err(ActionError::State(error.to_string()));
                }
                self.take_levels(torrent, held, reopened, self.inner.clock.unix_ms());
                Ok(None)
            })
            .await?;
        if let Some((store, asked)) = copy {
            self.start_copy(job_id, store, asked).await;
            self.publish().await;
            return Ok(());
        }
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    // The copy out of the side store a change of levels needs, started off
    // the request, its handle kept on the change it is for.
    pub(super) async fn start_copy(
        &self,
        job_id: JobId,
        store: Arc<TorrentStore>,
        asked: Vec<FilePriority>,
    ) {
        let service = self.clone();
        let task = tokio::spawn(async move { service.copy_out(job_id, store, asked).await });
        // A job already over has left nothing to keep its handle on.
        let _ = self
            .with_torrent(job_id, |torrent| {
                if let Some(leveling) = &mut torrent.leveling {
                    leveling.task = Some(task);
                }
                Ok(())
            })
            .await;
    }

    // The job of a change of levels that copies bytes out of the side store:
    // the levels are stored first, then the copy runs off the runtime, with
    // every read and write of the torrent's pieces waiting on it, and only
    // then are the levels taken and pushed to the session; then the change
    // asked meanwhile, if any, the same way. A copy that fails leaves the
    // levels as they were, stored back, and the bytes where they were,
    // drops the changes after it, and puts the torrent in error, stopped
    // (libtorrent `torrent::on_file_priority`, torrent.cpp:5695-5720 at
    // v2.0.15).
    async fn copy_out(
        &self,
        job_id: JobId,
        store: Arc<TorrentStore>,
        mut asked: Vec<FilePriority>,
    ) {
        loop {
            let held: Vec<FilePriority> = asked
                .iter()
                .zip(store.layout().files())
                .map(|(level, file)| level.held_by(file.padding))
                .collect();
            let ignored = ignored_of(&held);
            let copying = Arc::clone(&store);
            let copied = tokio::task::spawn_blocking(move || copying.relevel(&ignored))
                .await
                .unwrap_or_else(|error| Err(std::io::Error::other(error)));
            let now = self.inner.clock.unix_ms();
            let next = self
                .with_torrent(job_id, |torrent| {
                    if !torrent
                        .store
                        .as_ref()
                        .is_some_and(|own| Arc::ptr_eq(own, &store))
                    {
                        return Ok((None, None));
                    }
                    let mut landing = None;
                    let mut next = None;
                    let reopened = torrent
                        .leveling
                        .as_ref()
                        .and_then(|leveling| leveling.reopened);
                    match copied {
                        Ok(()) => {
                            // One that completed while its copy was out is
                            // short of complete again now. The bytes are
                            // out of the side store whatever is stored, so
                            // the levels are taken all the same, and one
                            // that cannot be stored so is still reopened in
                            // memory: its completion is stored again at its
                            // next, and a restart before that reopens it
                            // from its stored levels (`Live::reopens`).
                            let reopened = match reopened {
                                Some(place) => Some(place),
                                None if torrent.reopens(&held) => {
                                    match self.save_levels(torrent, &asked, &held) {
                                        Ok(reopened) => reopened,
                                        Err(error) => {
                                            tracing::warn!(job_id = job_id.0, error = %error, "could not store a torrent short of complete again");
                                            Some(torrent.record.queue_position)
                                        }
                                    }
                                }
                                None => None,
                            };
                            self.take_levels(torrent, held, reopened, now);
                            next = torrent
                                .leveling
                                .as_mut()
                                .and_then(|leveling| leveling.next.take());
                            // The change asked meanwhile is stored before its
                            // copy, as the first was; one that cannot be is
                            // dropped, and the levels stay as they are now.
                            if let Some(asked) = &next {
                                let held: Vec<FilePriority> = asked
                                    .iter()
                                    .zip(store.layout().files())
                                    .map(|(level, file)| level.held_by(file.padding))
                                    .collect();
                                match self.save_levels(torrent, asked, &held) {
                                    Ok(reopened) => {
                                        if let Some(leveling) = &mut torrent.leveling {
                                            leveling.asked = asked.clone();
                                            leveling.reopened = reopened;
                                        }
                                    }
                                    Err(error) => {
                                        tracing::warn!(job_id = job_id.0, error = %error, "could not store a torrent's file levels");
                                        next = None;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(job_id = job_id.0, error = %error, "a file's bytes did not copy out of a torrent's side store");
                            // The levels were stored before the copy; those
                            // memory still holds are stored back.
                            self.store_back(torrent, reopened.is_some());
                            landing = self.stop_torrent(torrent, now);
                            torrent.copy_error = Some(format!(
                                "could not copy a file's bytes out of the side store: {error}"
                            ));
                        }
                    }
                    if next.is_none() {
                        torrent.leveling = None;
                    }
                    Ok((next, landing))
                })
                .await
                .unwrap_or((None, None));
            let (next, landing) = next;
            if landing.is_some() {
                let _ = self.landed(landing).await;
            }
            self.apply_picking().await;
            self.publish().await;
            match next {
                Some(next) => asked = next,
                None => return,
            }
        }
    }

    // Store `asked` as the torrent's levels, which it is to take as `held`,
    // and change nothing in memory: `take_levels` does that once the change
    // has landed. A complete torrent that is to want a file it lacks is
    // stored short of complete again in the same write, at the end of the
    // download queue, the place it is to take returned (qBittorrent
    // `TorrentImpl::prioritizeFiles`, torrentimpl.cpp:3045-3057; libtorrent
    // `torrent::resume_download` clears `m_completed_time` and takes
    // `last_pos`, torrent.cpp:8653-8676 at v2.0.15). A write that fails
    // stores nothing, and gives its place back unless another torrent took
    // the next one since.
    fn save_levels(
        &self,
        torrent: &super::live::Live,
        asked: &[FilePriority],
        held: &[FilePriority],
    ) -> Result<Option<i64>, crate::StateError> {
        let job_id = torrent.record.job_id;
        if !torrent.reopens(held) {
            self.inner.db.set_torrent_file_priorities(job_id, asked)?;
            return Ok(None);
        }
        let place = self
            .inner
            .next_queue_position
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Err(error) = self
            .inner
            .db
            .set_torrent_levels_and_completion(job_id, asked, None, place)
        {
            self.give_back_place(place);
            return Err(error);
        }
        // Reopened while the stored torrents are still being read, it goes
        // past every one of them at the load's end, as a torrent added then
        // does: the places past those read so far are not all known yet.
        let mut marks = super::locked(&self.inner.load_marks);
        if self.loading() {
            marks.asked.push(super::Asked::Added(job_id));
        }
        Ok(Some(place))
    }

    // Give back a place taken at the end of the download queue for a change
    // that did not land, unless another torrent took the next one since.
    fn give_back_place(&self, place: i64) {
        use std::sync::atomic::Ordering::Relaxed;
        let _ = self
            .inner
            .next_queue_position
            .compare_exchange(place + 1, place, Relaxed, Relaxed);
    }

    // Take `held`, stored by `save_levels`, in memory. One it stored short
    // of complete again is settled first, so the time up to `now` counts as
    // what the torrent was doing then, seeding, and none after; it then
    // downloads, shows so and takes a download slot and rank again.
    fn take_levels(
        &self,
        torrent: &mut super::live::Live,
        held: Vec<FilePriority>,
        reopened: Option<i64>,
        now: i64,
    ) {
        if let Some(place) = reopened {
            torrent.settle(now);
            torrent.record.completed_at_ms = None;
            torrent.record.queue_position = place;
            self.inner.signal.want_pass();
        }
        torrent.levels = held;
        // No piece comes for a change that leaves nothing it wants
        // missing: the settle is told, as an add's is (libtorrent
        // `update_peer_interest` calls `finished()`).
        if torrent.completes() {
            self.inner.signal.look(torrent.record.job_id);
        }
    }

    // Store again what memory holds of a torrent whose change of levels,
    // stored before its copy, did not land: its levels, and, when that
    // change stored it short of complete, when it completed and its place.
    // Whether it was stored.
    fn store_back(&self, torrent: &super::live::Live, reopened: bool) -> bool {
        let job_id = torrent.record.job_id;
        let asked: Vec<FilePriority> = torrent
            .levels
            .iter()
            .zip(torrent.layout.iter().flat_map(|layout| layout.files()))
            .map(|(level, file)| {
                if file.padding {
                    FilePriority::Normal
                } else {
                    *level
                }
            })
            .collect();
        let stored = if reopened {
            self.inner.db.set_torrent_levels_and_completion(
                job_id,
                &asked,
                torrent.record.completed_at_ms,
                torrent.record.queue_position,
            )
        } else {
            self.inner.db.set_torrent_file_priorities(job_id, &asked)
        };
        if let Err(error) = stored {
            // The store opens with the levels stored, and copies a file's
            // bytes out of the side store first when they want it
            // (`TorrentStore::with_side`).
            tracing::warn!(job_id = job_id.0, error = %error, "could not store a torrent's file levels back");
            return false;
        }
        true
    }

    /// Whether the torrent's pieces are fetched in order.
    pub async fn set_sequential(&self, job_id: JobId, on: bool) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_sequential(job_id, on)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.sequential = on;
            Ok(())
        })
        .await?;
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    /// Whether each file's first and last pieces are fetched first.
    pub async fn set_first_last_piece_first(
        &self,
        job_id: JobId,
        on: bool,
    ) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_first_last_piece_first(job_id, on)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.first_last_piece_first = on;
            Ok(())
        })
        .await?;
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    // The scopes that run on a route of their own, with their names: the
    // default, then each category's and tracker rule's that asks for one.
    fn route_scopes(&self) -> Vec<(TorrentScope, Option<String>)> {
        let categories = self
            .settings()
            .categories
            .into_iter()
            .filter(|category| category.torrent_route)
            .map(|category| (TorrentScope::Category(category.id), Some(category.name)));
        let rules = self.rules();
        let rules = rules.iter().filter(|rule| rule.torrent_route).map(|rule| {
            (
                TorrentScope::TrackerRule(rule.id),
                Some(rule.host_pattern.clone()),
            )
        });
        std::iter::once((TorrentScope::Default, None))
            .chain(categories)
            .chain(rules)
            .collect()
    }

    /// Every torrent route, by the scopes that plan to it, in their order,
    /// then any session left on a route no scope plans to. A scope whose
    /// route cannot be planned is listed on its own, with why.
    pub async fn routes(&self) -> Vec<TorrentRoute> {
        let sessions = &self.inner.sessions;
        let mut listed: Vec<TorrentRoute> = Vec::new();
        for (scope, name) in self.route_scopes() {
            // A pool route is listed as each member's session.
            let plan = sessions.routes().plan(scope);
            let planned: Vec<_> = if plan.members.is_empty() {
                vec![(plan.key, plan.down, plan.names)]
            } else {
                plan.members
                    .into_iter()
                    .map(|member| (member.key, member.down, member.names))
                    .collect()
            };
            for (key, down, names) in planned {
                match listed.iter_mut().find(|route| route.key == key) {
                    Some(route) => route.scopes.push((scope, name.clone())),
                    None => listed.push(TorrentRoute {
                        key,
                        scopes: vec![(scope, name.clone())],
                        session: None,
                        down: sessions.off().or(down),
                        names,
                    }),
                }
            }
        }
        for view in self.inner.sessions.views().await {
            match listed.iter_mut().find(|route| route.key == view.key) {
                Some(route) => route.session = Some(view),
                None => listed.push(TorrentRoute {
                    key: view.key.clone(),
                    scopes: Vec::new(),
                    session: Some(view),
                    down: None,
                    names: None,
                }),
            }
        }
        listed
    }

    // A route's session no torrent reads as its route is closed. The
    // torrents' routes are read, and the sessions swept, under the
    // torrents' lock: a torrent reads as its route under that lock before it
    // asks the route's session to open (`planned`), so a session opening
    // or opened for a torrent is never taken for idle. The sessions' lock
    // is only ever taken inside the torrents', never around it.
    pub(super) async fn drop_idle_sessions(&self) {
        {
            let live = self.inner.live.lock().await;
            let used: BTreeSet<&RouteKey> = live
                .values()
                .filter_map(|torrent| torrent.route.as_ref())
                .collect();
            #[cfg(test)]
            self.sweep_seam().await;
            for view in self.inner.sessions.views().await {
                if !used.contains(&view.key) {
                    self.inner.sessions.drop_route(&view.key).await;
                }
            }
        }
        // The routes that exist are the ones a scope plans to.
        let routes = self.inner.sessions.routes();
        let existing: BTreeSet<RouteKey> = self
            .route_scopes()
            .into_iter()
            .map(|(scope, _)| routes.plan(scope))
            .flat_map(|plan| plan.keys().cloned().collect::<Vec<_>>())
            .collect();
        self.inner.sessions.forget_routes(existing).await;
    }

    // While a test holds the sweep of idle sessions, it waits here, and
    // says it reached the hold.
    #[cfg(test)]
    async fn sweep_seam(&self) {
        let mut held = self.inner.sweep_held.subscribe();
        if !*held.borrow_and_update() {
            return;
        }
        self.inner.sweeps_held.send_modify(|reached| *reached += 1);
        let _ = held.wait_for(|held| !held).await;
    }

    /// Hold the sweep of idle sessions once it has read the routes the
    /// torrents use, before it reads the sessions, or let it go.
    #[cfg(test)]
    pub(crate) fn hold_sweep(&self, held: bool) {
        self.inner.sweep_held.send_replace(held);
    }

    /// Counts the sweeps of idle sessions that reached a test's hold.
    #[cfg(test)]
    pub(crate) fn sweeps_held(&self) -> watch::Receiver<u64> {
        self.inner.sweeps_held.subscribe()
    }
}

// Resolves once `reloads` rings after it was last seen; never when nothing
// reloads the routes, or once what did is gone.
async fn reloaded(reloads: &mut Option<watch::Receiver<u64>>) {
    if let Some(receiver) = reloads.as_mut()
        && receiver.changed().await.is_ok()
    {
        return;
    }
    *reloads = None;
    std::future::pending().await
}

// A change of a torrent's files' levels under way: the job that copies
// their bytes out of the side store and then takes the levels, and the
// levels asked for while it runs, which it takes after (libtorrent
// `torrent::m_deferred_file_priorities`, torrent.cpp:5803-5807 at v2.0.15).
pub(super) struct Leveling {
    pub(super) task: Option<tokio::task::JoinHandle<()>>,
    // The levels the job takes, as stored.
    pub(super) asked: Vec<FilePriority>,
    pub(super) next: Option<Vec<FilePriority>>,
    // The place `asked`, stored before its copy, took at the end of the
    // download queue when it stored the torrent short of complete again.
    pub(super) reopened: Option<i64>,
}
