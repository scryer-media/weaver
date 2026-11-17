// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::intake::{Fetched, Parsed, parse};
use super::live::Live;
use super::retire::{held_key, held_of, held_tops};
use super::settings::TorrentSettings;
use super::tick::Leveling;
use super::{
    PeerTerms, RESTORES_AT_ONCE, Signal, TorrentService, certificate_of, fetch_levels, files_dir,
    held_by, identity_of, ignored_of, in_table_order, load_of, locked, placed, queued,
    ratelimits_of, ssl_identity, state_error, tracker_tiers, unpacks,
};
use crate::LoadError;
use crate::jobs::JobId;
use crate::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};
use crate::pipeline::graph::job::Storage;
use crate::pipeline::graph::limits;
use crate::pipeline::graph::limits::Caps;
use crate::proxies::{
    Consumer, LegTenancy, LegTenant, NetworkRuntime, SessionTraffic, TorrentScope,
};
use crate::runtime::panic_trap::JobScope;
use crate::settings::model::{TrackerCertificate, TrackerRule};
use crate::torrents::files::Files;
use crate::torrents::intake::{Holds, Intake, Room};
use crate::torrents::layout::PieceLayout;
use crate::torrents::sessions::{OPENING, RouteKey, RoutePlan};
use crate::torrents::status::{Engine, Held};
use crate::torrents::storage::{PieceStorageFactory, Pieces};
use crate::torrents::store::{Kept, Replayed, TorrentStore};
use crate::torrents::tee::{Parts, Plan, Tee};
use crate::torrents::{FilePriority, KeptIntent, TorrentCounters, TorrentIntent, TrackerEntry};
use bytes::Bytes;
use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use weaver_torrent::api::TorrentIdOrHash;
use weaver_torrent::limits::LimitsConfig;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, BF, Checkpoint, Identity, ManagedTorrent,
    ResumePeers, Session, TorrentTotals,
};

// The one place a torrent's archive passwords are chosen: the passwords of
// the rule it is under, then the global list, a value already in the list
// skipped so its first copy keeps its place. Only this reads a rule's
// passwords, and it hands them to the graph of a torrent under that rule
// alone: an NZB job's harvest reads no rule, a torrent under another rule or
// none gets only the global list, and nothing writes a rule's passwords to
// the global list.
pub(super) fn torrent_archive_passwords(
    rule: Option<&TrackerRule>,
    global: &[ArchivePasswordCandidate],
) -> Vec<ArchivePasswordCandidate> {
    let tracker = rule
        .into_iter()
        .flat_map(|rule| rule.archive_passwords.as_slice())
        .map(|value| ArchivePasswordCandidate::new(ArchivePasswordSource::Tracker, value.clone()));
    let mut candidates: Vec<ArchivePasswordCandidate> = Vec::new();
    for candidate in tracker.chain(global.iter().cloned()) {
        if candidates
            .iter()
            .all(|kept| kept.value() != candidate.value())
        {
            candidates.push(candidate);
        }
    }
    candidates
}

// A torrent's place in its session.
pub(super) struct Attached {
    pub(super) session: Arc<Session>,
    pub(super) handle: Arc<ManagedTorrent>,
    // The terms the session took it under.
    pub(super) terms: PeerTerms,
    // The certificate an SSL torrent was given from its rule.
    pub(super) ssl: Option<TrackerCertificate>,
    // The leg the session runs it on, which counts what it moves.
    pub(super) leg: Option<OnLeg>,
    // How it names, and the scope it names as.
    pub(super) names: Arc<crate::torrents::naming::TorrentNames>,
    pub(super) scope: TorrentScope,
}

// The leg of a scope's route a session runs on.
pub(super) struct OnLeg {
    pub(super) network: Arc<NetworkRuntime>,
    pub(super) egress_id: u32,
    // Whether the session dials from the leg's own sockets, past any route.
    pub(super) own_sockets: bool,
    // The torrent's place on the leg, which counts what it holds there
    // until the torrent leaves its session.
    pub(super) tenancy: Arc<LegTenancy>,
}

// A torrent on its leg, as its run holds it now: its established peers
// count on every leg. A session on the leg's own sockets passes no route,
// so the leg hears from the torrent its peers in their handshake and what
// it receives too.
struct LegTorrent {
    handle: Arc<ManagedTorrent>,
    own_sockets: bool,
}

impl LegTenant for LegTorrent {
    fn traffic(&self) -> SessionTraffic {
        let Some(run) = self.handle.gauges() else {
            return SessionTraffic::default();
        };
        let peers = run.peers();
        let (opening, bytes_per_second) = if self.own_sockets {
            (u64::from(peers.connecting), run.download_bps())
        } else {
            (0, 0)
        };
        SessionTraffic {
            open: u64::from(peers.live),
            opening,
            bytes_per_second,
            ..SessionTraffic::default()
        }
    }
}

// What a torrent joins its route's session with, read before the session is
// asked for, and the plan of that route.
struct Joining {
    plan: RoutePlan,
    scope: TorrentScope,
    bytes: Bytes,
    layout: Arc<PieceLayout>,
    intake: Arc<Intake>,
    dir: String,
    paused: bool,
    ratelimits: LimitsConfig,
    have: (
        Option<bitvec::boxed::BitBox<u8, bitvec::order::Msb0>>,
        bool,
        Arc<TorrentStore>,
    ),
    identity: Identity,
    totals: TorrentTotals,
    terms: PeerTerms,
    ssl: Option<TrackerCertificate>,
    picking: (Vec<FilePriority>, bool, bool),
    saved: ResumePeers,
    listed: Arc<[TrackerEntry]>,
    verified: Option<Option<Vec<u8>>>,
}

impl TorrentService {
    // Everything but the tick.
    pub(crate) async fn open(&self) -> Result<(), String> {
        if let Some(task) = self.inner.env.hear_alone() {
            *locked(&self.inner.hearing) = Some(task);
        }
        self.spawn_settle();
        self.spawn_follow_disk_room();
        self.spawn_follow_trackers();
        self.spawn_follow_egress_limits();
        self.spawn_follow_interfaces();
        self.restore().await?;
        // Torrents turned on while a wind-down's stops were kept, with no
        // pass since to resume them, resume now.
        if self.settings().torrent.enabled {
            self.resume_wound_down().await;
        }
        self.retry_retiring().await;
        Ok(())
    }

    // The archive passwords a graph built for `torrent` now takes. One
    // already extracting keeps the list it started with.
    pub(super) fn archive_passwords(
        &self,
        torrent: &Live,
        global: &[ArchivePasswordCandidate],
    ) -> Vec<ArchivePasswordCandidate> {
        torrent_archive_passwords(torrent.rule(&self.rules()), global)
    }

    // The archive passwords every torrent may try, as the settings give
    // them now.
    pub(super) fn global_passwords(&self) -> Arc<[ArchivePasswordCandidate]> {
        self.inner
            .db
            .global_archive_password_candidates()
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "could not load global archive passwords");
                Vec::new()
            })
            .into()
    }

    // The tee's job's storage, in its own directory `work`.
    fn storage(
        &self,
        work: PathBuf,
        layout: &PieceLayout,
        plan: &Plan,
        passwords: Vec<ArchivePasswordCandidate>,
    ) -> Storage {
        let db = self.inner.db.clone();
        let policy: limits::PolicyRead = Arc::new(move || {
            db.post_processing_settings()
                .map(Arc::new)
                .map_err(state_error)
        });
        let env = &self.inner.env;
        Storage {
            working_dir: work,
            output: None,
            accountant: Arc::clone(&env.accountant),
            decode: Arc::clone(&env.decode),
            free_disk: env.free_disk.clone(),
            slots: env.slots,
            mail: env.mail.clone(),
            passwords,
            caps: Caps::new(Arc::clone(&env.limits), policy, plan.declared(layout)),
            nesting: limits::NESTING,
            hardware: Default::default(),
            metrics: None,
            // The tee writes its files itself as each byte is placed; its
            // writes do not go through the volumes' writers.
            disk: None,
            data_dir: env.data_dir.clone(),
            window_ledger: env.window_ledger.clone(),
        }
    }

    // The torrent's store over its files, with the pieces it kept, and the
    // tee its archive files go to when it unpacks and is not complete: the
    // one its checkpoint gives back, or a new one. A checkpoint that does
    // not restore is said, with why, and the tee starts as a new one. The
    // pieces it kept that the tee's checkpoint lacks a share of go to the
    // tee from the files, once; a piece whose files do not read is had no
    // longer, and is checked or fetched again. Nothing here fails the
    // torrent: a tee that cannot start ends its unpacking, and the torrent
    // keeps its files. It reads the database and the files, so it runs off
    // the tick's task. The pieces it kept whose bytes its files no longer
    // hold, a file gone or cut short, are had no longer either: each file
    // with a piece had is looked at by its size, once, as the store is made.
    // When none of those files is there, the pieces are kept as they were,
    // none is read for the tee, and the notes say so with `Note::Absent`,
    // which holds the torrent for its files missing. A restore whose
    // checkpoint the database could not
    // read makes no store: the torrent waits, its own rows as they were,
    // and the next restore tries it again, as a usenet job's restore is
    // held on the same error (`RestoreSeat::restore`).
    fn store<W>(
        &self,
        job_id: JobId,
        made: Making,
        hold: Hold<W>,
    ) -> Result<(TorrentStore, Vec<Note>), Unmade<W>> {
        let _job = JobScope::enter(job_id);
        let Making {
            files,
            layout,
            tee,
            passwords,
            seeded,
            side,
            ignored,
            kept,
        } = made;
        // One policy for both of a restore's database reads, the pieces it
        // kept and the tee's checkpoint: one the database fails to give
        // holds the restore until it gives it, as a usenet job's restore is
        // held. What it gives and this build cannot use is no record: the
        // pieces are checked again, the tee starts fresh.
        let kept = match kept.map_or_else(
            || {
                self.inner
                    .db
                    .torrent_pieces(job_id)
                    .map_err(|error| hold(error.to_string()))
            },
            Ok,
        ) {
            Ok(kept) => kept,
            Err(Ok(wait)) => return Err(Unmade::Waits(wait)),
            Err(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's pieces; they are checked again");
                None
            }
        };
        let writes = self.inner.env.disk.writes(job_id.0, &files);
        let files = Files::new(&layout, files).map_err(|error| Unmade::Fails(error.to_string()))?;
        let mut notes = Vec::new();
        let tee = match tee.then(|| Plan::of(&layout)).flatten() {
            Some(plan) => {
                let (tee, fresh) = self.tee(job_id, &layout, plan, passwords, hold)?;
                notes.extend(fresh);
                Some(tee)
            }
            None => None,
        };
        let store = TorrentStore::new(layout, files, kept.as_ref(), tee, writes)
            .with_side(side, &ignored)
            .map_err(|error| {
                Unmade::Fails(format!("could not open the torrent's side store: {error}"))
            })?;
        if let Some(reason) = store.side_reset() {
            tracing::warn!(
                job_id = job_id.0,
                reason,
                "a torrent's side store was not valid and was removed; its pieces are checked again"
            );
        }
        let unbacked = store.drop_unbacked();
        if let Some(tee) = store.tee() {
            // With none of its files there, nothing is read from them: the
            // pieces stay for when they are.
            let replayed = if seeded || unbacked.is_none() {
                Replayed::default()
            } else {
                store.replay_had()
            };
            if replayed.taken > 0 {
                tracing::debug!(
                    job_id = job_id.0,
                    replayed = replayed.taken,
                    "a torrent's tee took again the pieces its checkpoint lacked"
                );
            }
            if let Some(error) = replayed.unread {
                notes.push(Note::Dropped {
                    pieces: replayed.dropped,
                    error: error.to_string(),
                });
            }
            // A run that will not start is kept as the tee's refusal.
            let _ = tee.resume();
            if tee.refusal().is_some() {
                self.inner.signal.mark(job_id);
            }
        }
        match unbacked {
            Some(0) => {}
            Some(missing) => {
                notes.push(Note::Missing { pieces: missing });
                self.inner.signal.marked(job_id, missing);
            }
            None => notes.push(Note::Absent {
                pieces: store.count(),
            }),
        }
        Ok((store, notes))
    }

    // The tee of a torrent whose files `plan` names, restored from its
    // checkpoint or new, and why it is new when it had a checkpoint. One
    // whose directory cannot be made ready refuses at once, for why. Rows
    // this build cannot read are no checkpoint; a read the database failed
    // holds a restore, and starts a new torrent's tee fresh.
    pub(super) fn tee<W>(
        &self,
        job_id: JobId,
        layout: &Arc<PieceLayout>,
        plan: Plan,
        passwords: Vec<ArchivePasswordCandidate>,
        hold: Hold<W>,
    ) -> Result<(Arc<Tee>, Option<Note>), Unmade<W>> {
        let work = self.unpacking_dir(job_id);
        let (rows, mut fresh) = match self.inner.db.load_graph_checkpoint(job_id) {
            Ok(rows) => (rows, None),
            Err(LoadError::Read(error)) => match hold(error.to_string()) {
                Ok(wait) => return Err(Unmade::Waits(wait)),
                Err(error) => (None, Some(Note::Unread(error))),
            },
            Err(error) => (None, Some(Note::Unread(error.to_string()))),
        };
        let env = &self.inner.env;
        let parts = || Parts {
            job_id,
            layout: Arc::clone(layout),
            plan: plan.clone(),
            work: work.clone(),
            repairs: Arc::clone(&env.repairs),
            signal: Arc::clone(&self.inner.signal),
            outside: Arc::clone(&env.outside),
            limit: env.slots,
            db: self.inner.db.clone(),
            mail: env.written.clone(),
        };
        let storage = || self.storage(work.clone(), layout, &plan, passwords.clone());
        // The directory is claimed the job's before anything reads or writes
        // in it; one another job's marker holds is never taken.
        let claimed = crate::jobs::working_dir::claim_named_working_dir(&env.work, &work, job_id);
        if let (Some(rows), Ok(_)) = (rows, &claimed) {
            match Tee::restore(parts(), storage(), rows) {
                Ok(tee) => return Ok((tee, None)),
                // A rebuild that failed writing or stepping is held, as a
                // usenet job's is: only a checkpoint this build cannot read
                // starts fresh.
                Err(error) if !error.not_this_builds() => match hold(error.to_string()) {
                    Ok(wait) => return Err(Unmade::Waits(wait)),
                    Err(error) => fresh = Some(Note::Unrestored(error)),
                },
                Err(error) => fresh = Some(Note::Unrestored(error.to_string())),
            }
        }
        let tee = Tee::fresh(parts(), storage());
        // Nothing the job wrote before is its now: its directory goes
        // through the owner that proved it the job's, and is claimed again.
        let cleared = claimed.and_then(|_| {
            crate::jobs::working_dir::remove_job_working_dir(&env.work, &work, job_id)?;
            crate::jobs::working_dir::claim_named_working_dir(&env.work, &work, job_id)
        });
        if let Err(error) = cleared {
            tee.refuse(format!(
                "could not claim the torrent's unpacking directory: {error}"
            ));
        }
        Ok((tee, fresh))
    }

    // What a torrent's store is made from, read under the torrent's lock.
    fn making(&self, torrent: &Live, global: &[ArchivePasswordCandidate]) -> Option<Making> {
        Some(Making {
            files: files_dir(torrent),
            layout: Arc::clone(torrent.layout.as_ref()?),
            tee: unpacks(torrent) && !torrent.complete(),
            passwords: self.archive_passwords(torrent, global),
            seeded: torrent.record.seed_mode,
            side: self.side_store(torrent.record.job_id),
            ignored: ignored_of(&torrent.levels),
            kept: None,
        })
    }

    // The store joined to what feeds it: the intake the session hands its
    // pieces to.
    fn give_store(&self, torrent: &mut Live, store: TorrentStore) {
        let job_id = torrent.record.job_id;
        let env = &self.inner.env;
        let store = Arc::new(store);
        let signal = Arc::clone(&self.inner.signal);
        // With disks to read, the pieces book on the disk the torrent's
        // folder lies on. A store handed again over the same folder keeps
        // the disk found for it; any other is looked up now, when the
        // torrent has bytes to fetch, and no piece is let through until it
        // is known.
        let dir = files_dir(torrent);
        let room = match &env.capacity {
            None => Room::new(Arc::clone(&env.limits), env.free_disk.clone()),
            Some(_) => match torrent
                .intake
                .as_ref()
                .and_then(|intake| intake.room().place())
                .filter(|place| place.dir == dir)
            {
                Some(place) => Room::placed(Arc::clone(&env.limits), place),
                None => Room::aimed(Arc::clone(&env.limits), dir.clone()),
            },
        };
        let intake = Arc::new(Intake::new(
            Arc::clone(&store) as Arc<dyn Pieces>,
            room,
            Box::new(move |heard| signal.heard(job_id, heard)),
        ));
        if env.capacity.is_some() && !torrent.complete() && intake.room().place().is_none() {
            self.look_for_place(torrent.record.job_id, &intake, dir);
        }
        torrent.hold(store.file_have());
        if let Some(tee) = store.tee() {
            tee.hold_own_clock(torrent.record.intent() == TorrentIntent::Paused);
        }
        torrent.store = Some(store);
        torrent.intake = Some(intake);
    }

    // Where a torrent's bytes in memory are counted: the accountant every
    // job shares, under the torrent's namespace.
    pub(super) fn resident(&self, job_id: JobId) -> Holds {
        Holds {
            accountant: Arc::clone(&self.inner.env.accountant),
            namespace: weaver_graph::holds::HoldsNamespace {
                scope: job_id.0,
                node: weaver_graph::NodeId::ROOT,
            },
        }
    }

    // The torrent's metainfo and its store over it, at once. A store that
    // cannot be made fails the torrent. The files `left_out`, by their
    // index among the manifest's paths, are not downloaded: their level is
    // `Ignored`, kept as the operator's own would be.
    pub(super) fn give_metainfo(
        &self,
        torrent: &mut Live,
        parsed: &Parsed,
        left_out: &[usize],
    ) -> Result<(), String> {
        let job_id = torrent.record.job_id;
        let mut stored = self.inner.db.torrent_file_priorities(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
            Vec::new()
        });
        if !left_out.is_empty() {
            let left_out: HashSet<usize> = left_out.iter().copied().collect();
            stored.resize(parsed.layout.file_count(), FilePriority::Normal);
            let listed = parsed
                .layout
                .files()
                .enumerate()
                .filter(|(_, file)| !file.padding)
                .map(|(index, _)| index);
            for (path, index) in listed.enumerate() {
                if left_out.contains(&path) {
                    stored[index] = FilePriority::Ignored;
                }
            }
            self.inner
                .db
                .set_torrent_file_priorities(job_id, &stored)
                .map_err(state_error)?;
        }
        self.describe(torrent, parsed, stored);
        let Some(made) = self.making(torrent, &self.global_passwords()) else {
            return Ok(());
        };
        let (store, notes) = self
            .store::<std::convert::Infallible>(job_id, made, Err)
            .map_err(|unmade| match unmade {
                Unmade::Fails(error) => error,
                Unmade::Waits(never) => match never {},
            })?;
        for note in &notes {
            note.say(job_id);
        }
        torrent.files_missing = Note::absent(&notes);
        self.give_store(torrent, store);
        Ok(())
    }

    // What the metainfo says of the torrent, without its job. Whether it
    // unpacks is fixed here and kept, so a restart builds the same tree.
    fn describe(&self, torrent: &mut Live, parsed: &Parsed, stored: Vec<FilePriority>) {
        let job_id = torrent.record.job_id;
        if torrent.record.unpack_in_force.is_none() {
            let unpack = self
                .settings()
                .unpack(&torrent.record, torrent.rule(&self.rules()));
            if let Err(error) = self.inner.db.set_torrent_unpack_in_force(job_id, unpack) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record whether a torrent unpacks");
            }
            torrent.record.unpack_in_force = Some(unpack);
        }
        torrent.names = parsed.manifest.paths.clone();
        torrent.total = parsed.total;
        torrent.layout = Some(Arc::clone(&parsed.layout));
        torrent.levels = parsed
            .layout
            .files()
            .enumerate()
            .map(|(index, file)| {
                let level = stored.get(index).copied().unwrap_or_default();
                level.held_by(file.padding)
            })
            .collect();
        torrent.about = parsed.about.clone();
        torrent.private = parsed.private;
    }

    // The jobs of the torrents brought back at start with their records
    // only, each restored from its checkpoint, RESTORES_AT_ONCE at a time. A
    // torrent reads as it was kept until its job is back.
    pub(super) async fn restore_jobs(&self) {
        let waiting: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.store.is_none() && torrent.layout.is_some())
            .map(|torrent| torrent.record.job_id)
            .collect();
        // RESTORES_AT_ONCE in flight, each torrent attached as soon as its
        // job is back rather than at the next tick, so the first seeds
        // without waiting for the last to restore (qBittorrent's
        // startUpTorrents keeps MAX_PROCESSING_RESUMEDATA_COUNT in flight
        // into libtorrent's async_add_torrent).
        // The global archive passwords, read once for all of them and
        // outside the live lock.
        let global = (!waiting.is_empty()).then(|| self.global_passwords());
        let mut restoring = tokio::task::JoinSet::new();
        for job_id in waiting {
            if restoring.len() >= RESTORES_AT_ONCE {
                restoring.join_next().await;
            }
            let service = self.clone();
            let global = global.clone();
            restoring.spawn(async move {
                if service.restore_job_with(job_id, global).await {
                    service.attach(job_id, None).await;
                }
            });
        }
        while restoring.join_next().await.is_some() {}
    }

    // The job of a torrent that waits for one, restored on a blocking thread
    // under no lock but its own job's and a restore permit, and given
    // to the torrent if it still waits: one removed meanwhile drops it. A
    // restore that panics is a checkpoint that does not restore. Whether it
    // gave the torrent its job, unheld, so it may attach now.
    pub(super) async fn restore_job(&self, job_id: JobId) -> bool {
        self.restore_job_with(job_id, None).await
    }

    // [`Self::restore_job`] with the global archive passwords read for it,
    // or none to read them when its store is made.
    async fn restore_job_with(
        &self,
        job_id: JobId,
        global: Option<Arc<[ArchivePasswordCandidate]>>,
    ) -> bool {
        // However the restore ends, even cancelled or with the runtime, its
        // entry goes: a torrent with its job, or gone, waits for no restore.
        struct Done<'a>(&'a TorrentService, JobId);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0
                    .inner
                    .restoring
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&self.1);
            }
        }
        let this_job = Arc::clone(
            self.inner
                .restoring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(job_id)
                .or_default(),
        );
        let guard = (
            this_job.lock_owned().await,
            Arc::clone(&self.inner.restore_permits)
                .acquire_owned()
                .await
                .expect("the restore permits are never closed"),
        );
        let _done = Done(self, job_id);
        // A torrent whose work crashed the daemon would restore into the
        // same panic: it is kept paused, with why, and its job is not
        // rebuilt until a resume takes its marker.
        if self.park(job_id).await {
            return false;
        }
        let waiting = {
            let mut live = self.inner.live.lock().await;
            live.get_mut(&job_id)
                .filter(|torrent| torrent.store.is_none())
                .and_then(|torrent| {
                    let global = global.unwrap_or_else(|| self.global_passwords());
                    let made = self.making(torrent, &global)?;
                    Some(Making {
                        kept: torrent.kept_at_boot.take(),
                        ..made
                    })
                })
        };
        let Some(made) = waiting else {
            return false;
        };
        let restoring = {
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                service
                    .inner
                    .written
                    .send_modify(|written| written.checkpoints_read += 1);
                let store = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.store(job_id, made, Ok)
                }))
                .unwrap_or_else(|_| Err(Unmade::Fails("the restore panicked".to_string())));
                (guard, store)
            })
        };
        // The blocking task ends only with its result, or with the runtime.
        let Ok((_restoring, store)) = restoring.await else {
            return false;
        };
        let (store, missing) = match store {
            Ok((store, notes)) => {
                for note in &notes {
                    note.say(job_id);
                }
                (store, Note::absent(&notes))
            }
            Err(Unmade::Waits(error)) => {
                // Said once while it stands; the torrent shows it as its
                // error until a restore reads it.
                let mut live = self.inner.live.lock().await;
                if let Some(torrent) = live.get_mut(&job_id) {
                    let why = format!("its restore is held: {error}");
                    if torrent.restore_held.replace(why).is_none() {
                        tracing::warn!(job_id = job_id.0, error = %error, "a torrent's restore is held; it is tried again");
                    } else {
                        tracing::debug!(job_id = job_id.0, error = %error, "a torrent's restore is still held");
                    }
                }
                return false;
            }
            Err(Unmade::Fails(error)) => {
                self.fail(job_id, error).await;
                return false;
            }
        };
        let (started, copy) = match self
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .filter(|torrent| torrent.store.is_none())
        {
            Some(torrent) => {
                torrent.restore_held = None;
                torrent.files_missing = missing;
                self.give_store(torrent, store);
                let copy = self.catch_up_levels(torrent);
                (torrent.held.is_none() && !missing, copy)
            }
            None => {
                // Never handed to a session: it has nothing queued, and the
                // wait is none.
                store.forget().await;
                (false, None)
            }
        };
        if let Some((store, asked)) = copy {
            self.start_copy(job_id, store, asked).await;
        }
        started
    }

    // A store made from the levels of when its restore began, brought to
    // those of now: a change asked while it was made has the files it sets
    // aside, and, when that has bytes to copy out of the side store, the
    // copy to start, as a change of levels starts it.
    fn catch_up_levels(
        &self,
        torrent: &mut Live,
    ) -> Option<(Arc<TorrentStore>, Vec<FilePriority>)> {
        let store = torrent.store.as_ref()?;
        let ignored = ignored_of(&torrent.levels);
        if store.needs_copy(&ignored) {
            let asked: Vec<FilePriority> = torrent
                .levels
                .iter()
                .zip(store.layout().files())
                .map(|(level, file)| {
                    if file.padding {
                        FilePriority::Normal
                    } else {
                        *level
                    }
                })
                .collect();
            torrent.leveling = Some(Leveling {
                task: None,
                asked: asked.clone(),
                next: None,
            });
            return Some((Arc::clone(store), asked));
        }
        // Nothing to copy: only which files are set aside changes, in
        // memory.
        if let Err(error) = store.relevel(&ignored) {
            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not set a torrent's files aside");
        }
        None
    }

    // Keep a torrent that crashed the daemon paused, its restore held for
    // why, until a resume takes its marker. Its marker is read under the
    // torrent's lock, which a resume takes after the marker, so a resume
    // is never undone. Whether the torrent waits for a resume.
    pub(super) async fn park(&self, job_id: JobId) -> bool {
        let mut live = self.inner.live.lock().await;
        let Some(why) = self.crashed(job_id) else {
            return false;
        };
        let Some(torrent) = live.get_mut(&job_id) else {
            return true;
        };
        if torrent.record.intent() == TorrentIntent::Resumed {
            if let Err(error) = self
                .inner
                .db
                .update_torrent_intent(job_id, TorrentIntent::Paused)
            {
                tracing::warn!(job_id = job_id.0, error = %error, "could not keep a crashed torrent's pause");
            }
            torrent.settle(self.inner.clock.unix_ms());
            torrent.record.kept = KeptIntent::Intent(TorrentIntent::Paused);
        }
        torrent.restore_held = Some(why);
        true
    }

    // Put a torrent in its route's session, or hold it while the route is
    // down. It runs under its own identity, and counts on from what it had
    // moved in the session it ran in before. A torrent whose metadata was
    // just `fetched` takes over the fetch's trackers when it starts running
    // now, on the fetch's session under the identity the fetch announced, so
    // they hear one torrent; otherwise the fetch's trackers are let go,
    // which sends them `stopped`, and the run starts its own.
    // Returns the route whose session did not open for it, and why.
    pub(super) async fn attach(
        &self,
        job_id: JobId,
        fetched: Option<Fetched>,
    ) -> Option<(RouteKey, Held)> {
        let joining = self.joining(job_id).await?;
        let session = match self.open_session(&joining.plan, &joining.identity).await {
            Ok(session) => session,
            Err(error) => {
                let held = held_by(error);
                self.held(job_id, Some(joining.plan.key.clone()), held.clone())
                    .await;
                return Some((joining.plan.key, held));
            }
        };
        self.join(job_id, joining, session, fetched).await;
        None
    }

    // `attach`, answered at once: a torrent whose route's session runs
    // joins it now, and one whose route must open first waits for it off
    // the caller, reading as opening meanwhile, and goes on by itself once
    // the open ends, in the session or held for why it did not open. Nothing
    // of it reaches the route before its session is open. What it joins
    // with is read once the open ends, so a pause, a move or a category
    // change made while it waited holds; one whose route changed meanwhile waits
    // for the route it runs on now. A torrent already waiting on its route's
    // open is left to that wait, reading as the route it runs on now.
    pub(super) async fn spawn_attach(&self, job_id: JobId) {
        let Some(attaching) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| torrent.attaching.is_some())
        else {
            return;
        };
        let Some((plan, identity)) = self.route_for(job_id).await else {
            return;
        };
        if attaching {
            return;
        }
        if let Some(session) = self.inner.sessions.running_session(&plan).await {
            self.read_sessions().await;
            if let Some(joining) = self.joining(job_id).await {
                self.join(job_id, joining, session, None).await;
            }
            return;
        }
        // A route its plan holds down builds nothing, so it says why at
        // once.
        if plan.down.is_some() {
            self.attach(job_id, None).await;
            return;
        }
        let service = self.clone();
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return;
        };
        if torrent.attaching.is_some() {
            return;
        }
        // The task takes this lock before it touches the torrent, so the
        // handle is the torrent's before the task lets it go.
        let task = tokio::spawn(async move {
            let (mut plan, mut identity) = (plan, identity);
            loop {
                let session = match service.open_session(&plan, &identity).await {
                    Ok(session) => session,
                    Err(error) => {
                        service.held(job_id, Some(plan.key), held_by(error)).await;
                        break;
                    }
                };
                let Some(joining) = service.joining(job_id).await else {
                    break;
                };
                if joining.plan.key == plan.key {
                    service.join(job_id, joining, session, None).await;
                    break;
                }
                // Its route changed while that one opened: it waits for the
                // one it runs on now, and keeps what it read for the join.
                if let Some(torrent) = service.inner.live.lock().await.get_mut(&job_id)
                    && torrent.verified_at_boot.is_none()
                {
                    torrent.verified_at_boot = joining.verified;
                }
                (plan, identity) = (joining.plan, joining.identity);
            }
            if let Some(torrent) = service.inner.live.lock().await.get_mut(&job_id) {
                torrent.attaching = None;
            }
            service.publish_job(job_id).await;
            service.inner.attaches.send_modify(|ended| *ended += 1);
        });
        torrent.attaching = Some(task.abort_handle());
    }

    // The route a torrent that may join a session runs on, and who opens
    // that route's session for it: none when it is gone, in a session
    // already, has no metainfo yet, or its files are missing. From here it
    // reads as that route.
    async fn route_for(&self, job_id: JobId) -> Option<(RoutePlan, Identity)> {
        let settings = self.settings();
        let rules = self.rules();
        let (scope, identity) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            if torrent.attached.is_some()
                || torrent.files_missing
                || torrent.record.metainfo.is_none()
                || torrent.layout.is_none()
                || torrent.intake.is_none()
                || torrent.store.is_none()
            {
                return None;
            }
            let rule = torrent.rule(&rules);
            (
                settings.scope(torrent.record.category_id, rule),
                identity_of(rule, &torrent.identity),
            )
        };
        Some((self.planned(job_id, scope).await, identity))
    }

    // The plan of the route `job_id` runs on under `scope`. It reads as its
    // route does while it waits for the route's session: opening, or why the
    // route's last open failed.
    async fn planned(&self, job_id: JobId, scope: TorrentScope) -> RoutePlan {
        let plan = self
            .pinned(job_id, self.inner.sessions.routes().plan(scope))
            .await;
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id)
            && torrent.attached.is_none()
            && torrent.held.is_none()
        {
            torrent.route = Some(plan.key.clone());
            torrent.held = Some(Held::Session(OPENING));
        }
        plan
    }

    // Whether `torrent` joins its session paused: its intent, its turn in
    // the queue, a hold over it, or a disk too short for it to start stops
    // it.
    fn held_back(
        &self,
        settings: &TorrentSettings,
        rules: &[TrackerRule],
        torrent: &Live,
        now: i64,
    ) -> bool {
        !torrent.resumed()
            || torrent.queued
            || self
                .holds()
                .reason_for(queued(settings, rules, torrent, now).target())
                .is_some()
            || self.kept_from_starting(torrent)
    }

    // What a torrent joins its route's session with, read as it stands, and
    // the plan of the route it joins: none when it is gone, in a session
    // already, has no metainfo yet, or its files are missing. From here it
    // reads as its route does while it waits for the route's session.
    async fn joining(&self, job_id: JobId) -> Option<Joining> {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.unix_ms();
        let (
            scope,
            bytes,
            layout,
            intake,
            dir,
            paused,
            ratelimits,
            have,
            identity,
            totals,
            terms,
            ssl,
            picking,
            saved,
            listed,
            verified,
        ) = {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id)?;
            // One whose files are missing joins no session: it would claim
            // pieces it cannot read and write where its files are not.
            if torrent.attached.is_some() || torrent.files_missing {
                return None;
            }
            let (Some(bytes), Some(layout), Some(intake), Some(store)) = (
                torrent.record.metainfo.clone(),
                torrent.layout.clone(),
                torrent.intake.clone(),
                torrent.store.clone(),
            ) else {
                return None;
            };
            let rule = torrent.rule(&rules);
            let ratelimits =
                ratelimits_of(&torrent.record, settings.upload_cap(&torrent.record, rule));
            (
                settings.scope(torrent.record.category_id, rule),
                bytes,
                layout,
                intake,
                torrent.record.storage_dir.clone().unwrap_or_default(),
                self.held_back(&settings, &rules, torrent, now),
                ratelimits,
                (
                    store.initial_have(placed(&torrent.record)),
                    torrent.record.seed_mode,
                    store,
                ),
                identity_of(rule, &torrent.identity),
                torrent.seen,
                PeerTerms::of(&settings, rule),
                certificate_of(rule),
                (
                    fetch_levels(torrent),
                    torrent.record.sequential,
                    torrent.record.first_last_piece_first,
                ),
                // libtorrent `torrent::read_resume_data`: the saved peers
                // are known from the start and the saved bans hold.
                torrent.resume.clone(),
                // The trackers it lists now, each with who named it, as a
                // tracker edit leaves them under this lock: they stand in
                // for its metainfo's.
                torrent.trackers.clone(),
                torrent.verified_at_boot.take(),
            )
        };
        let plan = self.planned(job_id, scope).await;
        Some(Joining {
            plan,
            scope,
            bytes,
            layout,
            intake,
            dir,
            paused,
            ratelimits,
            have,
            identity,
            totals,
            terms,
            ssl,
            picking,
            saved,
            listed,
            verified,
        })
    }

    // Put a torrent in `session`, its route's, with what `joining` read of
    // it.
    async fn join(
        &self,
        job_id: JobId,
        joining: Joining,
        session: Arc<Session>,
        fetched: Option<Fetched>,
    ) {
        let Joining {
            plan,
            scope,
            bytes,
            layout,
            intake,
            dir,
            paused,
            ratelimits,
            have,
            identity,
            totals,
            terms,
            ssl,
            picking,
            saved,
            listed,
            verified,
        } = joining;
        // The session takes it only while it is still here: a remove made
        // while the route's session opened has deleted its files, and the
        // session would make them again. A remove made from here on waits
        // until the session has it, then takes it out.
        let Some(gate) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| Arc::clone(&torrent.gate))
        else {
            return;
        };
        let _taking = gate.lock().await;
        if self.inner.live.lock().await.get(&job_id).is_none() {
            return;
        }
        let (identity, tracker_handover) = match fetched {
            Some(fetched)
                if !paused
                    && Arc::ptr_eq(&fetched.session, &session)
                    && fetched.identity == identity =>
            {
                (fetched.identity, Some(fetched.trackers))
            }
            _ => (identity, None),
        };
        let pieces = intake as Arc<dyn Pieces>;
        let factory = PieceStorageFactory::new(
            Arc::clone(&layout),
            Arc::clone(&pieces),
            self.resident(job_id),
        );
        // Its trackers and web seeds name by its route's profile, and follow
        // it to another route on the same session.
        let names = self.inner.sessions.torrent_names(&plan, &session).await;
        let mut options = AddTorrentOptions {
            naming: Some(names.naming()),
            paused,
            // A write that finds no room waits for this service to see room
            // made, not for a timer.
            owner_wakes_full_disk: self.inner.env.capacity.is_some(),
            overwrite: true,
            output_folder: Some(dir),
            storage_factory: Some(factory.boxed_factory()),
            ratelimits,
            identity: Some(identity),
            totals,
            tracker_tiers: Some(tracker_tiers(&listed)),
            initial_peers: (!saved.peers.is_empty()).then(|| saved.peers.clone()),
            banned_peers: saved.banned.clone(),
            found_peers: saved.found.clone(),
            file_priorities: Some(picking.0),
            sequential: picking.1,
            first_last_piece_first: picking.2,
            tracker_handover,
            ..Default::default()
        };
        terms.apply(&mut options);
        options.ssl_identity = ssl_identity(ssl.as_ref());
        let (have, seed_mode, store) = have;
        apply_initial_have(&mut options, have);
        options.seed_mode = seed_mode;
        // The pieces that passed their check in seed mode before are passed
        // again (libtorrent restores `verified_pieces` on a seed-mode add).
        // Lost, they are checked again as a peer asks for them.
        if seed_mode {
            let verified = verified.map_or_else(|| self.inner.db.torrent_seed_verified(job_id), Ok);
            options.seed_verified = match verified {
                Ok(verified) => {
                    verified.map(|bytes| BF::from_boxed_slice(bytes.into_boxed_slice()))
                }
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read the pieces a torrent checked in seed mode");
                    None
                }
            };
        }
        let added = session
            .add_torrent(AddTorrent::from_bytes(bytes), Some(options))
            .await;
        let handle = match added.map(AddTorrentResponse::into_handle) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                self.fail(job_id, "the session did not take the torrent".into())
                    .await;
                return;
            }
            Err(error) => {
                self.fail(job_id, format!("{error:#}")).await;
                return;
            }
        };
        let mut live = self.inner.live.lock().await;
        // Another attach of this torrent may have finished while this one
        // waited: the session it put the torrent in keeps it, and a second
        // copy in another session is let go.
        let attached_meanwhile = live
            .get(&job_id)
            .and_then(|torrent| torrent.attached.as_ref())
            .map(|attached| {
                Arc::ptr_eq(&attached.session, &session) && attached.handle.id() == handle.id()
            });
        if let Some(same) = attached_meanwhile {
            drop(live);
            if !same
                && let Err(error) = session
                    .delete(TorrentIdOrHash::Id(handle.id()), false)
                    .await
            {
                tracing::debug!(job_id = job_id.0, error = %error, "a second copy of a torrent was already gone from its session");
            }
            return;
        }
        match live.get_mut(&job_id) {
            Some(torrent) => {
                // A ban made while the session took the torrent holds in
                // it too.
                let since: Vec<IpAddr> = torrent
                    .resume
                    .banned
                    .iter()
                    .filter(|ip| !saved.banned.contains(ip))
                    .copied()
                    .collect();
                if !since.is_empty() {
                    handle.ban_peers(&since);
                }
                // A tracker edit made while the session took the torrent,
                // which found it in no session, reaches it too.
                if torrent.trackers != listed {
                    handle.replace_trackers(tracker_tiers(&torrent.trackers));
                }
                torrent.settle(self.inner.clock.unix_ms());
                let leg = self
                    .queue()
                    .and_then(|queue| queue.proxy_runtime())
                    .map(|proxies| OnLeg {
                        network: proxies.network.clone(),
                        egress_id: plan.egress_id,
                        own_sockets: plan.kind.own_sockets(),
                        tenancy: proxies.network.tenant_torrent(
                            Consumer::Torrent(scope),
                            crate::torrents::sessions::TORRENT_LEG,
                            LegTorrent {
                                handle: handle.clone(),
                                own_sockets: plan.kind.own_sockets(),
                            },
                        ),
                    });
                if seed_mode {
                    adopt_placed(
                        &handle,
                        &store,
                        job_id,
                        Arc::clone(&self.inner.signal),
                        session.cancellation_token().clone().cancelled_owned(),
                    );
                }
                // Nothing is said as the run's start from what was saved
                // ends: the torrent is shown once it has, not at the next
                // tick. A run removed from its session ends the start, and
                // a session that stops ends the wait.
                {
                    let started = Arc::clone(&handle);
                    let stopping = session.cancellation_token().clone().cancelled_owned();
                    let service = self.clone();
                    self.keep(tokio::spawn(async move {
                        tokio::select! {
                            _ = started.wait_until_initialized() => {}
                            () = stopping => return,
                        }
                        drop(started);
                        service.publish_job(job_id).await;
                    }));
                }
                torrent.attached = Some(Attached {
                    session,
                    handle,
                    terms,
                    ssl,
                    leg,
                    names,
                    scope,
                });
                torrent.route = Some(plan.key);
                torrent.held = None;
                torrent.run_error = None;
                // A pause, resume or turn made while the session took it,
                // which found it in no session, reaches it too.
                let paused_now = self.held_back(
                    &self.settings(),
                    &self.rules(),
                    torrent,
                    self.inner.clock.unix_ms(),
                );
                let waits = !paused_now && torrent.start_waits_on_move();
                if let Some(attached) = &torrent.attached
                    && attached.handle.is_paused() != paused_now
                    && !waits
                {
                    let result = if paused_now {
                        attached.session.pause(&attached.handle).await
                    } else {
                        attached.session.unpause(&attached.handle).await
                    };
                    torrent.ran(result);
                }
            }
            None => {
                drop(live);
                if let Err(error) = session
                    .delete(TorrentIdOrHash::Id(handle.id()), false)
                    .await
                {
                    tracing::debug!(job_id = job_id.0, error = %error, "a removed torrent was already gone from its session");
                }
            }
        }
    }

    // The plan a torrent not yet kept would run on, and the pool member it
    // would be pinned to: placed among the torrents kept now.
    pub(super) async fn placed(
        &self,
        plan: &RoutePlan,
        pinned: Option<u32>,
    ) -> (RoutePlan, Option<u32>) {
        let load = load_of(self.inner.live.lock().await.values());
        plan.place(pinned, |key| load.get(key).copied().unwrap_or(0))
    }

    // The plan `job_id` runs on: on a pool route its member's, kept or
    // placed as `RoutePlan::place` says among the other torrents, its pin
    // written as it changes. It counts on that member at once, so the next
    // torrent placed sees it there.
    pub(super) async fn pinned(&self, job_id: JobId, plan: RoutePlan) -> RoutePlan {
        let mut live = self.inner.live.lock().await;
        let load = load_of(
            live.values()
                .filter(|torrent| torrent.record.job_id != job_id),
        );
        let Some(torrent) = live.get_mut(&job_id) else {
            return plan;
        };
        let (plan, pin) = plan.place(torrent.record.pool_member, |key| {
            load.get(key).copied().unwrap_or(0)
        });
        self.pin(torrent, pin);
        if pin.is_some() {
            torrent.route = Some(plan.key.clone());
        }
        plan
    }

    // `torrent` is pinned to pool member `pin` from here, or to none.
    pub(super) fn pin(&self, torrent: &mut Live, pin: Option<u32>) {
        if torrent.record.pool_member == pin {
            return;
        }
        torrent.record.pool_member = pin;
        if let Err(error) = self
            .inner
            .db
            .set_torrent_pool_member(torrent.record.job_id, pin)
        {
            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not record the pool member a torrent is pinned to");
        }
    }

    pub(super) async fn held(&self, job_id: JobId, key: Option<RouteKey>, held: Held) {
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.settle(self.inner.clock.unix_ms());
            torrent.route = key.or(torrent.route.take());
            torrent.held = Some(held);
        }
    }

    // Take a torrent out of its session, its checkpoint written first when
    // it is behind the job, and what changed in its ledger and peers taken
    // before the session lets it go. Its tee stays with it, and writes its
    // own checkpoint when that is due, as it does in a session.
    pub(super) async fn detach(&self, torrent: &mut Live) {
        self.leave(torrent, false).await;
    }

    // `detach` for a route move: the torrent goes on in another session, so
    // its trackers hear no `stopped`, and the session it joins goes on from
    // where its announces stand.
    pub(super) async fn move_out(&self, torrent: &mut Live) {
        self.leave(torrent, true).await;
    }

    async fn leave(&self, torrent: &mut Live, moving: bool) {
        let job_id = torrent.record.job_id;
        let now = self.inner.clock.unix_ms();
        if self.inner.signal.take_unsaved_one(job_id)
            && let Some(save) = self.queue_checkpoint(torrent, now)
        {
            self.checkpoint_landed(save).await;
        }
        torrent.settle(now);
        let landing = torrent
            .attached
            .is_some()
            .then(|| self.queue_resume(torrent, now, true))
            .flatten();
        if let Some(attached) = torrent.attached.take() {
            if moving {
                attached.handle.hand_over();
            }
            let id = TorrentIdOrHash::Id(attached.handle.id());
            if let Err(error) = attached.session.delete(id, false).await {
                tracing::debug!(error = %error, "a torrent was already gone from its session");
            }
        }
        let _ = self.landed(landing).await;
        torrent.engine = Engine::Absent;
        torrent.run_error = None;
        self.end_disk_room(torrent);
    }

    // A torrent held for its files missing looks at them again, off the
    // runtime, as its store did when it was made. Still not one there, it
    // stays held; otherwise the pieces its files lost are checked or
    // fetched again and it is let go. Whether it was let go, and so may
    // join its session now.
    pub(super) async fn look_again(&self, job_id: JobId) -> bool {
        let store = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .filter(|torrent| torrent.files_missing)
            .and_then(|torrent| torrent.store.clone());
        let Some(store) = store else {
            return false;
        };
        let looking = Arc::clone(&store);
        let Ok(unbacked) = tokio::task::spawn_blocking(move || looking.drop_unbacked()).await
        else {
            return false;
        };
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id).filter(|torrent| {
            torrent.files_missing
                && torrent
                    .store
                    .as_ref()
                    .is_some_and(|held| Arc::ptr_eq(held, &store))
        }) else {
            return false;
        };
        let Some(lost) = unbacked else {
            Note::Absent {
                pieces: store.count(),
            }
            .say(job_id);
            return false;
        };
        if lost > 0 {
            Note::Missing { pieces: lost }.say(job_id);
            self.inner.signal.marked(job_id, lost);
        }
        torrent.files_missing = false;
        torrent.hold(store.file_have());
        true
    }

    // Every torrent held for its files that its own intent would run looks
    // at them again, RESTORES_AT_ONCE at a time, as a start of every torrent
    // asks (qBittorrent `torrents/start` with `all` calls `TorrentImpl::start`
    // on each). One its own pause stops stays as it is, as the start leaves
    // it. Those let go join their sessions once every look is done, without
    // the start waiting on their routes, as a start of one does not.
    pub(super) async fn look_again_held(&self) {
        let held: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.files_missing && torrent.resumed())
            .map(|torrent| torrent.record.job_id)
            .collect();
        if held.is_empty() {
            return;
        }
        let mut looking = tokio::task::JoinSet::new();
        let mut found = Vec::new();
        for job_id in held {
            if looking.len() >= RESTORES_AT_ONCE
                && let Some(Ok(Some(job_id))) = looking.join_next().await
            {
                found.push(job_id);
            }
            let service = self.clone();
            looking.spawn(async move { service.look_again(job_id).await.then_some(job_id) });
        }
        while let Some(looked) = looking.join_next().await {
            if let Ok(Some(job_id)) = looked {
                found.push(job_id);
            }
        }
        if found.is_empty() {
            return;
        }
        self.apply_queue().await;
        for job_id in found {
            self.spawn_attach(job_id).await;
        }
        self.publish().await;
    }

    // Bring back every stored torrent, its record only. Nothing here waits
    // on a route or reads a torrent's files: a torrent's job is restored
    // and it goes into its route's session at the tick, as one whose route
    // changed does, and a magnet waits for its session on the task that
    // fetches its metainfo.
    pub(super) async fn restore(&self) -> Result<(), String> {
        self.load_rules()?;
        // Each table a restore and a first attach read, read once for every
        // torrent. A torrent kept with no tracker list would write its next
        // edit over the rows it has, and one kept with no ledger would write
        // zeros over its counts at its next save, so those reads, like the
        // records', fail the restore when they fail. A history that does
        // not read is said, and no torrent shows its failure. One
        // torrent's part that does not read is said, and that torrent goes
        // on without it: its pieces and seed-mode pieces are then read
        // again, each by the one it is for.
        let records = self.inner.db.restored_torrents().map_err(state_error)?;
        let mut trackers = self
            .inner
            .db
            .torrent_trackers_by_job()
            .map_err(state_error)?;
        let mut counters = self
            .inner
            .db
            .torrent_counters_by_job()
            .map_err(state_error)?;
        let mut failures = self
            .inner
            .db
            .failed_torrent_histories()
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "could not read the torrents' history");
                BTreeMap::new()
            });
        let now = self.inner.clock.unix_ms();
        if let Some(last) = records.iter().map(|row| row.record.queue_position).max() {
            self.inner
                .next_queue_position
                .fetch_max(last + 1, Ordering::Relaxed);
        }
        // Torrents kept before their held paths were written, each with
        // what it holds by name, written in one transaction after the
        // loop, from the metainfo the restore reads anyway.
        let mut unheld = Vec::new();
        for row in records {
            let record = row.record;
            let job_id = record.job_id;
            let (ledger, resume, kept) = match counters.remove(&job_id) {
                Some(stored) => (
                    stored.counters.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's ledger");
                        TorrentCounters::default()
                    }),
                    stored.peers.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's saved peers");
                        ResumePeers::default()
                    }),
                    stored.kept.map_err(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's pieces");
                    }).ok(),
                ),
                None => (
                    TorrentCounters::default(),
                    ResumePeers::default(),
                    Some(None),
                ),
            };
            // The rule is the one its trackers match now (libtorrent keeps
            // the trackers, not what was made of them).
            let mut torrent = Live::new(record.clone(), ledger, resume, now);
            torrent.trackers = in_table_order(trackers.remove(&job_id).unwrap_or_default());
            torrent.rule_id = self.rule_id(torrent.trackers.iter());
            match record.metainfo.as_deref().map(parse) {
                Some(Ok(parsed)) => {
                    if !row.held_set {
                        let tops = row.unheld_outputs.as_ref().and_then(|outputs| {
                            held_tops(
                                parsed
                                    .manifest
                                    .paths
                                    .iter()
                                    .chain(outputs)
                                    .map(String::as_str),
                            )
                        });
                        unheld.push((
                            job_id,
                            record.storage_dir.clone(),
                            record.files_dir.clone(),
                            tops,
                        ));
                    }
                    let levels = row.levels.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
                        Vec::new()
                    });
                    self.describe(&mut torrent, &parsed, levels);
                    torrent.kept(
                        failures.remove(&job_id),
                        kept.as_ref().and_then(Option::as_ref),
                    );
                    torrent.kept_at_boot = kept;
                    if torrent.record.seed_mode {
                        torrent.verified_at_boot = row
                            .seed_verified
                            .map_err(|error| {
                                tracing::warn!(job_id = job_id.0, error = %error, "could not read the pieces a torrent checked in seed mode");
                            })
                            .ok();
                    }
                    self.inner.live.lock().await.insert(job_id, torrent);
                }
                Some(Err(error)) => {
                    self.inner.live.lock().await.insert(job_id, torrent);
                    self.fail(job_id, error.to_string()).await;
                }
                None => {
                    self.inner.live.lock().await.insert(job_id, torrent);
                    self.resolve(job_id).await;
                }
            }
        }
        if !unheld.is_empty() {
            let db = self.inner.db.clone();
            let written = tokio::task::spawn_blocking(move || {
                let held: Vec<(JobId, Vec<String>)> = unheld
                    .into_iter()
                    .map(|(job_id, storage, files, tops)| {
                        (
                            job_id,
                            held_of(storage.as_deref(), files.as_deref(), tops.as_ref()),
                        )
                    })
                    .collect();
                db.set_torrents_held(&held)
            })
            .await;
            match written {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "could not write what the torrents hold; each holds its whole folder until its next start")
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not write what the torrents hold; each holds its whole folder until its next start")
                }
            }
        }
        // A folder made in another spelling before a torrent's first write
        // holds its files from then on: each start spells what the
        // torrents hold as the filesystem does.
        self.respell_held(None).await;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    // What the torrents hold, or only `job_id`, spelled as the filesystem
    // spells it now ([`held_key`]): a part the torrent named before it was
    // there, and that came to be in another case or through a link, is
    // held as it is on disk. One read, and one write only for the rows that
    // differ; none when none does. Off the runtime.
    pub(super) async fn respell_held(&self, job_id: Option<JobId>) {
        let db = self.inner.db.clone();
        let respelled = tokio::task::spawn_blocking(move || {
            let changed: Vec<(JobId, String, String)> = db
                .held_rows(job_id)?
                .into_iter()
                .filter_map(|(job_id, was)| {
                    let now = held_key(Path::new(&was)).to_string_lossy().into_owned();
                    (now != was).then_some((job_id, was, now))
                })
                .collect();
            db.respell_held(&changed)
        })
        .await;
        match respelled {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not spell what the torrents hold as it is on disk");
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not spell what the torrents hold as it is on disk");
            }
        }
    }
}

// The checkpoint the session starts from, so it does not check the files:
// the pieces the job has, and no block of any other. The storage keeps a
// piece's blocks in memory until the piece verifies, so no block of a piece
// not had is ever written to the job's files. None while the job vouches
// for nothing: the session checks every piece.
fn apply_initial_have(
    options: &mut AddTorrentOptions,
    have: Option<bitvec::boxed::BitBox<u8, bitvec::order::Msb0>>,
) {
    options.checkpoint = have.map(Checkpoint::pieces);
}

// A seed-mode torrent's pieces are had from where its files were placed,
// piece by piece on a blocking thread, while its session seeds them, and
// what its tee waits on goes to it from there: the store comes to have what
// the session took as had. The session leaving seed mode
// stops it before any check of every piece begins, and so does the torrent
// leaving its session. A piece is read from the files outside seed mode's
// hold, so a peer's request never waits on that read.
fn adopt_placed(
    handle: &Arc<ManagedTorrent>,
    store: &Arc<TorrentStore>,
    job_id: JobId,
    signal: Arc<Signal>,
    stopping: impl Future<Output = ()> + Send + 'static,
) {
    let handle = Arc::downgrade(handle);
    let store = Arc::downgrade(store);
    tokio::spawn(async move {
        let adopting = (handle.clone(), store.clone());
        let adopted = tokio::task::spawn_blocking(move || {
            let (handle, store) = adopting;
            let count = store.upgrade()?.layout().piece_count();
            for piece in 0..count {
                let (Some(handle), Some(store)) = (handle.upgrade(), store.upgrade()) else {
                    return None;
                };
                if !handle.seed_mode() {
                    return None;
                }
                match store.adopt(piece, |take| handle.while_seeding(take)) {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        tracing::warn!(piece, error = %format!("{error:#}"), "a seed-mode torrent's placed files could not be read");
                        return None;
                    }
                    None => return None,
                }
            }
            Some(count)
        })
        .await;
        let Ok(Some(count)) = adopted else {
            return;
        };
        // The pieces the tee waits on are checked now, as a peer's request
        // would check them, and go to it as they pass: the graph takes no
        // byte a piece check has not passed. One that fails ends seed mode,
        // and the check of every piece that follows gives the tee the
        // pieces it finds and the session fetches the rest. The session
        // takes the torrent through its first check on its own task, and a
        // piece is checked only once that has ended, the torrent running or
        // paused; one that ends in an error has its pieces checked as peers
        // ask. The wait holds the torrent only while its session runs: a
        // session that stops leaves a torrent in its first check as it is,
        // and this ends with it, the torrent going with its session. One
        // removed from its session ends its first check, and the wait.
        let checks = match (handle.upgrade(), store.upgrade()) {
            (Some(torrent), Some(placed)) if placed.tee().is_some() => {
                drop(placed);
                tokio::select! {
                    started = torrent.wait_until_initialized() => started.is_ok(),
                    () = stopping => return,
                }
            }
            _ => false,
        };
        if checks {
            let checking = tokio::task::spawn_blocking(move || {
                let (Some(handle), Some(store)) = (handle.upgrade(), store.upgrade()) else {
                    return;
                };
                for piece in 0..count {
                    if !store.tee_wants(piece) {
                        continue;
                    }
                    match handle.check_seeded(piece) {
                        Some(Ok(true)) => {
                            if let Err(error) = store.replay(piece) {
                                tracing::warn!(piece, error = %error, "a seed-mode torrent's checked piece could not be read");
                                break;
                            }
                        }
                        Some(Ok(false)) | None => break,
                        Some(Err(error)) => {
                            tracing::warn!(piece, error = %format!("{error:#}"), "a seed-mode torrent's piece could not be checked");
                            break;
                        }
                    }
                }
            });
            if checking.await.is_err() {
                return;
            }
        }
        // The files were whole as placed: the next settle completes or
        // unpacks the torrent, shows it seeding, and saves the pieces it
        // adopted, as qBittorrent saves a torrent's resume data once it is
        // added, rather than leaving every add's to one tick. Adds that come
        // together share one settle.
        signal.due(job_id);
    });
}

// Why a torrent has no store: one that cannot be made fails the torrent,
// and one whose checkpoint the database did not give waits to be tried
// again.
pub(super) enum Unmade<W> {
    Fails(String),
    Waits(W),
}

// What a store makes of a database read that failed, or of a checkpoint
// whose rebuild failed writing or stepping: a restore waits on it (`Ok`),
// as a usenet job's restore is held; a torrent given its metainfo now has
// nothing saved to wait for (`Err`, with `W` uninhabited), and goes on.
type Hold<W> = fn(String) -> Result<W, String>;

// What a torrent's store made of what it found: why its tee starts fresh
// though it had a checkpoint, and the pieces its files no longer gave.
// Said where the restore is awaited, so the line goes where the caller's
// lines go.
pub(super) enum Note {
    Unread(String),
    Unrestored(String),
    Dropped { pieces: u32, error: String },
    Missing { pieces: u32 },
    Absent { pieces: u32 },
}

impl Note {
    // Whether `notes` found none of the torrent's files with a piece had.
    fn absent(notes: &[Self]) -> bool {
        notes.iter().any(|note| matches!(note, Self::Absent { .. }))
    }

    fn say(&self, job_id: JobId) {
        match self {
            Self::Unread(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking checkpoint could not be read; it starts fresh");
            }
            Self::Unrestored(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking checkpoint did not restore; it starts fresh");
            }
            Self::Dropped { pieces, error } => {
                tracing::warn!(job_id = job_id.0, pieces, error = %error, "a torrent's pieces did not read from its files; they are fetched again");
            }
            Self::Missing { pieces } => {
                tracing::warn!(
                    job_id = job_id.0,
                    pieces,
                    "a torrent's pieces are not in its files; they are checked or fetched again"
                );
            }
            Self::Absent { pieces } => {
                tracing::warn!(
                    job_id = job_id.0,
                    pieces,
                    "none of a torrent's files is there; its pieces are kept and it is held until a start, a recheck or a move"
                );
            }
        }
    }
}

// What a torrent's store is made from: the directory its files are in,
// its layout, whether it unpacks into a tee, and the archive passwords the
// tee takes.
struct Making {
    files: PathBuf,
    layout: Arc<PieceLayout>,
    tee: bool,
    passwords: Vec<ArchivePasswordCandidate>,
    // In seed mode: its pieces had are unchecked, and go to the tee only
    // as their seed-mode check passes.
    seeded: bool,
    // Its side store, and the files set aside in it.
    side: PathBuf,
    ignored: Vec<bool>,
    // The pieces it kept as the boot read them; none to read its row.
    kept: Option<Option<Kept>>,
}
