// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_room::PlaceBatch;
use super::intake::{Fetched, Parsed, parse};
use super::live::Live;
use super::retire::{held_key, held_of, held_tops};
use super::settings::TorrentSettings;
use super::tick::Leveling;
use super::{
    Asked, Load, PeerTerms, RESTORES_AT_ONCE, Restores, Signal, TorrentService, certificate_of,
    fetch_levels, files_dir, held_by, identity_of, ignored_of, in_table_order, locked, placed,
    queued, ratelimits_of, ssl_identity, state_error, tracker_tiers,
};
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::settings::archive_passwords::SettingsArchivePasswordsInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::persistence::TorrentsPersistenceLiveHeldPathsInternalQueries;
use crate::torrents::queries::TorrentsInternalQueries;
use crate::torrents::queries::TorrentsQueries;
use tokio::sync::watch;

use crate::LoadError;
use crate::jobs::JobId;
use crate::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};
use crate::pipeline::graph::job::Storage;
use crate::pipeline::graph::limits;
use crate::pipeline::graph::limits::Caps;
use crate::proxies::{
    Consumer, LegTenancy, LegTenant, NetworkRuntime, SessionTraffic, TorrentScope,
};
use crate::settings::model::{TrackerCertificate, TrackerRule};
use crate::torrents::files::Files;
use crate::torrents::intake::{Holds, Intake, Room};
use crate::torrents::layout::PieceLayout;
use crate::torrents::queries::RestoredTorrent;
use crate::torrents::sessions::{OPENING, RouteKey, RoutePlan};
use crate::torrents::status::{Engine, Held};
use crate::torrents::storage::{PieceStorageFactory, Pieces};
use crate::torrents::store::{Kept, Replayed, TorrentStore};
use crate::torrents::tee::{Parts, Plan, Reading, Tee};
use crate::torrents::{FilePriority, KeptIntent, TorrentCounters, TorrentIntent, TrackerEntry};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use weaver_server_base::runtime::panic_trap::JobScope;
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
pub fn torrent_archive_passwords(
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
pub struct Attached {
    pub session: Arc<Session>,
    pub handle: Arc<ManagedTorrent>,
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
    have: (Option<Checkpoint>, bool, Arc<TorrentStore>),
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
    // Everything but the tick, every stored torrent read before it ends.
    pub async fn open(&self) -> Result<(), String> {
        let through = self.begin().await?;
        self.load(through).await?;
        self.after_load().await;
        Ok(())
    }

    // What a start does before it reads a torrent: the tasks that follow
    // the jobs and the settings, the tracker rules, and the last job a
    // torrent row has, by the table's key, which is as far as the load
    // reads. From here the stored torrents are being read.
    pub(super) async fn begin(&self) -> Result<Option<JobId>, String> {
        if let Some(task) = self.inner.env.hear_alone() {
            *locked(&self.inner.hearing) = Some(task);
        }
        self.spawn_settle();
        self.spawn_follow_disk_room();
        self.spawn_follow_trackers();
        self.spawn_follow_tags();
        self.spawn_follow_egress_limits();
        self.spawn_follow_interfaces();
        self.load_rules().await?;
        let through = self
            .db(|db| db.last_torrent_job())
            .await
            .map_err(state_error)?;
        self.inner.snapshot.unready();
        self.inner.load.send_replace(Load::Running(through));
        Ok(through)
    }

    // The stored torrents read, with no task of a start begun.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn restore(&self) -> Result<(), String> {
        self.load_rules().await?;
        let through = self
            .db(|db| db.last_torrent_job())
            .await
            .map_err(state_error)?;
        self.inner.snapshot.unready();
        self.inner.load.send_replace(Load::Running(through));
        self.load(through).await
    }

    // What a start does once every stored torrent is read.
    pub(super) async fn after_load(&self) {
        // Torrents turned on while a wind-down's stops were kept, with no
        // pass since to resume them, resume now.
        if self.settings().torrent.enabled {
            self.resume_wound_down().await;
        }
        self.retry_retiring().await;
    }

    // The archive passwords a graph built for `torrent` now takes. One
    // already extracting keeps the list it started with.
    pub fn archive_passwords(
        &self,
        torrent: &Live,
        global: &[ArchivePasswordCandidate],
    ) -> Vec<ArchivePasswordCandidate> {
        torrent_archive_passwords(torrent.rule(&self.rules()), global)
    }

    // The archive passwords every torrent may try, as the settings give
    // them now.
    pub async fn global_passwords(&self) -> Arc<[ArchivePasswordCandidate]> {
        self.db(|db| Ok(db.global_archive_password_candidates()))
            .await
            .map_err(|error| error.to_string())
            .and_then(|read| read.map_err(|error| error.to_string()))
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
            stamps: boot_stamps,
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
        // Each file's time as taken with the pieces: as the boot read them
        // with the pieces, or read here as they were, only for a torrent
        // that kept some, under the same policy. One the database fails to
        // give leaves each file judged by its size.
        let stamps = if let Some(boot) = boot_stamps {
            boot.into_iter().collect()
        } else if kept.is_some() {
            match self
                .inner
                .db
                .torrent_file_stamps(job_id)
                .map_err(|error| hold(error.to_string()))
            {
                Ok(stamps) => stamps,
                Err(Ok(wait)) => return Err(Unmade::Waits(wait)),
                Err(Err(error)) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file times; its files are judged by their sizes");
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        let writes = self.inner.env.disk.writes(job_id.0, &files);
        let files = Files::new(&layout, files).map_err(|error| Unmade::Fails(error.to_string()))?;
        let mut notes = Vec::new();
        // Pieces too large for the resident budget are written block by
        // block and read back to the tee from disk: chosen here as the
        // storage chooses (`storage::writes_blocks`), at every restore.
        let blocks =
            crate::torrents::storage::writes_blocks(layout.piece_length(), &self.resident(job_id));
        let tee = match tee
            .then(|| Plan::of(&layout).map(|plan| plan.reading_back(blocks)))
            .flatten()
        {
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
        store.set_stamps(stamps);
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
    pub fn tee<W>(
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
            reading: plan.reads_back().then(|| {
                Reading::new(
                    Arc::new(
                        self.storage(work.clone(), layout, &plan, Vec::new())
                            .scratch(job_id.0),
                    ),
                    self.resident(job_id),
                )
            }),
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
            tee: torrent.unpacking() && !torrent.complete(),
            passwords: self.archive_passwords(torrent, global),
            seeded: torrent.record.seed_mode,
            side: self.side_store(torrent.record.job_id),
            ignored: ignored_of(&torrent.levels),
            kept: None,
            stamps: None,
        })
    }

    // The store joined to what feeds it: the intake the session hands its
    // pieces to.
    fn give_store(&self, torrent: &mut Live, store: TorrentStore) {
        self.give_store_in(torrent, store, None);
    }

    // `give_store`, the look for its disk one of `batch`'s when it is one.
    fn give_store_in(
        &self,
        torrent: &mut Live,
        store: TorrentStore,
        batch: Option<&Arc<PlaceBatch>>,
    ) {
        let job_id = torrent.record.job_id;
        let env = &self.inner.env;
        let store = store.shared();
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
            self.look_for_place_in(torrent.record.job_id, &intake, dir, batch);
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
    pub fn resident(&self, job_id: JobId) -> Holds {
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
    // `Ignored`, kept as the operator's own would be. True when the store
    // already holds every file the torrent wants, as one whose every file is
    // left out does: no piece is coming to ask for it to be judged, so the
    // caller has the next settle look at it once the torrent is where a
    // settle finds it (libtorrent `piece_picker::is_finished` with every
    // piece filtered).
    pub(super) async fn give_metainfo(
        &self,
        torrent: &mut Live,
        parsed: &Parsed,
        left_out: &[usize],
    ) -> Result<bool, String> {
        let job_id = torrent.record.job_id;
        let mut stored = self
            .db(move |db| db.torrent_file_priorities(job_id))
            .await
            .unwrap_or_else(|error| {
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
            let levels = stored.clone();
            self.db(move |db| db.set_torrent_file_priorities(job_id, &levels))
                .await
                .map_err(state_error)?;
        }
        if let Some(unpack) = self.describe(torrent, parsed, stored)
            && let Err(error) = self
                .db(move |db| db.set_torrent_unpack_in_force(job_id, unpack))
                .await
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not record whether a torrent unpacks");
        }
        let global = self.global_passwords().await;
        let Some(made) = self.making(torrent, &global) else {
            return Ok(false);
        };
        // Off the runtime, as a restore makes it: it reads the database and
        // the files. One that panics fails the torrent.
        let making = {
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.store::<std::convert::Infallible>(job_id, made, Err)
                }))
                .unwrap_or_else(|_| {
                    Err(Unmade::Fails(
                        "making the torrent's store panicked".to_string(),
                    ))
                })
            })
        };
        let (store, notes) =
            making
                .await
                .map_err(|error| error.to_string())?
                .map_err(|unmade| match unmade {
                    Unmade::Fails(error) => error,
                    Unmade::Waits(never) => match never {},
                })?;
        for note in &notes {
            note.say(job_id);
        }
        torrent.files_missing = Note::absent(&notes);
        let wanted = |file: usize| {
            torrent
                .levels
                .get(file)
                .copied()
                .unwrap_or_default()
                .wanted()
        };
        let whole = store.has_files(wanted);
        self.give_store(torrent, store);
        Ok(whole)
    }

    // What the metainfo says of the torrent, without its job. Whether it
    // unpacks is fixed here, so a restart builds the same tree: given back
    // when it was not fixed before, for the caller to keep, one torrent's
    // alone or a page's in one write.
    fn describe(
        &self,
        torrent: &mut Live,
        parsed: &Parsed,
        stored: Vec<FilePriority>,
    ) -> Option<bool> {
        let fixed = torrent.record.unpack_in_force.is_none().then(|| {
            let unpack = self
                .settings()
                .unpack(&torrent.record, torrent.rule(&self.rules()));
            torrent.record.unpack_in_force = Some(unpack);
            unpack
        });
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
        fixed
    }

    // `restore_jobs` as a pass of its own, begun off the caller's path: the
    // tick asks for it and goes on at once, so start-up, the tick and every
    // request answer while the jobs come back. One pass runs at a time; one
    // asked while a pass runs follows it, for a torrent a held restore or a
    // resume left waiting meanwhile.
    pub(super) fn start_restores(&self) {
        // Nothing came to wait for its job since the last pass was asked
        // for: no pass, and no task.
        if !self.inner.restore_wanted.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut begin = false;
        self.inner.restores.send_if_modified(|run| match run {
            Restores::Idle => {
                *run = Restores::Running;
                begin = true;
                true
            }
            Restores::Running => {
                *run = Restores::Again;
                true
            }
            Restores::Again => false,
        });
        if !begin {
            return;
        }
        // Ended with the service part way, the pass is none again.
        struct Ends<'a> {
            restores: &'a watch::Sender<Restores>,
            armed: bool,
        }
        impl Drop for Ends<'_> {
            fn drop(&mut self) {
                if self.armed {
                    self.restores.send_replace(Restores::Idle);
                }
            }
        }
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            let mut ends = Ends {
                restores: &service.inner.restores,
                armed: true,
            };
            loop {
                service.restore_jobs().await;
                let mut again = false;
                service.inner.restores.send_modify(|run| {
                    again = *run == Restores::Again;
                    *run = if again {
                        Restores::Running
                    } else {
                        Restores::Idle
                    };
                });
                if !again {
                    ends.armed = false;
                    return;
                }
            }
        }));
    }

    // Once no restore pass runs: what a test waits on after a tick to read
    // the jobs that tick restored.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn restored(&self) {
        self.inner
            .restores
            .subscribe()
            .wait_for(|run| *run == Restores::Idle)
            .await
            .expect("the service keeps its restore pass's sender");
    }

    // The jobs of the torrents brought back at start with their records
    // only, each restored from its checkpoint, RESTORES_AT_ONCE at a time,
    // in the order of their job ids. A torrent reads as it was kept, its
    // restore shown, until its job is back.
    pub async fn restore_jobs(&self) {
        let waiting: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.awaits_restore())
            .map(|torrent| torrent.record.job_id)
            .collect();
        // RESTORES_AT_ONCE in flight, each torrent attached as soon as its
        // job is back rather than at the next tick, so the first seeds
        // without waiting for the last to restore (qBittorrent's
        // startUpTorrents keeps MAX_PROCESSING_RESUMEDATA_COUNT in flight
        // into libtorrent's async_add_torrent).
        // The global archive passwords, read once for all of them and
        // outside the live lock.
        if waiting.is_empty() {
            return;
        }
        let global = Some(self.global_passwords().await);
        // The looks for their disks ask one pass between them.
        let looks = PlaceBatch::new(self);
        let mut restoring = tokio::task::JoinSet::new();
        for job_id in waiting {
            if restoring.len() >= RESTORES_AT_ONCE {
                restoring.join_next().await;
            }
            let service = self.clone();
            let global = global.clone();
            let looks = Arc::clone(&looks);
            restoring.spawn(async move {
                if service.restore_job_with(job_id, global, Some(&looks)).await {
                    service.attach(job_id, None).await;
                }
            });
        }
        while restoring.join_next().await.is_some() {}
        drop(looks);
        // Those back read as their sessions have them now, not at the next
        // tick, as the tick reads them after a pass it waited on.
        self.read_sessions().await;
        self.publish().await;
    }

    // The job of a torrent that waits for one, restored on a blocking thread
    // under no lock but its own job's and a restore permit, and given
    // to the torrent if it still waits: one removed meanwhile drops it. A
    // restore that panics is a checkpoint that does not restore. Whether it
    // gave the torrent its job, unheld, so it may attach now.
    pub async fn restore_job(&self, job_id: JobId) -> bool {
        self.restore_job_with(job_id, None, None).await
    }

    // [`Self::restore_job`] with the global archive passwords read for it,
    // or none to read them when its store is made.
    async fn restore_job_with(
        &self,
        job_id: JobId,
        global: Option<Arc<[ArchivePasswordCandidate]>>,
        looks: Option<&Arc<PlaceBatch>>,
    ) -> bool {
        self.ensure(&[job_id]).await;
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
        let waiting = 'waiting: {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live
                .get_mut(&job_id)
                .filter(|torrent| torrent.store.is_none())
            else {
                break 'waiting None;
            };
            let global = match global {
                Some(global) => global,
                None => self.global_passwords().await,
            };
            let Some(made) = self.making(torrent, &global) else {
                break 'waiting None;
            };
            Some(Making {
                kept: torrent.kept_at_boot.take(),
                stamps: torrent.stamps_at_boot.take(),
                ..made
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
                    // It still waits for its job: the next tick tries again.
                    self.inner.restore_wanted.store(true, Ordering::Release);
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
                // One restored short of complete that holds every file it
                // wants is judged at the next settle, as an add is.
                let levels = &torrent.levels;
                let whole = !torrent.complete()
                    && store
                        .has_files(|file| levels.get(file).copied().unwrap_or_default().wanted());
                self.give_store_in(torrent, store, looks);
                if whole {
                    self.inner.signal.look(job_id);
                }
                let copy = self.catch_up_levels(torrent).await;
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
    async fn catch_up_levels(
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
                reopened: None,
            });
            return Some((Arc::clone(store), asked));
        }
        // Nothing to copy: only which files are set aside changes, in
        // memory. It waits on the torrent's reads and writes under way, so
        // it runs off the runtime.
        if let Err(error) = super::tick::relevel_off(Arc::clone(store), ignored).await {
            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not set a torrent's files aside");
        }
        None
    }

    // Keep a torrent that crashed the daemon paused, its restore held for
    // why, until a resume takes its marker. Its marker is read under the
    // torrent's lock, which a resume takes after the marker, so a resume
    // is never undone. Whether the torrent waits for a resume.
    pub async fn park(&self, job_id: JobId) -> bool {
        let mut live = self.inner.live.lock().await;
        let Some(why) = self.crashed(job_id) else {
            return false;
        };
        let Some(torrent) = live.get_mut(&job_id) else {
            return true;
        };
        if torrent.record.intent() == TorrentIntent::Resumed {
            if let Err(error) = self
                .db(move |db| db.update_torrent_intent(job_id, TorrentIntent::Paused))
                .await
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
    pub async fn attach(
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
            torrent
                .route
                .set(Some(plan.key.clone()), &self.inner.route_loads);
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
        rules: &super::live::Rules,
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
                    store.initial_checkpoint(placed(&torrent.record)),
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
        // One join of a torrent at a time holds its gate, so a torrent
        // another join put in a session while this one waited is left
        // there: nothing of it reaches a second session, on this route or
        // another, and no tracker hears it start twice.
        if self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .is_none_or(|torrent| torrent.attached.is_some())
        {
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
            let verified = match verified {
                Some(verified) => Ok(verified),
                None => self.db(move |db| db.torrent_seed_verified(job_id)).await,
            };
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
                let had: std::collections::HashSet<&IpAddr> = saved.banned.iter().collect();
                let since: Vec<IpAddr> = torrent
                    .resume
                    .banned
                    .iter()
                    .filter(|ip| !had.contains(ip))
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
                // A recheck made while it was in no session is judged once
                // this session's check of its files ends.
                if std::mem::take(&mut torrent.check_owed) {
                    self.rejudge_after_check(job_id, &session, &handle);
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
                torrent.route.set(Some(plan.key), &self.inner.route_loads);
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
        let _live = self.inner.live.lock().await;
        plan.place(pinned, |key| self.inner.route_loads.of(key))
    }

    // The plan `job_id` runs on: on a pool route its member's, kept or
    // placed as `RoutePlan::place` says among the other torrents, its pin
    // written as it changes. It counts on that member at once, so the next
    // torrent placed sees it there.
    pub(super) async fn pinned(&self, job_id: JobId, plan: RoutePlan) -> RoutePlan {
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return plan;
        };
        // Counted among the others: not on the route it reads as now.
        let own = torrent.route.key();
        let (plan, pin) = plan.place(torrent.record.pool_member, |key| {
            self.inner
                .route_loads
                .of(key)
                .saturating_sub(usize::from(own == Some(key)))
        });
        self.pin(torrent, pin).await;
        if pin.is_some() {
            torrent
                .route
                .set(Some(plan.key.clone()), &self.inner.route_loads);
        }
        plan
    }

    // `torrent` is pinned to pool member `pin` from here, or to none.
    pub(super) async fn pin(&self, torrent: &mut Live, pin: Option<u32>) {
        if torrent.record.pool_member == pin {
            return;
        }
        torrent.record.pool_member = pin;
        let job_id = torrent.record.job_id;
        if let Err(error) = self
            .db(move |db| db.set_torrent_pool_member(job_id, pin))
            .await
        {
            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not record the pool member a torrent is pinned to");
        }
    }

    pub(super) async fn held(&self, job_id: JobId, key: Option<RouteKey>, held: Held) {
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.settle(self.inner.clock.unix_ms());
            if key.is_some() {
                torrent.route.set(key, &self.inner.route_loads);
            }
            torrent.held = Some(held);
        }
    }

    // Take a torrent out of its session, its checkpoint written first when
    // it is behind the job, and what changed in its ledger and peers taken
    // before the session lets it go. Its tee stays with it, and writes its
    // own checkpoint when that is due, as it does in a session.
    pub async fn detach(&self, torrent: &mut Live) {
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

    // Bring back every stored torrent, its record only, a page of them at a
    // time in the order of their job ids, as far as `through`. Each page is
    // read, opened and parsed off the runtime, then joins the live
    // torrents; no more than a page's rows are held while it is. Nothing
    // here waits on a route or reads a torrent's files: a torrent's job is
    // restored and it goes into its route's session at the tick, as one
    // whose route changed does, and a magnet waits for its session on the
    // task that fetches its metainfo. What was asked of the queue's order
    // meanwhile is done, the queue applied and the torrents published
    // once, when the last page is in. A page the database fails to give
    // leaves the load where it stands, with why: the torrents read so far
    // stay, and the tick reads that page again ([`Self::resume_load`]).
    pub(super) async fn load(&self, through: Option<JobId>) -> Result<(), String> {
        match self.load_pages(through).await {
            Ok(()) => {
                self.end_load().await;
                Ok(())
            }
            Err(error) => {
                // Said once for each reason, not once for each try.
                let news = {
                    let mut marks = locked(&self.inner.load_marks);
                    let news = marks.fault.as_deref() != Some(error.as_str());
                    marks.fault = Some(error.clone());
                    news
                };
                if news {
                    tracing::error!(%error, "the stored torrents did not all load; those read so far run, and the rest are read again");
                }
                Err(error)
            }
        }
    }

    // A load whose page did not read reads it again, off the tick that
    // asks: one read at a time, from the page that failed. The tick's
    // period is its only timer, and a read that fails again writes
    // nothing and says nothing new.
    pub(super) fn resume_load(&self) {
        if let Some(resumed) = self.resumed_load() {
            self.keep(tokio::spawn(resumed));
        }
    }

    // The read `resume_load` begins, when there is one to begin.
    pub fn resumed_load(&self) -> Option<impl Future<Output = ()> + Send + 'static> {
        let through = {
            let mut marks = locked(&self.inner.load_marks);
            if marks.fault.is_none() || marks.resuming {
                return None;
            }
            let Load::Running(through) = *self.inner.load.borrow() else {
                return None;
            };
            marks.resuming = true;
            through
        };
        let service = self.clone();
        Some(async move {
            let loaded = service.load(through).await;
            locked(&service.inner.load_marks).resuming = false;
            if loaded.is_ok() {
                tracing::info!("the stored torrents are all loaded");
                service.after_load().await;
            }
        })
    }

    // The load's end: what was asked of the queue's order while it ran is
    // done, in the order asked, and the load ends under the lock those are
    // asked under, so none is asked for and never done.
    async fn end_load(&self) {
        loop {
            let asked = std::mem::take(&mut locked(&self.inner.load_marks).asked);
            self.order_as_asked(asked).await;
            let ended = {
                let mut marks = locked(&self.inner.load_marks);
                let ended = marks.asked.is_empty();
                if ended {
                    marks.fault = None;
                    self.inner.load.send_replace(Load::Done);
                    // A read still in flight may hold a torrent taken out
                    // since: it lets go of them when it ends.
                    if marks.readers == 0 {
                        marks.gone.clear();
                    }
                }
                ended
            };
            if ended {
                break;
            }
        }
        self.apply_queue().await;
        self.publish().await;
    }

    // What was asked of the queue's order under the load, done now that
    // every place is known: a torrent added goes past every stored one,
    // and a move is made among all of them, each in the order asked. A
    // torrent taken out since is left out of its move.
    async fn order_as_asked(&self, asked: Vec<Asked>) {
        let mut added = Vec::new();
        for ask in asked {
            match ask {
                Asked::Added(job_id) => added.push(job_id),
                Asked::Reorder(mut jobs, target) => {
                    self.place_last(std::mem::take(&mut added)).await;
                    {
                        let live = self.inner.live.lock().await;
                        jobs.retain(|job_id| live.contains_key(job_id));
                    }
                    if let Err(error) = self.reorder_now(&jobs, target).await {
                        tracing::warn!(error = %error, "a queue move asked for while the stored torrents loaded was not made");
                    }
                }
                Asked::Moves(mut moves) => {
                    self.place_last(std::mem::take(&mut added)).await;
                    {
                        let live = self.inner.live.lock().await;
                        moves.retain(|(job_id, _)| live.contains_key(job_id));
                    }
                    for (job_id, moved) in self.move_now(&moves).await.0 {
                        if let Err(error) = moved {
                            tracing::warn!(job_id = job_id.0, error = %error, "a queue move asked for while the stored torrents loaded was not made");
                        }
                    }
                }
            }
        }
        self.place_last(added).await;
    }

    async fn load_pages(&self, through: Option<JobId>) -> Result<(), String> {
        let Some(through) = through else {
            return Ok(());
        };
        loop {
            #[cfg(any(test, feature = "test-support"))]
            {
                let mut held = self.inner.load_seam.held.subscribe();
                let _ = held.wait_for(|held| !held).await;
                let fails = self.inner.load_seam.fail_in.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |left| left.checked_sub(1),
                );
                if fails == Ok(1) {
                    return Err("a test failed the read of this page".into());
                }
            }
            let after = locked(&self.inner.load_marks).after;
            if after.is_some_and(|after| after >= through) {
                return Ok(());
            }
            let rows = self.page_rows();
            let db = self.inner.db.clone();
            let sized =
                tokio::task::spawn_blocking(move || db.torrent_page_sizes(after, through, rows))
                    .await
                    .map_err(|error| format!("the read of the stored torrents ended: {error}"))?
                    .map_err(state_error)?;
            let Some(&(last, _)) = sized.last() else {
                return Ok(());
            };
            self.bring(sized).await?;
            locked(&self.inner.load_marks).after = Some(last);
        }
    }

    // The rows of a page of the load.
    fn page_rows(&self) -> usize {
        #[cfg(any(test, feature = "test-support"))]
        {
            let rows = self.inner.load_seam.page_rows.load(Ordering::Relaxed);
            if rows > 0 {
                return rows;
            }
        }
        LOAD_PAGE
    }

    // The torrents of `jobs` the load has not reached, read now, each by
    // its key: an action or a reading that names a torrent finds it,
    // however far the load is. Nothing is read once the load has ended, or
    // for a torrent here already, and one the load or another asker is
    // reading now is waited for, that one alone, not read again.
    //
    // Every reading of a torrent comes through here, and what a torrent
    // read here goes on to do reads torrents in turn, so the future is
    // named: its type would otherwise hold itself.
    pub(super) fn ensure<'a>(
        &'a self,
        jobs: &'a [JobId],
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        if !self.loading() {
            return Box::pin(std::future::ready(()));
        }
        // Counted as in flight under the lock the load ends under: a
        // torrent taken out while this read runs is kept as gone until it
        // ends, though the load ends first.
        let Some((reader, through)) = self.reader() else {
            return Box::pin(std::future::ready(()));
        };
        Box::pin(async move {
            let _reader = reader;
            let missing: Vec<JobId> = {
                let live = self.inner.live.lock().await;
                jobs.iter()
                    .filter(|job_id| **job_id <= through && !live.contains_key(job_id))
                    .copied()
                    .collect()
            };
            if missing.is_empty() {
                return;
            }
            let db = self.inner.db.clone();
            let sized = match tokio::task::spawn_blocking(move || db.torrent_sizes_of(&missing))
                .await
            {
                Ok(Ok(sized)) => sized,
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "could not read a torrent the load has not reached");
                    return;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not read a torrent the load has not reached");
                    return;
                }
            };
            if let Err(error) = self.bring(sized).await {
                tracing::warn!(error = %error, "could not read a torrent the load has not reached");
            }
        })
    }

    // A read of torrents the load has not reached, begun: counted under
    // the lock the load ends under, with how far the load reads. None once
    // the load has ended.
    pub fn reader(&self) -> Option<(Reader, JobId)> {
        let mut marks = locked(&self.inner.load_marks);
        let Load::Running(Some(through)) = *self.inner.load.borrow() else {
            return None;
        };
        marks.readers += 1;
        Some((
            Reader {
                service: self.clone(),
            },
            through,
        ))
    }

    // The stored torrents of `sized`, each with the bytes its metainfo
    // takes, brought to join the live ones: a page of them at a time, each
    // torrent read by one asker however many ask. One another asker is
    // reading is waited for, that torrent's read alone, and read here only
    // if that read did not bring it.
    async fn bring(&self, mut wanted: Vec<(JobId, u64)>) -> Result<(), String> {
        loop {
            {
                let live = self.inner.live.lock().await;
                wanted.retain(|(job_id, _)| !live.contains_key(job_id));
            }
            if wanted.is_empty() {
                return Ok(());
            }
            let (claim, theirs) = self.claim(&mut wanted);
            for page in pages(&wanted, self.page_rows()) {
                let page = self.read_page(page).await?;
                self.take_page(page).await;
            }
            drop(claim);
            if theirs.is_empty() {
                return Ok(());
            }
            wanted.clear();
            for (job_id, bytes, mut ended) in theirs {
                // Ended too when its reader is gone.
                let _ = ended.wait_for(|ended| *ended).await;
                wanted.push((job_id, bytes));
            }
        }
    }

    // Of `wanted`, the torrents no asker is reading are left in it and
    // claimed, this asker's to read until the claim is dropped; the others
    // are given with what tells that their read has ended.
    pub fn claim(
        &self,
        wanted: &mut Vec<(JobId, u64)>,
    ) -> (Claim, Vec<(JobId, u64, watch::Receiver<bool>)>) {
        let (ended, ends) = watch::channel(false);
        let mut theirs = Vec::new();
        let mut marks = locked(&self.inner.load_marks);
        wanted.retain(|&(job_id, bytes)| match marks.reading.get(&job_id) {
            Some(reading) => {
                theirs.push((job_id, bytes, reading.clone()));
                false
            }
            None => {
                marks.reading.insert(job_id, ends.clone());
                true
            }
        });
        let claim = Claim {
            service: self.clone(),
            jobs: wanted.iter().map(|(job_id, _)| *job_id).collect(),
            ended,
        };
        (claim, theirs)
    }

    // The stored torrents of `jobs`, each as it joins the live ones: read,
    // opened and parsed on a blocking thread. A torrent here already is
    // read, never parsed.
    //
    // A torrent kept with no tracker list would write its next edit over
    // the rows it has, and one kept with no ledger would write zeros over
    // its counts at its next save, so those rows, like the records', fail
    // the page when they fail. One torrent's part that does not read is
    // said, and that torrent goes on without it: its pieces and seed-mode
    // pieces are then read again, each by the one it is for.
    pub async fn read_page(&self, jobs: Vec<JobId>) -> Result<Page, String> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let rows = service
                .inner
                .db
                .restored_torrents(&jobs)
                .map_err(state_error)?;
            let here: Vec<bool> = {
                let live = service.inner.live.blocking_lock();
                rows.iter()
                    .map(|row| live.contains_key(&row.record.job_id))
                    .collect()
            };
            let now = service.inner.clock.unix_ms();
            let mut page = Page {
                read: Vec::with_capacity(rows.len()),
            };
            for (row, here) in rows.into_iter().zip(here) {
                if !here {
                    page.read.push(service.read_torrent(row, now));
                }
            }
            Ok(page)
        })
        .await
        .map_err(|error| format!("the read of the stored torrents ended: {error}"))?
    }

    // One stored torrent as it joins the live ones, and what follows.
    fn read_torrent(&self, row: RestoredTorrent, now: i64) -> Read {
        let record = row.record;
        let job_id = record.job_id;
        let (ledger, resume, kept, stamps) = match row.counters {
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
                stored.stamps,
            ),
            None => (
                TorrentCounters::default(),
                ResumePeers::default(),
                Some(None),
                Vec::new(),
            ),
        };
        // The rule is the one its trackers match now (libtorrent keeps
        // the trackers, not what was made of them).
        let mut torrent = Live::new(record, ledger, resume, now);
        let (listed, unopened) = match row.trackers {
            Ok(listed) => (listed, None),
            Err(error) => (Vec::new(), Some(format!("a tracker URL: {error}"))),
        };
        torrent.trackers = in_table_order(listed);
        torrent.rule_id = self.rule_id(torrent.trackers.iter());
        // A torrent whose stored secrets do not decrypt with the
        // instance key is held with why, as it was kept: never restored
        // from what it lacks, nor fetched again, nor failed.
        if let Some(why) = row
            .sealed
            .map(|error| format!("its metainfo: {error}"))
            .or(unopened)
        {
            let why = format!("its stored secrets do not decrypt: {why}");
            tracing::warn!(job_id = job_id.0, error = %why, "a torrent is held");
            torrent.restore_held = Some(why.clone());
            torrent.sealed = Some(why);
            return Read {
                torrent,
                then: Then::Stands,
                unheld: None,
                unpack: None,
            };
        }
        let parsed = torrent.record.metainfo.as_deref().map(parse);
        match parsed {
            Some(Ok(parsed)) => {
                // Kept before its held paths were written: what it holds
                // by name, from the metainfo the load reads anyway.
                let unheld = (!row.held_set).then(|| {
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
                    held_of(
                        torrent.record.storage_dir.as_deref(),
                        torrent.record.files_dir.as_deref(),
                        tops.as_ref(),
                    )
                });
                let levels = row.levels.unwrap_or_else(|error| {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
                    Vec::new()
                });
                let unpack = self.describe(&mut torrent, &parsed, levels);
                torrent.kept(kept.as_ref().and_then(Option::as_ref));
                torrent.kept_at_boot = kept;
                torrent.stamps_at_boot = Some(stamps);
                if torrent.record.seed_mode {
                    torrent.verified_at_boot = row
                        .seed_verified
                        .map_err(|error| {
                            tracing::warn!(job_id = job_id.0, error = %error, "could not read the pieces a torrent checked in seed mode");
                        })
                        .ok();
                }
                Read {
                    torrent,
                    then: Then::Stands,
                    unheld,
                    unpack,
                }
            }
            Some(Err(error)) => Read {
                torrent,
                then: Then::Fails(error.to_string()),
                unheld: None,
                unpack: None,
            },
            None => Read {
                torrent,
                then: Then::Resolves,
                unheld: None,
                unpack: None,
            },
        }
    }

    // A page read joins the live torrents: each torrent not here by now,
    // and not taken out since the page was read. What follows for each is
    // done after, under no lock; what the torrents hold and whether each
    // unpacks, where neither was kept before, are written for the page in
    // one go, and what they hold is spelled as the filesystem spells it,
    // off the runtime.
    pub async fn take_page(&self, page: Page) {
        let mut then = Vec::new();
        let mut unheld = Vec::new();
        let mut unpacks = Vec::new();
        let mut taken = Vec::new();
        let mut reopened = Vec::new();
        let mut late = Vec::new();
        let mut waits = false;
        {
            let mut live = self.inner.live.lock().await;
            let mut marks = locked(&self.inner.load_marks);
            for mut read in page.read {
                let job_id = read.torrent.record.job_id;
                if marks.gone.contains(&job_id) || live.contains_key(&job_id) {
                    continue;
                }
                // Stored complete with levels that want a file the pieces it
                // kept lack, as a change of levels stored while its copy was
                // out leaves it when its reopen was not: short of complete
                // again, past every stored torrent at the load's end, as a
                // change of levels would have left it, its unpacking over.
                // The same rule as a change of levels made now
                // (`Live::reopens`, `save_levels`).
                if read.torrent.reopens(&read.torrent.levels) {
                    read.torrent.record.completed_at_ms = None;
                    read.torrent.record.unpack_over = true;
                    // A read that outlasted the load places it now, every
                    // stored place being known.
                    if self.loading() {
                        marks.asked.push(Asked::Added(job_id));
                    } else {
                        late.push(job_id);
                    }
                    reopened.push(job_id);
                }
                self.inner
                    .next_queue_position
                    .fetch_max(read.torrent.record.queue_position + 1, Ordering::Relaxed);
                self.inner.presence.send_if_modified(|presence| {
                    let was = *presence;
                    presence.note(&read.torrent);
                    *presence != was
                });
                waits |= read.torrent.awaits_restore();
                live.insert(job_id, read.torrent);
                taken.push(job_id);
                if let Some(held) = read.unheld {
                    unheld.push((job_id, held));
                }
                if let Some(unpack) = read.unpack {
                    unpacks.push((job_id, unpack));
                }
                then.push((job_id, read.then));
            }
        }
        if waits {
            self.inner.restore_wanted.store(true, Ordering::Release);
        }
        for (job_id, then) in then {
            match then {
                Then::Stands => {}
                Then::Fails(error) => self.fail(job_id, error).await,
                Then::Resolves => self.resolve(job_id).await,
            }
        }
        if !unheld.is_empty() || !unpacks.is_empty() || !reopened.is_empty() {
            let db = self.inner.db.clone();
            let written = tokio::task::spawn_blocking(move || {
                (
                    db.set_torrents_held(&unheld),
                    db.set_torrents_unpack_in_force(&unpacks),
                    db.clear_torrents_completed(&reopened),
                )
            })
            .await;
            match written {
                Ok((held, unpacks, reopened)) => {
                    if let Err(error) = held {
                        tracing::warn!(error = %error, "could not write what the torrents hold; each holds its whole folder until its next start");
                    }
                    if let Err(error) = unpacks {
                        tracing::warn!(error = %error, "could not record whether the torrents unpack");
                    }
                    if let Err(error) = reopened {
                        // Read the same way at the next start.
                        tracing::warn!(error = %error, "could not store the torrents short of complete again");
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not write what the torrents hold; each holds its whole folder until its next start")
                }
            }
        }
        self.place_last(late).await;
        // A folder made in another spelling before a torrent's first write
        // holds its files from then on: each start spells what the
        // torrents hold as the filesystem does.
        self.respell_held(taken).await;
    }

    // The torrents added while the stored ones were read, placed past every
    // one of them in the queue, in the order they were added, in one write
    // made off the runtime with no torrent's lock held.
    async fn place_last(&self, added: Vec<JobId>) {
        if added.is_empty() {
            return;
        }
        let positions: Vec<(JobId, i64)> = {
            let live = self.inner.live.lock().await;
            let mut placed: Vec<(i64, JobId)> = added
                .into_iter()
                .filter_map(|job_id| Some((live.get(&job_id)?.record.queue_position, job_id)))
                .collect();
            placed.sort_unstable();
            placed
                .into_iter()
                .map(|(_, job_id)| {
                    (
                        job_id,
                        self.inner
                            .next_queue_position
                            .fetch_add(1, Ordering::Relaxed),
                    )
                })
                .collect()
        };
        if positions.is_empty() {
            return;
        }
        let db = self.inner.db.clone();
        let rows = positions.clone();
        let written =
            tokio::task::spawn_blocking(move || db.set_torrent_queue_positions(&rows)).await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not keep the queue places of the torrents added at start");
                return;
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not keep the queue places of the torrents added at start");
                return;
            }
        }
        let mut live = self.inner.live.lock().await;
        for (job_id, position) in positions {
            if let Some(torrent) = live.get_mut(&job_id) {
                torrent.record.queue_position = position;
            }
        }
    }

    // What the torrents of `jobs` hold, spelled as the filesystem spells it
    // now ([`held_key`]): a part the torrent named before it was there, and
    // that came to be in another case or through a link, is held as it is
    // on disk. One read, and one write only for the rows that differ; none
    // when none does. Off the runtime.
    pub(super) async fn respell_held(&self, jobs: Vec<JobId>) {
        if jobs.is_empty() {
            return;
        }
        let db = self.inner.db.clone();
        let respelled = tokio::task::spawn_blocking(move || {
            let changed: Vec<(JobId, String, String)> = db
                .held_rows(&jobs)?
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
// the pieces the job has, and no block of any other but the pieces kept in
// a file changed since the last run, which the session checks alone before
// it starts (`TorrentStore::initial_checkpoint`). The storage keeps a
// piece's blocks in memory until the piece verifies, so no block of a piece
// not had is ever written to the job's files. None while the job vouches
// for nothing: the session checks every piece.
fn apply_initial_have(options: &mut AddTorrentOptions, have: Option<Checkpoint>) {
    options.checkpoint = have;
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
pub enum Unmade<W> {
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
pub enum Note {
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

// How many stored torrents a start reads at a time, and the bytes of stored
// metainfo a page reads at most. A torrent's metainfo is most often tens of
// kilobytes, so the rows bound a page; one may be as large as the engine
// takes, `MAX_TORRENT_FILE_BYTES`, a hundred megabytes, and 64 of those
// would be gigabytes held at once. A page stops before the torrent that
// would take it past 32 MiB, so it holds 32 MiB of stored metainfo or one
// torrent's, whichever is more, with what parsing makes of it.
const LOAD_PAGE: usize = 64;
pub const LOAD_PAGE_BYTES: u64 = 32 * 1024 * 1024;

// `sized` in pages of `rows` torrents and `LOAD_PAGE_BYTES` at most, a
// torrent larger than that a page alone.
pub fn pages(sized: &[(JobId, u64)], rows: usize) -> Vec<Vec<JobId>> {
    let mut pages: Vec<Vec<JobId>> = Vec::new();
    let mut bytes = 0u64;
    for &(job_id, size) in sized {
        match pages.last_mut() {
            Some(page) if page.len() < rows && bytes.saturating_add(size) <= LOAD_PAGE_BYTES => {
                page.push(job_id);
                bytes += size;
            }
            _ => {
                pages.push(vec![job_id]);
                bytes = size;
            }
        }
    }
    pages
}

// A read of torrents the load had not reached, in flight: while one is,
// the torrents taken out are kept as gone, though the load ends.
pub struct Reader {
    service: TorrentService,
}

impl Drop for Reader {
    fn drop(&mut self) {
        let mut marks = locked(&self.service.inner.load_marks);
        marks.readers -= 1;
        if marks.readers == 0 && !self.service.loading() {
            marks.gone.clear();
        }
    }
}

// The torrents one asker reads now: theirs until this is dropped, which
// tells those waiting for one that its read has ended.
pub struct Claim {
    service: TorrentService,
    jobs: Vec<JobId>,
    pub ended: watch::Sender<bool>,
}

impl Drop for Claim {
    fn drop(&mut self) {
        {
            let mut marks = locked(&self.service.inner.load_marks);
            for job_id in &self.jobs {
                marks.reading.remove(job_id);
            }
        }
        self.ended.send_replace(true);
    }
}

// A page of stored torrents read.
pub struct Page {
    read: Vec<Read>,
}

// One stored torrent as it joins the live ones, what it holds by name when
// that was never written, and what follows once it has joined.
struct Read {
    torrent: Live,
    then: Then,
    unheld: Option<Vec<String>>,
    // Whether it unpacks, fixed as it was read and not yet kept.
    unpack: Option<bool>,
}

enum Then {
    Stands,
    // Its metainfo does not parse: it fails.
    Fails(String),
    // A magnet with no metainfo yet: it is fetched.
    Resolves,
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
    // Its files' times as the boot read them; none to read its rows.
    stamps: Option<Vec<(u32, i64)>>,
}
