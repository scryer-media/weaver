// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{ActionError, TorrentService, files_dir, give_back, locked, state_error};
use crate::jobs::JobId;
use crate::pipeline::claim_complete_destination_path;
use crate::torrents::relocate;
use crate::torrents::relocate::{Existing, MoveError};
use crate::torrents::storage::Pieces;
use crate::torrents::store::TorrentStore;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl TorrentService {
    /// Move the torrent's directory under `parent`, where it goes on
    /// downloading or seeding (qBittorrent `TorrentImpl::setSavePath`,
    /// libtorrent `torrent::move_storage`). The move holds off every read
    /// and write of its files and happens whole; the torrent shows as
    /// moving until it did.
    /// A directory of the torrent's name is claimed under `parent`, so
    /// nothing there is replaced or kept. Files placed outside the
    /// torrent's directory move straight into `parent` instead, where they
    /// sat with no folder of their own; a file of theirs already there
    /// stays, and the torrent is checked again from there. A move that
    /// fails leaves the torrent where it was, and says why when it failed
    /// at once.
    pub async fn set_location(&self, job_id: JobId, parent: String) -> Result<(), ActionError> {
        self.move_storage(job_id, parent, Existing::Keep).await
    }

    // The move of `set_location`. A placed file already at the new place
    // stays, its would-be replacement stays where it was, and the torrent
    // is checked again from the new place, as qBittorrent's move of a
    // user's asks for (`TorrentImpl::moveStorage`, `MoveStorageMode::
    // KeepExistingFiles`, which is libtorrent's `move_flags_t::dont_replace`;
    // libtorrent `torrent::on_storage_moved` rechecks on `need_full_check`).
    // `Existing::Replace` gives way instead, as its move of a finished
    // torrent under its save path does (`TorrentImpl::adjustStorageLocation`,
    // `MoveStorageMode::Overwrite`).
    pub(super) async fn move_storage(
        &self,
        job_id: JobId,
        parent: String,
        existing: Existing,
    ) -> Result<(), ActionError> {
        // The move runs on its own task: a request dropped part way, as a
        // client that goes away drops it, cannot leave the torrent shown
        // moving. Once begun, the move ends there, as a removal does
        // (`remove_as`).
        let service = self.clone();
        tokio::spawn(async move { service.move_storage_now(job_id, parent, existing).await })
            .await
            .map_err(|error| ActionError::State(format!("the move did not finish: {error}")))?
    }

    // `move_storage`, on a task of its own.
    async fn move_storage_now(
        &self,
        job_id: JobId,
        parent: String,
        existing: Existing,
    ) -> Result<(), ActionError> {
        let parent = PathBuf::from(parent.trim());
        if !parent.is_absolute() {
            return Err(ActionError::Invalid(
                "the location must be an absolute path".to_owned(),
            ));
        }
        let (store, from, placed, held) = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                if torrent.removing {
                    return Err(ActionError::Invalid(
                        "the torrent is being removed".to_owned(),
                    ));
                }
                match (torrent.store.clone(), torrent.record.storage_dir.clone()) {
                    (Some(store), Some(dir)) => Ok((
                        store,
                        PathBuf::from(dir),
                        torrent.record.files_dir.clone().map(PathBuf::from),
                        torrent.files_missing,
                    )),
                    _ => Err(ActionError::Invalid(
                        "the torrent has no files to move yet".to_owned(),
                    )),
                }
            })
            .await?;
        if let Some(placed) = placed {
            return self
                .move_placed(job_id, store, &placed, parent, existing)
                .await;
        }
        if from.parent() == Some(parent.as_path()) {
            return Ok(());
        }
        let name = from
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ActionError::State("the torrent's directory has no name".to_owned()))?;
        if held {
            return self.place_held(job_id, &store, parent.join(name)).await;
        }
        // A location the torrent's directory cannot be made under is the
        // caller's to fix (qBittorrent `TorrentsController::setLocationAction`
        // answers "Cannot make save path" as a conflict).
        let to = claim_complete_destination_path(&parent, name, job_id, &HashSet::new())
            .await
            .map_err(|error| {
                ActionError::Invalid(format!("could not make the torrent's directory: {error}"))
            })?;
        let started = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                if torrent.removing {
                    return Err(ActionError::Invalid(
                        "the torrent is being removed".to_owned(),
                    ));
                }
                torrent.settle(self.inner.clock.unix_ms());
                torrent.moving = Some(Moving::Dir(to.clone()));
                Ok(())
            })
            .await;
        if let Err(error) = started {
            give_back(&to.to_string_lossy(), job_id).await;
            return Err(error);
        }
        // The queue shows the torrent moving (qBittorrent's `moving` state)
        // until `moved` or `move_refused` ends it and the job shows again.
        self.publish_job(job_id).await;
        let destination = (Some(to.to_string_lossy().into_owned()), None);
        if let Err(error) = self.hold_destination(job_id, destination).await {
            self.move_refused(job_id).await;
            give_back(&to.to_string_lossy(), job_id).await;
            return Err(ActionError::State(format!(
                "the torrent was not moved: what it holds there could not be recorded ({error})"
            )));
        }
        let outcome = tokio::task::spawn_blocking(move || {
            store.relocate(|dir| {
                // Its marker is made for the folder's path, so the moved
                // folder takes one made for where it went, as the job's own.
                let owned = crate::jobs::working_dir::output_dir_owner(dir)
                    == Some(crate::jobs::working_dir::OutputDirOwner::Job(job_id));
                relocate::move_directory(dir, &to)?;
                if owned
                    && let Err(error) =
                        crate::jobs::working_dir::mark_weaver_owned_output_dir(&to, job_id)
                {
                    tracing::warn!(job_id = job_id.0, dir = %to.display(), error = %error, "could not mark a torrent's moved folder; history leaves its files");
                }
                Ok((to.clone(), (to, false)))
            })
        })
        .await
        .unwrap_or_else(|error| {
            Err(MoveError {
                path: from,
                source: std::io::Error::other(error),
            })
        });
        let moved = self.moved(job_id, outcome).await;
        self.publish().await;
        moved
    }

    // A torrent held for its files has none to move: its directory's name
    // under the new location becomes its directory, unclaimed and as it is,
    // and its files are looked for there. It runs if they are there and
    // stays held if not; nothing is made or moved at either place
    // (libtorrent `move_storage` passes over a file that is not there,
    // storage_utils.cpp:236-239 at v2.0.15, and qBittorrent
    // `TorrentImpl::handleMoveStorageJobFinished` looks again from the new
    // path, torrentimpl.cpp:2140-2146).
    async fn place_held(
        &self,
        job_id: JobId,
        store: &Arc<TorrentStore>,
        to: PathBuf,
    ) -> Result<(), ActionError> {
        let dir = to.to_string_lossy().into_owned();
        self.with_torrent(job_id, |torrent| {
            if torrent.moving.is_some() {
                return Err(ActionError::Invalid(
                    "the torrent is already moving".to_owned(),
                ));
            }
            if torrent.removing {
                return Err(ActionError::Invalid(
                    "the torrent is being removed".to_owned(),
                ));
            }
            let still_held = torrent.files_missing
                && torrent
                    .store
                    .as_ref()
                    .is_some_and(|held| Arc::ptr_eq(held, store));
            if !still_held {
                return Err(ActionError::Invalid(
                    "the torrent's files were found while it was moved; move it again".to_owned(),
                ));
            }
            self.inner
                .db
                .set_torrent_storage_dir(job_id, &dir)
                .map_err(|error| ActionError::State(state_error(error)))?;
            store.place(to.clone());
            torrent.record.storage_dir = Some(dir.clone());
            Ok(())
        })
        .await?;
        self.sync_held(job_id).await;
        if self.look_again(job_id).await {
            self.apply_queue().await;
            self.spawn_attach(job_id).await;
        }
        self.publish().await;
        Ok(())
    }

    // Move a torrent's files placed in `from`, outside its directory, to
    // `to` (libtorrent `torrent::move_storage` moves every file of a
    // torrent from its save path to the new one), with what unpacking
    // placed beside them. The directory claimed for it stays where it was.
    async fn move_placed(
        &self,
        job_id: JobId,
        store: Arc<TorrentStore>,
        from: &Path,
        to: PathBuf,
        existing: Existing,
    ) -> Result<(), ActionError> {
        if from == to {
            return Ok(());
        }
        // qBittorrent `TorrentsController::setLocationAction` makes the
        // location first, and answers a conflict when it cannot.
        tokio::fs::create_dir_all(&to).await.map_err(|error| {
            ActionError::Invalid(format!("could not make the torrent's directory: {error}"))
        })?;
        let storage = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                if torrent.removing {
                    return Err(ActionError::Invalid(
                        "the torrent is being removed".to_owned(),
                    ));
                }
                torrent.settle(self.inner.clock.unix_ms());
                torrent.moving = Some(Moving::Files);
                Ok(torrent.record.storage_dir.clone())
            })
            .await?;
        // Shown moving until the move ends, as a move of its directory is.
        self.publish_job(job_id).await;
        let destination = (storage, Some(to.to_string_lossy().into_owned()));
        if let Err(error) = self.hold_destination(job_id, destination).await {
            self.move_refused(job_id).await;
            return Err(ActionError::State(format!(
                "the torrent was not moved: what it holds there could not be recorded ({error})"
            )));
        }
        let outputs = self.inner.db.torrent_outputs(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent unpacked");
            Vec::new()
        });
        let from = from.to_path_buf();
        let outcome =
            tokio::task::spawn_blocking(move || move_listed(&store, &outputs, to, existing))
                .await
                .unwrap_or_else(|error| {
                    Err(MoveError {
                        path: from,
                        source: std::io::Error::other(error),
                    })
                });
        let moved = self.moved(job_id, outcome).await;
        self.publish().await;
        moved
    }

    /// Check every piece of a torrent again from what the job holds, trusting
    /// nothing saved (libtorrent `torrent::force_recheck`, qBittorrent's
    /// `torrents/recheck`). A running torrent runs again once checked, a
    /// paused one stays paused, and one being checked is left to its check.
    pub async fn recheck(&self, job_id: JobId) -> Result<(), ActionError> {
        // One held for its files missing is in no session: it lets go of
        // the hold and of every piece, and the session it joins checks
        // them all (qBittorrent `TorrentImpl::forceRecheck` clears its
        // missing files, torrentimpl.cpp:1736-1744).
        let held = self
            .with_torrent(job_id, |torrent| {
                if !torrent.files_missing {
                    return Ok(false);
                }
                if let Some(store) = &torrent.store {
                    Pieces::checking(&**store)
                        .map_err(|error| ActionError::State(format!("{error:#}")))?;
                    torrent.hold(store.file_have());
                }
                torrent.files_missing = false;
                Ok(true)
            })
            .await?;
        if held {
            self.apply_queue().await;
            self.spawn_attach(job_id).await;
            self.publish().await;
            return Ok(());
        }
        {
            // Held across the recheck, as the queue holds it across a pause
            // or resume, so neither undoes the other halfway.
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let attached = torrent.attached.as_ref().ok_or_else(|| {
                ActionError::Invalid("the torrent is in no session to check it".into())
            })?;
            attached
                .session
                .force_recheck(&attached.handle)
                .await
                .map_err(|error| ActionError::State(format!("{error:#}")))?;
            // The check runs its pieces again: a wait for room from before
            // it ends, and a write it refuses holds it anew.
            self.end_disk_room(torrent);
        }
        self.publish().await;
        Ok(())
    }

    // A move refused before any file moved: the torrent is as it was, and
    // an unpacking that waited on the move goes on.
    async fn move_refused(&self, job_id: JobId) {
        let _ = self
            .with_torrent(job_id, |torrent| {
                torrent.settle(self.inner.clock.unix_ms());
                torrent.moving = None;
                Ok(())
            })
            .await;
        self.inner.signal.mark(job_id);
        // It no longer shows moving.
        self.publish_job(job_id).await;
    }

    // The torrent's files moved, or could not: the record follows a move,
    // and a failed one gives its claim back and leaves the torrent where it
    // was. Files placed outside its directory take their save path and
    // their directory with them; a file already where one of them went was
    // kept, which the outcome says. A failed move says why.
    async fn moved(
        &self,
        job_id: JobId,
        outcome: Result<(PathBuf, bool), MoveError>,
    ) -> Result<(), ActionError> {
        let (moving, store) = self
            .with_torrent(job_id, |torrent| {
                torrent.settle(self.inner.clock.unix_ms());
                Ok((torrent.moving.take(), torrent.store.clone()))
            })
            .await
            .unwrap_or((None, None));
        // An unpacking that waited on the move places its files now.
        self.inner.signal.mark(job_id);
        match outcome {
            Ok((to, kept_existing)) => {
                let to = to.to_string_lossy().into_owned();
                let files = matches!(moving, Some(Moving::Files));
                // Kept on its row before the torrent shows it, under the
                // same hold, so nothing that sees the new place finds the
                // row behind.
                let recorded = self
                    .with_torrent(job_id, |torrent| {
                        let recorded = if files {
                            self.inner
                                .db
                                .set_torrent_files_dir(job_id, Some(&to))
                                .and_then(|_| {
                                    self.inner.db.set_torrent_save_path(job_id, Some(&to))
                                })
                        } else {
                            self.inner.db.set_torrent_storage_dir(job_id, &to)
                        };
                        if files {
                            torrent.record.files_dir = Some(to.clone());
                            torrent.record.save_path = Some(to.clone());
                        } else {
                            torrent.record.storage_dir = Some(to.clone());
                        }
                        // Its pieces land on the disk of its new folder,
                        // looked up now: none is let through until then.
                        // A look out takes the new folder at its end; a
                        // torrent listed to look again is looked up now.
                        let dir = files_dir(torrent);
                        let look = torrent.intake.clone().filter(|intake| {
                            intake.room().aim(&dir) && {
                                locked(&self.inner.relook).jobs.remove(&job_id);
                                intake.room().begin_placing()
                            }
                        });
                        Ok((recorded, look.map(|intake| (intake, dir))))
                    })
                    .await;
                let recorded = recorded.map(|(recorded, look)| {
                    if let Some((intake, dir)) = look {
                        self.spawn_place(job_id, &intake, dir, false);
                    }
                    recorded
                });
                if let Ok(Err(error)) = recorded {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's new directory");
                }
                self.sync_held(job_id).await;
                if files
                    && kept_existing
                    && let Some(store) = &store
                {
                    self.check_kept(job_id, store).await;
                }
                // A move looks again for files that were missing
                // (qBittorrent `TorrentImpl::handleMoveStorageJobFinished`,
                // torrentimpl.cpp:2140-2146).
                if self.look_again(job_id).await {
                    self.apply_queue().await;
                    self.spawn_attach(job_id).await;
                }
                Ok(())
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's move failed; it stays where it was");
                if let Some(Moving::Dir(claimed)) = moving {
                    give_back(&claimed.to_string_lossy(), job_id).await;
                }
                Err(ActionError::State(format!(
                    "the torrent's move failed, and it stays where it was: {error}"
                )))
            }
        }
    }

    // A move kept a file already at the torrent's new place, so its files
    // there are not all its own: every piece is checked again (libtorrent
    // `torrent::on_storage_moved` rechecks on `need_full_check`). A torrent
    // in no session forgets its pieces instead, and the session it next
    // joins checks them, as it checks any torrent whose store vouches for
    // nothing.
    async fn check_kept(&self, job_id: JobId, store: &Arc<TorrentStore>) {
        let attached = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|torrent| torrent.attached.is_some());
        let checked = if attached {
            self.recheck(job_id).await.map_err(anyhow::Error::from)
        } else {
            Pieces::checking(&**store).map(|_| ())
        };
        if let Err(error) = checked {
            tracing::warn!(job_id = job_id.0, error = %error, "could not check a torrent again after its move kept files there");
        }
    }
}

// What a torrent's move takes.
#[derive(Debug, Clone)]
pub(super) enum Moving {
    // Its directory, to the directory claimed for it.
    Dir(PathBuf),
    // Its files placed outside its directory.
    Files,
    // What its unpacking made, going beside its files.
    Outputs,
}

// The torrent's listed files and what unpacking put beside them, moved to
// `to` under the lock no read or write gets past, and read and written
// there from then on; where they went, and whether a file already there
// was kept in place of one of them.
pub(in crate::torrents) fn move_listed(
    store: &TorrentStore,
    outputs: &[String],
    to: PathBuf,
    existing: Existing,
) -> Result<(PathBuf, bool), MoveError> {
    let names: Vec<PathBuf> = store
        .files()
        .names()
        .flatten()
        .map(Path::to_path_buf)
        .chain(outputs.iter().map(PathBuf::from))
        .collect();
    store.relocate(|dir| {
        let moves: Vec<(PathBuf, PathBuf)> = names
            .iter()
            .map(|name| (dir.join(name), to.join(name)))
            .collect();
        let kept = relocate::move_files(dir, &moves, existing)?;
        Ok((to.clone(), (to, kept)))
    })
}
