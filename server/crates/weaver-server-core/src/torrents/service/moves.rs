// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::intake::OUTSIDE_SAVE_FOLDERS;
use super::retire::{held_of, held_tops};
use super::{ActionError, TorrentService, files_dir, give_back, locked, state_error};
use crate::StateError;
use crate::auth::CallerScope;
use crate::categories::model;
use crate::jobs::JobId;
use crate::pipeline::claim_complete_destination_path;
use crate::runtime::fs::JobMoves;
use crate::torrents::files;
use crate::torrents::model::Adopted;
use crate::torrents::relocate;
use crate::torrents::relocate::{Existing, MoveError};
use crate::torrents::storage::Pieces;
use crate::torrents::store::TorrentStore;
use crate::torrents::tee::Placed;
use std::collections::{HashMap, HashSet};
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
    /// This is the server's own move: `parent` is not checked against a
    /// caller's. An entry point that takes a location from a caller moves
    /// through `set_location_as`.
    pub async fn set_location(&self, job_id: JobId, parent: String) -> Result<(), ActionError> {
        self.move_storage(job_id, parent, Existing::Keep).await
    }

    /// `set_location` for `caller`, who must be allowed `parent`
    /// (`may_place`); refused, nothing moves.
    pub async fn set_location_as(
        &self,
        caller: CallerScope,
        job_id: JobId,
        parent: String,
    ) -> Result<(), ActionError> {
        if !self.may_place(caller, Path::new(parent.trim())) {
            return Err(ActionError::Forbidden(OUTSIDE_SAVE_FOLDERS.to_owned()));
        }
        self.set_location(job_id, parent).await
    }

    /// Whether `caller` may have a torrent saved or moved under `path`, by
    /// the settings in force (`categories::model::may_place`).
    pub fn may_place(&self, caller: CallerScope, path: &Path) -> bool {
        let settings = self.settings();
        model::may_place(caller, path, &settings.complete_dir, &settings.categories)
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
                Ok((to.clone(), Moved::of_dir(to)))
            }, |_| false)
        })
        .await
        .unwrap_or_else(|error| {
            Err(MoveError {
                path: from,
                source: std::io::Error::other(error),
            })
        })
        .map_err(NotMoved::from);
        let moved = self.moved(job_id, outcome, false).await;
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
    // placed beside them, and its row follows in the one write the move
    // itself makes (`move_listed`). The directory claimed for it stays
    // where it was.
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
        // Whether the new place is there is looked at before it is made: a
        // folder this move makes was not there before the torrent.
        let to_made = tokio::fs::symlink_metadata(&to).await.is_err();
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
        let claimed = storage.clone();
        // Shown moving until the move ends, as a move of its directory is.
        self.publish_job(job_id).await;
        let destination = (storage, Some(to.to_string_lossy().into_owned()));
        if let Err(error) = self.hold_destination(job_id, destination).await {
            self.move_refused(job_id).await;
            return Err(ActionError::State(format!(
                "the torrent was not moved: what it holds there could not be recorded ({error})"
            )));
        }
        // A file already there under one of the torrent's names is kept,
        // and the torrent's pieces of it are then not what its files hold.
        // They are forgotten, and its peers let go, before any read is made
        // from the new place: nothing is served from a kept file until the
        // check after the move has passed it (libtorrent closes every
        // connection and forgets every piece on `need_full_check`,
        // `torrent::on_storage_moved` and `torrent::force_recheck`). The
        // session is asked with no lock of the store held.
        let forgot = existing == Existing::Keep && {
            let (files, at) = (Arc::clone(&store), to.clone());
            tokio::task::spawn_blocking(move || {
                files
                    .files()
                    .names()
                    .flatten()
                    .any(|name| std::fs::symlink_metadata(at.join(name)).is_ok())
            })
            .await
            .unwrap_or(true)
        };
        if forgot && let Err(error) = self.forget_pieces(job_id).await {
            self.move_refused(job_id).await;
            // The new place, held for the move, is let go.
            self.sync_held(job_id).await;
            return Err(ActionError::State(format!(
                "the torrent was not moved: its pieces could not be set to check again ({error:#})"
            )));
        }
        #[cfg(test)]
        if forgot {
            let held = super::locked(&super::MOVE_FORGOT_HOLD).take();
            if let Some((reached, release)) = held {
                let _ = reached.send(());
                let _ = release.await;
            }
        }
        let from = from.to_path_buf();
        let db = self.inner.db.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let not_begun = |error: StateError| {
                NotMoved::from(MoveError {
                    path: from.clone(),
                    source: std::io::Error::other(format!(
                        "what the torrent made could not be read or set aside ({error})"
                    )),
                })
            };
            // What the row says the torrent made, read once: what unpacking
            // placed, and what was there before the torrent.
            let outputs = db.torrent_outputs(job_id).map_err(not_begun)?;
            let adopted = db.torrent_adopted(job_id).map_err(not_begun)?;
            // The row lets go of all of it before any file moves, and takes
            // it back in the write that records the move, or that a failed
            // move ends with. One write, whatever the number of files.
            //
            // A crash between this write and that one leaves the row set
            // aside for good; nothing marks the move to undo it at the
            // next start. It errs to never deleting. What it leaves:
            // - The row names no file as the torrent's to delete, at either
            //   place. Weaver then never deletes the torrent's own files or
            //   what it unpacked, by a removal with its files or otherwise,
            //   and so never a file someone put at a name the torrent's
            //   moved away from. They stay on disk for the operator.
            // - What it unpacked is off the row: those files are no longer
            //   the torrent's, wherever the crash left them.
            // - The row still says the place the files were. Those that
            //   had moved are not there, and show as missing at the next
            //   start, until the operator sets the location to where they
            //   went: the move then keeps them there and checks them.
            // - An unpacking after it numbers its files beside the ones
            //   left, as beside any file it did not place.
            let aside = Adopted {
                all_files: true,
                files: Vec::new(),
                dirs: with(adopted.as_ref().map_or(&[][..], |a| &a.dirs), [String::new()]),
            };
            // The saved pieces are marked to be checked with it: a start
            // that follows a run ended mid-move trusts none of them, so a
            // file left at one of the torrent's names that is not its own
            // is checked before any of it is had. The write that ends the
            // move says what holds from then.
            db.set_torrent_made(job_id, &[], Some(&aside), true)
                .map_err(not_begun)?;
            #[cfg(test)]
            if let Some(meets) = super::locked(&super::MOVE_BEGIN_MEETS).take() {
                meets();
            }
            let moving = Listed {
                job_id,
                storage: claimed.as_deref(),
                outputs: &outputs,
                existing,
                to_made,
                forgot,
            };
            let moved = move_listed(&store, moving, to, |moved| {
                #[cfg(test)]
                if let Some(meets) = super::locked(&super::MOVE_RECORD_MEETS).take() {
                    meets();
                }
                #[cfg(test)]
                if super::MOVE_RECORD_DIES.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    panic!("the run ends here, as a crash would end it");
                }
                #[cfg(test)]
                if super::MOVE_RECORD_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    return Err(StateError::Database("refused".to_owned()));
                }
                // What was there before the torrent gains what this move
                // found there and kept: a removal of the torrent with its
                // files leaves those, as it leaves any file it did not
                // make. Not known stays not known, and takes no file.
                let adopted = adopted.as_ref().map(|adopted| Adopted {
                    all_files: adopted.all_files,
                    files: match adopted.all_files {
                        true => Vec::new(),
                        false => with(&adopted.files, moved.kept.iter().cloned()),
                    },
                    // The folders are those of the new place alone: one
                    // that was there before the torrent where its files
                    // were says nothing of where they are now.
                    dirs: moved.dirs.clone(),
                });
                db.record_torrent_files_move(
                    job_id,
                    &moved.to.to_string_lossy(),
                    &moved.outputs,
                    adopted.as_ref(),
                    &moved.held,
                    // A file kept there is not the torrent's own: a start
                    // before its pieces are forgotten checks them all.
                    store.unvouched() || !moved.kept.is_empty(),
                )
            });
            moved.inspect_err(|not_moved| {
                // The row takes back what is where it was. A file of the
                // torrent's that would not go back left its name to someone
                // else's file: that name is adopted, never deleted as the
                // torrent's. An output that would not go back is on no row.
                let adopted = adopted.as_ref().map(|adopted| Adopted {
                    all_files: adopted.all_files,
                    files: match adopted.all_files {
                        true => Vec::new(),
                        false => with(&adopted.files, not_moved.stranded.iter().cloned()),
                    },
                    dirs: adopted.dirs.clone(),
                });
                #[cfg(test)]
                let refused = super::MOVE_UNDO_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel);
                #[cfg(not(test))]
                let refused = false;
                let written = match refused {
                    true => Err(StateError::Database("refused".to_owned())),
                    false => db.set_torrent_made(
                        job_id,
                        &not_moved.outputs,
                        adopted.as_ref(),
                        store.unvouched() || !not_moved.stranded.is_empty(),
                    ),
                };
                if let Err(error) = written {
                    // The row stays as the move set it aside: it names no
                    // file as the torrent's, now and after a restart.
                    tracing::warn!(job_id = job_id.0, error = %error, "a torrent's failed move could not write back what the torrent made; its row names none of its files, and a removal leaves them on disk");
                }
            })
        })
        .await
        .unwrap_or_else(|error| {
            Err(NotMoved::from(MoveError {
                path: PathBuf::new(),
                source: std::io::Error::other(error),
            }))
        });
        let moved = self.moved(job_id, outcome, forgot).await;
        self.publish().await;
        moved
    }

    // The torrent's session forgets every piece of it and lets its peers
    // go, a check under way stopped first, and begins no check: the first
    // half of `check_kept`, for a move about to keep a file. One in no
    // session serves nothing, and forgets its pieces after the move.
    async fn forget_pieces(&self, job_id: JobId) -> anyhow::Result<()> {
        let attached = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .and_then(|torrent| {
                let attached = torrent.attached.as_ref()?;
                Some((Arc::clone(&attached.session), Arc::clone(&attached.handle)))
            });
        match attached {
            Some((session, handle)) => session.forget_for_recheck(&handle).await,
            None => Ok(()),
        }
    }

    /// Check every piece of a torrent again from what the job holds, trusting
    /// nothing saved (libtorrent `torrent::force_recheck`, qBittorrent's
    /// `torrents/recheck`). A running torrent runs again once checked, a
    /// paused one stays paused, and one being checked is left to its check.
    pub async fn recheck(&self, job_id: JobId) -> Result<(), ActionError> {
        // A move owns the torrent's files, and its own forgetting and
        // checking of them, until it ends: a recheck asked for meanwhile is
        // refused, seen under the live lock the move began under.
        let moving = || ActionError::Invalid("the torrent is already moving".to_owned());
        // One held for its files missing is in no session: it lets go of
        // the hold and of every piece, and the session it joins checks
        // them all (qBittorrent `TorrentImpl::forceRecheck` clears its
        // missing files, torrentimpl.cpp:1736-1744).
        let held = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(moving());
                }
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
            if torrent.moving.is_some() {
                return Err(moving());
            }
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
    // their directory with them, on a row their move wrote already; a file
    // already where one of them went was kept, which the outcome says. A
    // failed move says why.
    async fn moved(
        &self,
        job_id: JobId,
        outcome: Result<Moved, NotMoved>,
        forgot: bool,
    ) -> Result<(), ActionError> {
        let (moving, store) = self
            .with_torrent(job_id, |torrent| {
                torrent.settle(self.inner.clock.unix_ms());
                Ok((torrent.moving.clone(), torrent.store.clone()))
            })
            .await
            .unwrap_or((None, None));
        // The torrent stays moving until the check of what the move left
        // has forgotten its pieces (`move_over`): a recheck asked for
        // before then is refused, as during the move, so none runs on
        // files the move's own check is about to forget, and a tracker
        // hears one `stopped` and one `started` of it, not two.
        match outcome {
            Ok(moved) => {
                let kept_existing = !moved.kept.is_empty();
                let to = moved.to.to_string_lossy().into_owned();
                let files = matches!(moving, Some(Moving::Files));
                // Kept on its row before the torrent shows it, under the
                // same hold, so nothing that sees the new place finds the
                // row behind.
                let recorded = self
                    .with_torrent(job_id, |torrent| {
                        // A move of placed files wrote its row itself, with
                        // the names its outputs moved to (`move_listed`).
                        let recorded = if files {
                            Ok(true)
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
                    && (kept_existing || forgot)
                    && let Some(store) = &store
                {
                    self.check_kept(job_id, store).await;
                }
                self.move_over(job_id).await;
                // A move looks again for files that were missing
                // (qBittorrent `TorrentImpl::handleMoveStorageJobFinished`,
                // torrentimpl.cpp:2140-2146).
                if self.look_again(job_id).await {
                    self.apply_queue().await;
                    self.spawn_attach(job_id).await;
                }
                Ok(())
            }
            Err(NotMoved {
                error, stranded, ..
            }) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's move failed; it stays where it was");
                if let Some(Moving::Dir(claimed)) = moving {
                    give_back(&claimed.to_string_lossy(), job_id).await;
                }
                if !stranded.is_empty() {
                    tracing::warn!(job_id = job_id.0, files = ?stranded, "files of a torrent could not be put back after its failed move: their names were taken meanwhile. What is at those names is not the torrent's; it is kept, and the torrent is checked again");
                }
                // What it holds is written again from its row: the place
                // it did not move to is let go, and so is the name of an
                // unpacked file that could not be put back.
                self.sync_held(job_id).await;
                // Its pieces were forgotten for the move, or a file at one
                // of its names is not its own now: they are checked where
                // its files stayed, every one forgotten first, so nothing
                // of a file that is not its own is served as had.
                if (forgot || !stranded.is_empty())
                    && let Some(store) = &store
                {
                    self.check_kept(job_id, store).await;
                }
                self.move_over(job_id).await;
                Err(ActionError::State(format!(
                    "the torrent's move failed, and it stays where it was: {error}"
                )))
            }
        }
    }

    // The move ended, its check begun where it owed one: the torrent is no
    // longer moving, and an unpacking that waited on the move places its
    // files now.
    async fn move_over(&self, job_id: JobId) {
        let _ = self
            .with_torrent(job_id, |torrent| {
                torrent.moving = None;
                Ok(())
            })
            .await;
        self.inner.signal.mark(job_id);
    }

    // A move kept a file already at the torrent's new place, so its files
    // there are not all its own: every piece is checked again (libtorrent
    // `torrent::on_storage_moved` rechecks on `need_full_check`). A torrent
    // in no session forgets its pieces instead, and the session it next
    // joins checks them, as it checks any torrent whose store vouches for
    // nothing.
    async fn check_kept(&self, job_id: JobId, store: &Arc<TorrentStore>) {
        #[cfg(test)]
        {
            let held = super::locked(&super::MOVE_CHECK_HOLD).take();
            if let Some((reached, release)) = held {
                let _ = reached.send(());
                let _ = release.await;
            }
        }
        let attached = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .and_then(|torrent| {
                let attached = torrent.attached.as_ref()?;
                Some((Arc::clone(&attached.session), Arc::clone(&attached.handle)))
            });
        #[cfg(test)]
        let attached =
            match super::MOVE_CHECK_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel) {
                true => {
                    tracing::warn!(
                        job_id = job_id.0,
                        "the check after a move is refused by its test"
                    );
                    return;
                }
                false => attached,
            };
        let checked = match attached {
            // A check under way read some of the files where they were: it
            // is stopped and begun again, so no piece it passed there
            // stays had for a file kept here.
            //
            // Its files are read again only once every piece is forgotten,
            // where the store forgets them (`TorrentStore::checking`): a
            // move that left a file among them that is not its own shut
            // them, and until here no read was made of them. The check
            // that follows is the first.
            Some((session, handle)) => {
                let checked = match session.forget_for_recheck(&handle).await {
                    Ok(()) => session.start_check(&handle),
                    Err(error) => Err(error),
                };
                // The check runs its pieces again: a wait for room from
                // before it ends, as a recheck asked for ends it.
                if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
                    self.end_disk_room(torrent);
                }
                checked
            }
            None => Pieces::checking(&**store).map(|_| ()),
        };
        if let Err(error) = checked {
            tracing::warn!(job_id = job_id.0, error = %error, "could not check a torrent again after its move kept files there; its files stay unread until a recheck, a restart or a new session forgets its pieces and checks them");
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

// What a move of a torrent's files came to.
#[derive(Debug, Default, PartialEq)]
pub(in crate::torrents) struct Moved {
    // Where its files are now.
    pub(in crate::torrents) to: PathBuf,
    // Its own files, as its metainfo names them, that did not move: a file
    // already at the new place was kept in place of each.
    pub(in crate::torrents) kept: Vec<String>,
    // The folders of its files that were at the new place before the move,
    // relative to it, `""` for the place itself.
    pub(in crate::torrents) dirs: Vec<String>,
    // What unpacking placed, under the names the move left them at.
    pub(in crate::torrents) outputs: Vec<String>,
    // What the torrent holds from now on.
    pub(in crate::torrents) held: Vec<String>,
}

// A move of a torrent's files that failed, with what it had moved put back.
#[derive(Debug)]
pub(in crate::torrents) struct NotMoved {
    pub(in crate::torrents) error: MoveError,
    // The torrent's own files, as its metainfo names them, that would not go
    // back: something took each one's name where it was. Each stays at the
    // new place, and the file at its old name is not the torrent's.
    pub(in crate::torrents) stranded: Vec<String>,
    // What unpacking placed that is where it was, as the row named it. One
    // that would not go back is not among them: it stays at the new place,
    // on no row.
    pub(in crate::torrents) outputs: Vec<String>,
}

impl From<MoveError> for NotMoved {
    fn from(error: MoveError) -> Self {
        Self {
            error,
            stranded: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

// `names` with `more`, each once, sorted.
fn with(names: &[String], more: impl IntoIterator<Item = String>) -> Vec<String> {
    let all: std::collections::BTreeSet<String> = names.iter().cloned().chain(more).collect();
    all.into_iter().collect()
}

impl Moved {
    // A move of its whole directory, which meets no file.
    fn of_dir(to: PathBuf) -> Self {
        Self {
            to,
            ..Self::default()
        }
    }
}

// The torrent a move of placed files is for.
pub(in crate::torrents) struct Listed<'a> {
    pub(in crate::torrents) job_id: JobId,
    // The folder claimed for it, which stays where it is.
    pub(in crate::torrents) storage: Option<&'a str>,
    // What unpacking placed beside its files, as its row names them.
    pub(in crate::torrents) outputs: &'a [String],
    // What a file already where one of the torrent's own goes meets.
    pub(in crate::torrents) existing: Existing,
    // Whether the new place was made for this move, not there before it.
    pub(in crate::torrents) to_made: bool,
    // Whether the torrent forgot its pieces for this move. One that did
    // not, and ends with a file among its own that is not its own (kept at
    // the new place, or in the place of one that would not go back), comes
    // out with its files shut to reads until it has (`Files::relocate`).
    pub(in crate::torrents) forgot: bool,
}

// The torrent's listed files and what unpacking put beside them, moved to
// `to` under the lock no read or write gets past, and read and written
// there from then on.
//
// The two move by different rules. The torrent's own files move as
// libtorrent moves them: a file already at one's place is kept or replaced
// as `existing` says. What unpacking placed is not the torrent's to stand
// in for: each moves by the never-replacing move its placing used, to the
// next numbered name when its own is taken (`tee::move_outputs`), so a file
// someone else has at the new place is never replaced, and never becomes
// one the torrent's row names.
//
// What unpacking placed in the way of one of the torrent's own files (at
// the very name that file moves to, as a move into a folder of the
// torrent's can have it) moves first, out of its way, so the torrent's file
// neither replaces it nor is taken for a file kept there; the rest move
// after the torrent's own. One in the way that finds its own new name still
// held by a file of the torrent's yet to move takes the next number.
//
// `record` writes the move to the torrent's row, the folder, the outputs'
// names there and what it holds in one write, before any read or write is
// let through at the new place. A move that fails, of a file or of that
// write, puts back every file it moved, the last moved first, each by a
// move that never replaces, and leaves the torrent and its row where they
// were; a folder that was at the new place before the move stays. A file
// that will not go back, its old name taken meanwhile, stays where the move
// put it, and the failure names it (`NotMoved`).
pub(in crate::torrents) fn move_listed(
    store: &TorrentStore,
    moving: Listed<'_>,
    to: PathBuf,
    record: impl FnOnce(&Moved) -> Result<(), StateError>,
) -> Result<Moved, NotMoved> {
    let names: Vec<PathBuf> = store
        .files()
        .names()
        .flatten()
        .map(Path::to_path_buf)
        .collect();
    let named: Vec<String> = names
        .iter()
        .map(|name| name.to_string_lossy().into_owned())
        .collect();
    let forgot = moving.forgot;
    let shut = |ended: Result<&Moved, &NotMoved>| {
        !forgot
            && match ended {
                Ok(moved) => !moved.kept.is_empty(),
                Err(not_moved) => !not_moved.stranded.is_empty(),
            }
    };
    let moved = |dir: &Path| {
        // What is at the new place before any file of the torrent's is,
        // and before any folder is made there for one.
        let mut dirs = files::adopted(&to, &named).dirs;
        if moving.to_made {
            dirs.retain(|folder| !folder.is_empty());
        }
        let moves: Vec<(PathBuf, PathBuf)> = names
            .iter()
            .map(|name| (dir.join(name), to.join(name)))
            .collect();
        let targets: HashSet<String> = moves
            .iter()
            .map(|(_, target)| crate::torrents::tee::key_of(target))
            .collect();
        let in_way = |source: &Path| targets.contains(&crate::torrents::tee::key_of(source));
        let job_moves = JobMoves::of(moving.job_id.0);
        let move_file = |from: &Path, to: &Path| job_moves.move_file(from, to);
        let own: HashSet<PathBuf> = names.iter().cloned().collect();
        let move_outputs = |only: &dyn Fn(&Path) -> bool| {
            crate::torrents::tee::move_outputs(dir, &to, &own, moving.outputs, only, move_file)
        };

        let mut first = move_outputs(&in_way);
        let (mut listed, mut rest, mut stranded) = (None, None, Vec::new());
        let mut failed = first.failed.take();
        if failed.is_none() {
            match relocate::move_files(dir, &moves, moving.existing) {
                Ok(moved) => listed = Some(moved),
                Err(error) => {
                    stranded = error.stranded;
                    failed = Some(error.error);
                }
            }
        }
        if failed.is_none() {
            let mut placed = move_outputs(&|source| !in_way(source));
            failed = placed.failed.take();
            rest = Some(placed);
        }
        // The folders at the new place that were there before the move.
        let found = first
            .there
            .iter()
            .chain(rest.iter().flat_map(|rest| &rest.there));
        let found: Vec<String> = found
            .map(|folder| folder.to_string_lossy().into_owned())
            .collect();
        let dirs = with(&dirs, found);
        let ended = match failed {
            Some(error) => Err(error),
            None => {
                // The outputs under the names they are at now, in the
                // order the row had them.
                let now: HashMap<&Path, &String> = first
                    .sources()
                    .zip(&first.names)
                    .chain(rest.iter().flat_map(|rest| rest.sources().zip(&rest.names)))
                    .collect();
                let outputs: Vec<String> = moving
                    .outputs
                    .iter()
                    .filter_map(|output| now.get(dir.join(output).as_path()).copied().cloned())
                    .collect();
                let tops = held_tops(named.iter().chain(&outputs).map(String::as_str));
                let kept = listed.as_ref().map_or(&[][..], |listed| &listed.kept);
                let moved = Moved {
                    to: to.clone(),
                    kept: kept.iter().map(|at| named[*at].clone()).collect(),
                    dirs: dirs.clone(),
                    outputs,
                    held: held_of(moving.storage, Some(&to.to_string_lossy()), tops.as_ref()),
                };
                record(&moved).map(|()| moved).map_err(|error| MoveError {
                    path: to.clone(),
                    source: std::io::Error::other(format!(
                        "the move could not be recorded ({error})"
                    )),
                })
            }
        };
        match ended {
            Ok(moved) => {
                // The folders the outputs left empty go, as those the
                // torrent's own files left did.
                let left = first.sources().chain(rest.iter().flat_map(Placed::sources));
                relocate::remove_emptied(dir, left);
                Ok((to.clone(), moved))
            }
            Err(error) => {
                // The last moved first, each by the move that brought it:
                // the outputs that moved after the torrent's own files,
                // those files, then the outputs that moved out of their
                // way. The folders made for them at the new place go when
                // that leaves them empty.
                let there: HashSet<PathBuf> = dirs.iter().map(|folder| to.join(folder)).collect();
                let mut stayed = HashSet::new();
                let mut back = |outputs: Placed| {
                    let made: Vec<PathBuf> = outputs.targets().map(Path::to_path_buf).collect();
                    stayed.extend(
                        outputs
                            .put_back(move_file)
                            .into_iter()
                            .map(|(from, _)| from),
                    );
                    relocate::remove_emptied_but(&to, made.iter().map(PathBuf::as_path), &there);
                };
                if let Some(rest) = rest {
                    back(rest);
                }
                if let Some(listed) = listed {
                    stranded.extend(listed.put_back(&to, &moves, &there));
                }
                back(first);
                Err(NotMoved {
                    error,
                    stranded: stranded.iter().map(|at| named[*at].clone()).collect(),
                    outputs: moving
                        .outputs
                        .iter()
                        .filter(|output| !stayed.contains(&dir.join(output)))
                        .cloned()
                        .collect(),
                })
            }
        }
    };
    store.relocate(moved, shut)
}
