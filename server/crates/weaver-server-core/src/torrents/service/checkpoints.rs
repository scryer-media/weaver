// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::live::Live;
use super::{TorrentService, state_error};
use crate::StateError;
use crate::jobs::JobId;
use crate::persistence::Database;
use crate::torrents::TorrentCounters;
use crate::torrents::store::TorrentStore;
use crate::torrents::tee::Tee;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::oneshot;
use weaver_torrent::ResumePeers;

// What the database holds of a torrent's ledger and peers, and when its
// peers were last taken.
pub(super) struct Ledger {
    // `None` while what it holds is not known: the next write is all of it.
    pub(super) written: Option<(TorrentCounters, ResumePeers)>,
    // When the peers were last taken from the session to be written.
    pub(super) peers_at_ms: i64,
    // When the ledger was last written: its times alone go unwritten until
    // TIMES_EVERY_MS has passed since.
    pub(super) times_at_ms: i64,
    // Set by a write that failed, so the next one writes all of it.
    pub(super) lost: Arc<AtomicBool>,
}

// A write of what changed in a torrent's ledger and peers.
pub(super) struct Resume {
    pub(super) counters: Option<TorrentCounters>,
    pub(super) peers: Option<ResumePeers>,
    pub(super) lost: Arc<AtomicBool>,
}

// A write of a torrent's checkpoint, with its ledger and peers when they
// changed, queued; `landed` is none when nothing changed.
pub(super) struct CheckpointSave {
    job_id: JobId,
    landed: Option<oneshot::Receiver<Result<(), StateError>>>,
    lost: Option<Arc<AtomicBool>>,
    // The store whose pieces went with it, told when the write failed.
    pieces: Option<Arc<TorrentStore>>,
}

// A write of a torrent's ledger and peers, queued.
pub(super) struct Landing {
    job_id: JobId,
    landed: oneshot::Receiver<Result<bool, StateError>>,
    lost: Arc<AtomicBool>,
}

impl TorrentService {
    // Write the checkpoints of `jobs`: each torrent's pieces with its
    // ledger. A tee checkpoints its job on its own, as a job of the
    // pipeline's does, once the bytes it took or their age make a write due.
    pub(super) async fn save_checkpoints(&self, jobs: BTreeSet<JobId>) {
        let now = self.inner.clock.now_ms();
        let saves: Vec<CheckpointSave> = {
            let mut live = self.inner.live.lock().await;
            jobs.into_iter()
                .filter_map(|job_id| self.queue_checkpoint(live.get_mut(&job_id)?, now))
                .collect()
        };
        for save in saves {
            self.checkpoint_landed(save).await;
        }
    }

    // Queue a write of what changed in the torrent's ledger and peers
    // (libtorrent `torrent::write_resume_data`), in the step that takes it.
    // The one writer of both: every ban, every peer list and every ledger
    // goes through here under the torrent's lock.
    pub(super) fn queue_resume(
        &self,
        torrent: &mut Live,
        now: i64,
        at_once: bool,
    ) -> Option<Landing> {
        let Resume {
            counters,
            peers,
            lost,
        } = torrent.take_resume(now, at_once)?;
        let job_id = torrent.record.job_id;
        let landed = self.queue_save(job_id, "torrent_resume", move |db| {
            db.save_torrent_resume(job_id, counters.as_ref(), peers.as_ref())
        });
        Some(Landing {
            job_id,
            landed,
            lost,
        })
    }

    // Wait for a write of a torrent's ledger and peers to land. One that
    // failed leaves what the database holds unknown, so the next writes
    // all of it.
    pub(super) async fn landed(&self, landing: Option<Landing>) -> Result<(), String> {
        let Some(Landing {
            job_id,
            landed,
            lost,
        }) = landing
        else {
            return Ok(());
        };
        let result = match landed.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(state_error(error)),
            Err(error) => Err(error.to_string()),
        };
        match &result {
            Ok(()) => self
                .inner
                .written
                .send_modify(|written| written.ledgers += 1),
            Err(error) => {
                lost.store(true, Ordering::Release);
                tracing::warn!(job_id = job_id.0, %error, "could not keep a torrent's ledger and peers");
            }
        }
        result
    }

    // Queue a save of a torrent's state on its job's lane of the database's
    // ordered writer (libtorrent sends every resume-data write of a torrent
    // through its disk job queue). The caller takes the state it saves in
    // the same step, with no await between, so saves land in the order
    // their states were taken: one taken later lands later, and the save of
    // a task stopped while it waits still lands before the next one. The
    // receiver has what the save returned once it has landed.
    pub(super) fn queue_save<T: Send + 'static>(
        &self,
        job_id: JobId,
        label: &'static str,
        save: impl FnOnce(&Database) -> Result<T, StateError> + Send + 'static,
    ) -> oneshot::Receiver<Result<T, StateError>> {
        let (landed, receiver) = oneshot::channel();
        if let Err(error) = self.inner.db.try_queue_job_write(job_id, label, move |db| {
            let _ = landed.send(save(db));
            Ok(())
        }) {
            tracing::warn!(job_id = job_id.0, label, error = %error, "could not queue a torrent's save");
        }
        receiver
    }

    // Queue a write of the pieces the torrent has, with what changed in its
    // ledger and peers as they stand now, in one transaction: what a crash
    // recovers is the pieces and the counters of one moment. The ledger is
    // taken in the step that queues it, as every write of it is. Its tee's
    // checkpoint is written apart from it, as the pipeline writes its
    // jobs': a share the tee's checkpoint lacks of a piece kept here goes to
    // it again from the files at the next start. None when the torrent has
    // no store.
    pub(super) fn queue_checkpoint(&self, torrent: &mut Live, now: i64) -> Option<CheckpointSave> {
        let job_id = torrent.record.job_id;
        let store = Arc::clone(torrent.store.as_ref()?);
        let pieces = store.unsaved();
        torrent.settle(now);
        let resume = torrent.take_resume(now, false);
        // Nothing changed since the last write: none is queued.
        if pieces.is_none() && resume.is_none() {
            return Some(CheckpointSave {
                job_id,
                landed: None,
                lost: None,
                pieces: None,
            });
        }
        let (counters, peers, lost) = resume.map_or((None, None, None), |resume| {
            (resume.counters, resume.peers, Some(resume.lost))
        });
        let wrote = pieces.is_some().then(|| Arc::clone(&store));
        let landed = self.queue_save(job_id, "torrent_checkpoint", move |db| {
            db.save_torrent_checkpoint(job_id, pieces.as_ref(), counters.as_ref(), peers.as_ref())
        });
        Some(CheckpointSave {
            job_id,
            landed: Some(landed),
            lost,
            pieces: wrote,
        })
    }

    // Queue a checkpoint of the torrent's unpacking on a blocking thread:
    // it waits on the job, which an intake writing to disk may hold. Its
    // write goes at once when `force`, a stop's, once every object out for
    // its work is back; otherwise once it is due.
    pub(super) async fn checkpoint_tee(&self, job_id: JobId, tee: Arc<Tee>, force: bool) {
        let queued = tokio::task::spawn_blocking(move || {
            if force {
                tee.checkpoint_stopped()
            } else {
                tee.checkpoint(false)
            }
        })
        .await;
        if let Ok(Err(error)) = queued {
            tracing::warn!(job_id = job_id.0, error = %error, "could not checkpoint a torrent's unpacking");
        }
    }

    // Wait for a checkpoint's write to land. One that failed leaves what
    // the database holds of the ledger unknown, so the next writes all of
    // it.
    pub(super) async fn checkpoint_landed(&self, save: CheckpointSave) {
        let CheckpointSave {
            job_id,
            landed,
            lost,
            pieces,
        } = save;
        let failed = match landed {
            None => None,
            Some(landed) => match landed.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            },
        };
        match (failed, lost) {
            (Some(error), lost) => {
                if let Some(lost) = lost {
                    lost.store(true, Ordering::Release);
                }
                if let Some(store) = pieces {
                    store.lost_saved();
                }
                tracing::warn!(job_id = job_id.0, %error, "could not checkpoint a torrent");
            }
            (None, Some(_)) => self
                .inner
                .written
                .send_modify(|written| written.ledgers += 1),
            (None, None) => {}
        }
        self.inner
            .written
            .send_modify(|written| written.checkpoints += 1);
    }
}
