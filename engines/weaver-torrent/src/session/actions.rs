// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::HashSet, path::Path, sync::Arc};

use crate::{
    FileInfos,
    api::TorrentIdOrHash,
    limits::LimitsConfig,
    piece_priority::FilePriority,
    storage::TorrentStorage,
    torrent_state::{ManagedTorrentHandle, ManagedTorrentState},
};
use anyhow::Context;
use tracing::{debug, error, info, warn};

use super::Session;

impl Session {
    pub async fn delete(&self, id: TorrentIdOrHash, delete_files: bool) -> anyhow::Result<()> {
        let id = match id {
            TorrentIdOrHash::Id(id) => id,
            TorrentIdOrHash::Hash(h) => self
                .db
                .read()
                .by_hash(h)
                .map(|(id, _)| id)
                .context("no such torrent in db")?,
        };
        let removed = self
            .db
            .write()
            .remove_torrent(id)
            .with_context(|| format!("torrent with id {id} did not exist"))?;

        if let Err(e) = removed.pause() {
            debug!("error pausing torrent before deletion: {e:#}")
        }

        let metadata = removed.metadata.load_full().expect("TODO");

        let storage = removed
            .with_state_mut(|s| match s.take() {
                ManagedTorrentState::Initializing(p) => p.files.take().ok(),
                ManagedTorrentState::Paused(p) => Some(p.files),
                ManagedTorrentState::Live(l) => l
                    .pause()
                    // inspect_err not available in 1.75
                    .map_err(|e| {
                        warn!(?id, "error pausing torrent: {e:#}");
                        e
                    })
                    .ok()
                    .map(|p| p.files),
                _ => None,
            })
            .map(Ok)
            .unwrap_or_else(|| {
                removed
                    .shared
                    .storage_factory
                    .create(removed.shared(), &metadata)
            });

        if let Some(p) = self.persistence.as_ref() {
            if let Err(e) = p.delete(id).await {
                error!(
                    ?id,
                    "error deleting torrent from persistence database: {e:#}"
                );
            } else {
                debug!(?id, "deleted torrent from persistence database")
            }
        }

        match (storage, delete_files) {
            (Err(e), true) => return Err(e).context("torrent deleted, but could not delete files"),
            (Ok(storage), true) => {
                debug!("will delete files");
                remove_files_and_dirs(&metadata.file_infos, &storage);
                if removed.shared().options.output_folder != self.output_folder
                    && let Err(e) = storage.remove_directory_if_empty(Path::new(""))
                {
                    warn!(
                        ?id,
                        "error removing {:?}: {e:#}",
                        removed.shared().options.output_folder
                    )
                }
            }
            (_, false) => {
                debug!("not deleting files")
            }
        };

        info!(id, "deleted torrent");
        Ok(())
    }

    async fn try_update_persistence_metadata(&self, handle: &ManagedTorrentHandle) {
        if let Some(p) = self.persistence.as_ref()
            && let Err(e) = p.update_metadata(handle.id(), handle).await
        {
            warn!(storage=?p, error=?e, "error updating metadata")
        }
    }

    pub async fn pause(&self, handle: &ManagedTorrentHandle) -> anyhow::Result<()> {
        handle.pause()?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    pub async fn unpause(self: &Arc<Self>, handle: &ManagedTorrentHandle) -> anyhow::Result<()> {
        let peer_rx = self.peer_source_managed_torrent(handle, true);
        handle.start(peer_rx, false)?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    /// Forget every piece of the torrent and check them all again, trusting
    /// no checkpoint or saved state (libtorrent `torrent::force_recheck`).
    /// A running torrent runs again once checked; a paused one stays paused.
    /// Nothing happens while it is being checked already.
    pub async fn force_recheck(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
    ) -> anyhow::Result<()> {
        handle.recheck()?;
        let paused = handle.is_paused();
        let peer_rx = if paused {
            None
        } else {
            self.peer_source_managed_torrent(handle, true)
        };
        handle.start(peer_rx, paused)
    }

    /// [`ManagedTorrent::set_file_priorities`], and the session's saved
    /// state follows.
    pub async fn set_file_priorities(
        self: &Arc<Self>,
        handle: &ManagedTorrentHandle,
        priorities: &[(usize, FilePriority)],
    ) -> anyhow::Result<()> {
        handle.set_file_priorities(priorities)?;
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }

    /// Change a torrent's upload and download caps while it runs (see
    /// `ManagedTorrent::set_ratelimits`) and persist them.
    pub async fn update_ratelimits(
        &self,
        handle: &ManagedTorrentHandle,
        limits: LimitsConfig,
    ) -> anyhow::Result<()> {
        handle.set_ratelimits(limits);
        self.try_update_persistence_metadata(handle).await;
        Ok(())
    }
}

fn remove_files_and_dirs(infos: &FileInfos, files: &dyn TorrentStorage) {
    let mut all_dirs = HashSet::new();
    for (id, fi) in infos.iter().enumerate() {
        if fi.attrs.padding {
            continue;
        }
        let mut fname = &*fi.relative_filename;
        if let Err(e) = files.remove_file(id, fname) {
            warn!(?fi.relative_filename, error=?e, "could not delete file");
        } else {
            debug!(?fi.relative_filename, "deleted the file")
        }
        while let Some(parent) = fname.parent() {
            if parent != Path::new("") {
                all_dirs.insert(parent);
            }
            fname = parent;
        }
    }

    let all_dirs = {
        let mut v = all_dirs.into_iter().collect::<Vec<_>>();
        v.sort_unstable_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
        v
    };
    for dir in all_dirs {
        if let Err(e) = files.remove_directory_if_empty(dir) {
            warn!("error removing {dir:?}: {e:#}");
        } else {
            debug!("removed {dir:?}")
        }
    }
}
