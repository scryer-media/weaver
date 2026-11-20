// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{
    ActionError, TorrentService, in_table_order, state_error, tidy_trackers, tracker_tiers,
};
use crate::jobs::JobId;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::{NamedBy, PeerInfo, TorrentTracker, TrackerEntry};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;
use weaver_torrent::tracker::redacted_tracker_url;

// Refuse an edit naming a tracker that is no URL.
fn tracker_urls(trackers: &[TrackerEntry]) -> Result<(), ActionError> {
    trackers
        .iter()
        .map(|tracker| tracker.url.trim())
        .filter(|url| !url.is_empty())
        .try_for_each(|url| {
            weaver_torrent::Url::parse(url).map(drop).map_err(|error| {
                ActionError::Invalid(format!("tracker {}: {error}", redacted_tracker_url(url)))
            })
        })
}

// Ban each of `ips` from the torrent, in its session and in the peers and
// bans its next start begins with.
fn ban(torrent: &mut super::Live, ips: &[IpAddr]) {
    match torrent
        .attached
        .as_ref()
        .and_then(|attached| attached.handle.ban_peers(ips))
    {
        Some(peers) => torrent.resume = peers,
        // In no session, or stopped by an error: the bans join the list its
        // next start begins with.
        None => ips.iter().for_each(|ip| torrent.resume.ban(*ip)),
    }
}

// The trackers `edit` makes of the torrent's list, as they are kept, or why
// the edit is refused. One held because its stored secrets do not decrypt
// lists no tracker it could not open: an edit would write the list back
// without them, so none is taken until the key is back.
fn edited(
    torrent: &super::Live,
    edit: impl FnOnce(Vec<TrackerEntry>) -> Result<Vec<TrackerEntry>, ActionError>,
) -> Result<Vec<TrackerEntry>, ActionError> {
    if let Some(why) = &torrent.sealed {
        return Err(ActionError::Invalid(format!(
            "the torrent's trackers are not changed while it is held: {why}"
        )));
    }
    let listed = &torrent.trackers;
    let trackers = edit(listed.to_vec())?;
    tracker_urls(&trackers)?;
    Ok(named_as_listed(listed, tidy_trackers(trackers)))
}

// `trackers` without those whose URL is one of `urls`, trimmed.
pub fn without_urls(mut trackers: Vec<TrackerEntry>, urls: &[String]) -> Vec<TrackerEntry> {
    let gone: HashSet<&str> = urls.iter().map(|url| url.trim()).collect();
    trackers.retain(|tracker| !gone.contains(tracker.url.as_str()));
    trackers
}

// Each of `trackers` as whoever first named its URL in `listed`, and as the
// operator when `listed` does not name it.
pub fn named_as_listed(listed: &[TrackerEntry], trackers: Vec<TrackerEntry>) -> Vec<TrackerEntry> {
    let mut named: HashMap<&str, NamedBy> = HashMap::with_capacity(listed.len());
    for known in listed {
        named.entry(known.url.as_str()).or_insert(known.named_by);
    }
    trackers
        .into_iter()
        .map(|tracker| TrackerEntry {
            named_by: named
                .get(tracker.url.as_str())
                .copied()
                .unwrap_or(NamedBy::Operator),
            ..tracker
        })
        .collect()
}

impl TorrentService {
    /// The torrent's connections to peers, by address (libtorrent
    /// `torrent_handle::get_peer_info`); none while it is in no session.
    pub async fn peers(&self, job_id: JobId) -> Result<Vec<PeerInfo>, ActionError> {
        Ok(self
            .handle_of(job_id)
            .await?
            .map(|handle| handle.peers())
            .unwrap_or_default())
    }

    /// Ban each of `ips` from the torrent: its connections close now and
    /// none is made or taken from then on (libtorrent `torrent::ban_peer`).
    /// The bans are written with the peers and bans the torrent's next
    /// start begins with, so they outlive a restart.
    pub async fn ban_peers(&self, job_id: JobId, ips: &[IpAddr]) -> Result<(), ActionError> {
        let now = self.inner.clock.unix_ms();
        let landing = self
            .with_torrent(job_id, |torrent| {
                ban(torrent, ips);
                Ok(self.queue_resume(torrent, now, true))
            })
            .await?;
        self.landed(landing).await.map_err(ActionError::State)
    }

    /// `ban_peers` on each of `job_ids` under one hold of the torrents, the
    /// writes of their ledgers and peers queued together and then awaited.
    /// A torrent not here is skipped; the first write that failed is the
    /// error.
    pub async fn ban_peers_many(&self, job_ids: &[JobId], ips: &[IpAddr]) -> Result<(), String> {
        let now = self.inner.clock.unix_ms();
        self.ensure(job_ids).await;
        let landings: Vec<_> = {
            let mut live = self.inner.live.lock().await;
            job_ids
                .iter()
                .filter_map(|job_id| {
                    let torrent = live.get_mut(job_id)?;
                    ban(torrent, ips);
                    self.queue_resume(torrent, now, true)
                })
                .collect()
        };
        let mut failed = Ok(());
        for landing in landings {
            if let Err(error) = self.landed(Some(landing)).await {
                failed = failed.and(Err(error));
            }
        }
        failed
    }

    /// The torrent's trackers, each with how announcing to it went.
    pub async fn trackers(&self, job_id: JobId) -> Result<Vec<TorrentTracker>, ActionError> {
        self.torrent(job_id)
            .await
            .map(|view| view.trackers)
            .ok_or(ActionError::NotFound(job_id))
    }

    /// Add `tiers`, which the operator names, after the torrent's last tier,
    /// leaving out the trackers it already lists.
    pub async fn add_trackers(
        &self,
        job_id: JobId,
        tiers: Vec<Vec<String>>,
    ) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |mut trackers| {
            let next = trackers
                .iter()
                .map(|tracker| tracker.tier + 1)
                .max()
                .unwrap_or(0);
            let added = tiers.into_iter().filter(|tier| !tier.is_empty());
            for (tier, urls) in (next..).zip(added) {
                trackers.extend(urls.into_iter().map(|url| TrackerEntry {
                    tier,
                    url,
                    named_by: NamedBy::Operator,
                }));
            }
            Ok(trackers)
        })
        .await
    }

    /// List `urls`, which the operator names, in their tiers, in place of
    /// the torrent's own: announces to a tracker it no longer lists stop.
    pub async fn replace_trackers(
        &self,
        job_id: JobId,
        urls: Vec<(u32, String)>,
    ) -> Result<(), ActionError> {
        let trackers = urls
            .into_iter()
            .map(|(tier, url)| TrackerEntry {
                tier,
                url,
                named_by: NamedBy::Operator,
            })
            .collect();
        self.edit_trackers(job_id, |_| Ok(trackers)).await
    }

    /// Stop listing each of `urls`.
    pub async fn remove_trackers(&self, job_id: JobId, urls: &[String]) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |trackers| Ok(without_urls(trackers, urls)))
            .await
    }

    // Write the trackers `edit` makes of the torrent's list and hand them
    // to its session. A tracker the list already held keeps who named it;
    // the operator names any other. The rule they match is picked again; no
    // rule is stored, a restart picks it from the trackers written here.
    async fn edit_trackers(
        &self,
        job_id: JobId,
        edit: impl FnOnce(Vec<TrackerEntry>) -> Result<Vec<TrackerEntry>, ActionError>,
    ) -> Result<(), ActionError> {
        self.ensure(&[job_id]).await;
        {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let trackers = edited(torrent, edit)?;
            // The write runs off the runtime; the list stays held, so no
            // other change of this torrent lands between it and its own.
            let db = self.inner.db.clone();
            let written = trackers.clone();
            let stored =
                tokio::task::spawn_blocking(move || db.set_torrent_trackers(job_id, &written))
                    .await
                    .map_err(|error| ActionError::State(error.to_string()))?
                    .map_err(|error| ActionError::State(state_error(error)))?;
            if !stored {
                return Err(ActionError::NotFound(job_id));
            }
            self.take_trackers(torrent, trackers);
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// `add_trackers` on each of `job_ids`, as one unit (`edit_trackers_many`).
    pub async fn add_trackers_many(
        &self,
        job_ids: &[JobId],
        tiers: &[Vec<String>],
    ) -> super::batch::Outcome {
        self.edit_trackers_many(job_ids, |mut trackers| {
            let next = trackers
                .iter()
                .map(|tracker| tracker.tier + 1)
                .max()
                .unwrap_or(0);
            let added = tiers.iter().filter(|tier| !tier.is_empty());
            for (tier, urls) in (next..).zip(added) {
                trackers.extend(urls.iter().map(|url| TrackerEntry {
                    tier,
                    url: url.clone(),
                    named_by: NamedBy::Operator,
                }));
            }
            Ok(trackers)
        })
        .await
    }

    /// `remove_trackers` on each of `job_ids`, as one unit
    /// (`edit_trackers_many`).
    pub async fn remove_trackers_many(
        &self,
        job_ids: &[JobId],
        urls: &[String],
    ) -> super::batch::Outcome {
        self.edit_trackers_many(job_ids, |trackers| Ok(without_urls(trackers, urls)))
            .await
    }

    // `edit_trackers` of each torrent in turn, as one unit: one write of
    // their lists, then one pass of the queue and one publish. The first
    // edit refused ends them, as it ended them one at a time: those before
    // it are made and those after it are not. A torrent not here is passed
    // over, and one named twice is edited once.
    async fn edit_trackers_many(
        &self,
        job_ids: &[JobId],
        edit: impl Fn(Vec<TrackerEntry>) -> Result<Vec<TrackerEntry>, ActionError>,
    ) -> super::batch::Outcome {
        self.ensure(job_ids).await;
        let mut outcome = super::batch::Outcome::new();
        {
            let mut live = self.inner.live.lock().await;
            let mut seen = HashSet::with_capacity(job_ids.len());
            let mut edits = Vec::with_capacity(job_ids.len());
            for &job_id in job_ids {
                if !seen.insert(job_id) {
                    continue;
                }
                let Some(torrent) = live.get(&job_id) else {
                    outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                    continue;
                };
                match edited(torrent, &edit) {
                    Ok(trackers) => edits.push((job_id, trackers)),
                    Err(error) => {
                        outcome.insert(job_id, Err(error));
                        break;
                    }
                }
            }
            if edits.is_empty() {
                return outcome;
            }
            // Off the runtime, as one torrent's edit writes.
            let db = self.inner.db.clone();
            let written = edits.clone();
            let stored =
                match tokio::task::spawn_blocking(move || db.set_torrents_trackers(&written))
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|result| result.map_err(state_error))
                {
                    Ok(stored) => stored,
                    Err(error) => {
                        for (job_id, _) in edits {
                            outcome.insert(job_id, Err(ActionError::State(error.clone())));
                        }
                        return outcome;
                    }
                };
            for (job_id, trackers) in edits {
                match live.get_mut(&job_id) {
                    Some(torrent) if stored.contains(&job_id) => {
                        self.take_trackers(torrent, trackers);
                        outcome.insert(job_id, Ok(()));
                    }
                    _ => {
                        outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                    }
                }
            }
        }
        if outcome.values().any(Result::is_ok) {
            self.apply_queue().await;
            self.publish().await;
        }
        outcome
    }

    // The torrent lists `trackers`, as written: the rule they match is
    // picked again, and its session announces to them.
    fn take_trackers(&self, torrent: &mut super::Live, trackers: Vec<TrackerEntry>) {
        torrent.rule_id = self.rule_id(&trackers);
        if let Some(attached) = &torrent.attached {
            attached.handle.replace_trackers(tracker_tiers(&trackers));
        }
        torrent.trackers = in_table_order(trackers);
    }

    /// Announce to every tracker now, past the floor each set (libtorrent
    /// `torrent_handle::force_reannounce` with `ignore_min_interval`, as
    /// qBittorrent `TorrentImpl::forceReannounce` calls it). A stopped
    /// torrent announces nothing.
    pub async fn reannounce(&self, job_id: JobId) -> Result<(), ActionError> {
        if let Some(handle) = self.handle_of(job_id).await? {
            handle.force_reannounce(Duration::ZERO, None, true);
        }
        Ok(())
    }

    /// Announce to the tracker at `url` now, as [`Self::reannounce`] does to
    /// all of them.
    pub async fn reannounce_tracker(&self, job_id: JobId, url: &str) -> Result<(), ActionError> {
        let handle = self.handle_of(job_id).await?;
        let url = url.trim();
        let listed = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|torrent| torrent.trackers.iter().any(|tracker| tracker.url == url));
        if !listed {
            return Err(ActionError::Invalid(format!(
                "no tracker {}",
                redacted_tracker_url(url)
            )));
        }
        let url = weaver_torrent::Url::parse(url).map_err(|error| {
            ActionError::Invalid(format!("tracker {}: {error}", redacted_tracker_url(url)))
        })?;
        if let Some(handle) = handle {
            handle.force_reannounce(Duration::ZERO, Some(&url), true);
        }
        Ok(())
    }

    /// Look the torrent up on the DHT now (libtorrent
    /// `torrent_handle::force_dht_announce`).
    pub async fn dht_announce(&self, job_id: JobId) -> Result<(), ActionError> {
        if let Some(handle) = self.handle_of(job_id).await? {
            handle.force_dht_announce();
        }
        Ok(())
    }
}
