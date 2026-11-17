// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{
    ActionError, TorrentService, in_table_order, state_error, tidy_trackers, tracker_tiers,
};
use crate::jobs::JobId;
use crate::torrents::{NamedBy, PeerInfo, TorrentTracker, TrackerEntry};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;

// Refuse an edit naming a tracker that is no URL.
fn tracker_urls(trackers: &[TrackerEntry]) -> Result<(), ActionError> {
    trackers
        .iter()
        .map(|tracker| tracker.url.trim())
        .filter(|url| !url.is_empty())
        .try_for_each(|url| {
            weaver_torrent::Url::parse(url)
                .map(drop)
                .map_err(|error| ActionError::Invalid(format!("tracker {url}: {error}")))
        })
}

// `trackers` without those whose URL is one of `urls`, trimmed.
pub(super) fn without_urls(mut trackers: Vec<TrackerEntry>, urls: &[String]) -> Vec<TrackerEntry> {
    let gone: HashSet<&str> = urls.iter().map(|url| url.trim()).collect();
    trackers.retain(|tracker| !gone.contains(tracker.url.as_str()));
    trackers
}

// Each of `trackers` as whoever first named its URL in `listed`, and as the
// operator when `listed` does not name it.
pub(super) fn named_as_listed(
    listed: &[TrackerEntry],
    trackers: Vec<TrackerEntry>,
) -> Vec<TrackerEntry> {
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
                match torrent
                    .attached
                    .as_ref()
                    .and_then(|attached| attached.handle.ban_peers(ips))
                {
                    Some(peers) => torrent.resume = peers,
                    // In no session, or stopped by an error: the bans join
                    // the list its next start begins with.
                    None => ips.iter().for_each(|ip| torrent.resume.ban(*ip)),
                }
                Ok(self.queue_resume(torrent, now, true))
            })
            .await?;
        self.landed(landing).await.map_err(ActionError::State)
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
            // One held because its stored secrets do not decrypt lists no
            // tracker it could not open: an edit would write the list back
            // without them, so none is taken until the key is back.
            if let Some(why) = &torrent.sealed {
                return Err(ActionError::Invalid(format!(
                    "the torrent's trackers are not changed while it is held: {why}"
                )));
            }
            let listed = &torrent.trackers;
            let trackers = edit(listed.to_vec())?;
            tracker_urls(&trackers)?;
            let trackers = named_as_listed(listed, tidy_trackers(trackers));
            let stored = self
                .inner
                .db
                .set_torrent_trackers(job_id, &trackers)
                .map_err(|error| ActionError::State(state_error(error)))?;
            if !stored {
                return Err(ActionError::NotFound(job_id));
            }
            torrent.rule_id = self.rule_id(&trackers);
            if let Some(attached) = &torrent.attached {
                attached.handle.replace_trackers(tracker_tiers(&trackers));
            }
            torrent.trackers = in_table_order(trackers);
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
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
            return Err(ActionError::Invalid(format!("no tracker {url}")));
        }
        let url = weaver_torrent::Url::parse(url)
            .map_err(|error| ActionError::Invalid(format!("tracker {url}: {error}")))?;
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
