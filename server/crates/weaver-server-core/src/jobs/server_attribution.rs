//! Which servers actually served a job's articles.
//!
//! Weaver already knows, per article, which server answered: the download
//! result carries the serving index, and the completion path reads it for the
//! per-server metric counters. That fact was never kept per job, so nothing
//! could say afterwards that a backup provider went untouched, or that a
//! primary carried four articles in five.
//!
//! This is the per-job ledger for it. It is deliberately small and lossy in
//! one direction only: an article it fails to attribute is left out of every
//! count rather than charged to the wrong server, so a share computed from it
//! understates a server's contribution and never invents one.

use serde::{Deserialize, Serialize};

/// The most servers one job will account for.
///
/// A configuration reaching this many providers is already beyond what the
/// share display can render usefully, and the cap keeps a pathological config
/// from growing this per active job.
const MAX_TRACKED_SERVERS: usize = 32;

/// One server's contribution to a single job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobServerContribution {
    /// The durable server id, stable across pool rebuilds.
    #[serde(rename = "s")]
    pub server_id: u32,
    /// Articles this server landed for the job.
    #[serde(rename = "a")]
    pub articles: u32,
    /// Wire bytes credited to those articles, before decoding.
    #[serde(rename = "b")]
    pub wire_bytes: u64,
}

/// Per-job article and byte counts, keyed by durable server id.
///
/// A linear-scanned vector, not a map. Servers are few enough that the scan
/// stays inside one cache line, and this runs once per landed article: hashing
/// would cost more than the walk it replaces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobServerAttribution {
    contributions: Vec<JobServerContribution>,
}

impl JobServerAttribution {
    /// Credit one landed article to the server that served it.
    ///
    /// The hot path: a walk of at most a handful of entries and two adds.
    /// Counts saturate rather than wrap, because a wrong-way-round share is
    /// worse than a stuck one.
    pub fn note_article(&mut self, server_id: u32, wire_bytes: u64) {
        if let Some(existing) = self
            .contributions
            .iter_mut()
            .find(|entry| entry.server_id == server_id)
        {
            existing.articles = existing.articles.saturating_add(1);
            existing.wire_bytes = existing.wire_bytes.saturating_add(wire_bytes);
            return;
        }

        // A server this job has not seen before. Past the cap the job stops
        // tracking new servers rather than growing without bound; the ones it
        // already counts keep counting.
        if self.contributions.len() >= MAX_TRACKED_SERVERS {
            return;
        }

        self.contributions.push(JobServerContribution {
            server_id,
            articles: 1,
            wire_bytes,
        });
    }

    /// Every server that served this job, in first-contact order.
    pub fn contributions(&self) -> &[JobServerContribution] {
        &self.contributions
    }

    /// Articles this ledger attributed, which is at most the job's article
    /// count and never more.
    pub fn attributed_articles(&self) -> u64 {
        self.contributions
            .iter()
            .map(|entry| u64::from(entry.articles))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.contributions.is_empty()
    }

    /// The compact JSON written to the history row, or `None` when nothing was
    /// attributed and there is nothing worth storing.
    pub fn to_storage_json(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        serde_json::to_string(self).ok()
    }

    /// Read back a stored ledger. A row written by a newer version, or one
    /// corrupted in place, reads as no attribution rather than failing the
    /// history query it belongs to.
    pub fn from_storage_json(raw: &str) -> Self {
        serde_json::from_str(raw).unwrap_or_default()
    }

    pub fn into_contributions(self) -> Vec<JobServerContribution> {
        self.contributions
    }
}

/// Read a history row's stored attribution, if it has one.
///
/// A job finished before this was recorded stores nothing, and reads back as
/// no attribution — which the presentation must show as unknown, never as a
/// job that no server served.
pub fn contributions_from_storage(raw: Option<&str>) -> Vec<JobServerContribution> {
    raw.map(JobServerAttribution::from_storage_json)
        .map(JobServerAttribution::into_contributions)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_accumulate_per_server() {
        let mut attribution = JobServerAttribution::default();
        attribution.note_article(7, 100);
        attribution.note_article(7, 200);
        attribution.note_article(9, 50);

        assert_eq!(
            attribution.contributions(),
            &[
                JobServerContribution {
                    server_id: 7,
                    articles: 2,
                    wire_bytes: 300,
                },
                JobServerContribution {
                    server_id: 9,
                    articles: 1,
                    wire_bytes: 50,
                },
            ]
        );
        assert_eq!(attribution.attributed_articles(), 3);
    }

    #[test]
    fn tracking_stops_growing_past_the_cap_without_losing_known_servers() {
        let mut attribution = JobServerAttribution::default();
        for server_id in 0..MAX_TRACKED_SERVERS as u32 {
            attribution.note_article(server_id, 1);
        }
        attribution.note_article(9_999, 1);
        assert_eq!(attribution.contributions().len(), MAX_TRACKED_SERVERS);

        // A server already being counted keeps counting past the cap.
        attribution.note_article(0, 10);
        assert_eq!(attribution.contributions()[0].articles, 2);
        assert_eq!(attribution.contributions()[0].wire_bytes, 11);
    }

    #[test]
    fn an_empty_ledger_stores_nothing() {
        assert!(JobServerAttribution::default().to_storage_json().is_none());
    }

    #[test]
    fn storage_json_round_trips() {
        let mut attribution = JobServerAttribution::default();
        attribution.note_article(3, 4_096);
        attribution.note_article(3, 4_096);
        attribution.note_article(4, 1_024);

        let stored = attribution.to_storage_json().expect("a non-empty ledger");
        assert_eq!(JobServerAttribution::from_storage_json(&stored), attribution);
    }

    /// The stored shape is a persisted format: rows written by an older build
    /// must stay readable, so the short keys are pinned here deliberately.
    #[test]
    fn storage_json_keeps_the_compact_persisted_shape() {
        let mut attribution = JobServerAttribution::default();
        attribution.note_article(7, 4_096);
        attribution.note_article(7, 4_096);
        assert_eq!(
            attribution.to_storage_json().as_deref(),
            Some(r#"[{"s":7,"a":2,"b":8192}]"#)
        );
    }

    #[test]
    fn unreadable_storage_reads_as_no_attribution() {
        assert!(JobServerAttribution::from_storage_json("not json").is_empty());
        assert!(JobServerAttribution::from_storage_json("{\"s\":1}").is_empty());
    }

    #[test]
    fn counts_saturate_rather_than_wrap() {
        let mut attribution = JobServerAttribution::default();
        attribution.note_article(1, u64::MAX);
        attribution.note_article(1, 10);
        assert_eq!(attribution.contributions()[0].wire_bytes, u64::MAX);
    }
}
