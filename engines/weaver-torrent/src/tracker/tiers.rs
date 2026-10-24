// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Edits to a torrent's tracker tiers (BEP 12): each tracker once, where it
//! is first named, and no tier left empty.

use std::collections::HashSet;

use url::Url;

use crate::core::net::NamedBy;

/// A tracker a torrent announces to, and who named it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Tracker {
    pub url: Url,
    #[serde(default)]
    pub named_by: NamedBy,
}

impl Tracker {
    /// A tracker that came with the torrent.
    pub fn torrent(url: Url) -> Self {
        Self {
            url,
            named_by: NamedBy::Torrent,
        }
    }

    /// A tracker the operator named.
    pub fn operator(url: Url) -> Self {
        Self {
            url,
            named_by: NamedBy::Operator,
        }
    }
}

/// `tiers` with every tracker kept where its URL is first named, as it was
/// named there, and no tier left empty.
pub fn tidy_tiers(tiers: impl IntoIterator<Item = Vec<Tracker>>) -> Vec<Vec<Tracker>> {
    let mut seen = HashSet::new();
    tiers
        .into_iter()
        .map(|tier| {
            tier.into_iter()
                .filter(|t| seen.insert(t.url.clone()))
                .collect::<Vec<_>>()
        })
        .filter(|tier| !tier.is_empty())
        .collect()
}

/// `current` with `added` after its last tier, each tier of `added` a tier
/// of its own, and a tracker `current` already has left where it is, as it
/// was named.
pub fn added_tiers(current: &[Vec<Tracker>], added: Vec<Vec<Tracker>>) -> Vec<Vec<Tracker>> {
    tidy_tiers(current.iter().cloned().chain(added))
}

/// `current` without the trackers at `removed`, a tier left empty dropped.
pub fn without_trackers(current: &[Vec<Tracker>], removed: &[Url]) -> Vec<Vec<Tracker>> {
    tidy_tiers(current.iter().map(|tier| {
        tier.iter()
            .filter(|t| !removed.contains(&t.url))
            .cloned()
            .collect()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(name: &str) -> Url {
        Url::parse(&format!("http://{name}/announce")).unwrap()
    }

    fn tiers(names: &[&[&str]]) -> Vec<Vec<Tracker>> {
        names
            .iter()
            .map(|tier| tier.iter().map(|n| Tracker::torrent(url(n))).collect())
            .collect()
    }

    #[test]
    fn added_trackers_follow_the_last_tier_and_are_not_repeated() {
        let current = tiers(&[&["a", "b"], &["c"]]);
        assert_eq!(
            added_tiers(&current, tiers(&[&["d", "a"], &["e"]])),
            tiers(&[&["a", "b"], &["c"], &["d"], &["e"]])
        );
        assert_eq!(added_tiers(&current, tiers(&[&["b"]])), current);
        // A tracker added again keeps who first named it.
        let again = vec![vec![Tracker::operator(url("a"))]];
        assert_eq!(added_tiers(&current, again), current);
    }

    #[test]
    fn removing_a_tiers_last_tracker_drops_the_tier() {
        let current = tiers(&[&["a", "b"], &["c"], &["d"]]);
        assert_eq!(
            without_trackers(&current, &[url("c"), url("a"), url("x")]),
            tiers(&[&["b"], &["d"]])
        );
    }

    #[test]
    fn a_tracker_named_twice_keeps_its_first_place() {
        assert_eq!(
            tidy_tiers(tiers(&[&["a"], &[], &["b", "a"]])),
            tiers(&[&["a"], &["b"]])
        );
    }
}
