// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Edits to a torrent's tracker tiers (BEP 12), as libtorrent's
//! `tracker_list` makes them.

use std::collections::HashSet;

use url::Url;

/// `tiers` with every tracker kept where it is first named and no tier left
/// empty (libtorrent `tracker_list::replace` drops a repeated URL).
pub fn tidy_tiers(tiers: impl IntoIterator<Item = Vec<Url>>) -> Vec<Vec<Url>> {
    let mut seen = HashSet::new();
    tiers
        .into_iter()
        .map(|tier| {
            tier.into_iter()
                .filter(|t| seen.insert(t.clone()))
                .collect::<Vec<_>>()
        })
        .filter(|tier| !tier.is_empty())
        .collect()
}

/// `current` with `added` after its last tier, each tier of `added` a tier
/// of its own, and a tracker `current` already has left where it is
/// (libtorrent `tracker_list::add_tracker` with each tier past the last).
pub fn added_tiers(current: &[Vec<Url>], added: Vec<Vec<Url>>) -> Vec<Vec<Url>> {
    tidy_tiers(current.iter().cloned().chain(added))
}

/// `current` without `removed`, a tier left empty dropped.
pub fn without_trackers(current: &[Vec<Url>], removed: &[Url]) -> Vec<Vec<Url>> {
    tidy_tiers(current.iter().map(|tier| {
        tier.iter()
            .filter(|t| !removed.contains(t))
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

    fn tiers(names: &[&[&str]]) -> Vec<Vec<Url>> {
        names
            .iter()
            .map(|tier| tier.iter().map(|n| url(n)).collect())
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
