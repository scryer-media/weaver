// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A list picked from the torrents' records gives the views that filtering
// and paging every torrent's view gives.

use super::*;
use crate::torrents::service::listing::{CategoryPick, FileTotals, Pick, Shown, page};
use crate::torrents::service::updates::Follow;

const TORRENTS: usize = 120;

// TORRENTS paused torrents: every other in "tv", every third private, two
// added at each instant so that the order falls back to the job id.
async fn seeded(rig: &Rig, clock: &SetClock) -> Vec<JobId> {
    rig.service.open().await.unwrap();
    let mut jobs = Vec::with_capacity(TORRENTS);
    for at in 0..TORRENTS {
        clock.0.store(
            EPOCH_MS + (TORRENTS - at / 2) as i64 * 1000,
            Ordering::SeqCst,
        );
        let name = format!("t{at}");
        let bytes = metainfo(
            &name,
            &[(&format!("{name}/a.bin"), &[(at % 251) as u8; 3000])],
            1024,
            at % 3 == 0,
        );
        let job_id = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                category: (at % 2 == 0).then(|| "tv".to_owned()),
                tags: if at % 5 == 0 {
                    vec!["hd".to_owned()]
                } else {
                    Vec::new()
                },
                ..request(AddSource::Metainfo(bytes))
            })
            .await
            .unwrap();
        jobs.push(job_id);
    }
    clock.0.store(EPOCH_MS + 10_000_000, Ordering::SeqCst);
    settled(&rig.service, TORRENTS).await;
    jobs
}

// Once each of `count` torrents is past checking what was saved of it, as
// a follower of every torrent hears it: a view read before then can say
// it is checking, and one read after says it is paused.
pub(super) async fn settled(service: &TorrentService, count: usize) {
    let mut updates = service.follow(Follow::Every, std::time::Duration::from_millis(10));
    let mut states = std::collections::HashMap::new();
    loop {
        let changes = updates.next().await;
        for view in changes.torrents {
            states.insert(view.job_id, view.state);
        }
        for job_id in changes.removed {
            states.remove(&job_id);
        }
        if states.len() == count
            && states
                .values()
                .all(|state| *state != TorrentState::CheckingResumeData)
        {
            return;
        }
    }
}

fn ids(views: &[TorrentView]) -> Vec<JobId> {
    views.iter().map(|view| view.job_id).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_picked_page_is_the_page_of_every_view() {
    let clock = set_clock();
    let rig = rig(clock.clone());
    seeded(&rig, &clock).await;
    let every = rig.service.torrents(None).await;
    let whole = rig.service.torrents_with_files().await;
    assert_eq!(every.len(), TORRENTS);
    assert_eq!(rig.service.count().await, TORRENTS);
    // The list is in the order of adding, not of job id.
    let mut by_id = ids(&every);
    by_id.sort_unstable();
    assert_ne!(ids(&every), by_id);

    for (offset, first) in [
        (0, 10),
        (7, 33),
        (110, 50),
        (200, 5),
        (0, 0),
        (0, usize::MAX),
    ] {
        let picked = rig
            .service
            .picked(Pick::default(), page(offset, first), Shown::LISTED)
            .await;
        let old: Vec<TorrentView> = every.iter().skip(offset).take(first).cloned().collect();
        assert_eq!(picked, old, "page {offset}+{first}");
    }

    // A category's torrents, as filtering the views by their category.
    let tv: Vec<TorrentView> = rig.service.torrents(Some("TV")).await;
    let old: Vec<TorrentView> = every
        .iter()
        .filter(|view| {
            view.category
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("tv"))
        })
        .cloned()
        .collect();
    assert_eq!(tv.len(), TORRENTS / 2);
    assert_eq!(tv, old);
    let picked = rig
        .service
        .picked(
            Pick {
                category: Some(CategoryPick::Named("Tv")),
                ..Pick::default()
            },
            page(5, 20),
            Shown::LISTED,
        )
        .await;
    assert_eq!(
        picked,
        old.iter().skip(5).take(20).cloned().collect::<Vec<_>>()
    );

    // Hashes, a category with its subcategories and privacy, as the
    // facade filtered every view with its files.
    let named: Vec<String> = whole
        .iter()
        .step_by(7)
        .map(|view| view.infohash.to_uppercase())
        .chain(["nothex".to_owned(), String::new(), "ab".repeat(20)])
        .collect();
    let named: Vec<&str> = named.iter().map(String::as_str).collect();
    for private in [None, Some(true), Some(false)] {
        for category in [None, Some(""), Some("tv")] {
            let old: Vec<TorrentView> = whole
                .iter()
                .filter(|view| {
                    named
                        .iter()
                        .any(|hash| view.infohash.eq_ignore_ascii_case(hash))
                        && private.is_none_or(|private| view.private == private)
                        && category.is_none_or(|name: &str| {
                            let category = view.category.as_deref().unwrap_or_default();
                            category.eq_ignore_ascii_case(name)
                                || category.get(..name.len() + 1).is_some_and(|parent| {
                                    parent.eq_ignore_ascii_case(&format!("{name}/"))
                                })
                        })
                })
                .cloned()
                .collect();
            let picked = rig
                .service
                .picked(
                    Pick {
                        hashes: Some(&named),
                        category: category.map(CategoryPick::WithSubcategories),
                        private,
                    },
                    |kept| 0..kept,
                    Shown::WHOLE,
                )
                .await;
            assert!(!old.is_empty() || private == Some(true));
            assert_eq!(picked, old, "private {private:?}, category {category:?}");
        }
    }

    // A view without its trackers and tags is the listed view without them.
    let bare = rig
        .service
        .picked(Pick::default(), |kept| 0..kept, Shown::BARE)
        .await;
    assert_eq!(rig.service.job_ids().await, ids(&every));
    let old: Vec<TorrentView> = every
        .iter()
        .cloned()
        .map(|view| TorrentView {
            trackers: Vec::new(),
            tags: Vec::new(),
            ..view
        })
        .collect();
    assert_eq!(bare, old);
    assert!(every.iter().any(|view| !view.tags.is_empty()));

    // One torrent by its hash, as `torrent` gives it.
    for view in whole.iter().step_by(11) {
        assert_eq!(
            rig.service
                .torrent_by_hash(&format!(" {} ", view.infohash.to_uppercase()))
                .await,
            rig.service.torrent(view.job_id).await
        );
    }
    assert_eq!(rig.service.torrent_by_hash("nothex").await, None);
    assert_eq!(rig.service.torrent_by_hash(&"cd".repeat(20)).await, None);
    rig.service.stop().await;
}

// A follower of a few torrents is given what every torrent's follower is
// given of those, and hears nothing of the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_followed_set_is_given_its_torrents_of_every_torrent() {
    use std::collections::BTreeSet;
    use std::time::Duration;

    let clock = set_clock();
    let rig = rig(clock.clone());
    let jobs = seeded(&rig, &clock).await;
    let followed: BTreeSet<JobId> = jobs
        .iter()
        .step_by(4)
        .copied()
        .chain([JobId(999_999)])
        .collect();
    let beat = Duration::from_millis(10);
    let every = rig.service.follow(Follow::Every, beat).next().await;
    let mut some = rig.service.follow(Follow::Some(followed.clone()), beat);
    let first = some.next().await;
    assert!(first.full_update);
    let old: Vec<TorrentView> = every
        .torrents
        .into_iter()
        .filter(|view| followed.contains(&view.job_id))
        .collect();
    assert_eq!(old.len(), TORRENTS / 4);
    assert_eq!(first.torrents, old);
    assert_eq!(first.torrents, rig.service.torrents_of(&followed).await);

    // A torrent it does not follow goes unheard; one it follows goes as
    // gone. What a removal moves in the others is told of those it follows.
    rig.service.remove(jobs[1], false, false).await.unwrap();
    rig.service.remove(jobs[4], false, false).await.unwrap();
    loop {
        let next = some.next().await;
        assert!(!next.full_update);
        assert!(
            next.torrents
                .iter()
                .all(|view| followed.contains(&view.job_id))
        );
        assert!(next.removed.iter().all(|job_id| *job_id == jobs[4]));
        if !next.removed.is_empty() {
            break;
        }
    }
    let now = rig.service.torrents_of(&followed).await;
    assert_eq!(now.len(), TORRENTS / 4 - 1);
    assert!(now.iter().all(|view| view.job_id != jobs[4]));
    rig.service.stop().await;
}

// A list's row read without its files gives the view read with them, its
// files left out, and the totals those files give: over torrents of one
// file named as the torrent, one file in a folder, and many files at mixed
// levels, every page and pick reading the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_reads_the_totals_its_files_give() {
    use FilePriority::*;
    let clock = set_clock();
    let rig = rig(clock.clone());
    rig.service.open().await.unwrap();
    let mut jobs = Vec::new();
    for at in 0..24usize {
        clock.0.store(EPOCH_MS + at as i64 * 1000, Ordering::SeqCst);
        let name = format!("r{at}");
        let paths: Vec<String> = match at % 3 {
            0 => vec![name.clone()],
            1 => vec![format!("{name}/only.bin")],
            _ => (0..=at % 7)
                .map(|file| format!("{name}/part{file}.bin"))
                .collect(),
        };
        let bodies: Vec<Vec<u8>> = (0..paths.len())
            .map(|file| vec![(at + file) as u8; 700 + 311 * file + at])
            .collect();
        let files: Vec<(&str, &[u8])> = paths
            .iter()
            .zip(&bodies)
            .map(|(path, body)| (path.as_str(), body.as_slice()))
            .collect();
        let job_id = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                ..request(AddSource::Metainfo(metainfo(&name, &files, 1024, false)))
            })
            .await
            .unwrap();
        if paths.len() > 2 {
            rig.service
                .set_file_priorities(job_id, &[(0, Ignored), (2, High)])
                .await
                .unwrap();
        } else if at % 4 == 0 {
            rig.service
                .set_file_priorities(job_id, &[(0, Ignored)])
                .await
                .unwrap();
        }
        jobs.push(job_id);
    }
    settled(&rig.service, jobs.len()).await;

    let whole = rig
        .service
        .picked(Pick::default(), |kept| 0..kept, Shown::WHOLE)
        .await;
    assert_eq!(whole.len(), jobs.len());
    // Every shape is there: one file named as the torrent, one file in a
    // folder, many files, and a file not fetched.
    assert!(whole.iter().any(TorrentView::is_one_file));
    assert!(
        whole
            .iter()
            .any(|view| view.files.len() == 1 && !view.is_one_file())
    );
    assert!(whole.iter().any(|view| view.files.len() > 2));
    assert!(
        whole
            .iter()
            .flat_map(|view| &view.files)
            .any(|file| !file.selected())
    );
    let without = |view: &TorrentView| TorrentView {
        files: Vec::new(),
        ..view.clone()
    };
    let rows = rig
        .service
        .picked_rows(Pick::default(), |kept| 0..kept, Shown::LISTED)
        .await;
    assert_eq!(rows.len(), whole.len());
    for ((view, totals), old) in rows.iter().zip(&whole) {
        assert_eq!(*view, without(old));
        assert_eq!(*totals, FileTotals::of(old), "{}", old.name);
        assert_eq!(view.paths_as(totals.one_file), old.paths());
    }
    // With its files asked for, the row is the whole view.
    let rows = rig
        .service
        .picked_rows(Pick::default(), |kept| 0..kept, Shown::WHOLE)
        .await;
    assert_eq!(
        rows.iter()
            .map(|(view, _)| view.clone())
            .collect::<Vec<_>>(),
        whole
    );
    // A page and a pick of hashes read the rows `picked` reads.
    let named: Vec<String> = whole
        .iter()
        .step_by(5)
        .map(|view| view.infohash.clone())
        .collect();
    let named: Vec<&str> = named.iter().map(String::as_str).collect();
    for pick in [
        Pick::default(),
        Pick {
            hashes: Some(&named),
            ..Pick::default()
        },
    ] {
        for (offset, first) in [(0, 5), (3, 100), (30, 2)] {
            let picked = rig
                .service
                .picked(pick, page(offset, first), Shown::WHOLE)
                .await;
            let rows = rig
                .service
                .picked_rows(pick, page(offset, first), Shown::LISTED)
                .await;
            assert_eq!(
                rows,
                picked
                    .iter()
                    .map(|view| (without(view), FileTotals::of(view)))
                    .collect::<Vec<_>>()
            );
        }
    }
    rig.service.stop().await;
}
