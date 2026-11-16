// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A list picked from the torrents' records gives the views that filtering
// and paging every torrent's view gives.

use super::*;
use crate::torrents::service::listing::{CategoryPick, FileTotals, Pick, Shown, Window, page};
use crate::torrents::service::updates::Follow;

const TORRENTS: usize = 120;

// TORRENTS paused torrents: every other in "tv", every third private, two
// added at each instant so that the order falls back to the job id.
async fn seeded(rig: &Rig, clock: &ManualClock) -> Vec<JobId> {
    rig.service.open().await.unwrap();
    let mut jobs = Vec::with_capacity(TORRENTS);
    for at in 0..TORRENTS {
        clock.set_unix_ms(EPOCH_MS + (TORRENTS - at / 2) as i64 * 1000);
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
    clock.set_unix_ms(EPOCH_MS + 10_000_000);
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
            .await
            .unwrap();
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
        .await
        .unwrap();
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
                    Window::Every,
                    Shown::WHOLE,
                )
                .await
                .unwrap();
            assert!(!old.is_empty() || private == Some(true));
            assert_eq!(picked, old, "private {private:?}, category {category:?}");
        }
    }

    // A view without its trackers and tags is the listed view without them.
    let bare = rig
        .service
        .picked(Pick::default(), Window::Every, Shown::BARE)
        .await
        .unwrap();
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
                .await
                .unwrap(),
            rig.service.torrent(view.job_id).await
        );
    }
    assert_eq!(rig.service.torrent_by_hash("nothex").await.unwrap(), None);
    assert_eq!(
        rig.service.torrent_by_hash(&"cd".repeat(20)).await.unwrap(),
        None
    );
    rig.service.stop().await;
}

// The table cannot be read: a list that reads it fails, never answering an
// empty list or no torrent, and a list of every torrent, which reads no
// table, still answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_that_cannot_read_the_table_fails() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(metainfo(
                "t",
                &[("t/a.bin", &[7; 3000])],
                1024,
                false,
            )))
        })
        .await
        .unwrap();
    settled(&rig.service, 1).await;
    let hash = rig.service.torrent(job_id).await.unwrap().infohash;
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "ALTER TABLE torrents RENAME TO torrents_away",
                &[] as &[SqlArg],
            )
            .await?;
            Ok(())
        })
        .unwrap();

    assert!(
        rig.service
            .picked(Pick::default(), page(0, 10), Shown::LISTED)
            .await
            .is_err()
    );
    let named = [hash.as_str()];
    let by_hashes = Pick {
        hashes: Some(&named),
        ..Pick::default()
    };
    assert!(
        rig.service
            .picked_rows(by_hashes, Window::Every, Shown::LISTED)
            .await
            .is_err()
    );
    assert!(rig.service.torrent_by_hash(&hash).await.is_err());
    let every = rig
        .service
        .picked(Pick::default(), Window::Every, Shown::BARE)
        .await
        .unwrap();
    assert_eq!(ids(&every), vec![job_id]);
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
        clock.set_unix_ms(EPOCH_MS + at as i64 * 1000);
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
        .picked(Pick::default(), Window::Every, Shown::WHOLE)
        .await
        .unwrap();
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
        .picked_rows(Pick::default(), Window::Every, Shown::LISTED)
        .await
        .unwrap();
    assert_eq!(rows.len(), whole.len());
    for ((view, totals), old) in rows.iter().zip(&whole) {
        assert_eq!(*view, without(old));
        assert_eq!(*totals, FileTotals::of(old), "{}", old.name);
        assert_eq!(view.paths_as(totals.one_file), old.paths());
    }
    // With its files asked for, the row is the whole view.
    let rows = rig
        .service
        .picked_rows(Pick::default(), Window::Every, Shown::WHOLE)
        .await
        .unwrap();
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
                .await
                .unwrap();
            let rows = rig
                .service
                .picked_rows(pick, page(offset, first), Shown::LISTED)
                .await
                .unwrap();
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

// The table's pick over thousands of torrents gives the ids the walk of
// every torrent's record gave, in the same order, for every category match,
// named hashes past one statement's binds, privacy and window. Ties in the
// time added fall back to the job id; some torrents go by a v2 hash alone,
// some sit in a category the settings no longer name. A third carry a
// private metainfo, a third a public one and a third are magnets with none,
// whose privacy the table holds as the add and the metadata's arrival write
// it, and the walk reads as the restore reads it from the metainfo.
#[test]
fn the_tables_pick_is_the_walk_of_every_record() {
    picks_are_the_walk_of_every_record(&Database::open_in_memory().unwrap());
}

// The same on PostgreSQL, in a schema of its own on the server
// `WEAVER_TEST_POSTGRES_URL` names; skipped when it names none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tables_pick_is_the_walk_of_every_record_on_postgres() {
    use crate::persistence::database_target::DatabaseTarget;
    let Some(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping the torrent list on PostgreSQL; WEAVER_TEST_POSTGRES_URL is not set");
        return;
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("weaver_torrent_list_{}_{suffix}", std::process::id());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    tokio::task::spawn_blocking(move || {
        let db = Database::open_target(DatabaseTarget::PostgresUrl(url)).unwrap();
        picks_are_the_walk_of_every_record(&db);
    })
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await
    .unwrap();
}

fn picks_are_the_walk_of_every_record(db: &Database) {
    use crate::schema_migrations::torrent_private_v125::tiny_metainfo;
    use crate::torrents::service::metainfo_is_private;
    const SEEDED: u64 = 3000;
    let category = |id: u32, name: &str| CategoryConfig {
        upload_cap: Default::default(),
        id,
        name: name.into(),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route: true,
        unpack: None,
    };
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: "/complete".into(),
        // A second category of id 1 is never the one its torrents are in.
        categories: vec![
            category(1, "tv"),
            category(2, "TV/hd"),
            category(3, "movies"),
            category(1, "shadowed"),
            category(4, ""),
        ],
        seeding: None,
        torrent: TorrentConfig::default(),
    };
    let mut live = BTreeMap::new();
    for job_id in 1..=SEEDED {
        let mut v1 = [0u8; 20];
        v1[..8].copy_from_slice(&job_id.to_be_bytes());
        let mut v2 = [0u8; 32];
        v2[..8].copy_from_slice(&job_id.to_be_bytes());
        v2[31] = 7;
        let v2_only = job_id % 7 == 0;
        let record = TorrentRecord {
            unpack_in_force: None,
            job_id: JobId(job_id),
            infohash_v1: (!v2_only).then_some(v1),
            infohash_v2: (v2_only || job_id % 5 == 0).then_some(v2),
            name: format!("t{job_id}"),
            source: TorrentSource::Magnet,
            metainfo: (job_id % 3 != 2)
                .then(|| tiny_metainfo(&format!("t{job_id}"), job_id % 3 == 0).into()),
            category_id: [None, Some(1), Some(2), Some(3), Some(9), Some(4)][job_id as usize % 6],
            // Three at each instant, added out of job order.
            added_at_ms: EPOCH_MS + ((SEEDED - job_id) / 3) as i64,
            kept: KeptIntent::Intent(TorrentIntent::Paused),
            forced: false,
            completed_at_ms: None,
            storage_dir: None,
            policy_override: None,
            unpack: None,
            metadata: Vec::new(),
            upload_limit: None,
            download_limit: None,
            sequential: false,
            first_last_piece_first: false,
            save_path: None,
            content_layout: None,
            files_dir: None,
            seed_mode: false,
            move_to: None,
            queue_position: job_id as i64,
            pool_member: None,
            private: false,
        };
        // The privacy the add writes: what the metainfo reads as.
        let record = TorrentRecord {
            private: record.metainfo.as_deref().is_some_and(metainfo_is_private),
            ..record
        };
        db.insert_torrent(&record, &[]).unwrap();
        let mut torrent = Live::new(
            record,
            TorrentCounters::default(),
            ResumePeers::default(),
            EPOCH_MS,
        );
        // The privacy the restore reads from the metainfo; a magnet with
        // none is public.
        torrent.private = torrent.record.metainfo.as_deref().is_some_and(|bytes| {
            crate::torrents::service::intake::parse(bytes).is_ok_and(|parsed| parsed.private)
        });
        assert_eq!(torrent.private, job_id % 3 == 0);
        live.insert(JobId(job_id), torrent);
    }
    let hex = |live: &Live| hashes_of(&live.record).best().as_string();
    let some: Vec<String> = live.values().step_by(11).map(hex).collect();
    // More hashes than one statement binds: 900 binds hold 300.
    let many: Vec<String> = live
        .values()
        .step_by(2)
        .map(|torrent| hex(torrent).to_uppercase())
        .chain(["nothex".to_owned(), String::new(), "ab".repeat(20)])
        .chain(live.values().take(40).map(hex))
        .collect();
    let some: Vec<&str> = some.iter().map(String::as_str).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(many.len() > 1000);

    let windows = || -> Vec<(String, Window)> {
        vec![
            ("every".into(), Window::Every),
            ("0+10".into(), page(0, 10)),
            ("1234+500".into(), page(1234, 500)),
            ("2990+50".into(), page(2990, 50)),
            ("5000+5".into(), page(5000, 5)),
            ("0+0".into(), page(0, 0)),
            ("0+max".into(), page(0, usize::MAX)),
            (
                "last 20".into(),
                Window::Counted(Box::new(|kept| kept.saturating_sub(20)..kept)),
            ),
        ]
    };
    let mut compared = 0;
    let mut nonempty = 0;
    for hashes in [None, Some(&some[..]), Some(&many[..])] {
        for category in [
            None,
            Some(CategoryPick::Named("TV")),
            Some(CategoryPick::Named("nothing")),
            Some(CategoryPick::WithSubcategories("tv")),
            Some(CategoryPick::WithSubcategories("")),
            Some(CategoryPick::WithSubcategories("movies")),
        ] {
            for private in [None, Some(true), Some(false)] {
                let pick = Pick {
                    hashes,
                    category,
                    private,
                };
                // The table's count is the walk's.
                assert_eq!(
                    db.count_torrent_list(&pick.filter(&settings)).unwrap(),
                    pick.paged(&settings, &live, Window::Every).len(),
                    "hashes {:?}, {category:?}, private {private:?}",
                    hashes.map(<[&str]>::len)
                );
                let (old, new) = (windows(), windows());
                for ((name, old), (_, new)) in old.into_iter().zip(new) {
                    let old = pick.paged(&settings, &live, old);
                    let new = pick.picked(&settings, db, new).unwrap().under(&live);
                    assert_eq!(
                        new,
                        old,
                        "{name}, hashes {:?}, {category:?}, private {private:?}",
                        hashes.map(<[&str]>::len)
                    );
                    nonempty += usize::from(!old.is_empty());
                    compared += 1;
                }
            }
        }
    }
    assert_eq!(compared, 3 * 6 * 3 * 8);
    assert!(nonempty > compared / 3, "{nonempty} of {compared}");
}

// A torrent removed whose history row waits for room is retiring: it is in
// no page and no count of the list, which pages over the live torrents as
// though it were gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retiring_torrent_is_in_no_page_and_no_count() {
    use crate::persistence::ArchiveNews;
    let clock = set_clock();
    let rig = rig(clock.clone());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let mut jobs = Vec::new();
    for at in 0..3_u8 {
        clock.set_unix_ms(EPOCH_MS + i64::from(at) * 1000);
        let name = format!("r{at}");
        let bytes = metainfo(
            &name,
            &[(&format!("{name}/a.bin"), &[at; 3000])],
            1024,
            false,
        );
        jobs.push(
            rig.service
                .add_opened(AddRequest {
                    paused: true,
                    ..request(AddSource::Metainfo(bytes))
                })
                .await
                .unwrap(),
        );
    }
    settled(&rig.service, 3).await;
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(jobs[1], false, true).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(jobs[1])));
    assert_eq!(
        rig.db.torrent_rows().unwrap(),
        (vec![jobs[0], jobs[2]], vec![jobs[1]])
    );

    let picked = rig
        .service
        .picked(Pick::default(), page(0, 10), Shown::LISTED)
        .await
        .unwrap();
    assert_eq!(ids(&picked), [jobs[0], jobs[2]]);
    let second = rig
        .service
        .picked(Pick::default(), page(1, 1), Shown::LISTED)
        .await
        .unwrap();
    assert_eq!(ids(&second), [jobs[2]]);
    assert_eq!(
        rig.db
            .count_torrent_list(&crate::torrents::queries::TorrentListFilter::default())
            .unwrap(),
        2
    );
    rig.service.stop().await;
}
