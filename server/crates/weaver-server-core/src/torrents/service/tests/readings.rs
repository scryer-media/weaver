// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Followers read the snapshot of every torrent's row: each is told from it
// what `changes` tells a follower that reads the views for itself, and an
// idle one costs no view and no statement.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::listing::{as_row, settled};
use super::*;
use crate::torrents::service::listing::FileTotals;
use crate::torrents::service::snapshot::{Snapshot, TorrentRow, TrackerSummary};
use crate::torrents::service::updates::{Follow, TorrentChanges};

const TORRENTS: u64 = 10_000;

// A view of one torrent, read from a rig, to copy.
async fn one_view() -> TorrentView {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("copied", 3))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    rig.service.stop().await;
    view
}

fn copy(view: &TorrentView, id: u64) -> TorrentView {
    TorrentView {
        job_id: JobId(id),
        name: format!("t{id}"),
        ratio: (id % 13) as f64,
        ..view.clone()
    }
}

// The views at each step: TORRENTS of them, then steady fields moved in
// some, torrents gone and new ones come, and steps where nothing moves.
fn steps(view: &TorrentView) -> Vec<Vec<TorrentView>> {
    let mut views: Vec<TorrentView> = (0..TORRENTS).map(|id| copy(view, id)).collect();
    let mut steps = vec![views.clone()];
    let mut next_id = TORRENTS;
    for step in 1..14u64 {
        match step % 4 {
            // Nothing moves.
            0 => {}
            1 => {
                for view in views.iter_mut().skip(step as usize).step_by(97) {
                    view.ratio += 0.5;
                }
                for view in views.iter_mut().skip(3).step_by(501) {
                    view.state = TorrentState::Paused;
                }
            }
            2 => {
                let gone = (step as usize * 31) % 9_000;
                views.drain(gone..gone + 40);
                for _ in 0..25 {
                    views.push(copy(view, next_id));
                    next_id += 1;
                }
            }
            _ => {
                for view in views.iter_mut().skip(step as usize).step_by(211) {
                    view.tags = vec![format!("tag{step}")];
                }
                // A clock field alone, which a list's follower leaves out.
                for view in views.iter_mut().step_by(7) {
                    view.active_seconds += 1;
                }
            }
        }
        steps.push(views.clone());
    }
    steps
}

// `changes` as a follower of `ids` reads it: its push, each torrent as a row
// carries it.
fn rows_told(changes: Option<TorrentChanges>) -> Option<TorrentChanges> {
    changes.map(|changes| TorrentChanges {
        torrents: changes.torrents.into_iter().map(as_row).collect(),
        ..changes
    })
}

// Followers of a few torrents on several schedules are told from the
// snapshot what `changes` tells each of them over the same views, read for
// itself (the path the snapshot replaces): one at every step, one at every
// other, one at every third, one from the fifth, over sets of up to 500 that
// lose torrents and name ones that come later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_snapshot_tells_each_follower_what_its_own_reading_would() {
    use fingerprint::ViewParts::Steady;

    let view = one_view().await;
    let steps = steps(&view);
    let snapshot = Snapshot::default();

    struct Follower {
        ids: BTreeSet<JobId>,
        from: usize,
        every: usize,
        old: Option<HashMap<JobId, u64>>,
        new: Option<HashMap<JobId, u64>>,
        told: usize,
    }
    let sets: [BTreeSet<JobId>; 4] = [
        (0..TORRENTS).step_by(20).map(JobId).collect(),
        (0..500).map(|id| JobId(id * 13 % TORRENTS)).collect(),
        (TORRENTS - 300..TORRENTS + 200).map(JobId).collect(),
        (0..TORRENTS)
            .skip(31)
            .step_by(37)
            .take(500)
            .map(JobId)
            .collect(),
    ];
    let mut followers: Vec<Follower> = sets
        .into_iter()
        .zip([(0, 1), (0, 2), (0, 3), (5, 1)])
        .map(|(ids, (from, every))| Follower {
            ids,
            from,
            every,
            old: None,
            new: None,
            told: 0,
        })
        .collect();
    assert!(followers.iter().all(|follower| follower.ids.len() <= 500));
    let mut moved_told = 0;
    for (step, views) in steps.iter().enumerate() {
        let rows: Vec<(TorrentView, FileTotals)> = views
            .iter()
            .map(|view| (view.clone(), FileTotals::default()))
            .collect();
        snapshot.replace_every(&rows);
        for follower in &mut followers {
            if step < follower.from || (step - follower.from) % follower.every != 0 {
                continue;
            }
            let own: Vec<TorrentView> = views
                .iter()
                .filter(|view| follower.ids.contains(&view.job_id))
                .cloned()
                .collect();
            let old = rows_told(updates::changes(&mut follower.old, own, Steady));
            let new = updates::changes_of_rows(&mut follower.new, &snapshot.rows_of(&follower.ids));
            assert_eq!(new, old, "step {step}, every {}", follower.every);
            if let Some(changes) = &new {
                follower.told += 1;
                moved_told += usize::from(!changes.full_update);
            }
        }
    }
    assert!(followers.iter().all(|follower| follower.told > 1));
    assert!(moved_told > 10);
}

// Twenty followers of a few torrents hear one tag change, read from the
// snapshot: no view is built for any of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn twenty_followers_hear_a_tag_change_with_no_view_built() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut jobs = Vec::new();
    for (name, seed) in [("one", 1), ("two", 2), ("three", 3)] {
        let job_id = rig
            .service
            .add_opened(AddRequest {
                paused: true,
                ..request(AddSource::Metainfo(plain(name, seed)))
            })
            .await
            .unwrap();
        jobs.push(job_id);
    }
    settled(&rig.service, jobs.len()).await;
    rig.service.publish().await;

    let ids: BTreeSet<JobId> = jobs.iter().copied().collect();
    let mut followers: Vec<_> = (0..20)
        .map(|_| rig.service.follow(Follow::Some(ids.clone())).unwrap())
        .collect();
    let every: Vec<TorrentView> = rig
        .service
        .torrents(None)
        .await
        .into_iter()
        .map(as_row)
        .collect();
    let built = rig.service.views_built();
    for follower in &mut followers {
        let first = follower.next().await;
        assert!(first.full_update);
        assert_eq!(first.torrents, every);
    }

    rig.db.add_job_tags(&[jobs[1]], ["shared"]).unwrap();
    for follower in &mut followers {
        let changes = follower.next().await;
        assert!(!changes.full_update);
        assert_eq!(changes.torrents.len(), 1);
        assert_eq!(changes.torrents[0].job_id, jobs[1]);
        assert_eq!(changes.torrents[0].tags, ["shared"]);
        assert!(changes.removed.is_empty());
    }
    assert_eq!(rig.service.views_built(), built);
    rig.service.stop().await;
}

// Whether `next` has nothing to give yet: polled once, it waits.
fn idle(next: impl std::future::Future<Output = TorrentChanges>) -> bool {
    let next = std::pin::pin!(next);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    next.poll(&mut context).is_pending()
}

// Stored paused magnets, `count` of them, written as the add writes them.
fn stored_magnets(db: &Database, count: u64) -> Vec<JobId> {
    (1..=count)
        .map(|job_id| {
            let record = TorrentRecord {
                job_id: JobId(job_id),
                infohash_v1: Some(Sha1::digest(job_id.to_le_bytes()).into()),
                infohash_v2: None,
                name: format!("t{job_id}"),
                source: TorrentSource::Magnet,
                metainfo: None,
                category_id: None,
                added_at_ms: EPOCH_MS + job_id as i64,
                kept: KeptIntent::Intent(TorrentIntent::Paused),
                forced: false,
                completed_at_ms: None,
                storage_dir: None,
                policy_override: None,
                unpack: None,
                unpack_in_force: None,
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
            db.insert_torrent(&record, &[]).unwrap();
            JobId(job_id)
        })
        .collect()
}

// With ten thousand torrents loaded and nothing changing, followers woken
// fifty times each build no view and take the database for no statement,
// and no row moves; one torrent changing builds its view alone, and is the
// one torrent its follower is told of.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_followers_build_no_view_and_read_nothing() {
    let rig = rig(set_clock());
    let jobs = stored_magnets(&rig.db, TORRENTS);
    rig.service.restore().await.unwrap();
    rig.service.publish().await;
    assert_eq!(rig.service.snapshot().len(), TORRENTS as usize);
    assert!(rig.service.listing_ready().is_ok());

    let pages: Vec<BTreeSet<JobId>> = jobs
        .chunks(500)
        .take(3)
        .map(|page| page.iter().copied().collect())
        .collect();
    let mut followers: Vec<_> = pages
        .iter()
        .map(|page| rig.service.follow(Follow::Some(page.clone())).unwrap())
        .collect();
    for follower in &mut followers {
        assert_eq!(follower.next().await.torrents.len(), 500);
    }

    let (views, statements) = (rig.service.views_built(), rig.db.datastore_takes());
    let revision = rig.service.snapshot().revision();
    for _ in 0..50 {
        for follower in &mut followers {
            assert!(idle(follower.next()));
        }
    }
    assert_eq!(rig.service.views_built(), views);
    assert_eq!(rig.db.datastore_takes(), statements);
    assert_eq!(rig.service.snapshot().revision(), revision);
    assert!(rig.service.snapshot().since(revision).moved.is_empty());

    // One torrent changes, and is published alone, as a settle publishes
    // the torrents that spoke.
    let changed = jobs[700];
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&changed)
        .unwrap()
        .record
        .forced = true;
    rig.service.publish_job(changed).await;
    assert_eq!(rig.service.views_built(), views + 1);
    let since = rig.service.snapshot().since(revision);
    assert_eq!(since.moved.len(), 1);
    assert_eq!(since.moved[0].view.job_id, changed);
    let told = followers[1].next().await;
    assert_eq!(
        told.torrents
            .iter()
            .map(|view| view.job_id)
            .collect::<Vec<_>>(),
        [changed]
    );
    assert!(told.torrents[0].forced);
    for (index, follower) in followers.iter_mut().enumerate() {
        if index != 1 {
            assert!(idle(follower.next()));
        }
    }
    assert_eq!(rig.service.views_built(), views + 1);
    rig.service.stop().await;
}

// The heap a row's view holds beyond its own size: its strings and lists.
fn heap_of(view: &TorrentView) -> usize {
    let text = |text: &String| text.capacity();
    let some = |text: &Option<String>| text.as_ref().map_or(0, String::capacity);
    text(&view.name)
        + text(&view.infohash)
        + text(&view.magnet)
        + some(&view.detail)
        + some(&view.category)
        + some(&view.created_by)
        + some(&view.comment)
        + some(&view.storage_dir)
        + some(&view.files_dir)
        + some(&view.seeding_left)
        + some(&view.tracker_rule)
        + some(&view.route)
        + view.files.capacity() * std::mem::size_of::<TorrentFile>()
        + view.file_have.capacity() * std::mem::size_of::<u64>()
        + view.trackers.capacity() * std::mem::size_of::<TorrentTracker>()
        + view.metadata.capacity() * std::mem::size_of::<(String, String)>()
        + view
            .metadata
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
        + view.tags.capacity() * std::mem::size_of::<String>()
        + view.tags.iter().map(String::capacity).sum::<usize>()
}

// A row holds the list fields alone: no files, no file bytes, no trackers,
// whatever its torrent has. Measures a row, and the rows of ten thousand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_holds_the_list_fields_only() {
    let view = one_view().await;
    assert!(!view.files.is_empty());
    let snapshot = Snapshot::default();
    let views: Vec<(TorrentView, FileTotals)> = (0..TORRENTS)
        .map(|id| (copy(&view, id), FileTotals::of(&view)))
        .collect();
    snapshot.replace_every(&views);
    drop(views);
    let (_, rows) = snapshot.every();
    assert_eq!(rows.len(), TORRENTS as usize);
    let mut heap = 0;
    for row in &rows {
        assert!(row.view.files.is_empty());
        assert!(row.view.file_have.is_empty());
        assert!(row.view.trackers.is_empty());
        heap += heap_of(&row.view)
            + row
                .trackers
                .current
                .as_ref()
                .map_or(0, |tracker| tracker.url.capacity());
    }
    let own = std::mem::size_of::<TorrentRow>();
    // An entry of the id map, the place set and the moved set, each.
    let index = std::mem::size_of::<(JobId, Arc<TorrentRow>)>()
        + std::mem::size_of::<(i64, JobId)>()
        + std::mem::size_of::<(u64, JobId)>();
    let each = own + 2 * std::mem::size_of::<usize>() + index + heap / rows.len();
    eprintln!(
        "row: {own} bytes, index {index} bytes, heap {} bytes; {each} bytes a row, {} KiB at {TORRENTS}",
        heap / rows.len(),
        each * rows.len() / 1024,
    );
}

// A publish of every torrent that read them before a newer reading of one
// landed does not take that one back, nor bring back one a newer reading
// found gone, nor drop one added after it began; tags read after it stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_reading_never_takes_a_row_back() {
    let view = one_view().await;
    let snapshot = Snapshot::default();
    let row = |id: u64, state: TorrentState| {
        (
            TorrentView {
                state,
                ..copy(&view, id)
            },
            FileTotals::of(&view),
        )
    };
    snapshot.replace_every(&[row(1, TorrentState::Paused), row(2, TorrentState::Paused)]);

    // A publish of every torrent begins: it reads 1 checking and 2 there.
    let old = snapshot.stamp();
    // Newer readings land first: 1 is seeding, 2 is gone, 3 came, 1 tagged.
    snapshot.put_one(
        JobId(1),
        Some(&row(1, TorrentState::Seeding).0),
        snapshot.stamp(),
    );
    snapshot.put_one(JobId(2), None, snapshot.stamp());
    snapshot.put_one(
        JobId(3),
        Some(&row(3, TorrentState::Paused).0),
        snapshot.stamp(),
    );
    snapshot.retag(
        |job_id| {
            if job_id == JobId(1) {
                vec!["late".to_owned()]
            } else {
                Vec::new()
            }
        },
        snapshot.stamp(),
    );
    let revision = snapshot.revision();
    snapshot.replace(
        &[
            row(1, TorrentState::CheckingResumeData),
            row(2, TorrentState::Paused),
        ],
        true,
        old,
    );
    assert_eq!(snapshot.revision(), revision);
    let (_, rows) = snapshot.every();
    let ids: Vec<JobId> = rows.iter().map(|row| row.view.job_id).collect();
    assert_eq!(ids, [JobId(1), JobId(3)]);
    assert_eq!(rows[0].view.state, TorrentState::Seeding);
    assert_eq!(rows[0].view.tags, ["late"]);

    // A newer publish sets every row, keeping the tags read after it began
    // only when they are newer than it.
    snapshot.replace(
        &[row(1, TorrentState::Paused), row(3, TorrentState::Paused)],
        true,
        snapshot.stamp(),
    );
    let (_, rows) = snapshot.every();
    assert_eq!(rows[0].view.state, TorrentState::Paused);
    assert!(rows[0].view.tags.is_empty());
}

// A page of the queue under a state filter, and a follow of the page's
// torrents, do not go stale as a torrent moves from downloading to seeding:
// the settle that sees it complete publishes it, which moves both its queue
// job (status Seeding, a new job revision, which wakes `queuePageUpdates`)
// and its row in the snapshot (which wakes the page's follower). A torrent
// gone and one come move them again, and a follow made for the page's new
// ids starts from the rows as they stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_seeds_moves_its_page_and_its_row() {
    use crate::jobs::model::JobStatus;

    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("page/a.bin", data.as_slice())];
    let bytes = metainfo("page", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let seeder = seeder(seeds.path(), &bytes, &files).await;
    let std::net::SocketAddr::V4(peer) = seeder.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (url, _answered) = answering_tracker(peer).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    let handle = queue();
    let jobs = handle.state();
    let mut job_changes = jobs.subscribe_job_changes();
    rig.service.publish_to(handle.clone());
    rig.service.open().await.unwrap();
    let other = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("other", 7)))
        })
        .await
        .unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes.to_vec())))
        .await
        .unwrap();
    let page: BTreeSet<JobId> = [job_id, other].into();
    let mut follower = rig.service.follow(Follow::Some(page.clone())).unwrap();
    assert!(follower.next().await.full_update);

    let before = jobs.job_revision();
    assert_ne!(jobs.get_job(job_id).unwrap().status, JobStatus::Seeding);
    tracked(&rig.service, job_id, &url).await;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    // The queue's job says seeding, after a revision a page waits on.
    let seeding_at = loop {
        job_changes.borrow_and_update();
        if jobs.get_job(job_id).unwrap().status == JobStatus::Seeding {
            break jobs.job_revision();
        }
        job_changes.changed().await.unwrap();
    };
    assert!(seeding_at > before);
    // A page under a downloading filter loses it, one under a seeding
    // filter gains it: both read the queue's jobs, whose revision moved.
    let seeding: Vec<JobId> = jobs
        .list_jobs()
        .into_iter()
        .filter(|job| job.status == JobStatus::Seeding)
        .map(|job| job.job_id)
        .collect();
    assert_eq!(seeding, [job_id]);
    // Its row says so, and its follower is told.
    let row = rig.service.snapshot().row(job_id).unwrap();
    assert_eq!(row.view.state, TorrentState::Seeding);
    loop {
        let changes = follower.next().await;
        assert!(!changes.full_update);
        if changes
            .torrents
            .iter()
            .any(|view| view.job_id == job_id && view.state == TorrentState::Seeding)
        {
            break;
        }
    }

    // One gone and one come: the queue moves, and the follower hears the
    // one it names that went.
    let revision = jobs.job_revision();
    let added = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("added", 9)))
        })
        .await
        .unwrap();
    rig.service.remove(other, false, true).await.unwrap();
    loop {
        job_changes.borrow_and_update();
        // A removed torrent's job retires as cancelled: a page of paused
        // jobs loses it.
        let listed = jobs.list_jobs();
        let status = |job_id| {
            listed
                .iter()
                .find(|job| job.job_id == job_id)
                .map(|job| job.status.clone())
        };
        let cancelled = JobStatus::Failed {
            error: "cancelled".into(),
        };
        if status(other) == Some(cancelled) && status(added) == Some(JobStatus::Paused) {
            break;
        }
        job_changes.changed().await.unwrap();
    }
    assert!(jobs.job_revision() > revision);
    loop {
        let changes = follower.next().await;
        if changes.removed.contains(&other) {
            break;
        }
    }

    // The page's new ids: a follow of them starts from their rows as they
    // stand, the seed whole in hand.
    let mut fresh = rig
        .service
        .follow(Follow::Some([job_id, added].into()))
        .unwrap();
    let first = fresh.next().await;
    assert!(first.full_update);
    let ids: Vec<JobId> = first.torrents.iter().map(|view| view.job_id).collect();
    assert_eq!(ids, [job_id, added]);
    assert_eq!(first.torrents[0].state, TorrentState::Seeding);
    assert_eq!(first.torrents[0].have_bytes, first.torrents[0].total_bytes);
    assert_eq!(first.torrents[1].state, TorrentState::Paused);
    rig.service.stop().await;
}

// A torrent shows what it has the moment it seeds, and after a restart it
// seeds from what it kept: nothing is fetched again and its counters stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_torrent_seeds_after_a_restart_without_fetching_again() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    on_loopback(&rig.routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let total = data.len() as u64;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&rig.service, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    let sent = uploaded(&seeding);
    assert!(sent >= total, "the seeder sent {sent} of {total}");
    // Every piece verified, so nothing fetched was wasted.
    assert_eq!(view.wasted_bytes, 0);
    assert_eq!(
        view.pieces,
        Some(TorrentPieces {
            count: 4,
            size: 16_384,
            have: 4,
        })
    );
    // The ledger as the running tick keeps it: the time between two ticks
    // the torrent ran in its session is active, and it ran complete. The
    // tick writes the ledger it changed, and a clean stop writes the ledger
    // as it stands.
    rig.service.tick().await;
    rig.service.restored().await;
    clock.shift_unix_ms(30_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!(kept.active_duration_s, 30);
    rig.service.stop().await;
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            ledger.downloaded_bytes,
            ledger.active_duration_s,
            ledger.last_seen_complete_ms,
        ),
        (total, 30, Some(EPOCH_MS + 30_000))
    );
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    again.publish_to(handle);
    again.open().await.unwrap();
    // Before the tick restores its job, it shows what was stored: complete,
    // so its file whole and every piece.
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (kept.have_bytes, kept.progress, kept.file_have),
        (total, 1.0, vec![total])
    );
    assert_eq!(kept.pieces.map(|pieces| pieces.have), Some(4));
    again.tick().await;
    again.restored().await;
    handle_of(&again, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&again, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    assert_eq!(view.file_have, [total]);
    assert_eq!((view.active_seconds, view.wasted_bytes), (30, 0));
    assert_eq!(uploaded(&seeding), sent);
    again.stop().await;
    seeding.stop().await;
}

// A torrent's averages run over the seconds it reads as active, a second
// begun counting whole: its first second reads its bytes as its rate, not
// 0, and past it the bytes over the seconds is the average.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_first_second_reads_a_rate() {
    let seeded = seeded().await;
    seeded.advance(500);
    let view = seeded.view().await;
    assert!(view.downloaded_bytes > 0);
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (1, view.downloaded_bytes)
    );
    seeded.advance(3_443);
    let view = seeded.view().await;
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (4, view.downloaded_bytes / 4)
    );
    seeded.stop().await;
}

// A torrent's active time is read as of now, with its bytes: a view between
// ticks counts the time since the last one, its averages are its bytes
// over that time, and the tick lands the same figures in the ledger with no
// step. Seeding time runs with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_active_time_reads_as_of_now_between_ticks() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    let kept = seeded.kept().await;
    assert_eq!(at_tick.active_seconds, 1);
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds,
            at_tick.active_seconds,
            at_tick.downloaded_bytes,
        )
    );

    seeded.advance(7_000);
    let later = seeded.view().await;
    assert_eq!(later.active_seconds, at_tick.active_seconds + 7);
    assert_eq!(
        (later.download_avg_bps, later.upload_avg_bps),
        (
            later.downloaded_bytes / later.active_seconds,
            later.uploaded_bytes / later.active_seconds,
        )
    );

    seeded.advance(3_000);
    let before = seeded.view().await;
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let after = seeded.view().await;
    assert_eq!(before.active_seconds, at_tick.active_seconds + 10);
    assert_eq!(
        (after.active_seconds, after.download_avg_bps),
        (before.active_seconds, before.download_avg_bps)
    );
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (after.active_seconds, after.active_seconds)
    );
    seeded.stop().await;
}

// A pause takes the torrent's reading first: the time it ran since the
// last tick is active and seeding, and the time it stays paused is
// neither, between ticks or across one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_credits_the_time_before_it_and_nothing_after() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    seeded.rig.service.pause(seeded.job_id).await.unwrap();
    let paused = seeded.view().await;
    assert_eq!(paused.state, TorrentState::Paused);
    assert_eq!(paused.active_seconds, at_tick.active_seconds + 7);
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    seeded.advance(3_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.stop().await;
}

// A pause ends the current seeding run: all the seeding there has been
// keeps the time before it, the run goes back to zero, and a resume starts
// a new run from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_ends_the_seeding_run_and_keeps_the_whole() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;
    assert_eq!(seeded.kept().await.consecutive_seeding_s, whole);
    let view = seeded.view().await;
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, whole)
    );

    // The pause takes the torrent's reading first, so the run it ends goes
    // to zero at the first reading that finds it paused.
    seeded.rig.service.pause(seeded.job_id).await.unwrap();
    assert_eq!(seeded.kept().await.finished_duration_s, whole);
    seeded.advance(6_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    let view = seeded.view().await;
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, 0)
    );

    seeded.rig.service.resume(seeded.job_id).await.unwrap();
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    seeded.advance(4_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole + 4, 4)
    );
    seeded.stop().await;
}

// A route that goes down ends the current seeding run the same way, and
// all the seeding there has been keeps what it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_route_going_down_ends_the_seeding_run() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;

    seeded.rig.routes.set_down(Some("tunnel gone"));
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    seeded.advance(5_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let kept = seeded.kept().await;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole, 0)
    );
    seeded.stop().await;
}

// The download and upload limits a session holds now.
fn session_limits(session: &Session) -> (Option<u32>, Option<u32>) {
    (
        session.ratelimits.get_download_bps().into(),
        session.ratelimits.get_upload_bps().into(),
    )
}

// An egress's upload quota reaches the open session on it live. Past the
// soft cap its uploads trickle and the torrent still seeds; at the hard cap
// they move nothing, and the torrent still seeds and still announces; lifted,
// the rate comes back. A torrent on another egress is untouched throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_egress_upload_quota_throttles_its_seeds_and_never_pauses_them() {
    use crate::bandwidth::ClassRates;

    let mut seeded = seeded().await;
    let service = seeded.rig.service.clone();
    let routes = Arc::clone(&seeded.rig.routes);
    let mut applied = service.inner.sessions.egress_limits_applied();
    // The follower put the limits in force once as it started, and waits.
    applied.wait_for(|applied| *applied >= 1).await.unwrap();
    let mut rings = *applied.borrow_and_update();
    let mut ring = |egress_id, rates| {
        routes.set_egress_rates(egress_id, rates);
        rings += 1;
        rings
    };

    routes.route(TorrentScope::Category(1), 2);
    routes.route_on_egress(TorrentScope::Category(1), 7);
    let other = service
        .add_opened(AddRequest {
            category: Some("tv".into()),
            ..request(AddSource::Metainfo(plain("other", 2)))
        })
        .await
        .unwrap();
    service.tick().await;
    service.restored().await;
    let seeding = session_of(&service, seeded.job_id).await;
    let elsewhere = session_of(&service, other).await;
    assert!(!Arc::ptr_eq(&seeding, &elsewhere));
    assert_eq!(session_limits(&seeding), (None, None));

    // Past the soft cap: the egress's uploads trickle.
    let trickle = ClassRates {
        download: None,
        upload: Some(crate::proxies::DEFAULT_UPLOAD_TRICKLE_BPS),
    };
    let at = ring(SYSTEM_EGRESS_ID, trickle);
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(
        session_limits(&seeding),
        (
            None,
            Some(crate::proxies::DEFAULT_UPLOAD_TRICKLE_BPS as u32)
        )
    );
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);

    // At the hard cap: a budget of no bytes. The torrent still seeds, and
    // its next announce still goes out as a seed's.
    let stopped = ClassRates {
        download: None,
        upload: Some(0),
    };
    let at = ring(SYSTEM_EGRESS_ID, stopped);
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(session_limits(&seeding), (None, Some(0)));
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);
    assert_eq!(seeded.reannounced().await[0].0, "");
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);

    // Lifted: the session moves as it did.
    let at = ring(SYSTEM_EGRESS_ID, ClassRates::default());
    applied.wait_for(|applied| *applied >= at).await.unwrap();
    assert_eq!(session_limits(&seeding), (None, None));
    assert_eq!(session_limits(&elsewhere), (None, None));
    assert!(Arc::ptr_eq(
        &seeding,
        &session_of(&service, seeded.job_id).await
    ));
    seeded.stop().await;
}

// Time Weaver was down credits neither counter, and the restore starts the
// seeding run again: all the seeding there has been comes back as it was,
// the run at zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_keeps_the_whole_and_starts_the_seeding_run_again() {
    let seeded = seeded().await;
    seeded.advance(10_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let whole = seeded.kept().await.finished_duration_s;
    assert_eq!(seeded.kept().await.consecutive_seeding_s, whole);
    seeded.rig.service.stop().await;

    seeded.advance(600_000);
    let again = service_on(
        &seeded.rig.db,
        seeded.rig.dir.path(),
        &seeded.rig.routes,
        job_env(),
        Arc::clone(&seeded.clock) as Arc<dyn Clock>,
    );
    reopen(&again).await;
    // The restored torrent's session has checked it: the tick below reads
    // it seeding, not still at its check.
    handle_of(&again, seeded.job_id)
        .await
        .wait_until_initialized()
        .await
        .unwrap();
    let view = again.torrent(seeded.job_id).await.unwrap();
    assert_eq!(
        (view.seeding_seconds, view.consecutive_seeding_seconds),
        (whole, 0)
    );
    seeded.advance(3_000);
    again.tick().await;
    again.restored().await;
    let kept = again.inner.live.lock().await[&seeded.job_id].counters;
    assert_eq!(
        (kept.finished_duration_s, kept.consecutive_seeding_s),
        (whole + 3, 3)
    );
    again.stop().await;
    seeded.stop().await;
}

// A torrent that wants none of its files has every piece it wants: it seeds
// at 100 % with nothing in hand and no peers, though its job is not
// complete, and its seeding time runs. Wanting the file again takes it back
// to fetching, and its seeding time stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_that_wants_nothing_seeds_until_it_wants_a_file() {
    use FilePriority::*;
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, Ignored)])
        .await
        .unwrap();
    rig.service.resume(job_id).await.unwrap();
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (
            view.state,
            view.progress,
            view.have_bytes,
            view.total_bytes,
            view.peers,
            view.completed_at_ms,
        ),
        (TorrentState::Seeding, 1.0, 0, 3000, 0, None)
    );

    rig.service.tick().await;
    rig.service.restored().await;
    clock.shift_unix_ms(5_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (5, 5));

    rig.service
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.progress, view.completed_at_ms),
        (TorrentState::Stalled, 0.0, None)
    );
    clock.shift_unix_ms(5_000);
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = rig.service.inner.live.lock().await[&job_id].counters;
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (10, 5));
    rig.service.stop().await;
}

// Turning torrents off reads every torrent before its session closes: the
// time it ran since the last tick is in the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turning_torrents_off_keeps_the_time_they_ran_since_the_tick() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    let mut settings = seeded.rig.service.settings();
    settings.torrent.enabled = false;
    seeded.rig.service.set_settings(settings).await;
    let kept = seeded.kept().await;
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds + 7,
            at_tick.active_seconds + 7,
            at_tick.downloaded_bytes,
        )
    );
    let off = seeded.view().await;
    assert_eq!(off.active_seconds, at_tick.active_seconds + 7);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, off.active_seconds);
    seeded.stop().await;
}

// A clock stepped back credits nothing for the reading that sees it, and
// the time from there on counts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_stepped_back_credits_nothing_and_counts_on() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let at_tick = seeded.view().await.active_seconds;
    seeded.advance(-60_000);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.kept().await.active_duration_s, at_tick);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, at_tick + 7);
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    assert_eq!(seeded.kept().await.active_duration_s, at_tick + 7);
    seeded.stop().await;
}

// No part of a second is lost at a tick: ticks 2.4 s, 2.4 s, 1.6 s and
// 1.6 s apart add the 8 s they span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ticks_lose_no_part_of_a_second() {
    let seeded = seeded().await;
    seeded.rig.service.tick().await;
    seeded.rig.service.restored().await;
    let base = seeded.view().await.active_seconds;
    for ms in [2_400, 2_400] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
        seeded.rig.service.restored().await;
    }
    assert_eq!(seeded.kept().await.active_duration_s, base + 4);
    for ms in [1_600, 1_600] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
        seeded.rig.service.restored().await;
    }
    assert_eq!(seeded.kept().await.active_duration_s, base + 8);
    assert_eq!(seeded.view().await.active_seconds, base + 8);
    seeded.stop().await;
}

// Readings 2.4 s and 2.4 s apart add 4 s, the 4.8 s rounded down once;
// 1.6 s and 1.6 s add 3. A duration the torrent was not in takes nothing.
#[test]
fn a_reading_carries_the_part_of_a_second_to_the_next() {
    let run = |steps: &[i64]| {
        let mut counters = TorrentCounters::default();
        let mut timed = Timed::at(EPOCH_MS);
        let mut now = EPOCH_MS;
        for step in steps {
            now += step;
            timed = timed.advanced(now, true, false, &mut counters);
        }
        (counters.active_duration_s, counters.finished_duration_s)
    };
    assert_eq!(run(&[2_400, 2_400]), (4, 0));
    assert_eq!(run(&[1_600, 1_600]), (3, 0));
    assert_eq!(run(&[999]), (0, 0));
    assert_eq!(run(&[999, 1]), (1, 0));
    // Stepped back 60 s: nothing for that reading, then 7 s from there.
    assert_eq!(run(&[1_000, -60_000]), (1, 0));
    assert_eq!(run(&[1_000, -60_000, 7_000]), (8, 0));
}

// All the seeding there has been keeps running across a break; the current
// run starts again from zero at the first reading that is not seeding, and
// the part of a second it held goes with it.
#[test]
fn a_break_in_seeding_keeps_the_whole_and_restarts_the_run() {
    let run = |steps: &[(i64, bool)]| {
        let mut counters = TorrentCounters::default();
        let mut timed = Timed::at(EPOCH_MS);
        let mut now = EPOCH_MS;
        for (step, seeding) in steps {
            now += step;
            timed = timed.advanced(now, true, *seeding, &mut counters);
        }
        (counters.finished_duration_s, counters.consecutive_seeding_s)
    };
    assert_eq!(run(&[(10_000, true), (5_000, true)]), (15, 15));
    assert_eq!(
        run(&[(10_000, true), (5_000, false), (7_000, true)]),
        (17, 7)
    );
    // The 900 ms the run held is dropped by the break, so the 600 ms after
    // it make no second.
    assert_eq!(run(&[(1_900, true), (1_000, false), (1_600, true)]), (3, 1));
}

// A peer on `listener` that says it saw a complete copy `ago` seconds ago,
// then that it has piece 0: the BitTorrent handshake with the extension
// bit, an extended handshake carrying `complete_ago` (BEP 10), and a have.
// Returns once the session says it is interested, which it reads after the
// extended handshake. A connection that opens encrypted is let go: the
// session tries plaintext next.
async fn claims_complete(listener: &tokio::net::TcpListener, ago: u64) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PROTOCOL: &[u8] = b"\x13BitTorrent protocol";
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut theirs = [0u8; 68];
        if stream.read_exact(&mut theirs).await.is_err() || &theirs[..20] != PROTOCOL {
            continue;
        }
        let mut ours = PROTOCOL.to_vec();
        ours.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
        ours.extend_from_slice(&theirs[28..48]);
        ours.extend_from_slice(b"-XX0000-claimscomplt");
        let extended = format!("d12:complete_agoi{ago}e1:mdee").into_bytes();
        let mut message = |id: &[u8], payload: &[u8]| {
            ours.extend_from_slice(
                &u32::try_from(id.len() + payload.len())
                    .unwrap()
                    .to_be_bytes(),
            );
            ours.extend_from_slice(id);
            ours.extend_from_slice(payload);
        };
        message(&[20, 0], &extended);
        message(&[4], &0u32.to_be_bytes());
        stream.write_all(&ours).await.unwrap();
        loop {
            let mut len = [0u8; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            stream.read_exact(&mut body).await.unwrap();
            if body.first() == Some(&2) {
                return stream;
            }
        }
    }
}

// libtorrent `torrent::second_tick` keeps `m_swarm_last_seen_complete`, the
// latest of our own sighting and each connected peer's `complete_ago`, and
// `torrent::status` reports it as `last_seen_complete` (qBittorrent's
// `seen_complete`). A peer's word that it saw a complete copy dates the
// torrent's, though nobody connected is a seed; once the peer is gone the
// date stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peers_sighting_of_a_complete_copy_dates_the_torrents() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
        panic!("the peer listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let seen = || {
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .last_seen_complete_ms
    };
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(seen(), None);

    let connection = claims_complete(&listener, 100).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let at = rig
        .service
        .torrent(job_id)
        .await
        .unwrap()
        .last_seen_complete_ms
        .expect("the peer's sighting is the torrent's");
    assert!(at <= EPOCH_MS - 100_000, "{at}");

    // With the peer gone, as a pause disconnects every peer, nobody is
    // left to say it: the date kept stands, written as the pause is.
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(seen(), Some(at));
    let handle = handle_of(&rig.service, job_id).await;
    assert!(handle.live().is_none());
    assert_eq!(rig.service.torrent(job_id).await.unwrap().peers, 0);
    drop(connection);
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(seen(), Some(at));
    rig.service.stop().await;
}

// A torrent's ETA is over the bytes it wants and has not: an Ignored file,
// never to come, adds nothing to it (qBittorrent `TorrentImpl::eta`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_eta_counts_only_the_wanted_bytes_left() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let files = [
        ("set/a.bin", &[1u8; 6_000][..]),
        ("set/b.bin", &[2u8; 30_000][..]),
    ];
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(metainfo("set", &files, 4_096, false)))
        })
        .await
        .unwrap();
    let settings = rig.service.settings();
    let mut live = rig.service.inner.live.lock().await;
    let torrent = live.get_mut(&job_id).unwrap();
    torrent.levels = vec![FilePriority::Normal, FilePriority::Ignored];
    let mut reading = torrent.reading(0);
    reading.file_have = vec![2_000, 10_000];
    reading.speeds = (1_000, 0);
    let view = rig.service.view_of(&settings, torrent, reading, 0);
    drop(live);
    assert_eq!((view.wanted_bytes, view.wanted_have_bytes), (6_000, 2_000));
    assert_eq!(view.eta_seconds, Some(4));
    rig.service.stop().await;
}

// Stored torrents, `count` of them, as a restart finds them: odd ones
// magnets, even ones with their metainfo and their folder named, every
// tenth paused and the rest resumed. The resumed magnets, by job.
fn stored_resumed(db: &Database, dir: &Path, count: u64) -> Vec<JobId> {
    let mut magnets = Vec::new();
    for job_id in 1..=count {
        let name = format!("t{job_id}");
        let magnet = job_id % 2 == 1;
        let paused = job_id % 10 == 0;
        let (infohash_v1, metainfo, storage_dir) = if magnet {
            (Some(Sha1::digest(job_id.to_le_bytes()).into()), None, None)
        } else {
            let bytes = plain(&name, (job_id % 251) as u8);
            let parsed = parse(&bytes).unwrap();
            (
                parsed.hashes.v1.map(|hash| hash.0),
                Some(Bytes::from(bytes)),
                Some(
                    dir.join("complete")
                        .join(&name)
                        .to_string_lossy()
                        .into_owned(),
                ),
            )
        };
        let record = TorrentRecord {
            job_id: JobId(job_id),
            infohash_v1,
            infohash_v2: None,
            name,
            source: if magnet {
                TorrentSource::Magnet
            } else {
                TorrentSource::Metainfo
            },
            metainfo,
            category_id: None,
            added_at_ms: EPOCH_MS + job_id as i64,
            kept: KeptIntent::Intent(if paused {
                TorrentIntent::Paused
            } else {
                TorrentIntent::Resumed
            }),
            forced: false,
            completed_at_ms: None,
            storage_dir,
            policy_override: None,
            unpack: Some(false),
            unpack_in_force: Some(false),
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
        db.insert_torrent(&record, &[]).unwrap();
        if magnet && !paused {
            magnets.push(JobId(job_id));
        }
    }
    magnets
}

// Every torrent's row, its view, file totals and trackers, by job.
fn rows(service: &TorrentService) -> Vec<(TorrentView, FileTotals, TrackerSummary)> {
    let (_, mut rows) = service.snapshot().every();
    rows.sort_by_key(|row| row.view.job_id);
    rows.iter()
        .map(|row| (row.view.clone(), row.totals, row.trackers.clone()))
        .collect()
}

const RESTARTED: u64 = 2_000;

// A start of many stored torrents, most of them resumed, on routes that do
// not open: every resumed magnet's fetch ends at once, and every torrent
// with its metainfo is held as its job comes back. The views the start
// builds grow with the torrents, a few for each, not with the torrents
// times the steps of each: each fetch that ends shows its torrent alone.
// The rows it leaves are the rows a publish of every torrent makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_on_held_routes_builds_views_in_step_with_the_torrents() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("the leg is down"));
    let magnets = stored_resumed(&rig.db, rig.dir.path(), RESTARTED);
    let resumed = magnets.len() as u64;

    rig.service.open().await.unwrap();
    // Each resumed magnet fetched once as it was read, and its fetch ended
    // on its route held.
    rig.service.fetches_ended(resumed).await;
    // The tick restores the jobs, holds them on their routes, and fetches
    // each held magnet again.
    rig.service.tick().await;
    rig.service.restored().await;
    rig.service.fetches_ended(2 * resumed).await;
    let built = rig.service.views_built();
    assert_eq!(rig.service.snapshot().len(), RESTARTED as usize);
    // Two publishes of every torrent (the load's end, the tick's), one
    // after the restores, and a row for each fetch that ended: well under
    // six views a torrent. One publish of every torrent for each fetch
    // would build hundreds a torrent.
    assert!(
        built <= 6 * RESTARTED as usize,
        "{built} views built for {RESTARTED} torrents"
    );
    // Each row as a publish of every torrent shows it.
    let shown = rows(&rig.service);
    for job_id in &magnets {
        assert_eq!(shown[job_id.0 as usize - 1].0.job_id, *job_id);
    }
    rig.service.publish().await;
    assert_eq!(rows(&rig.service), shown);
    rig.service.stop().await;
}

// The same start with torrents off: no route opens, every resumed magnet's
// fetch ends at once, and the views the start builds grow with the
// torrents.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_with_torrents_off_builds_views_in_step_with_the_torrents() {
    let rig = rig(set_clock());
    let magnets = stored_resumed(&rig.db, rig.dir.path(), RESTARTED);
    let resumed = magnets.len() as u64;
    rig.service.inner.sessions.turn(false).await;

    rig.service.open().await.unwrap();
    rig.service.run(false).await;
    rig.service.fetches_ended(resumed).await;
    let built = rig.service.views_built();
    assert_eq!(rig.service.snapshot().len(), RESTARTED as usize);
    assert!(
        built <= 4 * RESTARTED as usize,
        "{built} views built for {RESTARTED} torrents"
    );
    let shown = rows(&rig.service);
    rig.service.publish().await;
    assert_eq!(rows(&rig.service), shown);
    rig.service.stop().await;
}

// A recheck of many torrents whose jobs wait for the tick restores each and
// sets every piece to be checked, as one unit: one pass of the queue and one
// publish, not one of each a torrent. The rows it leaves, and what each
// torrent then holds and is checked for, are those of the same rechecks
// made one torrent at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recheck_of_many_torrents_is_one_unit() {
    const STORED: u64 = 300;
    let start = |rig: &Rig| {
        rig.routes.set_down(Some("the leg is down"));
        stored_resumed(&rig.db, rig.dir.path(), STORED)
    };
    let (many, alone) = (rig(set_clock()), rig(set_clock()));
    let resumed = start(&many).len() as u64;
    start(&alone);
    for rig in [&many, &alone] {
        rig.service.open().await.unwrap();
        rig.service.fetches_ended(resumed).await;
    }
    // The torrents with their metainfo, whose jobs the tick has not yet
    // restored.
    let held: Vec<JobId> = (1..=STORED)
        .filter(|job_id| job_id % 2 == 0)
        .map(JobId)
        .collect();

    let views = many.service.views_built();
    let outcome = many.service.recheck_many(&held).await;
    let built = many.service.views_built() - views;
    assert_eq!(outcome.len(), held.len());
    assert!(outcome.values().all(Result::is_ok), "{outcome:?}");
    // One publish of every torrent, where one at a time built one for each.
    assert!(
        built <= 2 * STORED as usize,
        "{built} views built for {STORED} torrents"
    );
    for &job_id in &held {
        alone.service.recheck(job_id).await.unwrap();
    }
    for rig in [&many, &alone] {
        rig.service.restored().await;
        rig.service.fetches_ended(resumed).await;
    }
    // Each rig's folders are its own: the rows are compared below them.
    let below = |rig: &Rig, dir: &mut Option<String>| {
        let root = rig.dir.path().to_string_lossy().into_owned();
        if let Some(dir) = dir {
            *dir = dir.replacen(&root, "", 1);
        }
    };
    let shown = |rig: &Rig| {
        let mut shown = rows(&rig.service);
        for (view, _, _) in &mut shown {
            below(rig, &mut view.storage_dir);
        }
        shown
    };
    let stored = |rig: &Rig| {
        let mut stored = rig.db.list_torrents().unwrap();
        for record in &mut stored {
            below(rig, &mut record.storage_dir);
        }
        stored
    };
    assert_eq!(shown(&many), shown(&alone));
    assert_eq!(stored(&many), stored(&alone));
    many.service.stop().await;
    alone.service.stop().await;
}

// A move of many torrents moves each whole, in the order asked, as one unit:
// one pass of the queue and one publish once the last has moved, not one of
// each a move. The first refused ends the request, those before it moved and
// those after it not. What it answers, the rows and records it leaves and
// the files where they went are those of the same moves made one torrent at
// a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_of_many_torrents_is_one_unit() {
    const ADDED: u64 = 120;
    // A magnet, with no files to move, part way: the moves end there.
    const REFUSED: u64 = ADDED / 2;
    let add = async |rig: &Rig| {
        rig.routes.set_down(Some("the leg is down"));
        rig.service.open().await.unwrap();
        let mut added = Vec::new();
        for n in 1..=ADDED {
            let source = if n == REFUSED {
                AddSource::Magnet(format!("magnet:?xt=urn:btih:{}", "ab".repeat(20)))
            } else {
                AddSource::Metainfo(plain(&format!("t{n}"), (n % 251) as u8))
            };
            added.push(rig.service.add_opened(request(source)).await.unwrap());
        }
        added
    };
    let (many, alone) = (rig(set_clock()), rig(set_clock()));
    let ids = add(&many).await;
    assert_eq!(add(&alone).await, ids);
    let location = |rig: &Rig| {
        rig.dir
            .path()
            .join("elsewhere")
            .to_string_lossy()
            .into_owned()
    };

    let views = many.service.views_built();
    let outcome = many
        .service
        .set_location_many_as(crate::auth::CallerScope::Admin, &ids, &location(&many))
        .await;
    let built = many.service.views_built() - views;
    // One publish of every torrent and the row of each shown moving, where
    // one at a time built a publish of every torrent for each.
    assert!(
        built <= 3 * ADDED as usize,
        "{built} views built for {ADDED} torrents"
    );
    let mut answered = Vec::new();
    for &job_id in &ids {
        let moved = alone
            .service
            .set_location_as(crate::auth::CallerScope::Admin, job_id, location(&alone))
            .await;
        let ends = moved.is_err();
        answered.push((job_id, moved.map_err(|error| error.to_string())));
        if ends {
            break;
        }
    }
    let outcome: Vec<_> = ids
        .iter()
        .filter_map(|job_id| {
            let moved = outcome.get(job_id)?;
            Some((
                *job_id,
                moved.as_ref().map(|_| ()).map_err(ToString::to_string),
            ))
        })
        .collect();
    assert_eq!(outcome, answered);
    assert_eq!(outcome.len(), REFUSED as usize);
    assert!(outcome.last().unwrap().1.is_err());

    // Each rig's folders are its own: what is kept is compared below them.
    let below = |rig: &Rig, dir: &mut Option<String>| {
        let root = rig.dir.path().to_string_lossy().into_owned();
        if let Some(dir) = dir {
            *dir = dir.replacen(&root, "", 1);
        }
    };
    let shown = |rig: &Rig| {
        let mut shown = rows(&rig.service);
        for (view, _, _) in &mut shown {
            below(rig, &mut view.storage_dir);
        }
        shown
    };
    let stored = |rig: &Rig| {
        let mut stored = rig.db.list_torrents().unwrap();
        for record in &mut stored {
            below(rig, &mut record.storage_dir);
            below(rig, &mut record.files_dir);
            below(rig, &mut record.save_path);
        }
        stored
    };
    // Every file and folder below the rig's folder, by its path there.
    let on_disk = |rig: &Rig| {
        let mut found = BTreeSet::new();
        let mut dirs = vec![rig.dir.path().to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path.clone());
                }
                found.insert(path.strip_prefix(rig.dir.path()).unwrap().to_path_buf());
            }
        }
        found
    };
    for rig in [&many, &alone] {
        rig.service.publish().await;
    }
    assert_eq!(shown(&many), shown(&alone));
    assert_eq!(stored(&many), stored(&alone));
    assert_eq!(on_disk(&many), on_disk(&alone));
    // Those before the refused one moved.
    let moved = std::path::Path::new("elsewhere").join("t1");
    assert!(on_disk(&many).contains(&moved));
    many.service.stop().await;
    alone.service.stop().await;
}

// A removal of many torrents with their files reads what each made, and
// what was there before it, for all of them in a few statements, not two
// reads a torrent. What it deletes, keeps and stores is what the same
// removals made one torrent at a time delete, keep and store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_of_many_torrents_reads_what_they_made_at_once() {
    const ADDED: u64 = 120;
    // Each with every piece on disk, to be deleted or kept.
    let add = async |rig: &Rig| {
        rig.service.open().await.unwrap();
        let mut added = Vec::new();
        for n in 1..=ADDED {
            let (job_id, dir) = with_every_piece_of(rig, &format!("t{n}"), (n % 251) as u8).await;
            // A third had every file there before them, a third never
            // said, and a third had none there.
            match n % 3 {
                0 => {
                    let every = crate::torrents::model::Adopted {
                        all_files: true,
                        ..Default::default()
                    };
                    rig.db.set_torrent_adopted(job_id, &every).unwrap();
                }
                1 => {
                    rig.db.forget_torrent_adopted(job_id).unwrap();
                }
                _ => {
                    let none = crate::torrents::model::Adopted::default();
                    rig.db.set_torrent_adopted(job_id, &none).unwrap();
                }
            }
            added.push((job_id, dir));
        }
        added
    };
    let (many, alone) = (rig(set_clock()), rig(set_clock()));
    let added = add(&many).await;
    let alone_added = add(&alone).await;
    let ids: Vec<JobId> = added.iter().map(|(job_id, _)| *job_id).collect();
    assert_eq!(
        alone_added
            .iter()
            .map(|(job_id, _)| *job_id)
            .collect::<Vec<_>>(),
        ids
    );

    let removals: Vec<(JobId, bool)> = ids.iter().map(|job_id| (*job_id, true)).collect();
    let takes = many.db.datastore_takes();
    let outcome = many.service.remove_many(&removals, true).await;
    let took = many.db.datastore_takes() - takes;
    assert!(outcome.values().all(Result::is_ok), "{outcome:?}");
    // The mark of every removal, the reads of what they made, and the
    // history row each writes, where one at a time read twice a torrent
    // beside them.
    assert!(
        took <= 16 + ADDED as usize,
        "{took} takes for {ADDED} torrents"
    );
    for &job_id in &ids {
        alone.service.remove(job_id, true, true).await.unwrap();
    }
    // Every file and folder below the rig's folder, by its path there.
    let on_disk = |rig: &Rig| {
        let mut found = BTreeSet::new();
        let mut dirs = vec![rig.dir.path().to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path.clone());
                }
                found.insert(path.strip_prefix(rig.dir.path()).unwrap().to_path_buf());
            }
        }
        found
    };
    let kept = on_disk(&many);
    assert_eq!(kept, on_disk(&alone));
    // Those whose files were all there before them keep them, and so do
    // those that never said; the others lose them.
    for (n, (_, dir)) in (1..).zip(&added) {
        let left = files_under(dir).is_empty();
        assert_eq!(left, n % 3 == 2, "t{n}");
    }
    let raw: Vec<u64> = ids.iter().map(|job_id| job_id.0).collect();
    assert_eq!(
        many.db.list_history_job_ids(&raw).unwrap(),
        alone.db.list_history_job_ids(&raw).unwrap()
    );
    assert_eq!(
        many.db.list_torrents().unwrap(),
        alone.db.list_torrents().unwrap()
    );
    many.service.stop().await;
    alone.service.stop().await;
}
