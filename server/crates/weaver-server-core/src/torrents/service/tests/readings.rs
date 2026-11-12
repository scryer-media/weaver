// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every torrent's followers share their readings, and each is told from them
// what `changes` tells a follower that reads for itself.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::listing::settled;
use super::*;
use crate::torrents::service::updates::{Follow, Need, Reading};

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
                // A clock field alone, which every torrent's follower leaves
                // out.
                for view in views.iter_mut().step_by(7) {
                    view.active_seconds += 1;
                }
            }
        }
        steps.push(views.clone());
    }
    steps
}

// Followers on several schedules are told from the shared readings what
// `changes` tells each of them over the same views: one at every step, one
// at every other, one at every third, one from the fifth, and one that is
// given the same reading twice. Some readings are made with the one before
// them let go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_reading_tells_each_follower_what_its_own_reading_would() {
    use fingerprint::ViewParts::Steady;

    let view = one_view().await;
    let steps = steps(&view);
    let at = Instant::now();
    let mut readings: Vec<Arc<Reading>> = Vec::new();
    for (step, views) in steps.iter().enumerate() {
        let before = readings.last().filter(|_| step % 5 != 3).map(Arc::as_ref);
        let reading = Reading::of(
            step as u64 + 1,
            at,
            (None, None),
            Vec::new(),
            views.clone(),
            before,
        );
        readings.push(Arc::new(reading));
    }

    struct Follower {
        from: usize,
        every: usize,
        last: Option<HashMap<JobId, u64>>,
        held: Option<Arc<Reading>>,
        told: usize,
    }
    let mut followers: Vec<Follower> = [(0, 1), (0, 2), (0, 3), (5, 1)]
        .into_iter()
        .map(|(from, every)| Follower {
            from,
            every,
            last: None,
            held: None,
            told: 0,
        })
        .collect();
    let mut moved_told = 0;
    for (step, views) in steps.iter().enumerate() {
        for follower in &mut followers {
            if step < follower.from || (step - follower.from) % follower.every != 0 {
                continue;
            }
            let own = updates::changes(&mut follower.last, views.clone(), Steady);
            let shared = readings[step].changes_since(follower.held.as_deref());
            assert_eq!(shared, own, "step {step}, every {}", follower.every);
            if let Some(changes) = &shared {
                follower.told += 1;
                moved_told += usize::from(!changes.full_update);
            }
            follower.held = Some(Arc::clone(&readings[step]));
        }
    }
    assert!(followers.iter().all(|follower| follower.told > 1));
    assert!(moved_told > 10);

    // The same reading twice tells nothing the second time.
    let reading = &readings[4];
    assert!(reading.changes_since(None).unwrap().full_update);
    assert_eq!(reading.changes_since(Some(reading)), None);
}

// A reading serves a follower at the revisions it has seen or after, begun
// after the count it woke at, and younger than its beat.
#[test]
fn a_reading_serves_the_need_it_meets() {
    let at = Instant::now();
    let beat = Duration::from_secs(3);
    let reading = Reading::of(7, at, (Some(5), Some(2)), Vec::new(), Vec::new(), None);
    let need = |queue, tags, begun_after, younger_than| Need {
        queue,
        tags,
        begun_after,
        younger_than,
    };
    assert!(reading.serves(Need::default(), at + beat * 10));
    assert!(reading.serves(need(Some(5), Some(2), None, None), at));
    assert!(reading.serves(need(Some(4), None, None, None), at));
    assert!(!reading.serves(need(Some(6), None, None, None), at));
    assert!(!reading.serves(need(None, Some(3), None, None), at));
    assert!(reading.serves(need(None, None, Some(6), None), at));
    assert!(!reading.serves(need(None, None, Some(7), None), at));

    let young = need(Some(5), Some(2), None, Some(beat));
    assert!(reading.serves(young, at));
    assert!(reading.serves(young, at + beat - Duration::from_millis(1)));
    assert!(!reading.serves(young, at + beat));
    assert!(!reading.serves(young, at + beat * 2));

    // A reading taken with no queue serves no follower that has one.
    let unqueued = Reading::of(8, at, (None, Some(2)), Vec::new(), Vec::new(), None);
    assert!(!unqueued.serves(need(Some(0), None, None, None), at));
    assert!(unqueued.serves(need(None, Some(2), None, None), at));
}

// Twenty followers of every torrent hear one tag change from one reading,
// and the reading is let go once none of them is left. The rig has no
// queue, and the beat is far past the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn twenty_followers_hear_a_change_from_one_reading() {
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

    let beat = Duration::from_secs(86_400);
    let mut followers: Vec<_> = (0..20)
        .map(|_| rig.service.follow(Follow::Every, beat))
        .collect();
    let every = rig.service.torrents(None).await;
    for follower in &mut followers {
        let first = follower.next().await;
        assert!(first.full_update);
        assert_eq!(first.torrents, every);
    }

    let before = rig.service.readings_begun();
    rig.db.add_job_tags(&[jobs[1]], ["shared"]).unwrap();
    let tagged = rig
        .service
        .torrents(None)
        .await
        .into_iter()
        .find(|view| view.job_id == jobs[1])
        .unwrap();
    assert_eq!(tagged.tags, ["shared"]);
    for follower in &mut followers {
        let changes = follower.next().await;
        assert!(!changes.full_update);
        assert_eq!(changes.torrents, std::slice::from_ref(&tagged));
        assert!(changes.removed.is_empty());
    }
    assert_eq!(rig.service.readings_begun() - before, 1);

    assert!(rig.service.holds_reading().await);
    drop(followers);
    assert!(!rig.service.holds_reading().await);
    rig.service.stop().await;
}
