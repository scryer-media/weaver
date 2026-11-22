// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's download ending gives memory back by the release a finished
// NZB job asks for: once as each download ends, and once as the last one
// downloading stops, never while nothing changes.

use super::*;

// The releases asked for so far: (download ended, torrents went idle).
fn releases(rig: &Rig) -> (usize, usize) {
    let counts = &rig.service.inner.releases;
    (
        counts[MemoryRelease::Ended as usize].load(Ordering::Relaxed),
        counts[MemoryRelease::Idle as usize].load(Ordering::Relaxed),
    )
}

// A torrent downloading in its session, as an add leaves it.
async fn downloading(rig: &Rig, name: &str, seed: u8) -> JobId {
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain(name, seed))))
        .await
        .unwrap();
    assert!(rig.service.inner.live.lock().await[&job_id].downloading());
    job_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_whose_download_completes_gives_memory_back_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = downloading(&rig, "set", 1).await;
    // It fetched something in this run.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .counters
        .downloaded_bytes += 1;
    rig.service.completed(job_id).await;
    assert_eq!(releases(&rig).0, 1);
    // Complete already: a second word of it asks for nothing.
    rig.service.completed(job_id).await;
    assert_eq!(releases(&rig).0, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_complete_without_downloading_asks_for_no_release() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = downloading(&rig, "set", 1).await;
    rig.service.completed(job_id).await;
    assert_eq!(releases(&rig).0, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_downloading_torrent_ending_gives_memory_back_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    // Nothing downloads: walks and ticks ask for nothing.
    rig.service.publish().await;
    rig.service.tick().await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 0));

    let first = downloading(&rig, "one", 1).await;
    let second = downloading(&rig, "two", 2).await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 0));
    // One still downloads.
    complete(&rig, first).await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 0));
    // The last one ends.
    complete(&rig, second).await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 1));
    // Seeding on, nothing downloading: no walk or tick asks again.
    rig.service.publish().await;
    rig.service.tick().await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_downloading_torrent_removed_gives_memory_back_once_and_once_idle() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = downloading(&rig, "set", 1).await;
    rig.service.publish().await;
    rig.service.remove(job_id, false).await.unwrap();
    assert_eq!(releases(&rig).0, 1);
    rig.service.publish().await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (1, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_going_idle_leave_the_release_to_a_pipeline_still_driving() {
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    state.queue.set_driving(true);
    rig.service.open().await.unwrap();
    let job_id = downloading(&rig, "set", 1).await;
    rig.service.publish().await;
    complete(&rig, job_id).await;
    rig.service.publish().await;
    assert_eq!(releases(&rig), (0, 0));
}
