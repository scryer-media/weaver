// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Torrents at scale through the whole service: WEAVER_BENCH_TORRENTS small
// seeds (default 1000; a comma-separated list runs each count in turn) are
// added from files already in place, seeded, listed, stopped and brought
// back by a restarted service over the same database, as a server's boot
// brings them back. It reports the wall time of each phase, a tick's and a
// list's wall time with every torrent seeding, and the process's resident
// memory. No peer, tracker or reachable DHT router takes part.
//
// cargo nextest run --locked --cargo-profile matrix -p weaver-server-core
//   --run-ignored only --no-capture
//   -E 'test(=torrents::service::tests::scale_bench::torrent_service_scale)'

use std::time::Instant;

use super::*;

// The service the server builds, its torrents' sessions on `routes`, with
// any listen port and a DHT router no one answers at.
fn bench_service(db: &Database, dir: &Path, routes: &Arc<FakeRoutes>) -> TorrentService {
    let sessions = Sessions::new(
        Arc::clone(routes) as Arc<dyn Routes>,
        env(&dir.join("sessions")),
        db.clone(),
    );
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: dir.join("complete"),
        categories: vec![],
        seeding: None,
        torrent: TorrentConfig {
            enabled: true,
            base_port: 0,
            upnp: false,
            dht_bootstrap_nodes: "127.0.0.1:9".into(),
            ..TorrentConfig::default()
        },
    };
    TorrentService::new(
        db.clone(),
        sessions,
        settings,
        job_env(),
        Arc::new(SystemClock) as Arc<dyn Clock>,
    )
}

// Resident memory in MiB, as ps reads it; None where ps is not there.
fn resident_mib() -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    let kib: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kib / 1024.0)
}

fn mib(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.0}"))
}

// Every job in `jobs` seeding, waited for one after another on the queue's
// changes.
async fn all_seeding(service: &TorrentService, changes: &mut watch::Receiver<u64>, jobs: &[JobId]) {
    for &job_id in jobs {
        loop {
            changes.borrow_and_update();
            if service
                .torrent(job_id)
                .await
                .is_some_and(|view| view.state == TorrentState::Seeding)
            {
                break;
            }
            changes.changed().await.unwrap();
        }
    }
}

fn ms(from: Instant) -> f64 {
    from.elapsed().as_secs_f64() * 1000.0
}

async fn run(count: usize) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    on_loopback(&routes);
    let before = resident_mib();

    let service = bench_service(&db, dir.path(), &routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    service.publish_to(handle);
    service.start().await.unwrap();

    let started = Instant::now();
    let mut jobs = Vec::with_capacity(count);
    for at in 0..count {
        let name = format!("s{at}");
        let data = [(at % 251) as u8; 3000];
        let save = dir.path().join("placed").join(&name);
        std::fs::create_dir_all(save.join(&name)).unwrap();
        std::fs::write(save.join(&name).join("a.bin"), data).unwrap();
        let metainfo = metainfo(&name, &[(&format!("{name}/a.bin"), &data)], 1024, false);
        let job_id = service
            .add(AddRequest {
                placement: Placement {
                    save_path: Some(save.to_string_lossy().into_owned()),
                    seed_mode: true,
                    ..Placement::default()
                },
                ..request(AddSource::Metainfo(metainfo))
            })
            .await
            .unwrap();
        jobs.push(job_id);
    }
    let added = ms(started);
    all_seeding(&service, &mut changes, &jobs).await;
    let seeding = ms(started);
    let resident = resident_mib();

    let ticks = 3;
    let at = Instant::now();
    for _ in 0..ticks {
        service.tick().await;
    }
    let tick = ms(at) / f64::from(ticks);
    let at = Instant::now();
    for _ in 0..ticks {
        assert_eq!(service.torrents(None).await.len(), count);
    }
    let list = ms(at) / f64::from(ticks);

    let at = Instant::now();
    service.stop().await;
    let stopped = ms(at);
    db.prune_orphan_active_state(crate::jobs::ids::JobId(u64::MAX))
        .unwrap();

    let again = bench_service(&db, dir.path(), &routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    again.publish_to(handle);
    let at = Instant::now();
    again.start().await.unwrap();
    let opened = ms(at);
    all_seeding(&again, &mut changes, &jobs).await;
    let back = ms(at);
    let resident_back = resident_mib();
    again.stop().await;

    println!(
        "BENCH service scale {count} torrents: add {added:.0} ms, all seeding {seeding:.0} ms; \
         tick {tick:.1} ms, list {list:.1} ms; stop {stopped:.0} ms; restart: start {opened:.0} ms, \
         all seeding {back:.0} ms; resident MiB before {}, seeding {}, after restart {}",
        mib(before),
        mib(resident),
        mib(resident_back),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "bench: adds, seeds and restores thousands of torrents"]
async fn torrent_service_scale() {
    let counts: Vec<usize> = std::env::var("WEAVER_BENCH_TORRENTS")
        .unwrap_or_else(|_| "1000".to_owned())
        .split(',')
        .map(|count| count.trim().parse().unwrap())
        .collect();
    for count in counts {
        run(count).await;
    }
}
