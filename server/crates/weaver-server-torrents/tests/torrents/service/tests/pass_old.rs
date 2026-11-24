// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tick before the policy pass and the tick after it, run over the same
// torrents: two rigs set up alike, one ticked by `old_tick` (the bodies of
// e53173421, in `old_tick.rs`) and one by `tick`, must leave the same
// sessions, rows, views, script events, checkpoints and stored peers, and
// their trackers must hear the same announces. Then, against the new tick
// alone, a seed-mode write the database refuses is undone in memory to what
// the disk still holds (the old tick left a refused "left seed mode" false
// in memory), a stop decided in the same pass is kept, the next pass writes
// it again, and each kind of refused seed-mode row is warned of once per
// streak.

use super::pass::{add, done_then, events, written};
use super::*;
use crate::persistence::sql_runtime::SqlRuntime;
use crate::post_processing::model::QueueEvent;
use crate::settings::repository::SettingsRepositoryQueries;
use crate::torrents::KeptIntent;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::queries::TorrentsQueries;
use crate::torrents::service::disk_room::DiskHeld;
use std::sync::PoisonError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weaver_server_disk::operations::disk::{DiskSpace, StorageCapacity, StorageRoot};

// Trackers on loopback, one a torrent (a loopback tracker is asked only at
// `/announce`), answering every announce with no peers. They keep, in the
// order the announces came, the torrent each came for and its event ("" for
// none).
struct Heard {
    log_tx: Arc<watch::Sender<Vec<(String, String)>>>,
    log: watch::Receiver<Vec<(String, String)>>,
    urls: std::sync::Mutex<Vec<String>>,
}

fn heard() -> Heard {
    let (log_tx, log) = watch::channel(Vec::new());
    Heard {
        log_tx: Arc::new(log_tx),
        log,
        urls: std::sync::Mutex::new(Vec::new()),
    }
}

impl Heard {
    // The URL of a new tracker for the torrent `name`.
    async fn url(&self, name: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        self.urls.lock().unwrap().push(url.clone());
        let log_tx = Arc::clone(&self.log_tx);
        let name = name.to_owned();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let log_tx = Arc::clone(&log_tx);
                let name = name.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => request.extend_from_slice(&buf[..read]),
                        }
                    }
                    let request = String::from_utf8_lossy(&request).into_owned();
                    let path = request.split(' ').nth(1).unwrap_or_default();
                    let event = path
                        .split_once('?')
                        .map(|(_, query)| query)
                        .unwrap_or_default()
                        .split('&')
                        .find_map(|pair| pair.strip_prefix("event="))
                        .unwrap_or_default()
                        .to_owned();
                    // Kept before the answer goes out: once the torrent reads
                    // its answer, the announce is in the log.
                    log_tx.send_modify(|log| log.push((name, event)));
                    let body = b"d8:intervali1800e5:peers0:e";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(&[head.as_bytes(), body].concat()).await;
                });
            }
        });
        url
    }
}

// One side of a comparison: its rig, its tracker and what it keeps alive.
struct Scene {
    rig: Rig,
    heard: Heard,
}

#[derive(Clone, Copy)]
enum Run {
    // The whole tick.
    Tick,
    // The reading alone, writing every ledger now.
    ReadAtOnce,
}

// How each torrent stands in its session: none, paused or running.
async fn standing(service: &TorrentService, jobs: &[(&str, JobId)]) -> Vec<Option<bool>> {
    let live = service.inner.live.lock().await;
    jobs.iter()
        .map(|(_, job_id)| {
            live.get(job_id)
                .and_then(|torrent| torrent.attached.as_ref())
                .map(|attached| attached.handle.is_paused())
        })
        .collect()
}

// Each torrent's announces settled: none waiting for its answer, and a
// running one's `started` answered.
async fn quiet(service: &TorrentService, jobs: &[(&str, JobId)]) {
    for (&(_, job_id), stands) in jobs.iter().zip(standing(service, jobs).await) {
        let Some(paused) = stands else { continue };
        let Some(table) = handle_of(service, job_id).await.tracker_status() else {
            continue;
        };
        table
            .subscribe()
            .wait_for(|announces| {
                announces.iter().all(|announce| !announce.in_flight)
                    && (paused
                        || announces.iter().any(|announce| {
                            announce
                                .last
                                .as_ref()
                                .is_some_and(|last| last.result.is_ok())
                        }))
            })
            .await
            .unwrap();
    }
}

// After the run: each torrent whose run in its session changed has its
// tracker hear it after `from`, and no announce of it waits for its answer.
// One removed from its session while it ran has its tracker hear it after
// `from` too; its session no longer shows its announces.
async fn announced(scene: &Scene, jobs: &[(&str, JobId)], before: &[Option<bool>], from: usize) {
    let after = standing(&scene.rig.service, jobs).await;
    for ((&(name, job_id), was), now) in jobs.iter().zip(before).zip(&after) {
        let mut log = scene.heard.log.clone();
        let mut heard = async || {
            log.wait_for(|log| log[from..].iter().any(|(heard, _)| heard == name))
                .await
                .unwrap();
        };
        let (Some(was), Some(now)) = (was, now) else {
            if *was == Some(false) {
                heard().await;
            }
            continue;
        };
        if was == now {
            continue;
        }
        heard().await;
        let table = handle_of(&scene.rig.service, job_id)
            .await
            .tracker_status()
            .unwrap();
        table
            .subscribe()
            .wait_for(|announces| announces.iter().all(|announce| !announce.in_flight))
            .await
            .unwrap();
    }
}

// What the run left, as `written` writes it, with each torrent's kept
// pieces, stored peers and the announces of each event its tracker heard
// during the run.
async fn left(
    scene: &Scene,
    jobs: &[(&str, JobId)],
    events_before: &[(
        QueueEvent,
        Vec<crate::post_processing::events::EventContext>,
    )],
    from: usize,
) -> String {
    let mut out = written(&scene.rig, jobs, events_before).await;
    let log = scene.heard.log.borrow().clone();
    for (name, job_id) in jobs {
        let count = |event: &str| {
            log[from..]
                .iter()
                .filter(|(heard, said)| heard == name && said == event)
                .count()
        };
        out.push_str(&format!(
            "== {name} kept_pieces {} peers {:?} announces started {} stopped {} completed {} other {}\n",
            kept_pieces(&scene.rig.db, *job_id),
            scene.rig.db.torrent_peers(*job_id).ok(),
            count("started"),
            count("stopped"),
            count("completed"),
            count(""),
        ));
    }
    for url in scene.heard.urls.lock().unwrap().iter() {
        out = out.replace(url, "<tracker>");
    }
    out
}

// The line of `left` that counts `name`'s announces.
fn announces_of<'a>(left: &'a str, name: &str) -> &'a str {
    let head = format!("== {name} kept_pieces");
    left.lines().find(|line| line.starts_with(&head)).unwrap()
}

impl Scene {
    // The tracker named for each torrent of `jobs`, by its name, and every
    // announce that brings answered.
    async fn track(&self, jobs: &[(&str, JobId)]) {
        for (name, job_id) in jobs {
            let paused = self.rig.service.inner.live.lock().await[job_id]
                .attached
                .as_ref()
                .map(|attached| attached.handle.is_paused());
            let Some(paused) = paused else { continue };
            let from = self.heard.log.borrow().len();
            tracked(&self.rig.service, *job_id, &self.heard.url(name).await).await;
            // A running torrent whose run has not begun yet (a restart
            // still checks it) has no announces to wait on until it does:
            // its tracker hearing it is what says the run began.
            if !paused {
                self.heard
                    .log
                    .clone()
                    .wait_for(|log| log[from..].iter().any(|(heard, _)| heard == name))
                    .await
                    .unwrap();
            }
        }
        quiet(&self.rig.service, jobs).await;
    }

    // `run`, the old way or the new, and what it left.
    async fn run(&self, jobs: &[(&str, JobId)], run: Run, old: bool) -> String {
        quiet(&self.rig.service, jobs).await;
        let events_before = events(&self.rig.db).await;
        let before = standing(&self.rig.service, jobs).await;
        let from = self.heard.log.borrow().len();
        let service = &self.rig.service;
        match (run, old) {
            (Run::Tick, true) => service.old_tick().await,
            (Run::Tick, false) => service.tick().await,
            (Run::ReadAtOnce, true) => service.old_read(service.inner.clock.unix_ms(), true).await,
            (Run::ReadAtOnce, false) => service.read(service.inner.clock.unix_ms(), true).await,
        }
        self.rig.db.flush_write_queue().await.unwrap();
        announced(self, jobs, &before, from).await;
        left(self, jobs, &events_before, from).await
    }
}

// Two scenes `make` builds and `setup` sets up alike, one run the old way
// and one the new: what each leaves, which must be the same.
async fn compare(
    make: impl Fn() -> Rig,
    setup: impl AsyncFn(&mut Scene) -> Vec<(&'static str, JobId)>,
    run: Run,
) -> String {
    let mut out = Vec::new();
    for old in [true, false] {
        let mut scene = Scene {
            rig: make(),
            heard: heard(),
        };
        let jobs = setup(&mut scene).await;
        out.push(scene.run(&jobs, run, old).await);
        scene.rig.service.stop().await;
    }
    let new = out.pop().unwrap();
    let old = out.pop().unwrap();
    println!("{new}");
    assert_eq!(old, new);
    new
}

// The queue applied as the settle loop would apply it, so that what the
// adds started is in place before the trackers are named.
async fn settled_queue(scene: &Scene) {
    scene.rig.service.apply_queue_and_picking().await;
}

// A torrent still in seed mode after a restart, whose session has checked
// more pieces than the pass last kept: the pass keeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_still_in_seed_mode_keeps_its_checked_pieces_as_before() {
    let left = compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let job_id = restarted_in_seed_mode(scene).await;
            vec![("seed", job_id)]
        },
        Run::Tick,
    )
    .await;
    assert!(left.contains("seed_verified Some([160])"), "{left}");
}

// `scene`'s rig with one placed torrent added in seed mode, two of its
// four pieces checked and kept, then restarted, its live count one behind
// what its session holds checked. Its id.
async fn restarted_in_seed_mode(scene: &mut Scene) -> JobId {
    let (metainfo, data) = placed_torrent();
    let rig = &scene.rig;
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/a.bin"), &data).unwrap();
    let (job_id, _) = seeding_placed(
        rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            seed_mode: true,
            ..Placement::default()
        },
    )
    .await;
    rig.db
        .set_torrent_seed_verified(job_id, &[0b1010_0000])
        .unwrap();
    rig.service.stop().await;
    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    scene.rig.service = restarted;
    let handle = handle_of(&scene.rig.service, job_id).await;
    assert!(handle.seed_mode());
    assert_eq!(handle.seed_verified().unwrap().count_ones(), 2);
    rig_set_verified(&scene.rig, job_id, &[0b1000_0000], 1).await;
    scene.track(&[("seed", job_id)]).await;
    job_id
}

// The torrent's checked pieces as the pass last kept them: `bits` stored,
// `count` in memory.
async fn rig_set_verified(rig: &Rig, job_id: JobId, bits: &[u8], count: usize) {
    rig.db.set_torrent_seed_verified(job_id, bits).unwrap();
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .seed_verified = count;
}

// A torrent with a piece fetched since its last checkpoint: the tick
// writes its checkpoint, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_due_in_the_tick_is_written_as_before() {
    let left = compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            rig.service.open().await.unwrap();
            let base = || request(AddSource::Metainfo(Vec::new()));
            let fetching = add(rig, "fetching", 1, base()).await;
            let idle = add(rig, "idle", 2, base()).await;
            settled_queue(scene).await;
            let jobs = vec![("fetching", fetching), ("idle", idle)];
            scene.track(&jobs).await;
            fetch_pieces(&rig.service, fetching, &[1; 3000], Some(&[0]), |_| {}).await;
            jobs
        },
        Run::Tick,
    )
    .await;
    assert!(left.contains("== fetching kept_pieces 1"), "{left}");
}

// One download at a time: the torrent holding the turn meets its goal and
// is removed, and the one queued behind it starts in the same tick, as
// before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_turn_holder_lets_the_next_start_as_before() {
    let left = compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            rig.service.open().await.unwrap();
            let mut settings = rig.service.settings();
            settings.torrent.max_active_downloads = 1;
            rig.service.set_settings(settings).await;
            let base = || request(AddSource::Metainfo(Vec::new()));
            let holder = add(
                rig,
                "holder",
                1,
                AddRequest {
                    policy_override: done_then(SeedingAction::Remove),
                    ..base()
                },
            )
            .await;
            let next = add(rig, "next", 2, base()).await;
            settled_queue(scene).await;
            let jobs = vec![("holder", holder), ("next", next)];
            scene.track(&jobs).await;
            assert_eq!(
                standing(&rig.service, &jobs).await,
                [Some(false), Some(true)]
            );
            complete(rig, holder).await;
            jobs
        },
        Run::Tick,
    )
    .await;
    assert!(
        left.contains(
            "== next\nlive: kept Intent(Resumed) seed_mode false seed_verified 0 queued false"
        ),
        "{left}"
    );
    assert!(
        announces_of(&left, "holder").contains("stopped 1 "),
        "{left}"
    );
    assert!(announces_of(&left, "next").contains("started 1 "), "{left}");
}

// The schedule holds every torrent; a seed whose tracker rule never pauses
// runs through it once the tick sees the rule, and the other stays held,
// as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_and_a_never_pause_rule_apply_as_before() {
    let left = compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            let handle = queue();
            rig.service.publish_to(handle.clone());
            rig.service.open().await.unwrap();
            let rule = rig
                .db
                .insert_tracker_rule(
                    &TrackerRule {
                        transports: Default::default(),
                        archive_passwords: Default::default(),
                        bound: None,
                        id: 0,
                        name: "never.test".into(),
                        host_patterns: vec!["never.test".into()],
                        seeding: None,
                        never_pause: true,
                        unpack: None,
                        client: None,
                        torrent_route: false,
                        dht: None,
                        pex: None,
                        lsd: None,
                        encryption: None,
                        upload_cap: Default::default(),
                        forced: false,
                        ssl: None,
                    },
                    EPOCH_MS,
                )
                .unwrap();
            rig.service.load_rules().await.unwrap();
            let base = || request(AddSource::Metainfo(Vec::new()));
            let kept = add(rig, "kept", 1, base()).await;
            let held = add(rig, "held", 2, base()).await;
            for job_id in [kept, held] {
                complete(rig, job_id).await;
            }
            let jobs = vec![("kept", kept), ("held", held)];
            settled_queue(scene).await;
            scene.track(&jobs).await;
            let mut set = HoldSet::default();
            set.hold(Hold {
                origin: HoldOrigin::Scheduled,
                scope: HoldScope::Everything,
                owner: HoldOwner::Class(crate::torrents::CLASS),
            });
            let mut published = handle.state().subscribe_job_changes();
            handle.state().restore_holds(set);
            while standing(&rig.service, &jobs).await != [Some(true), Some(true)] {
                published.changed().await.unwrap();
            }
            // Each paused torrent's `stopped` heard before the tick.
            let mut log = scene.heard.log.clone();
            log.wait_for(|log| {
                ["kept", "held"].iter().all(|name| {
                    log.iter()
                        .any(|(heard, event)| heard == name && event == "stopped")
                })
            })
            .await
            .unwrap();
            quiet(&rig.service, &jobs).await;
            // Seen first by the tick: nothing else applies the rule.
            rig.service
                .inner
                .live
                .lock()
                .await
                .get_mut(&kept)
                .unwrap()
                .rule_id = Some(rule.id);
            jobs
        },
        Run::Tick,
    )
    .await;
    assert!(announces_of(&left, "kept").contains("started 1 "), "{left}");
    assert!(announces_of(&left, "held").contains("started 0 "), "{left}");
}

// A torrent with bytes to fetch whose turn comes on a disk with no room
// over the reserve is held from starting by the tick, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_disk_holds_a_start_as_before() {
    const RESERVE: u64 = 100;
    let roots = tempfile::tempdir().unwrap();
    let capacity = Arc::new(StorageCapacity::with_probe(
        [0, 1, 2].map(|_| roots.path().to_path_buf()),
        Duration::from_secs(365 * 24 * 60 * 60),
        Arc::new(|_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE,
            })
        }),
    ));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let env = || {
        let mut limits = ExtractionLimits::unbounded();
        limits.min_free_bytes = RESERVE;
        JobEnv {
            free_disk: capacity.reader(StorageRoot::Complete),
            capacity: Some(Arc::clone(&capacity)),
            limits: Arc::new(limits),
            ..JobEnv::unbounded()
        }
    };
    let left = compare(
        || rig_with(set_clock(), env()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            rig.service.open().await.unwrap();
            let short = add(rig, "short", 1, request(AddSource::Metainfo(Vec::new()))).await;
            let mut moved = rig.service.inner.disk_waits_moved.subscribe();
            loop {
                let place = rig.service.inner.live.lock().await[&short]
                    .intake
                    .as_ref()
                    .and_then(|intake| intake.room().place());
                if place.is_some() {
                    break;
                }
                moved.changed().await.unwrap();
            }
            settled_queue(scene).await;
            let jobs = vec![("short", short)];
            scene.track(&jobs).await;
            // Decided again by the tick: the hold the settle took is let go.
            let mut live = rig.service.inner.live.lock().await;
            let torrent = live.get_mut(&short).unwrap();
            assert_eq!(torrent.disk_held, Some(DiskHeld::Starting));
            torrent.disk_held = None;
            drop(live);
            jobs
        },
        Run::Tick,
    )
    .await;
    assert!(left.contains("disk_held Some(Starting)"), "{left}");
}

// An upload cap set at the global seeding level reaches every torrent's
// session in the tick, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upload_cap_from_the_seeding_policy_applies_as_before() {
    let left = compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            rig.service.open().await.unwrap();
            let base = || request(AddSource::Metainfo(Vec::new()));
            let capped = add(rig, "capped", 1, base()).await;
            let seed = add(rig, "seed", 2, base()).await;
            complete(rig, seed).await;
            settled_queue(scene).await;
            let jobs = vec![("capped", capped), ("seed", seed)];
            scene.track(&jobs).await;
            // Seen first by the tick.
            rig.service
                .inner
                .settings
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .upload_cap = Some(8192);
            jobs
        },
        Run::Tick,
    )
    .await;
    assert!(left.contains("upload_bps: Bps(8192)"), "{left}");
}

// A reading told to write every ledger now writes each, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reading_at_once_writes_every_ledger_as_before() {
    compare(
        || rig(set_clock()),
        async |scene: &mut Scene| {
            let rig = &scene.rig;
            on_loopback(&rig.routes);
            rig.service.open().await.unwrap();
            let base = || request(AddSource::Metainfo(Vec::new()));
            let one = add(rig, "one", 1, base()).await;
            let two = add(rig, "two", 2, base()).await;
            settled_queue(scene).await;
            let jobs = vec![("one", one), ("two", two)];
            scene.track(&jobs).await;
            jobs
        },
        Run::ReadAtOnce,
    )
    .await;
}

// Refuses every write of `torrents.seed_mode` or `torrents.seed_verified`
// until `allow_seed_writes` drops the trigger `name`. A refusal after one
// is dropped takes a new name: the same CREATE text again is refused as the
// trigger existing still.
fn refuse_seed_writes(db: &Database, name: &str) {
    let datastore = db.datastore();
    let sql = format!(
        "CREATE TRIGGER {name} BEFORE UPDATE OF seed_mode, seed_verified \
         ON torrents BEGIN SELECT RAISE(ABORT, 'refused'); END"
    );
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.write_exec(), &sql, &[]).await?;
        Ok(())
    })
    .unwrap();
}

fn allow_seed_writes(db: &Database, name: &str) {
    let datastore = db.datastore();
    let sql = format!("DROP TRIGGER {name}");
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.write_exec(), &sql, &[]).await?;
        Ok(())
    })
    .unwrap();
}

// The stored seed mode of `job_id`.
fn stored_seed_mode(db: &Database, job_id: JobId) -> (bool, Option<Vec<u8>>) {
    (
        db.get_torrent(job_id).unwrap().unwrap().seed_mode,
        db.torrent_seed_verified(job_id).unwrap(),
    )
}

// What the pass holds of `job_id`'s seed mode.
async fn live_seed_mode(service: &TorrentService, job_id: JobId) -> (bool, usize) {
    let live = service.inner.live.lock().await;
    let torrent = &live[&job_id];
    (torrent.record.seed_mode, torrent.seed_verified)
}

// A torrent whose session left seed mode, when the database refuses to
// record that: the pass holds it in seed mode with the pieces it had
// checked, as the disk still says, and the next pass records it. A seed
// meeting its goal in the same tick stops, its stop kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_seed_mode_clear_leaves_memory_as_the_disk_says() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let base = || request(AddSource::Metainfo(Vec::new()));
    let left = add(&rig, "left", 1, base()).await;
    complete(&rig, left).await;
    let met = add(
        &rig,
        "met",
        2,
        AddRequest {
            policy_override: done_then(SeedingAction::Stop),
            ..base()
        },
    )
    .await;
    complete(&rig, met).await;
    {
        let datastore = rig.db.datastore();
        let id = left.0;
        rig.db
            .run_sql_blocking(async move {
                SqlRuntime::execute(
                    datastore.write_exec(),
                    &format!("UPDATE torrents SET seed_mode = 1, seed_verified = X'80' WHERE job_id = {id}"),
                    &[],
                )
                .await?;
                Ok(())
            })
            .unwrap();
    }
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&left).unwrap();
        torrent.record.seed_mode = true;
        torrent.seed_verified = 1;
    }
    refuse_seed_writes(&rig.db, "refuse_seed_writes");

    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(live_seed_mode(&rig.service, left).await, (true, 1));
    assert_eq!(stored_seed_mode(&rig.db, left), (true, Some(vec![0x80])));
    assert_eq!(
        rig.db.get_torrent(met).unwrap().unwrap().intent(),
        TorrentIntent::Stopped
    );
    assert_eq!(
        rig.service.inner.live.lock().await[&met].record.kept,
        KeptIntent::Intent(TorrentIntent::Stopped)
    );

    allow_seed_writes(&rig.db, "refuse_seed_writes");
    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(live_seed_mode(&rig.service, left).await, (false, 0));
    assert_eq!(stored_seed_mode(&rig.db, left), (false, None));
    assert_eq!(
        rig.db.get_torrent(met).unwrap().unwrap().intent(),
        TorrentIntent::Stopped
    );
    rig.service.stop().await;
}

// Every warning logged on this thread, as its fields, one line each.
struct Warned(Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::Subscriber for Warned {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, " {field}={value:?}");
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

// How many times it was warned that `job_id`'s leaving seed mode was not
// recorded.
fn left_refusals(warned: &std::sync::Mutex<Vec<String>>, job_id: JobId) -> usize {
    refusals(
        warned,
        job_id,
        "could not record that a torrent left seed mode",
    )
}

// How many times it was warned that the pieces `job_id` checked in seed
// mode were not kept.
fn verified_refusals(warned: &std::sync::Mutex<Vec<String>>, job_id: JobId) -> usize {
    refusals(
        warned,
        job_id,
        "could not keep the pieces a torrent checked in seed mode",
    )
}

fn refusals(warned: &std::sync::Mutex<Vec<String>>, job_id: JobId, message: &str) -> usize {
    let id = format!(" job_id={} ", job_id.0);
    warned
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|line| line.contains(&id) && line.contains(message))
        .count()
}

// `job_id` back in seed mode, on disk and in the pass, its session out of
// it: the next pass decides it left.
async fn in_seed_mode_again(rig: &Rig, job_id: JobId) {
    let datastore = rig.db.datastore();
    let id = job_id.0;
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                &format!(
                    "UPDATE torrents SET seed_mode = 1, seed_verified = X'80' WHERE job_id = {id}"
                ),
                &[],
            )
            .await?;
            Ok(())
        })
        .unwrap();
    let mut live = rig.service.inner.live.lock().await;
    let torrent = live.get_mut(&job_id).unwrap();
    torrent.record.seed_mode = true;
    torrent.seed_verified = 1;
}

// Torrents whose leaving seed mode the database refuses tick after tick:
// each is written again every tick and warned of once, not every tick. A
// write kept ends the streak, so a later refusal warns again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_seed_mode_clear_warns_once_per_streak() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let base = || request(AddSource::Metainfo(Vec::new()));
    let first = add(&rig, "first", 1, base()).await;
    complete(&rig, first).await;
    let second = add(&rig, "second", 2, base()).await;
    complete(&rig, second).await;
    for job_id in [first, second] {
        in_seed_mode_again(&rig, job_id).await;
    }
    // The passes run on this thread when ticked: only their warnings are
    // heard.
    let warned = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(Warned(Arc::clone(&warned)));
    refuse_seed_writes(&rig.db, "refuse_seed_writes");

    for _ in 0..5 {
        rig.service.tick().await;
        rig.service.restored().await;
        rig.db.flush_write_queue().await.unwrap();
        assert_eq!(live_seed_mode(&rig.service, first).await, (true, 1));
        assert_eq!(live_seed_mode(&rig.service, second).await, (true, 1));
    }
    assert_eq!(left_refusals(&warned, first), 1);
    assert_eq!(left_refusals(&warned, second), 1);

    allow_seed_writes(&rig.db, "refuse_seed_writes");
    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(stored_seed_mode(&rig.db, first), (false, None));
    assert_eq!(stored_seed_mode(&rig.db, second), (false, None));
    assert!(!rig.service.inner.live.lock().await[&first].seed_left_refused);

    in_seed_mode_again(&rig, first).await;
    refuse_seed_writes(&rig.db, "refuse_seed_writes_again");
    for _ in 0..3 {
        rig.service.tick().await;
        rig.service.restored().await;
        rig.db.flush_write_queue().await.unwrap();
    }
    assert_eq!(live_seed_mode(&rig.service, first).await, (true, 1));
    assert_eq!(left_refusals(&warned, first), 2);
    assert_eq!(left_refusals(&warned, second), 1);
    rig.service.stop().await;
}

// A torrent still in seed mode whose newly checked pieces the database
// refuses to keep: the pass keeps the count it last wrote, and the next
// pass writes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_seed_verified_write_keeps_the_last_count() {
    let mut scene = Scene {
        rig: rig(set_clock()),
        heard: heard(),
    };
    let job_id = restarted_in_seed_mode(&mut scene).await;
    let rig = &scene.rig;
    refuse_seed_writes(&rig.db, "refuse_seed_writes");

    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(live_seed_mode(&rig.service, job_id).await, (true, 1));
    assert_eq!(
        stored_seed_mode(&rig.db, job_id),
        (true, Some(vec![0b1000_0000]))
    );

    allow_seed_writes(&rig.db, "refuse_seed_writes");
    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(live_seed_mode(&rig.service, job_id).await, (true, 2));
    assert_eq!(
        stored_seed_mode(&rig.db, job_id),
        (true, Some(vec![0b1010_0000]))
    );
    rig.service.stop().await;
}

// A torrent in seed mode whose checked pieces the database refuses to keep
// tick after tick: written again every tick and warned of once. A write
// kept ends the streak, so a later refusal warns again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_seed_verified_write_warns_once_per_streak() {
    let mut scene = Scene {
        rig: rig(set_clock()),
        heard: heard(),
    };
    let job_id = restarted_in_seed_mode(&mut scene).await;
    let rig = &scene.rig;
    // The passes run on this thread when ticked: only their warnings are
    // heard.
    let warned = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture = tracing::subscriber::set_default(Warned(Arc::clone(&warned)));
    refuse_seed_writes(&rig.db, "refuse_seed_writes");

    for _ in 0..5 {
        rig.service.tick().await;
        rig.service.restored().await;
        rig.db.flush_write_queue().await.unwrap();
        assert_eq!(live_seed_mode(&rig.service, job_id).await, (true, 1));
    }
    assert_eq!(verified_refusals(&warned, job_id), 1);

    allow_seed_writes(&rig.db, "refuse_seed_writes");
    rig.service.tick().await;
    rig.service.restored().await;
    rig.db.flush_write_queue().await.unwrap();
    assert_eq!(live_seed_mode(&rig.service, job_id).await, (true, 2));
    assert_eq!(
        stored_seed_mode(&rig.db, job_id),
        (true, Some(vec![0b1010_0000]))
    );
    assert!(!rig.service.inner.live.lock().await[&job_id].seed_verified_refused);

    rig_set_verified(rig, job_id, &[0b1000_0000], 1).await;
    refuse_seed_writes(&rig.db, "refuse_seed_writes_again");
    for _ in 0..3 {
        rig.service.tick().await;
        rig.service.restored().await;
        rig.db.flush_write_queue().await.unwrap();
    }
    assert_eq!(live_seed_mode(&rig.service, job_id).await, (true, 1));
    assert_eq!(verified_refusals(&warned, job_id), 2);
    assert_eq!(left_refusals(&warned, job_id), 0);
    rig.service.stop().await;
}
