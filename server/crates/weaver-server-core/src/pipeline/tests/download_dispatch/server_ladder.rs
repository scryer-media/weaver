// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An article missing everywhere walks the server ladder: one ask per server
// that may still answer, best level first, backfill only once every fill
// server has said no. A server that already said no for a queue costs that
// server one walk of the queue, not one walk per ask.

use super::*;
use crate::pipeline::download::scheduler::{Handout, set_scan_floors_off};

// A client over `levels`: each server's priority group and backfill flag.
// Nothing dials; the scheduler only reads the policy.
fn ladder_client(levels: &[(u32, bool)]) -> NntpClient {
    ladder_client_with_quota(levels, None)
}

// As [`ladder_client`], with one server metered by `quota`'s control.
fn ladder_client_with_quota(
    levels: &[(u32, bool)],
    quota: Option<(usize, Arc<weaver_nntp::transfer::ServerTransferControl>)>,
) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: levels
            .iter()
            .enumerate()
            .map(
                |(idx, &(group, backfill))| weaver_nntp::pool::ServerPoolConfig {
                    max_connections: 100,
                    group,
                    backfill,
                    stable_id: weaver_nntp::transfer::StableServerId(idx as u32),
                    transfer_control: quota
                        .as_ref()
                        .filter(|(metered, _)| *metered == idx)
                        .map(|(_, control)| Arc::clone(control)),
                    ..Default::default()
                },
            )
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

async fn ladder_pipeline(temp_dir: &TempDir, levels: &[(u32, bool)]) -> Pipeline {
    let (mut pipeline, _, _) = crate::pipeline::tests::new_scheduler_pipeline(temp_dir).await;
    pipeline.nntp = Arc::new(ladder_client(levels));
    pipeline
}

// Answer `work`, handed to `server`, "not found" through the result path a
// lane's 430 takes, so the exclusion, the requeue and any booking are the
// product's own.
async fn answer_not_found(pipeline: &mut Pipeline, server: usize, work: DownloadWork) {
    answer_failed(
        pipeline,
        server,
        work,
        DownloadFailureKind::ArticleNotFound,
        "430 no such article",
    )
    .await;
}

// Fail the connection under `work`, handed to `server`, through the result
// path a lane's lost connection takes: an ordinary retry, which refuses the
// article to no server.
async fn answer_connection_failed(pipeline: &mut Pipeline, server: usize, work: DownloadWork) {
    answer_failed(
        pipeline,
        server,
        work,
        DownloadFailureKind::EstablishedTransport,
        "connection closed",
    )
    .await;
}

async fn answer_failed(
    pipeline: &mut Pipeline,
    server: usize,
    work: DownloadWork,
    kind: DownloadFailureKind,
    said: &str,
) {
    let (job_id, segment_id) = (work.segment_id.file_id.job_id, work.segment_id);
    let (retry_count, exclude_servers) = (work.retry_count, work.exclude_servers);
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(DownloadError::fetch(kind, said)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(server),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
}

fn ask(pipeline: &mut Pipeline, server_idx: usize, want: usize) -> Vec<DownloadWork> {
    let pressure = pipeline.refresh_download_pressure();
    match pipeline.next_works(server_idx, want, None, pressure) {
        Handout::Works(works) => works,
        Handout::Idle => Vec::new(),
        Handout::Yield(reason) => panic!("a whole-link gate fired: {reason:?}"),
        Handout::Saturated(wake) => panic!("no lane share was booked: {wake:?}"),
    }
}

// `count` servers spread over ten levels so that pool order and level order
// differ; the two worst levels are backfill.
fn ten_levels(count: usize, stride: usize) -> Vec<(u32, bool)> {
    (0..count)
        .map(|idx| {
            let group = ((idx * stride) % 10) as u32;
            (group, group >= 8)
        })
        .collect()
}

// Run one article missing everywhere to its end. Each round asks every
// server in pool order; the one handed the article answers "not found".
// Returns the servers asked, in order, and the articles booked missing.
async fn run_ladder(levels: &[(u32, bool)], refusing: &[usize]) -> (Vec<usize>, u64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = ladder_pipeline(&temp_dir, levels).await;
    for server in refusing {
        pipeline
            .nntp
            .pool()
            .health()
            .lock()
            .await
            .record_failure(*server, true);
    }
    let job_id = JobId(42001);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Missing Everywhere", "payload.bin", &[128]),
    )
    .await;
    pipeline.active_download_passes.insert(job_id);

    let mut asked = Vec::new();
    // One round per server at most, plus the round that finds nothing.
    for _ in 0..=levels.len() {
        let mut handed = None;
        for server in 0..levels.len() {
            let mut works = ask(&mut pipeline, server, 1);
            if works.is_empty() {
                continue;
            }
            assert_eq!(works.len(), 1);
            handed = Some((server, works.pop().unwrap()));
            break;
        }
        let Some((server, work)) = handed else {
            break;
        };
        asked.push(server);
        answer_not_found(&mut pipeline, server, work).await;
    }
    let lost = pipeline.metrics.articles_not_found.load(Ordering::Relaxed);
    (asked, lost)
}

// Servers in ladder order: fill before backfill, then level, then pool order.
fn ladder_order(levels: &[(u32, bool)], skip: impl Fn(usize) -> bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..levels.len()).filter(|idx| !skip(*idx)).collect();
    order.sort_by_key(|idx| (levels[*idx].1, levels[*idx].0, *idx));
    order
}

async fn assert_ladder(levels: &[(u32, bool)]) {
    let (asked, lost) = run_ladder(levels, &[]).await;
    assert_eq!(
        asked,
        ladder_order(levels, |_| false),
        "one ask per server, level by level, backfill last"
    );
    assert_eq!(lost, 1, "booked missing once every server said no");
}

#[tokio::test]
async fn an_article_missing_everywhere_asks_25_servers_on_10_levels_once_each() {
    assert_ladder(&ten_levels(25, 3)).await;
}

#[tokio::test]
async fn an_article_missing_everywhere_asks_50_servers_on_10_levels_once_each() {
    assert_ladder(&ten_levels(50, 7)).await;
}

// A fill server refusing the credentials has answered: the ladder steps past
// it. It never opens backfill, which only a "not found" from every fill
// server does, so the article is booked missing without a backfill ask.
#[tokio::test]
async fn a_fill_server_refusing_credentials_is_passed_and_keeps_backfill_shut() {
    let levels = ten_levels(50, 7);
    let refusing = 3;
    assert!(!levels[refusing].1);
    let (asked, lost) = run_ladder(&levels, &[refusing]).await;
    assert_eq!(
        asked,
        ladder_order(&levels, |idx| idx == refusing || levels[idx].1)
    );
    assert_eq!(lost, 1);
}

// The field's layout: three servers at the top level, two at the next,
// three at the third, then backfill at two levels.
fn field_levels() -> Vec<(u32, bool)> {
    let mut levels = vec![(0, false); 3];
    levels.extend([(1, false); 2]);
    levels.extend([(2, false); 3]);
    levels.extend([(3, true); 2]);
    levels.extend([(4, true); 3]);
    levels
}

// A dead post, `files` files of `articles` articles each, no recovery,
// every article missing on every server, run with every server asking each
// round as a free lane would: each server with something it may take is
// handed one article, and every article handed out that round is answered
// "not found" through the lane result path. Returns the asks made until the
// post was given up (all of them when it never was), the asks a full walk
// would make, and the order of each article's asks.
struct DeadPostRun {
    asks_at_give_up: Option<usize>,
    rounds_at_give_up: Option<usize>,
    asks: usize,
    // (ask, article) for each article, in the order its walk ended.
    walked_out: Vec<(usize, SegmentId)>,
    per_article: HashMap<SegmentId, Vec<usize>>,
    reason: Option<String>,
    lost: u64,
}

async fn run_dead_post(levels: &[(u32, bool)], files: u32, articles: u32) -> DeadPostRun {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = ladder_pipeline(&temp_dir, levels).await;
    let mut spec = segmented_job_spec("Dead Field Post", "f00.mkv", &vec![128; articles as usize]);
    let template = spec.files[0].clone();
    spec.files = (0..files)
        .map(|file| {
            let mut spec = template.clone();
            spec.filename = format!("f{file:02}.mkv");
            for segment in &mut spec.segments {
                segment.message_ids = crate::jobs::MessageIds::new(vec![format!(
                    "f{file}-s{}@example.com",
                    segment.ordinal
                )]);
            }
            spec
        })
        .collect();
    spec.total_bytes *= u64::from(files);
    let job_id = JobId(42003);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.active_download_passes.insert(job_id);
    {
        let queue = &mut pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue;
        for file in 0..files {
            queue.note_first_article(held_segment(job_id, file, 0));
        }
    }
    rewrite_queue(&mut pipeline, job_id, |_| {});

    let mut run = DeadPostRun {
        asks_at_give_up: None,
        rounds_at_give_up: None,
        asks: 0,
        walked_out: Vec::new(),
        per_article: HashMap::new(),
        reason: None,
        lost: 0,
    };
    let mut rounds = 0;
    loop {
        rounds += 1;
        let handed: Vec<(usize, DownloadWork)> = (0..levels.len())
            .filter_map(|server| {
                let mut works = ask(&mut pipeline, server, 1);
                works.pop().map(|work| (server, work))
            })
            .collect();
        if handed.is_empty() {
            break;
        }
        for (server, work) in handed {
            run.asks += 1;
            let asked = run.per_article.entry(work.segment_id).or_default();
            asked.push(server);
            if asked.len() == levels.len() {
                run.walked_out.push((run.asks, work.segment_id));
            }
            answer_not_found(&mut pipeline, server, work).await;
            if run.asks_at_give_up.is_none() && pipeline.post_probes.dead.contains_key(&job_id) {
                run.asks_at_give_up = Some(run.asks);
                run.rounds_at_give_up = Some(rounds);
            }
        }
    }
    run.reason = pipeline.post_probes.dead.get(&job_id).cloned();
    run.lost = pipeline.metrics.articles_not_found.load(Ordering::Relaxed);
    run
}

// The field's shape: thirteen servers over five levels, 25 files of four
// articles, no recovery, all missing. Each article walks the servers in
// level order,
// and the post is given up, in the sample's words, on the ask that ends the
// twentieth first article's walk: 20 of 25 is the sample's 80% share. Its
// health crossed its line at the sixteenth, but a sample with first
// articles still to come rules first, so the files it would leave stay
// fetchable.
#[tokio::test]
async fn a_dead_post_on_the_field_layout_asks_first_articles_first_and_ends() {
    const FILES: u32 = 25;
    const ARTICLES: u32 = 4;
    let levels = field_levels();
    let run = run_dead_post(&levels, FILES, ARTICLES).await;
    let full_walk = (FILES * ARTICLES) as usize * levels.len();
    eprintln!(
        "field shape: give-up after {:?} of {} asks, round {:?} ({} made), {} booked lost: {:?}",
        run.asks_at_give_up, full_walk, run.rounds_at_give_up, run.asks, run.lost, run.reason
    );
    let twentieth_head = run
        .walked_out
        .iter()
        .filter(|(_, segment)| segment.segment_number == 0)
        .nth(19)
        .map(|(ask, _)| *ask);
    assert_eq!(run.asks_at_give_up, twentieth_head);
    assert!(
        run.reason
            .as_deref()
            .is_some_and(|reason| reason.contains("20 of 25 first articles")),
        "{:?} after {} asks",
        run.reason,
        run.asks
    );
    for file in 0..FILES {
        let head = held_segment(JobId(42003), file, 0);
        let asked = run.per_article.get(&head).cloned().unwrap_or_default();
        assert!(
            asked
                .windows(2)
                .all(|pair| (levels[pair[0]].1, levels[pair[0]].0)
                    <= (levels[pair[1]].1, levels[pair[1]].0)),
            "{head} walked the levels out of order: {asked:?}"
        );
    }
    // No sooner is possible: a first article is asked by one server a
    // round, so the last server first gets one in the round after the
    // twelve before it, and it asks one a round from then. A first article
    // some server already missed goes back ahead of the untried ones, so
    // the last server is never left waiting while one is open to it.
    assert_eq!(run.rounds_at_give_up, Some(levels.len() - 1 + 20));
}

// Fewer than ten files: the first-article sample does not rule, and the
// post's health ends it on the ask that ends the fourth article's walk, 4
// of 24 equal articles lost taking it to 83.3%, below its 85% line.
#[tokio::test]
async fn a_small_dead_post_on_the_field_layout_ends_on_its_health() {
    let levels = field_levels();
    let run = run_dead_post(&levels, 6, 4).await;
    eprintln!(
        "small shape: give-up after {:?} of {} asks, round {:?} ({} made), {} booked lost: {:?}",
        run.asks_at_give_up,
        6 * 4 * levels.len(),
        run.rounds_at_give_up,
        run.asks,
        run.lost,
        run.reason
    );
    assert_eq!(run.asks_at_give_up, Some(run.walked_out[3].0));
    assert!(
        run.reason
            .as_deref()
            .is_some_and(|reason| reason.contains("below critical")),
        "{:?}",
        run.reason
    );
}

// A quota of `limit_bytes` on the control of server `idx`.
fn metered(idx: u32, limit_bytes: u64) -> Arc<weaver_nntp::transfer::ServerTransferControl> {
    weaver_nntp::transfer::ServerTransferRegistry::new().configure(
        weaver_nntp::transfer::StableServerId(idx),
        quota_config(limit_bytes),
    )
}

fn quota_config(limit_bytes: u64) -> weaver_nntp::transfer::ServerTransferConfig {
    weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes,
            generation: 1,
            retry_at: None,
        }),
    }
}

// Charge `bytes` downloaded to `control`'s quota.
fn charge(control: &Arc<weaver_nntp::transfer::ServerTransferControl>, bytes: usize) {
    let mut permit = control.try_reserve(bytes as u64).unwrap();
    permit.record_unpaced(bytes);
    let _ = permit.finish();
}

// Three top servers that lack every article of a large queue, ten lower
// ones that hold them, and one more lower one on a quota that downloads
// between asks. However often a top server asks, it walks the queue once;
// a whole walk per ask is the servers-times-queue cost. Bytes charged to a
// quota with room to spare, and a job restored after a restart with bytes
// already down, keep it at one walk.
#[tokio::test]
async fn a_server_that_refused_a_queue_walks_it_once_however_often_it_asks() {
    const ARTICLES: usize = 2_000;
    const ASKS: usize = 40;
    let mut levels = vec![(0, false); 3];
    levels.extend(std::iter::repeat_n((1, false), 11));

    let mut walked = Vec::new();
    for (floors_off, restored) in [(true, false), (false, false), (false, true)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = ladder_pipeline(&temp_dir, &levels).await;
        let control = metered(13, 1 << 40);
        pipeline.nntp = Arc::new(ladder_client_with_quota(
            &levels,
            Some((13, Arc::clone(&control))),
        ));
        let job_id = JobId(42002);
        insert_active_job(
            &mut pipeline,
            job_id,
            segmented_job_spec(
                "Top Servers Lack It",
                "payload.bin",
                &vec![128u32; ARTICLES],
            ),
        )
        .await;
        rewrite_queue(&mut pipeline, job_id, |work| {
            work.exclude_servers = weaver_nntp::ServerSet::from([0, 1, 2]);
        });
        if restored {
            // Restored with bytes already down: the restart checkpoint is
            // in force, but it refuses none of what the top servers are
            // refused for.
            pipeline
                .jobs
                .get_mut(&job_id)
                .unwrap()
                .restored_download_floor_bytes = 4_096;
            assert!(!pipeline.checkpoint_admission(job_id).admits_every_article());
        }

        set_scan_floors_off(floors_off);
        let skipped = |pipeline: &Pipeline| {
            pipeline
                .metrics
                .download_scheduler_scan_items_skipped_total
                .load(Ordering::Relaxed)
        };
        let before = skipped(&pipeline);
        for _ in 0..ASKS {
            charge(&control, 700_000);
            for server in 0..3 {
                let pressure = pipeline.refresh_download_pressure();
                assert!(
                    pipeline
                        .take_servable_works_or_none(job_id, server, 8, pressure)
                        .is_none()
                );
            }
        }
        walked.push(skipped(&pipeline) - before);

        // The lower servers still take the whole queue, in queue order.
        let mut taken = 0;
        let mut server = 3;
        while let Some(works) = {
            let pressure = pipeline.refresh_download_pressure();
            pipeline.take_servable_works_or_none(job_id, server, 64, pressure)
        } {
            taken += works.len();
            server = 3 + (server - 2) % 10;
        }
        set_scan_floors_off(false);
        assert_eq!(taken, ARTICLES);
    }
    assert_eq!(
        walked[0],
        (3 * ASKS * ARTICLES) as u64,
        "a whole walk per ask"
    );
    assert_eq!(walked[1], (3 * ARTICLES) as u64, "one walk per server");
    assert_eq!(
        walked[2],
        (3 * ARTICLES) as u64,
        "one walk per server under a restored checkpoint"
    );
}

// A quota that runs out hands its level's work to the next level, and one
// that comes back takes it again, however the memo of the earlier walks
// stands: the walk is never kept past a refusal that no longer holds.
#[tokio::test]
async fn work_follows_a_quota_that_runs_out_and_comes_back() {
    let levels = vec![(0, false), (1, false)];
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = ladder_pipeline(&temp_dir, &levels).await;
    let control = metered(0, 1 << 30);
    pipeline.nntp = Arc::new(ladder_client_with_quota(
        &levels,
        Some((0, Arc::clone(&control))),
    ));
    let job_id = JobId(42003);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Metered Reel", "payload.bin", &vec![128u32; 300]),
    )
    .await;
    pipeline.active_download_passes.insert(job_id);

    // The top server has quota: the lower one is refused the whole queue,
    // twice, which leaves it a memo past the end.
    for _ in 0..2 {
        assert!(ask(&mut pipeline, 1, 4).is_empty());
    }
    assert_eq!(ask(&mut pipeline, 0, 4).len(), 4);

    // Bytes the top server downloads, with room to spare, change nothing.
    charge(&control, 100_000);
    assert!(ask(&mut pipeline, 1, 4).is_empty());

    // Its quota runs out: the lower server takes the work.
    control.update_config(quota_config(100_001));
    assert!(ask(&mut pipeline, 0, 4).is_empty());
    assert_eq!(ask(&mut pipeline, 1, 4).len(), 4);

    // The quota comes back: the top server takes the work again, and the
    // lower one is refused it.
    control.update_config(quota_config(1 << 30));
    assert!(ask(&mut pipeline, 1, 4).is_empty());
    assert_eq!(ask(&mut pipeline, 0, 4).len(), 4);
}

// The floors change how much is read, never what is handed out: the same
// seeded run hands out the same articles in the same order with the floors
// on and with every walk whole. Between asks the run moves everything a
// floor's memo may depend on: not-founds and plain retries, rotation hints,
// a quota that downloads, runs out and comes back, a server that starts
// refusing its credentials (out of the policy for good), the restart
// checkpoint coming into force with a progress article and lifting again, a
// declared recovery index settling under a job whose payload waited for it,
// and the cursor of a uuencoded file moving under a capped spool.
#[tokio::test]
async fn the_scan_floors_hand_out_what_a_whole_walk_does() {
    let levels: Vec<(u32, bool)> = vec![(0, false), (0, false), (1, false), (2, true)];
    const METERED: usize = 1;
    const REFUSES_LATER: usize = 2;
    let jobs = [JobId(42010), JobId(42011)];
    const WAITS_FOR_INDEX: JobId = JobId(42012);
    const UUENCODED: JobId = JobId(42013);
    let mut runs = Vec::new();
    for floors_off in [true, false] {
        set_scan_floors_off(floors_off);
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = ladder_pipeline(&temp_dir, &levels).await;
        let control = metered(METERED as u32, 1 << 30);
        pipeline.nntp = Arc::new(ladder_client_with_quota(
            &levels,
            Some((METERED, Arc::clone(&control))),
        ));
        for (index, job_id) in jobs.into_iter().enumerate() {
            insert_active_job(
                &mut pipeline,
                job_id,
                segmented_job_spec(
                    &format!("Seeded Reel {index}"),
                    "payload.bin",
                    &vec![128u32; 150],
                ),
            )
            .await;
            pipeline.active_download_passes.insert(job_id);
        }
        // A job whose payload waits for its declared recovery index, and a
        // uuencoded job under a capped spool, which hands out only the
        // article its cursor waits on.
        let index = build_test_par2_index("payload.mkv", &[7u8; 256], 64);
        insert_active_job(
            &mut pipeline,
            WAITS_FOR_INDEX,
            JobSpec {
                name: "Seeded Index".to_string(),
                password: None,
                total_bytes: index.len() as u64 + 40 * 128,
                category: None,
                metadata: vec![],
                files: vec![
                    FileSpec {
                        filename: "00-index.par2".to_string(),
                        role: FileRole::Par2 {
                            is_index: true,
                            recovery_block_count: 0,
                        },
                        groups: vec!["alt.binaries.test".to_string()],
                        posted_at_epoch: None,
                        segments: vec![segment_spec! {
                            number: 0,
                            bytes: index.len() as u32,
                            message_id: "seeded-index@example.com".to_string(),
                        }],
                    },
                    FileSpec {
                        filename: "payload.mkv".to_string(),
                        role: FileRole::Standalone,
                        groups: vec!["alt.binaries.test".to_string()],
                        posted_at_epoch: None,
                        segments: (0..40)
                            .map(|number| {
                                segment_spec! {
                                    number: number,
                                    bytes: 128,
                                    message_id: format!("seeded-payload-{number}@example.com"),
                                }
                            })
                            .collect(),
                    },
                ],
            },
        )
        .await;
        pipeline.active_download_passes.insert(WAITS_FOR_INDEX);
        insert_active_job(
            &mut pipeline,
            UUENCODED,
            segmented_job_spec("Seeded Uu", "payload.bin", &vec![128u32; 40]),
        )
        .await;
        pipeline.active_download_passes.insert(UUENCODED);
        let index_file = held_segment(WAITS_FOR_INDEX, 0, 0).file_id;
        let uu_file = held_segment(UUENCODED, 0, 0).file_id;
        pipeline.uu_files.insert(uu_file, UuFileAssembly::default());
        pipeline.uu_spool_max_segments = 0;
        pipeline.uu_spool_available_bytes_for_test = Some(Some(u64::MAX));
        let mut rng = Lcg(0x001a_dde7_5eed);
        let mut handouts = Vec::new();
        let mut changes = [0usize; 7];
        for step in 0..900 {
            // Something a memo may depend on moves, about one step in four.
            match rng.below(16) {
                // The metered server downloads, when its quota lets it.
                0 => {
                    let bytes = 1 + rng.below(200_000) as u64;
                    if let Ok(mut permit) = control.try_reserve(bytes) {
                        permit.record_unpaced(bytes as usize);
                        let _ = permit.finish();
                        changes[0] += 1;
                    }
                }
                // Its quota runs out, or comes back.
                1 => {
                    let limit = if rng.below(2) == 0 { 1 } else { 1 << 30 };
                    control.update_config(quota_config(limit));
                    changes[1] += 1;
                }
                // The restart checkpoint comes into force with a progress
                // article, or lifts.
                2 => {
                    let job_id = jobs[rng.below(2) as usize];
                    let floor = if rng.below(2) == 0 { 0 } else { 4_096 };
                    pipeline
                        .jobs
                        .get_mut(&job_id)
                        .unwrap()
                        .restored_download_floor_bytes = floor;
                    if floor == 0 {
                        pipeline.checkpoint_progress_articles.remove(&job_id);
                    } else {
                        pipeline
                            .checkpoint_progress_articles
                            .insert(job_id, (0, held_segment(job_id, 0, 0)));
                    }
                    changes[2] += 1;
                }
                // Halfway, a fill server starts refusing its credentials.
                3 if step >= 450 && changes[3] == 0 => {
                    pipeline
                        .nntp
                        .pool()
                        .health()
                        .lock()
                        .await
                        .record_failure(REFUSES_LATER, true);
                    changes[3] += 1;
                }
                _ => {}
            }
            let server = rng.below(levels.len() as u32) as usize;
            let want = 1 + rng.below(6) as usize;
            let works = ask(&mut pipeline, server, want);
            handouts.push((
                server,
                works.iter().map(|work| work.segment_id).collect::<Vec<_>>(),
            ));
            for mut work in works {
                match rng.below(4) {
                    // Not found here: back to the queue for the others.
                    // Half the time another server has said so too, so the
                    // article goes back refused to several floors at once.
                    0 => {
                        work.exclude_servers.insert(server);
                        if rng.below(2) == 0 {
                            work.exclude_servers
                                .insert(rng.below(levels.len() as u32) as usize);
                        }
                        pipeline.enqueue_download_work(work);
                    }
                    // A plain retry.
                    1 => pipeline.enqueue_download_work(work),
                    // A retry steered off this server.
                    2 => {
                        work.avoid_server = Some(server);
                        changes[4] += 1;
                        pipeline.enqueue_download_work(work);
                    }
                    // Delivered. A uuencoded part moves its file's cursor
                    // on; the recovery index settles, which lets the
                    // payload it held go.
                    _ => {
                        let delivered = work.segment_id;
                        if delivered.file_id == uu_file {
                            pipeline
                                .uu_files
                                .get_mut(&uu_file)
                                .unwrap()
                                .next_index
                                .set(delivered.segment_number + 1);
                            changes[5] += 1;
                        } else if delivered.file_id == index_file {
                            pipeline
                                .graph_jobs
                                .get_mut(&WAITS_FOR_INDEX)
                                .unwrap()
                                .lost(
                                    weaver_graph::VolumeIndex(0),
                                    0,
                                    &mut crate::pipeline::graph::status::RepairSlots::default(),
                                    0,
                                )
                                .unwrap();
                            changes[6] += 1;
                        }
                    }
                }
            }
        }
        set_scan_floors_off(false);
        assert!(
            changes.iter().all(|count| *count > 0),
            "every kind of change was made: {changes:?}"
        );
        runs.push((
            handouts,
            pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        ));
    }
    assert!(runs[0].0.iter().any(|(_, works)| !works.is_empty()));
    assert_eq!(runs[0], runs[1]);
}

// An article put back after a "not found" sends back over the queue only
// the servers that may take it. Two top servers, a lower level not reached
// yet and a backfill server, all asking through the dispatch path. Server 0
// is handed an article and answers "not found" each round: the first
// article first, which goes back at the head of the queue, then ordinary
// ones, which go back behind their band. The lower level and the backfill
// server are refused the whole queue once; an article that comes back is one
// more refusal each, never a new walk, so what they read does not grow with
// the queue.
#[tokio::test]
async fn a_requeued_article_sends_no_refused_server_back_over_the_queue() {
    const ROUNDS: usize = 12;
    let levels: Vec<(u32, bool)> = vec![(0, false), (0, false), (1, false), (2, true)];
    const REFUSED: [usize; 2] = [2, 3];
    let mut reread = Vec::new();
    // The same holds for an ordinary retry: the connection fails under the
    // article, which waits out its retry and goes back with no server
    // refused to it, steered off the server it failed on.
    for (articles, not_found) in [
        (400usize, true),
        (1_600, true),
        (400, false),
        (1_600, false),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = ladder_pipeline(&temp_dir, &levels).await;
        let job_id = JobId(42030);
        insert_active_job(
            &mut pipeline,
            job_id,
            segmented_job_spec("Comes Back", "payload.bin", &vec![128u32; articles]),
        )
        .await;
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .note_first_article(held_segment(job_id, 0, 0));
        rewrite_queue(&mut pipeline, job_id, |_| {});
        pipeline.active_download_passes.insert(job_id);
        // What one ask of a refused server reads of the queue to be told
        // there is nothing for it. A debug build checks every idle answer
        // with a whole walk past the floors, which reads the queue once
        // more; that check is taken out of the count.
        let refused_ask = |pipeline: &mut Pipeline, server: usize| {
            let skipped = |pipeline: &Pipeline| {
                pipeline
                    .metrics
                    .download_scheduler_scan_items_skipped_total
                    .load(Ordering::Relaxed)
            };
            let before = skipped(pipeline);
            assert!(ask(pipeline, server, 8).is_empty());
            let queued = pipeline.jobs[&job_id]
                .articles
                .download_queue()
                .map_or(0, DownloadQueue::len) as u64;
            let checked = if cfg!(debug_assertions) { queued } else { 0 };
            skipped(pipeline) - before - checked
        };

        // Each refused server walks the queue once.
        let mut walked = 0;
        for server in REFUSED {
            walked += refused_ask(&mut pipeline, server);
        }
        assert_eq!(walked, (REFUSED.len() * articles) as u64);

        let mut read = 0;
        for round in 0..ROUNDS {
            let mut works = ask(&mut pipeline, 0, 1);
            let work = works.pop().expect("server 0 is handed an article");
            assert_eq!(
                work.segment_id == held_segment(job_id, 0, 0),
                round == 0,
                "the first article goes out first"
            );
            if not_found {
                answer_not_found(&mut pipeline, 0, work).await;
            } else {
                answer_connection_failed(&mut pipeline, 0, work).await;
                assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
            }
            for server in REFUSED {
                read += refused_ask(&mut pipeline, server);
            }
        }
        reread.push(read);

        // Server 1 may take what came back: the first article leads.
        let back = ask(&mut pipeline, 1, ROUNDS);
        assert_eq!(back.len(), ROUNDS);
        assert_eq!(back[0].segment_id, held_segment(job_id, 0, 0));
        assert_eq!(
            back[0].exclude_servers,
            if not_found {
                weaver_nntp::ServerSet::from([0])
            } else {
                weaver_nntp::ServerSet::EMPTY
            }
        );
    }
    assert_eq!(
        reread,
        vec![0; 4],
        "an article put back is refused where it is put, not by a new walk, however long the queue"
    );
}

// A "not found" puts the article straight back for the other servers. It
// does not wait on the retry channel, which a busy link never drains.
#[tokio::test]
async fn a_not_found_article_is_back_on_the_queue_when_the_result_is_handled() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(42020);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Missing On One", "payload.bin", &[128]),
    )
    .await;
    {
        let active = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap();
        active.download_queue = DownloadQueue::new();
        active.recovery_queue = DownloadQueue::new();
    }
    let segment_id = held_segment(job_id, 0, 0);
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "430 no such article",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        })
        .await;

    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "nothing went round the channel"
    );
    let queued: Vec<_> = {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .drain_all()
            .into_iter()
            .map(|item| item.dispatched(&state.spec))
            .map(|work| (work.segment_id, work.exclude_servers))
            .collect()
    };
    assert_eq!(
        queued,
        vec![(segment_id, weaver_nntp::ServerSet::from([0]))]
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as u32) % bound
    }
}

fn rewrite_queue(pipeline: &mut Pipeline, job_id: JobId, mut edit: impl FnMut(&mut DownloadWork)) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let queue = &mut state.articles.active_mut().unwrap().download_queue;
    let mut works: Vec<DownloadWork> = queue
        .drain_all()
        .into_iter()
        .map(|item| item.dispatched(&state.spec))
        .collect();
    for work in &mut works {
        edit(work);
    }
    let queue = &mut state.articles.active_mut().unwrap().download_queue;
    for work in works {
        queue.push(work);
    }
}

// An article every server it may use said "not found", while another server
// refuses the credentials and is never asked, is booked missing and counts
// to the post's missing articles.
#[tokio::test]
async fn a_not_found_walk_past_a_refusing_server_is_booked_missing() {
    let levels = vec![(0, false), (1, false), (2, false)];
    let refusing = 2;
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = ladder_pipeline(&temp_dir, &levels).await;
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(refusing, true);
    let job_id = JobId(42005);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Refused Head", "payload.bin", &[128, 128]),
    )
    .await;
    pipeline.active_download_passes.insert(job_id);
    let head = held_segment(job_id, 0, 0);
    let mut asked = Vec::new();
    for server in [0, 1, refusing] {
        let mut works = ask(&mut pipeline, server, 1);
        if let Some(work) = works.pop() {
            assert_eq!(work.segment_id, head);
            asked.push(server);
            answer_not_found(&mut pipeline, server, work).await;
        }
    }
    assert_eq!(asked, vec![0, 1], "the refusing server is never handed it");

    assert_eq!(
        pipeline.segment_terminal_states.get(&head).copied(),
        Some(SegmentTerminalState::Missing)
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        1
    );
    // The post is missing that one article and no other. Its file cannot be
    // whole and nothing can rebuild it, so the job ends there and the
    // article it never asked for is abandoned, not missing.
    assert_eq!(
        pipeline
            .segment_terminal_states
            .values()
            .filter(|state| **state == SegmentTerminalState::Missing)
            .count(),
        1
    );
    let mut ended: Vec<_> = pipeline
        .segment_terminal_states
        .iter()
        .map(|(segment, state)| (segment.segment_number, *state))
        .collect();
    ended.sort_by_key(|(number, _)| *number);
    assert_eq!(
        ended,
        vec![
            (0, SegmentTerminalState::Missing),
            (1, SegmentTerminalState::Abandoned)
        ]
    );
    // The job ends on its health, for its missing article: the server that
    // refuses the login was never asked, and is not what ended it.
    assert_eq!(
        pipeline.semantic_terminal_causes.get(&job_id),
        Some(&crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
}

// A client over `servers`: each server's stable id and priority group. The
// same stable id is the same server in every client built here.
fn identified_client(servers: &[(u32, u32)]) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: servers
            .iter()
            .map(|&(stable, group)| weaver_nntp::pool::ServerPoolConfig {
                max_connections: 100,
                group,
                stable_id: weaver_nntp::transfer::StableServerId(stable),
                ..Default::default()
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

// The server list is edited while articles are in flight: A, B, C, D become
// C, B, A, E. Each article keeps the servers it was tried on, under their new
// numbers, whether it sat on the queue, came back unasked from a lane, or
// came back as a 430 from a server the rebuild removed. A and C are never
// asked again, the added server is, and no index from the old pool is read
// against the new one.
#[tokio::test]
async fn a_server_list_edited_mid_flight_keeps_whom_each_article_was_tried_on() {
    const A: u32 = 1;
    const B: u32 = 2;
    const C: u32 = 3;
    const D: u32 = 4;
    const E: u32 = 5;
    // Ladder order before: A, C, D, B.
    let before = [(A, 0), (B, 3), (C, 1), (D, 2)];
    // After: E and A lead, C, then B; D is gone.
    let after = [(C, 1), (B, 3), (A, 0), (E, 0)];
    let old_index = |id: u32| before.iter().position(|(stable, _)| *stable == id).unwrap();
    let new_index = |id: u32| after.iter().position(|(stable, _)| *stable == id).unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = crate::pipeline::tests::new_scheduler_pipeline(&temp_dir).await;
    pipeline.nntp = Arc::new(identified_client(&before));
    let job_id = JobId(42006);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Edited Mid Flight", "payload.bin", &[128; 3]),
    )
    .await;
    pipeline.active_download_passes.insert(job_id);
    let ours = [0, 1, 2].map(|segment| held_segment(job_id, 0, segment));

    // A and C say "not found" for all three.
    for server in [A, C] {
        let works = ask(&mut pipeline, old_index(server), 8);
        assert_eq!(works.len(), 3);
        for work in works {
            answer_not_found(&mut pipeline, old_index(server), work).await;
        }
    }
    // D is handed all three: it is asking for one when the list is edited,
    // holds another on its lane unasked, and the third goes back untried.
    let mut on_d = ask(&mut pipeline, old_index(D), 8);
    assert_eq!(on_d.len(), 3);
    on_d.sort_by_key(|work| work.segment_id.segment_number);
    let [back, unasked, asking] = <[DownloadWork; 3]>::try_from(on_d)
        .unwrap_or_else(|_| unreachable!("three articles were handed"));
    for work in [&back, &unasked, &asking] {
        assert_eq!(work.exclude_servers, [old_index(A), old_index(C)]);
    }
    pipeline.enqueue_download_work(back);
    pipeline.hold_test_lane_works(vec![unasked.clone(), asking.clone()], false);

    let old_generation = pipeline.pool_generation;
    pipeline.activate_nntp(identified_client(&after), 8, old_generation + 1);
    // The old pool has let go of its sockets. Set directly: the drain's own
    // handler runs a dispatch pass, and this test hands out by asking.
    pipeline.nntp_handoff_draining = false;

    // The lane gives back the article it never asked for, carrying the
    // old pool's numbers; the 430 from D arrives stamped with the old pool.
    let (event, _ack) = {
        let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
        (
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id: 0,
                results: Vec::new(),
                unrequested_works: vec![unasked],
                stats: weaver_nntp::blocking::BlockingLaneStats::default(),
                ack: Some(ack),
            },
            ack_rx,
        )
    };
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(event, &mut pending);
    assert!(pending.is_empty());
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: old_generation,
            segment_id: asking.segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "430 no such article",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(old_index(D)),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: asking.retry_count,
            exclude_servers: asking.exclude_servers,
            release_connection_slot: false,
        })
        .await;

    // All three are queued, tried on A and C under their new numbers.
    let tried = [new_index(A), new_index(C)];
    let mut tried_sorted = tried.to_vec();
    tried_sorted.sort_unstable();
    {
        let queue = &pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue;
        assert_eq!(queue.len(), 3);
        let carried = queue.count_matching(|work| {
            let mut set = work.exclude_servers.to_vec();
            set.sort_unstable();
            ours.contains(&work.segment_id) && set == tried_sorted
        });
        assert_eq!(carried, 3);
    }

    // Walk the new pool. E says "not found"; B is the last server left and
    // keeps what it is handed.
    let mut handed: HashMap<SegmentId, Vec<u32>> = HashMap::new();
    // One round per server at most, plus the round that finds nothing.
    for _ in 0..=after.len() {
        let mut any = false;
        for (index, &(stable, _)) in after.iter().enumerate() {
            for work in ask(&mut pipeline, index, 8) {
                any = true;
                handed.entry(work.segment_id).or_default().push(stable);
                if stable == E {
                    answer_not_found(&mut pipeline, index, work).await;
                }
            }
        }
        if !any {
            break;
        }
    }
    for segment in ours {
        assert_eq!(
            handed.get(&segment).map(Vec::as_slice),
            Some([E, B].as_slice()),
            "{segment}: A and C not asked again, E asked, B last"
        );
    }
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
}
