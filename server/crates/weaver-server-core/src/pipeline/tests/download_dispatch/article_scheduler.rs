//! The global per-server article scheduler.
//!
//! Two promises are under test together, because either one alone is easy and
//! useless: a server is never sent away empty while some runnable job holds an
//! article it may fetch, and every article a server is given comes from one
//! job at a time so that job finishes instead of creeping alongside nine
//! others.

use super::*;
use crate::pipeline::download::scheduler::{Handout, LaneShare, YieldReason};

const SERVER_A: usize = 0;
const SERVER_B: usize = 1;

/// Ask the scheduler for work, taking the pressure sample a caller would.
fn ask(
    pipeline: &mut Pipeline,
    server_idx: usize,
    want: usize,
    spill_in_flight: Option<JobId>,
) -> Handout {
    let pressure = pipeline.refresh_download_pressure();
    pipeline.next_works(server_idx, want, spill_in_flight, pressure)
}

/// The articles of a handout that must not be a yield.
fn taken(handout: Handout) -> Vec<DownloadWork> {
    match handout {
        Handout::Works(works) => works,
        Handout::Idle => Vec::new(),
        Handout::Yield(reason) => panic!("a whole-link gate fired unexpectedly: {reason:?}"),
        Handout::Saturated { below } => {
            panic!("a caller with no lane share was reported saturated below {below}")
        }
    }
}

/// A lane booked as holding `holds` of `job_id`'s articles, at `depth`.
fn lane_holding(pipeline: &mut Pipeline, job_id: JobId, holds: usize, depth: usize) -> LaneShare {
    let lane_id = Pipeline::next_download_lane_id();
    let outstanding = (0..holds)
        .map(|index| {
            let segment_id = SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: u32::MAX,
                },
                segment_number: index as u32,
            };
            let work = DownloadWork {
                segment_id,
                message_id: MessageId::new(&format!("held-{}-{index}@share.test", job_id.0)),
                groups: Arc::from(Vec::new()),
                priority: 0,
                byte_estimate: 512,
                retry_count: 0,
                is_recovery: false,
                completion_critical: false,
                exclude_servers: Vec::new(),
                avoid_server: None,
            };
            (segment_id, work)
        })
        .collect();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical: false,
            server_idx: Some(SERVER_A),
            connection: true,
            ip_replacement: false,
            outstanding,
        },
    );
    LaneShare { lane_id, depth }
}

/// Ask as `lane`.
fn ask_as_lane(
    pipeline: &mut Pipeline,
    server_idx: usize,
    want: usize,
    lane: LaneShare,
) -> Handout {
    let pressure = pipeline.refresh_download_pressure();
    pipeline.next_works_for_lane(server_idx, want, Some(lane), None, pressure)
}

/// The job every article in a handout came from, proving a handout never
/// spans jobs.
fn single_job(works: &[DownloadWork]) -> JobId {
    let job_id = works[0].segment_id.file_id.job_id;
    assert!(
        works
            .iter()
            .all(|work| work.segment_id.file_id.job_id == job_id),
        "a handout must never mix jobs"
    );
    job_id
}

/// Articles handed out per job over `calls` asks on one server.
fn article_counts(
    pipeline: &mut Pipeline,
    server_idx: usize,
    want: usize,
    calls: usize,
) -> HashMap<JobId, usize> {
    let mut counts: HashMap<JobId, usize> = HashMap::new();
    for _ in 0..calls {
        let works = taken(ask(pipeline, server_idx, want, None));
        if works.is_empty() {
            continue;
        }
        *counts.entry(single_job(&works)).or_default() += works.len();
    }
    counts
}

fn queued(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline.jobs.get(&job_id).unwrap().download_queue.len()
}

/// Hold a job's payload behind the restart checkpoint: an enforced durable
/// lead with a progress article already in flight refuses every non-recovery
/// article until the pipeline catches up. `false` when the job has no payload
/// left to hold back.
fn try_block_on_checkpoint(pipeline: &mut Pipeline, job_id: JobId) -> bool {
    let Some(segment_id) = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .peek_in_class(false)
        .map(|work| work.segment_id)
    else {
        return false;
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .restored_download_floor_bytes = 1;
    pipeline
        .checkpoint_progress_articles
        .insert(job_id, (0, segment_id));
    true
}

fn block_on_checkpoint(pipeline: &mut Pipeline, job_id: JobId) {
    assert!(
        try_block_on_checkpoint(pipeline, job_id),
        "the job under test needs queued payload"
    );
}

fn unblock_checkpoint(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline.checkpoint_progress_articles.remove(&job_id);
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .restored_download_floor_bytes = 0;
    pipeline
        .download_restart_durable_lead_retry_after
        .remove(&job_id);
}

/// Rewrite a job's whole queue through `edit`, keeping it in the same queue.
fn rewrite_queue(pipeline: &mut Pipeline, job_id: JobId, mut edit: impl FnMut(&mut DownloadWork)) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state.download_queue.drain_all();
    for work in &mut works {
        edit(work);
    }
    for work in works {
        state.download_queue.push(work);
    }
}

/// A job of `segments` equal-sized articles, appended to the dispatch order.
async fn add_job(pipeline: &mut Pipeline, job_id: JobId, name: &str, segments: usize) {
    let spec = segmented_job_spec(name, "payload.bin", &vec![512u32; segments]);
    insert_active_job(pipeline, job_id, spec).await;
}

/// [`add_job`] with a submitted dispatch priority.
async fn add_job_with_priority(
    pipeline: &mut Pipeline,
    job_id: JobId,
    name: &str,
    segments: usize,
    priority: &str,
) {
    let spec = with_priority(
        segmented_job_spec(name, "payload.bin", &vec![512u32; segments]),
        priority,
    );
    insert_active_job(pipeline, job_id, spec).await;
}

fn handouts_hot(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_scheduler_handouts_total_hot
        .load(Ordering::Relaxed)
}

fn handouts_spill(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_scheduler_handouts_total_spill
        .load(Ordering::Relaxed)
}

fn idle_with_servable(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_scheduler_idle_with_servable_total
        .load(Ordering::Relaxed)
}

/// Rule 2: while the hot job can serve this server, nothing else is touched.
#[tokio::test]
async fn hot_job_takes_every_handout_while_it_can_serve_the_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71001);
    let next = JobId(71002);
    let third = JobId(71003);
    add_job(&mut pipeline, hot, "Harbour Lights Reel", 800).await;
    add_job(&mut pipeline, next, "Tin Roof Sessions", 800).await;
    add_job(&mut pipeline, third, "Copper Valley Notes", 800).await;

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 100);

    assert_eq!(counts.get(&hot).copied(), Some(800));
    assert_eq!(counts.get(&next).copied(), None);
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(handouts_spill(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 1: the link is never left idle. Once the hot job runs out the calls
/// keep being answered, from the next job in order, and no third job opens.
#[tokio::test]
async fn the_next_job_takes_over_the_moment_the_hot_job_runs_dry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71011);
    let next = JobId(71012);
    let third = JobId(71013);
    add_job(&mut pipeline, hot, "Short Straw Cut", 20).await;
    add_job(&mut pipeline, next, "Long Meadow Cut", 800).await;
    add_job(&mut pipeline, third, "Quiet Siding Cut", 800).await;

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 100);

    assert_eq!(counts.get(&hot).copied(), Some(20));
    // 100 asks, three of which the short job answered: everything else came
    // from the one job behind it, and nothing was refused in between.
    assert_eq!(counts.get(&next).copied(), Some(97 * 8));
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 3: the restart checkpoint is one of the per-job blocks, and a blocked
/// hot job hands the server to the next job only — not to the one after it.
#[tokio::test]
async fn a_checkpoint_held_hot_job_passes_the_server_to_the_next_job_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71021);
    let next = JobId(71022);
    let third = JobId(71023);
    add_job(&mut pipeline, hot, "Held Ledger", 200).await;
    add_job(&mut pipeline, next, "Open Ledger", 200).await;
    add_job(&mut pipeline, third, "Spare Ledger", 200).await;
    block_on_checkpoint(&mut pipeline, hot);

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 20);

    assert_eq!(counts.get(&hot).copied(), None);
    assert_eq!(counts.get(&next).copied(), Some(160));
    assert_eq!(counts.get(&third).copied(), None);
    assert_eq!(queued(&pipeline, hot), 200);
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&hot),
        "a checkpoint-only refusal must leave the recheck note the queue owes"
    );
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 1 outranks "the next job only": the walk keeps going past a second
/// blocked job. A spill job with articles out on the server then keeps it
/// until that ring drains; only then does the earlier job take over.
#[tokio::test]
async fn the_walk_passes_two_blocked_jobs_and_a_spill_in_flight_keeps_its_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71031);
    let next = JobId(71032);
    let third = JobId(71033);
    let fourth = JobId(71034);
    add_job(&mut pipeline, hot, "First Ledger", 200).await;
    add_job(&mut pipeline, next, "Second Ledger", 200).await;
    add_job(&mut pipeline, third, "Third Ledger", 200).await;
    add_job(&mut pipeline, fourth, "Fourth Ledger", 200).await;
    block_on_checkpoint(&mut pipeline, hot);
    block_on_checkpoint(&mut pipeline, next);

    let counts = article_counts(&mut pipeline, SERVER_A, 8, 10);
    assert_eq!(counts.get(&third).copied(), Some(80));
    assert_eq!(counts.get(&fourth).copied(), None);
    assert_eq!(counts.get(&hot).copied(), None);
    assert_eq!(counts.get(&next).copied(), None);

    // The second job opens while the third still has work out on this
    // server: the third keeps the server until its ring drains, so no second
    // spill job opens beside it.
    unblock_checkpoint(&mut pipeline, next);
    let third_queued = queued(&pipeline, third);
    let works = taken(ask(&mut pipeline, SERVER_A, 8, Some(third)));
    assert_eq!(single_job(&works), third);
    assert_eq!(queued(&pipeline, third), third_queued - 8);

    // With nothing of the third's in flight, the earlier job takes over.
    let works = taken(ask(&mut pipeline, SERVER_A, 8, None));
    assert_eq!(single_job(&works), next);
    assert_eq!(queued(&pipeline, third), third_queued - 8);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 3: blocked is per server. The same call answers differently on two
/// servers of the same pool in the same actor state.
#[tokio::test]
async fn a_retention_excluded_hot_job_is_blocked_on_that_server_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71041);
    let next = JobId(71042);
    add_job(&mut pipeline, hot, "Deep Archive Reel", 200).await;
    add_job(&mut pipeline, next, "Fresh Post Reel", 200).await;
    pipeline
        .job_retention_exclude_cache
        .insert(hot, (std::time::Instant::now(), Arc::new(vec![SERVER_B])));

    let on_b = taken(ask(&mut pipeline, SERVER_B, 4, None));
    assert_eq!(single_job(&on_b), next);

    let on_a = taken(ask(&mut pipeline, SERVER_A, 4, None));
    assert_eq!(single_job(&on_a), hot);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 5: a short hot job is a short handout, never a handout topped up from
/// the job behind it.
#[tokio::test]
async fn a_handout_stops_at_the_hot_jobs_last_article() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71051);
    let next = JobId(71052);
    add_job(&mut pipeline, hot, "Three Frame Loop", 3).await;
    add_job(&mut pipeline, next, "Full Frame Loop", 200).await;

    let works = taken(ask(&mut pipeline, SERVER_A, 8, None));

    assert_eq!(works.len(), 3);
    assert_eq!(single_job(&works), hot);
    assert_eq!(queued(&pipeline, next), 200);
    assert_eq!(handouts_hot(&pipeline), 1);
    assert_eq!(handouts_spill(&pipeline), 0);
}

/// Rule 6: the completion-critical class orders work inside a job and only
/// inside it. Another job's promoted recovery does not pre-empt the hot job's
/// ordinary payload, and the hot job's own critical heap leads its ordinary
/// one.
#[tokio::test]
async fn completion_critical_work_orders_a_job_but_never_outranks_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71061);
    let filler = [JobId(71062), JobId(71063), JobId(71064)];
    let promoted = JobId(71065);
    add_job(&mut pipeline, hot, "Mixed Class Reel", 40).await;
    for (index, job_id) in filler.iter().enumerate() {
        add_job(&mut pipeline, *job_id, &format!("Filler Reel {index}"), 40).await;
    }
    add_job(&mut pipeline, promoted, "Repair Blocks Reel", 40).await;

    // The fifth job is entirely promoted recovery: the most urgent class the
    // queue has, on the job furthest from the front.
    rewrite_queue(&mut pipeline, promoted, |work| {
        work.completion_critical = true;
        work.is_recovery = true;
    });
    // The hot job carries a few critical articles of its own behind ordinary
    // payload that was queued first.
    let mut remaining_to_promote = 4;
    rewrite_queue(&mut pipeline, hot, |work| {
        if work.segment_id.segment_number >= 36 && remaining_to_promote > 0 {
            work.completion_critical = true;
            remaining_to_promote -= 1;
        }
    });

    let works = taken(ask(&mut pipeline, SERVER_A, 6, None));

    assert_eq!(single_job(&works), hot);
    assert!(
        works.iter().take(4).all(|work| work.completion_critical),
        "the hot job's own critical heap leads its ordinary heap"
    );
    assert!(
        works.iter().skip(4).all(|work| !work.completion_critical),
        "and the ordinary heap follows in the same handout"
    );
    assert_eq!(
        queued(&pipeline, promoted),
        40,
        "no other job's critical work pre-empts the hot job's ordinary work"
    );
}

/// Rule 7: soft byte pressure narrows the field to the hot job and clamps the
/// handout to a single article, with no spill while memory drains.
#[tokio::test]
async fn soft_pressure_clamps_to_one_article_of_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71071);
    let next = JobId(71072);
    add_job(&mut pipeline, hot, "Narrow Window Reel", 200).await;
    add_job(&mut pipeline, next, "Wide Window Reel", 200).await;
    let (_, _, write_soft, _) = pipeline.download_pressure_limits();
    pipeline
        .metrics
        .write_buffered_bytes
        .store(write_soft, Ordering::Relaxed);

    let works = taken(ask(&mut pipeline, SERVER_A, 8, None));
    assert_eq!(works.len(), 1);
    assert_eq!(single_job(&works), hot);

    // With the hot job blocked as well, soft pressure refuses to open a
    // second job rather than spilling into it.
    block_on_checkpoint(&mut pipeline, hot);
    assert!(matches!(
        ask(&mut pipeline, SERVER_A, 8, None),
        Handout::Idle
    ));
    assert_eq!(queued(&pipeline, next), 200);
    assert_eq!(
        idle_with_servable(&pipeline),
        0,
        "a listed gate is not an unexplained idle slot"
    );
}

/// The whole-link gates, each reported as itself. These are the only reasons
/// a slot may be left empty while a job has servable work.
#[tokio::test]
async fn every_whole_link_gate_yields_under_its_own_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71081);
    add_job(&mut pipeline, hot, "Gated Reel", 200).await;

    pipeline.global_paused = true;
    assert!(matches!(
        ask(&mut pipeline, SERVER_A, 8, None),
        Handout::Yield(YieldReason::Paused)
    ));
    pipeline.global_paused = false;

    pipeline.nntp_handoff_draining = true;
    assert!(matches!(
        ask(&mut pipeline, SERVER_A, 8, None),
        Handout::Yield(YieldReason::HandoffDraining)
    ));
    pipeline.nntp_handoff_draining = false;

    pipeline
        .metrics
        .write_buffered_bytes
        .store(u64::MAX, Ordering::Relaxed);
    assert!(matches!(
        ask(&mut pipeline, SERVER_A, 8, None),
        Handout::Yield(YieldReason::HardPressure)
    ));
    pipeline
        .metrics
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);

    let now = chrono::Local::now();
    let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
    pipeline
        .db
        .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 4096)
        .unwrap();
    pipeline
        .apply_bandwidth_cap_policy(Some(crate::bandwidth::IspBandwidthCapConfig {
            enabled: true,
            period: crate::bandwidth::IspBandwidthCapPeriod::Daily,
            limit_bytes: 512,
            reset_time_minutes_local: reset_minutes,
            weekly_reset_weekday: crate::bandwidth::IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }))
        .unwrap();
    assert!(matches!(
        ask(&mut pipeline, SERVER_A, 8, None),
        Handout::Yield(YieldReason::BandwidthCapExhausted)
    ));
    pipeline.apply_bandwidth_cap_policy(None).unwrap();

    // Nothing was taken from the queue while the gates were shut.
    assert_eq!(queued(&pipeline, hot), 200);
    assert_eq!(handouts_hot(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 3: the work's own exclusions and rotation hint are per server, and a
/// job whose whole queue refuses this server is blocked on it.
#[tokio::test]
async fn per_article_exclusions_decide_which_server_a_job_is_blocked_on() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot = JobId(71091);
    let next = JobId(71092);
    add_job(&mut pipeline, hot, "Failed Over Reel", 40).await;
    add_job(&mut pipeline, next, "Untouched Reel", 40).await;
    // Half the hot job's articles failed on this server, the other half were
    // merely rotated away from it: neither may come back to it.
    rewrite_queue(&mut pipeline, hot, |work| {
        if work.segment_id.segment_number % 2 == 0 {
            work.exclude_servers = vec![SERVER_A];
        } else {
            work.avoid_server = Some(SERVER_A);
        }
    });

    let on_a = taken(ask(&mut pipeline, SERVER_A, 8, None));
    assert_eq!(
        single_job(&on_a),
        next,
        "a job whose every queued article refuses this server is blocked on it"
    );
    assert_eq!(queued(&pipeline, hot), 40);

    let on_b = taken(ask(&mut pipeline, SERVER_B, 8, None));
    assert_eq!(single_job(&on_b), hot);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// Rule 2: the hot job is recomputed on every call, so a higher-priority job
/// submitted later takes the link on the next ask.
#[tokio::test]
async fn a_higher_priority_job_becomes_hot_on_the_next_call() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let first = JobId(71101);
    let urgent = JobId(71102);
    add_job(&mut pipeline, first, "Steady Reel", 200).await;

    let before = taken(ask(&mut pipeline, SERVER_A, 4, None));
    assert_eq!(single_job(&before), first);

    add_job_with_priority(&mut pipeline, urgent, "Jumped Queue Reel", 200, "high").await;

    let after = taken(ask(&mut pipeline, SERVER_A, 4, None));
    assert_eq!(single_job(&after), urgent);
    assert_eq!(handouts_spill(&pipeline), 0);
    assert_eq!(idle_with_servable(&pipeline), 0);
}

/// A small deterministic sequence, so a shuffled run reproduces exactly.
struct Lcg(u64);

impl Lcg {
    fn next_u32(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) as u32
    }

    fn below(&mut self, bound: u32) -> u32 {
        self.next_u32() % bound
    }
}

/// The guard counters: over a shuffled multi-job, multi-server run the
/// scheduler never answers "nothing to do" while a job could have been
/// served, and every handout it made is accounted for as hot or spill.
#[tokio::test]
async fn the_guard_counters_account_for_a_shuffled_multi_server_run() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let jobs = [
        JobId(71111),
        JobId(71112),
        JobId(71113),
        JobId(71114),
        JobId(71115),
    ];
    for (index, job_id) in jobs.iter().enumerate() {
        add_job(
            &mut pipeline,
            *job_id,
            &format!("Shuffled Reel {index}"),
            120,
        )
        .await;
    }

    let mut rng = Lcg(0x5eed_1234_9abc_def0);
    let mut answers = 0u64;
    for step in 0..400 {
        // Blocks and exclusions come and go under the scheduler, exactly as
        // they do while a run is live.
        let victim = jobs[rng.below(jobs.len() as u32) as usize];
        match rng.below(4) {
            0 => {
                try_block_on_checkpoint(&mut pipeline, victim);
            }
            1 => unblock_checkpoint(&mut pipeline, victim),
            2 => {
                let server = rng.below(2) as usize;
                rewrite_queue(&mut pipeline, victim, |work| {
                    work.exclude_servers = vec![server];
                });
            }
            _ => rewrite_queue(&mut pipeline, victim, |work| {
                work.exclude_servers.clear();
                work.avoid_server = None;
            }),
        }

        let server_idx = rng.below(2) as usize;
        let want = 1 + rng.below(8) as usize;
        let spill_in_flight = (step % 3 == 0).then(|| jobs[rng.below(jobs.len() as u32) as usize]);
        let works = taken(ask(&mut pipeline, server_idx, want, spill_in_flight));
        if works.is_empty() {
            continue;
        }
        single_job(&works);
        assert!(works.len() <= want);
        answers += 1;
    }

    assert!(answers > 0, "the run must actually have handed work out");
    assert_eq!(
        idle_with_servable(&pipeline),
        0,
        "the scheduler never left a server idle while a job could serve it"
    );
    assert_eq!(handouts_hot(&pipeline) + handouts_spill(&pipeline), answers);
    assert_eq!(
        pipeline
            .metrics
            .download_scheduler_handouts_total_probe
            .load(Ordering::Relaxed),
        0
    );
}

/// Rule 7: a lane holds its share of a job. One hundred connections asking
/// for a runway each would reserve a six-hundred-article job among the first
/// twenty; the share holds each of them to the job divided over the link.
#[tokio::test]
async fn a_small_job_is_shared_over_every_connection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(71301);
    let next = JobId(71302);
    add_job(&mut pipeline, hot, "Sixty Second Reel", 600).await;
    add_job(&mut pipeline, next, "Sixty Second Reel Two", 600).await;

    assert_eq!(
        pipeline.lane_share_of_job(hot, 8),
        9,
        "600 / 100 rounds up to 6, floored at depth + 1"
    );
    let lane = lane_holding(&mut pipeline, hot, 0, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, lane));
    assert_eq!(works.len(), 9, "a runway ask is held to the share");
    assert_eq!(single_job(&works), hot);

    // Sixty fresh lanes at that share leave the hot job with work for the
    // rest; none of them is sent on to the next job.
    for _ in 0..60 {
        let lane = lane_holding(&mut pipeline, hot, 0, 8);
        let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, lane));
        assert_eq!(single_job(&works), hot);
    }
    assert!(
        queued(&pipeline, hot) > 0,
        "the hot job is not reserved whole"
    );
    assert_eq!(queued(&pipeline, next), 600, "the next job is untouched");
    assert_eq!(handouts_spill(&pipeline), 0);
}

/// A lane already at its share is reported busy, not sent on to the next
/// job — the link must not open a second job while the hot one still has
/// work for other lanes.
#[tokio::test]
async fn a_lane_at_its_share_is_saturated_rather_than_spilled() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(71311);
    let next = JobId(71312);
    add_job(&mut pipeline, hot, "Held Share Reel", 600).await;
    add_job(&mut pipeline, next, "Held Share Reel Two", 600).await;

    let share = pipeline.lane_share_of_job(hot, 8);
    let full = lane_holding(&mut pipeline, hot, share, 8);
    match ask_as_lane(&mut pipeline, SERVER_A, 18, full) {
        Handout::Saturated { below } => assert_eq!(below, share),
        other => panic!(
            "a lane at its share must be saturated, got {:?}",
            taken(other).len()
        ),
    }
    assert_eq!(queued(&pipeline, hot), 600);
    assert_eq!(queued(&pipeline, next), 600);
    assert_eq!(handouts_spill(&pipeline), 0);

    // Holding fewer than the share opens the difference.
    let short = lane_holding(&mut pipeline, hot, share - 2, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, short));
    assert_eq!(works.len(), 2);
    assert_eq!(single_job(&works), hot);

    // Articles of another job do not count against this one's share.
    let elsewhere = lane_holding(&mut pipeline, next, share, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, elsewhere));
    assert_eq!(works.len(), share);
    assert_eq!(single_job(&works), hot);
}

/// A large job's share is beyond any runway, so its lanes are served exactly
/// as before: the full ask, every time.
#[tokio::test]
async fn a_large_job_hands_out_the_whole_runway() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(71321);
    add_job(&mut pipeline, hot, "Long Haul Pack", 20_000).await;

    assert_eq!(pipeline.lane_share_of_job(hot, 8), 200);
    let lane = lane_holding(&mut pipeline, hot, 30, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, lane));
    assert_eq!(works.len(), 18);
}

/// The share is measured against the job that would serve the lane. A lane at
/// its share of a job it cannot fetch from — retention rules this server out
/// — is not held on that job's account: the walk goes on, as rule 3 says.
#[tokio::test]
async fn saturation_is_only_charged_by_a_job_that_could_serve_the_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(71331);
    let next = JobId(71332);
    add_job(&mut pipeline, hot, "Elsewhere Only Reel", 600).await;
    add_job(&mut pipeline, next, "Here Too Reel", 600).await;
    rewrite_queue(&mut pipeline, hot, |work| {
        work.exclude_servers = vec![SERVER_A];
    });

    let share = pipeline.lane_share_of_job(hot, 8);
    let lane = lane_holding(&mut pipeline, hot, share, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, lane));
    assert_eq!(
        single_job(&works),
        next,
        "the blocked hot job does not hold the lane"
    );
    assert_eq!(handouts_spill(&pipeline), 1);
}

/// A lane's share is a floor of a full pipe plus one even for a job with a
/// handful of articles, so a tiny job still fills a pipelined socket.
#[tokio::test]
async fn the_share_never_starves_a_pipe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(71341);
    add_job(&mut pipeline, hot, "Three Article Note", 3).await;

    assert_eq!(pipeline.lane_share_of_job(hot, 8), 9);
    assert_eq!(pipeline.lane_share_of_job(hot, 1), 2);
    let lane = lane_holding(&mut pipeline, hot, 0, 8);
    let works = taken(ask_as_lane(&mut pipeline, SERVER_A, 18, lane));
    assert_eq!(
        works.len(),
        3,
        "everything the job has, well inside the floor"
    );
}
