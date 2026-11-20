// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::tests::new_direct_pipeline;
use crate::pipeline::tests::{insert_active_job, segmented_job_spec};

const JOB: JobId = JobId(42010);
const ARTICLES: usize = 2_000;

// The hold reason read by walking the queue for an article the
// checkpoint admits, counting each article read.
fn hold_reason_by_scan(
    pipeline: &Pipeline,
    job_id: JobId,
    examined: &mut usize,
) -> Option<&'static str> {
    let state = pipeline.jobs.get(&job_id)?;
    if !Pipeline::has_dispatchable_work(state) || pipeline.job_has_current_download_activity(job_id)
    {
        return None;
    }
    if crate::DownloadPressureState::from_code(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
    ) == crate::DownloadPressureState::Hard
    {
        return Some("byte_pressure");
    }
    if pipeline.nntp.pool().server_count() == 0 {
        return Some("no_server");
    }
    let checkpoint = pipeline.checkpoint_admission(job_id);
    if !checkpoint.enforced {
        return None;
    }
    let open = state
        .articles
        .download_queue()?
        .peek_first_matching(|work| {
            *examined += 1;
            checkpoint.decision(work, &[]).allows()
        });
    if open.is_some() {
        None
    } else {
        Some("checkpoint")
    }
}

fn client(servers: usize) -> weaver_nntp::client::NntpClient {
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers: (0..servers)
            .map(|idx| weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: format!("hold-{idx}.example.com"),
                    ..Default::default()
                },
                max_connections: 2,
                ..weaver_nntp::pool::ServerPoolConfig::default()
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    })
}

// A hold reason and the queued articles read to reach it.
type Read = (Option<&'static str>, usize);

// The answer, and the articles read for it, from the function and from
// the scan it replaces.
fn both(pipeline: &Pipeline) -> (Read, Read) {
    pipeline
        .dispatch_hold_items_examined
        .store(0, Ordering::Relaxed);
    let answer = pipeline.job_dispatch_hold_reason(JOB);
    let read = pipeline
        .dispatch_hold_items_examined
        .load(Ordering::Relaxed);
    let mut scanned = 0;
    let reference = hold_reason_by_scan(pipeline, JOB, &mut scanned);
    ((answer, read), (reference, scanned))
}

fn set_floor(pipeline: &mut Pipeline, floor: u64, downloaded: u64) {
    let state = pipeline.jobs.get_mut(&JOB).unwrap();
    state.restored_download_floor_bytes = floor;
    state.downloaded_bytes = downloaded;
}

fn hold_progress_article(pipeline: &mut Pipeline) {
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline
        .checkpoint_progress_articles
        .insert(JOB, (0, segment_id));
}

// Requeues the job's last article as recovery promoted into the
// download queue.
fn queue_recovery(pipeline: &mut Pipeline) {
    let last = (ARTICLES - 1) as u32;
    let item = pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop_first_matching(|work| work.segment_id.segment_number == last)
        .unwrap();
    let mut work = pipeline.dispatched(item);
    work.is_recovery = true;
    pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .push(work);
}

#[tokio::test]
async fn the_hold_reason_matches_the_queue_scan_and_reads_no_article_when_payload_is_held() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = Arc::new(client(1));
    insert_active_job(
        &mut pipeline,
        JOB,
        segmented_job_spec("Dispatch hold", "payload.bin", &vec![512u32; ARTICLES]),
    )
    .await;
    let limit = Pipeline::restart_durable_lead_limit_bytes();
    let queued = |pipeline: &Pipeline| {
        pipeline.jobs[&JOB]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
    };

    // No checkpoint: neither reads the queue.
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer, (None, 0));

    // A checkpoint with room admits the head.
    set_floor(&mut pipeline, 1, 0);
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer, (None, 1));

    // Past the lead with nothing to advance the floor: the head is the
    // one article sent to close the gap.
    set_floor(&mut pipeline, 1, limit + 4096);
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer, (None, 1));

    // The gap-closing article out: every payload article is held. The
    // scan reads the whole queue to learn it; the function reads none.
    hold_progress_article(&mut pipeline);
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer.0, reference.0);
    assert_eq!(answer.0, Some("checkpoint"));
    assert_eq!(reference.1, queued(&pipeline));
    assert_eq!(answer.1, 0);

    // Recovery queued behind the held payload is admitted, so the job is
    // not held, and both find it the same way.
    queue_recovery(&mut pipeline);
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer.0, None);
    assert!(answer.1 >= 1);

    // The gates ahead of the checkpoint answer first, unchanged.
    pipeline.metrics.download_pressure_state.store(
        crate::DownloadPressureState::Hard.as_code(),
        Ordering::Relaxed,
    );
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer, (Some("byte_pressure"), 0));
    pipeline.metrics.download_pressure_state.store(
        crate::DownloadPressureState::Clear.as_code(),
        Ordering::Relaxed,
    );
    pipeline.nntp = Arc::new(client(0));
    let (answer, reference) = both(&pipeline);
    assert_eq!(answer, reference);
    assert_eq!(answer, (Some("no_server"), 0));
}
// The job's login wait read by asking about every queued article in
// turn, counting each article read.
fn login_wait_by_scan(
    pipeline: &Pipeline,
    view: crate::pipeline::download::LoginRefusalView,
    examined: &mut usize,
) -> Option<crate::pipeline::download::LoginRefusalWait> {
    let queue = pipeline.jobs.get(&JOB)?.articles.download_queue()?;
    let mut servers = weaver_nntp::ServerSet::EMPTY;
    let mut every = true;
    queue.peek_first_matching(|work| {
        *examined += 1;
        match pipeline.login_refusal_waited_by(
            view,
            work.exclude_servers,
            weaver_nntp::ServerSet::EMPTY,
        ) {
            Some(waited) => servers = servers.union(waited),
            None => every = false,
        }
        false
    });
    (!servers.is_empty()).then_some(crate::pipeline::download::LoginRefusalWait { servers, every })
}

// Requeues every article of the job with the exclusions `exclude` gives
// for its place in the queue.
fn steer(pipeline: &mut Pipeline, exclude: impl Fn(usize) -> weaver_nntp::ServerSet) {
    let queue = &mut pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut works = queue.drain_all();
    for (n, work) in works.iter_mut().enumerate() {
        work.exclude_servers = exclude(n);
    }
    for work in works {
        queue.push(work);
    }
}

// One server answered that it lacks every article of a large job, a
// second refuses the login and a third may serve. The job's login wait
// is the one a walk of the whole queue finds, and is found by asking
// once per distinct set of exclusions, however many articles carry it.
#[tokio::test]
async fn the_login_wait_matches_a_queue_scan_and_asks_once_per_distinct_exclusion_set() {
    use crate::pipeline::download::LoginRefusalWait;
    use weaver_nntp::ServerSet;
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = Arc::new(client(3));
    insert_active_job(
        &mut pipeline,
        JOB,
        segmented_job_spec("Login wait", "payload.bin", &vec![512u32; ARTICLES]),
    )
    .await;
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(1, true);
    let view = pipeline.login_refusal_view();
    assert_eq!(view.waiting, ServerSet::from([1]));
    let both = |pipeline: &Pipeline| {
        pipeline
            .login_refusal_sets_examined
            .store(0, Ordering::Relaxed);
        let answer = pipeline.job_login_refusal_wait(JOB, view, None);
        let asked = pipeline.login_refusal_sets_examined.load(Ordering::Relaxed);
        let mut scanned = 0;
        let reference = login_wait_by_scan(pipeline, view, &mut scanned);
        assert_eq!(scanned, ARTICLES);
        assert_eq!(answer, reference);
        (answer, asked)
    };

    // Nothing steered: servers 0 and 2 may serve everything.
    assert_eq!(both(&pipeline), (None, 1));

    // Server 0 lacks every article; server 2 may still serve them.
    steer(&mut pipeline, |_| ServerSet::from([0]));
    assert_eq!(both(&pipeline), (None, 1));

    // Half of them server 2 lacks too: those wait on server 1.
    steer(&mut pipeline, |n| {
        if n % 2 == 0 {
            ServerSet::from([0])
        } else {
            ServerSet::from([0, 2])
        }
    });
    let waits = |every| {
        Some(LoginRefusalWait {
            servers: ServerSet::from([1]),
            every,
        })
    };
    assert_eq!(both(&pipeline), (waits(false), 2));

    // All of them: the whole job waits on server 1.
    steer(&mut pipeline, |_| ServerSet::from([0, 2]));
    assert_eq!(both(&pipeline), (waits(true), 1));

    // The job says so the same way while another article of it is
    // being fetched and while one is parked for its refused-login retry.
    let words = "login_refused:hold-1.example.com:563";
    let reason = |pipeline: &Pipeline| pipeline.dispatch_wait_reason(JOB, view);
    assert_eq!(reason(&pipeline).as_deref(), Some(words));
    pipeline.active_download_connections_by_job.insert(JOB, 1);
    assert!(pipeline.job_has_current_download_activity(JOB));
    assert_eq!(reason(&pipeline).as_deref(), Some(words));
    pipeline.download_wait_by_job.insert(
        JOB,
        crate::pipeline::DownloadWaitStatus {
            reason: crate::pipeline::AUTH_WAIT_REASON,
            retry_at_epoch_ms: None,
            pending_count: 1,
            exclude_servers: ServerSet::from([0, 2]),
        },
    );
    assert_eq!(reason(&pipeline).as_deref(), Some(words));
    pipeline.active_download_connections_by_job.remove(&JOB);
    assert_eq!(reason(&pipeline).as_deref(), Some(words));
}

// An IPv6 address is written in brackets, so its colons do not run into
// the port, and two servers are named in pool order.
#[tokio::test]
async fn the_login_wait_names_an_ipv6_server_in_brackets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let server = |host: &str| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: host.into(),
            port: 563,
            ..Default::default()
        },
        max_connections: 2,
        ..weaver_nntp::pool::ServerPoolConfig::default()
    };
    pipeline.nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server("2001:db8::1"), server("news.example.com")],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    assert_eq!(
        pipeline.login_refused_wait_words(weaver_nntp::ServerSet::from([0, 1])),
        "login_refused:[2001:db8::1]:563, news.example.com:563"
    );
    assert_eq!(
        pipeline.login_refused_wait_words(weaver_nntp::ServerSet::from([1])),
        "login_refused:news.example.com:563"
    );
}
