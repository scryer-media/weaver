use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};

use super::fixture::{FixtureArticle, HttpFixture, NntpFixture, loopback_tls, nzb};
use super::*;
use crate::proxies::{Consumer, Failover, LegPath, ProxyRuntime, Route, RouteLeg};
use crate::settings::Config;
use crate::{Database, PipelineMetrics, SchedulerCommand, SchedulerHandle, SharedPipelineState};

struct Rig {
    service: SpeedTestService,
    handle: SchedulerHandle,
    metrics: Arc<PipelineMetrics>,
    config: crate::settings::SharedConfig,
    _dir: tempfile::TempDir,
}

async fn rig() -> Rig {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::open_in_memory().unwrap();
    let config = Arc::new(RwLock::new(Config {
        data_dir: dir.path().display().to_string(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        propagation_delay_secs: None,
        cleanup_after_extract: None,
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));
    let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
    let (event_tx, _) = tokio::sync::broadcast::channel(8);
    let metrics = PipelineMetrics::new();
    let state = SharedPipelineState::new(metrics.clone(), vec![]);
    let scheduler_state = state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                SchedulerCommand::PauseAll { reply } => {
                    scheduler_state.set_paused(true);
                    let _ = reply.send(());
                }
                SchedulerCommand::ResumeAll { reply } => {
                    scheduler_state.set_paused(false);
                    let _ = reply.send(());
                }
                _ => {}
            }
        }
    });
    let handle = SchedulerHandle::new(cmd_tx, event_tx, state);
    handle.set_proxy_runtime(
        ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap(),
    );
    let service = SpeedTestService::new(handle.clone(), config.clone(), db, true);
    Rig {
        service,
        handle,
        metrics,
        config,
        _dir: dir,
    }
}

fn direct_route() -> Route {
    Route {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Direct,
        }],
        failover: Failover::Redistribute,
    }
}

async fn add_server(rig: &Rig, nntp: &NntpFixture, tls_ca: Option<&std::path::Path>) -> u32 {
    let server: crate::servers::ServerConfig = serde_json::from_value(serde_json::json!({
        "id": 1,
        "host": "127.0.0.1",
        "port": nntp.addr.port(),
        "tls": tls_ca.is_some(),
        "username": null,
        "password": null,
        "connections": 2,
        "active": true,
        "tls_ca_cert": tls_ca,
    }))
    .unwrap();
    rig.config.write().await.servers.push(server);
    1
}

async fn finished(rig: &Rig, id: u32) -> SpeedTestRun {
    let mut revision = rig.service.subscribe();
    loop {
        let run = rig.service.run(id).unwrap();
        if run.status != SpeedTestStatus::Running {
            return run;
        }
        revision.changed().await.unwrap();
    }
}

fn usenet_request(server: u32, nzb_url: String) -> SpeedTestRequest {
    SpeedTestRequest {
        consumer: Consumer::Server(server),
        route: Some(direct_route()),
        overrides: PathOverride::default(),
        kind: SpeedTestKind::Usenet {
            nzb: TestNzbSource::Url(nzb_url.parse().unwrap()),
            connections: None,
            max_duration: Duration::from_secs(600),
        },
    }
}

fn bandwidth_request(url: String) -> SpeedTestRequest {
    SpeedTestRequest {
        consumer: Consumer::Rss(1),
        route: Some(direct_route()),
        overrides: PathOverride::default(),
        kind: SpeedTestKind::Bandwidth {
            provider: BandwidthProvider::Custom {
                url: url.parse().unwrap(),
            },
        },
    }
}

#[tokio::test]
async fn usenet_run_over_tls_books_raw_bytes_and_counts_missing_articles() {
    let rig = rig().await;
    let (acceptor, _, pem) = loopback_tls();
    let ca = rig._dir.path().join("ca.pem");
    std::fs::write(&ca, pem).unwrap();
    let articles: Vec<FixtureArticle> = (0..4)
        .map(|i| FixtureArticle::patterned(&format!("part{i}@fixture"), 40_000 + i * 1000))
        .collect();
    let nntp = NntpFixture::start(articles, None, Some(acceptor)).await;
    let listed: Vec<(String, u64)> = (0..4)
        .map(|i| (format!("part{i}@fixture"), 41_000))
        .chain([("absent@fixture".to_string(), 41_000)])
        .collect();
    let listed: Vec<(&str, u64)> = listed.iter().map(|(id, b)| (id.as_str(), *b)).collect();
    let http = HttpFixture::start(nzb(&listed), 0, None).await;
    let server = add_server(&rig, &nntp, Some(&ca)).await;
    let before = rig.metrics.bytes_downloaded.load(Ordering::Acquire);

    let started = rig
        .service
        .start(usenet_request(
            server,
            format!("http://{}/test.nzb", http.addr),
        ))
        .await
        .unwrap();
    assert_eq!(started.status, SpeedTestStatus::Running);
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    let result = run.result.unwrap();
    let usenet = result.usenet.unwrap();
    assert_eq!(usenet.articles_requested, 5);
    assert_eq!(usenet.delivered, 4);
    assert_eq!(usenet.missing, 1);
    assert_eq!(usenet.failed, 0);
    assert_eq!(usenet.declared_bytes, 5 * 41_000);
    assert_eq!(usenet.decoded_bytes, 4 * 40_000 + 6000);
    assert_eq!(
        result.samples.iter().map(|s| s.decoded_bytes).sum::<u64>(),
        usenet.decoded_bytes,
        "the decoded bins hold every decoded byte"
    );
    assert!(usenet.tls);
    assert!(result.bytes > usenet.decoded_bytes);
    let booked = rig.metrics.bytes_downloaded.load(Ordering::Acquire) - before;
    assert_eq!(result.bytes, booked);
    assert_eq!(
        result.legs.iter().map(|l| l.bytes).sum::<u64>(),
        result.bytes
    );
    assert_eq!(run.tested.legs[0].target, 2);
    assert!(!rig.handle.is_globally_paused(), "the queue is given back");
}

#[tokio::test]
async fn a_queue_paused_before_the_run_stays_paused_after_it() {
    let rig = rig().await;
    let nntp = NntpFixture::start(
        vec![FixtureArticle::patterned("only@fixture", 9_000)],
        None,
        None,
    )
    .await;
    let http = HttpFixture::start(nzb(&[("only@fixture", 9_000)]), 0, None).await;
    let server = add_server(&rig, &nntp, None).await;
    rig.handle.pause_all().await.unwrap();

    let started = rig
        .service
        .start(usenet_request(
            server,
            format!("http://{}/test.nzb", http.addr),
        ))
        .await
        .unwrap();
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    assert!(!run.result.unwrap().usenet.unwrap().tls);
    assert!(
        rig.handle.is_globally_paused(),
        "the operator's pause is kept"
    );
}

#[tokio::test]
async fn a_cancel_while_draining_ends_without_a_result_and_resumes_the_queue() {
    let rig = rig().await;
    let nntp = NntpFixture::start(
        vec![FixtureArticle::patterned("only@fixture", 9_000)],
        None,
        None,
    )
    .await;
    let http = HttpFixture::start(nzb(&[("only@fixture", 9_000)]), 0, None).await;
    let server = add_server(&rig, &nntp, None).await;
    rig.metrics.set_active_downloads(1);

    let started = rig
        .service
        .start(usenet_request(
            server,
            format!("http://{}/test.nzb", http.addr),
        ))
        .await
        .unwrap();
    let mut revision = rig.service.subscribe();
    loop {
        let run = rig.service.run(started.id).unwrap();
        if run.phase_label().as_deref() == Some("draining 1") {
            break;
        }
        revision.changed().await.unwrap();
    }
    assert!(rig.handle.is_globally_paused());
    assert_eq!(
        *nntp.connections().borrow(),
        0,
        "nothing is fetched while draining"
    );
    assert!(rig.service.cancel(started.id));
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Cancelled);
    assert!(run.result.is_none());
    assert!(!rig.handle.is_globally_paused());
}

#[tokio::test]
async fn a_second_run_is_refused_while_one_runs() {
    let rig = rig().await;
    let nntp = NntpFixture::start(
        vec![FixtureArticle::patterned("only@fixture", 9_000)],
        None,
        None,
    )
    .await;
    let http = HttpFixture::start(nzb(&[("only@fixture", 9_000)]), 0, None).await;
    let server = add_server(&rig, &nntp, None).await;
    rig.metrics.set_active_downloads(1);
    let url = format!("http://{}/test.nzb", http.addr);
    let first = rig
        .service
        .start(usenet_request(server, url.clone()))
        .await
        .unwrap();

    let refused = rig
        .service
        .start(usenet_request(server, url))
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        SpeedTestError::Refused(crate::proxies::NetworkRefusal::SpeedTestAlreadyRunning {
            run: first.id
        })
    );
    rig.metrics.set_active_downloads(0);
    assert_eq!(
        finished(&rig, first.id).await.status,
        SpeedTestStatus::Completed
    );
}

#[tokio::test]
async fn a_bandwidth_connection_keeps_one_row_across_its_gets() {
    let rig = rig().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), 200_000, Some(acceptor)).await;
    rig.service.trust_roots_for_tests(vec![cert_der]);
    rig.service.measure_until_stopped_for_tests();

    let started = rig
        .service
        .start(bandwidth_request(format!(
            "https://127.0.0.1:{}/blob",
            http.addr.port()
        )))
        .await
        .unwrap();
    // The fixture closes the socket after every body, so each GET after the
    // first dials again on the same connection.
    let mut revision = rig.service.subscribe();
    loop {
        let run = rig.service.run(started.id).unwrap();
        assert_eq!(run.status, SpeedTestStatus::Running, "{:?}", run.error);
        if run
            .result
            .is_some_and(|result| result.connections.iter().any(|c| c.bytes >= 400_000))
        {
            break;
        }
        revision.changed().await.unwrap();
    }
    assert!(rig.service.cancel(started.id));
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Cancelled);
    let result = run.result.unwrap();
    assert!(result.usenet.is_none());
    let bandwidth = result.bandwidth.unwrap();
    assert_eq!(bandwidth.connections as usize, result.connections.len());
    assert_eq!(run.tested.legs[0].target as usize, result.connections.len());
    assert!(*http.requests().borrow() >= 2);
    assert_eq!(
        result.connections.iter().map(|c| c.bytes).sum::<u64>(),
        result.bytes
    );
    assert!(
        result
            .connections
            .iter()
            .all(|c| c.error.is_none() && c.path.egress_id == 0),
        "{:?}",
        result.connections
    );
    assert!(result.samples.iter().all(|s| s.decoded_bytes == 0));
    assert!(
        std::fs::read_dir(rig._dir.path()).unwrap().next().is_none(),
        "nothing downloaded is kept"
    );
}

// Give egress 0 a download quota of `limit` bytes.
fn egress_quota(rig: &Rig, limit: u64) {
    use weaver_nntp::transfer::{QuotaRuntimeConfig, ServerTransferConfig, StableServerId};

    rig.handle
        .proxy_runtime()
        .unwrap()
        .network
        .egress_controls
        .configure(
            StableServerId(0),
            ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(QuotaRuntimeConfig {
                    limit_bytes: limit,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
}

// The bytes egress 0's quota has been charged.
fn egress_charged(rig: &Rig) -> u64 {
    rig.handle
        .proxy_runtime()
        .unwrap()
        .network
        .egress_controls
        .snapshot(weaver_nntp::transfer::StableServerId(0))
        .quota_used_bytes
}

#[tokio::test]
async fn a_get_shorter_than_a_slice_reserves_only_its_length() {
    let rig = rig().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), 200_000, Some(acceptor)).await;
    rig.service.trust_roots_for_tests(vec![cert_der]);
    rig.service.measure_until_stopped_for_tests();
    egress_quota(&rig, 500_000);

    let started = rig
        .service
        .start(bandwidth_request(format!(
            "https://127.0.0.1:{}/blob",
            http.addr.port()
        )))
        .await
        .unwrap();
    let run = finished(&rig, started.id).await;

    // Each GET reserves its own 200 000 bytes rather than a whole slice, so
    // two fit the quota and a third does not, on any connection.
    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    let result = run.result.unwrap();
    assert_eq!(result.bytes, 400_000);
    assert_eq!(egress_charged(&rig), 400_000);
    assert_eq!(
        result.bandwidth.unwrap().connections as usize,
        result.connections.len()
    );
    assert!(
        result
            .connections
            .iter()
            .all(|c| c.error.as_deref() == Some("egress 0 has used its download quota")),
        "{:?}",
        result.connections
    );
}

#[tokio::test]
async fn a_quota_smaller_than_one_download_is_used_a_slice_at_a_time() {
    let slice = super::bandwidth::QUOTA_SLICE;
    let download = slice * 3 / 2;
    let limit = slice * 5 / 4;
    let rig = rig().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), download as usize, Some(acceptor)).await;
    rig.service.trust_roots_for_tests(vec![cert_der]);
    rig.service.measure_until_stopped_for_tests();
    egress_quota(&rig, limit);

    let started = rig
        .service
        .start(bandwidth_request(format!(
            "https://127.0.0.1:{}/blob",
            http.addr.port()
        )))
        .await
        .unwrap();
    let run = finished(&rig, started.id).await;

    // A whole download would not fit, but a slice does: the run measures,
    // and each connection ends when its next slice is refused.
    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    let result = run.result.unwrap();
    assert!(
        result.bytes >= slice && result.bytes < download,
        "{} bytes",
        result.bytes
    );
    assert!(
        result
            .connections
            .iter()
            .all(|c| c.error.as_deref() == Some("egress 0 has used its download quota")),
        "{:?}",
        result.connections
    );
    let charged = egress_charged(&rig);
    assert_eq!(charged, result.bytes, "every byte read is booked");
    assert!(
        charged <= limit + slice * result.connections.len() as u64,
        "{charged} charged against a quota of {limit}"
    );
}

#[test]
fn an_uploaded_nzb_is_read_as_a_fetched_one_is() {
    let read = |bytes: &[u8]| UploadedNzb::read("test.nzb".into(), bytes);
    let nzb_text = nzb(&[("one@fixture", 9_000)]);
    assert_eq!(read(nzb_text.as_bytes()).unwrap().filename(), "test.nzb");
    assert!(
        read(b"not an nzb")
            .unwrap_err()
            .starts_with("the test NZB is not readable: ")
    );
    let mut padded = nzb_text.into_bytes();
    padded.resize(super::http::NZB_LIMIT + 1, b'\n');
    assert_eq!(
        read(&padded).unwrap_err(),
        format!(
            "the test NZB is larger than {} bytes",
            super::http::NZB_LIMIT
        )
    );
}

#[tokio::test]
async fn an_uploaded_nzb_is_measured_and_held_with_its_run() {
    let rig = rig().await;
    let nntp = NntpFixture::start(
        vec![
            FixtureArticle::patterned("one@fixture", 9_000),
            FixtureArticle::patterned("two@fixture", 9_000),
        ],
        None,
        None,
    )
    .await;
    let server = add_server(&rig, &nntp, None).await;
    let upload = UploadedNzb::read(
        "test.nzb".into(),
        nzb(&[("one@fixture", 9_000), ("two@fixture", 9_000)]).as_bytes(),
    )
    .unwrap();
    let mut request = usenet_request(server, "http://127.0.0.1:9/unused.nzb".into());
    request.kind = SpeedTestKind::Usenet {
        nzb: TestNzbSource::Upload {
            nzb: upload.clone(),
            run: None,
        },
        connections: None,
        max_duration: Duration::from_secs(600),
    };

    let started = rig.service.start(request).await.unwrap();
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    assert_eq!(run.result.unwrap().usenet.unwrap().delivered, 2);
    assert_eq!(rig.service.held_nzb(started.id).unwrap(), upload);
    assert_eq!(
        rig.service.held_nzb(started.id + 1).unwrap_err(),
        SpeedTestError::Invalid(format!("run {} holds no uploaded NZB", started.id + 1))
    );
    assert!(
        std::fs::read_dir(rig._dir.path()).unwrap().next().is_none(),
        "the upload is never written"
    );
}

#[tokio::test]
async fn usenet_on_a_feed_is_refused() {
    let rig = rig().await;
    let mut request = usenet_request(1, "http://127.0.0.1:9/test.nzb".into());
    request.consumer = Consumer::Rss(1);
    assert_eq!(
        rig.service.start(request).await.unwrap_err(),
        SpeedTestError::Refused(crate::proxies::NetworkRefusal::SpeedTestUsenetNeedsServer)
    );
}

// Start a bandwidth run of `path` on an HTTPS fixture serving `blob_len`
// bytes at `/blob`, and wait for it to finish.
async fn bandwidth_run_of(path: &str, blob_len: usize) -> (SpeedTestRun, String) {
    let rig = rig().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), blob_len, Some(acceptor)).await;
    rig.service.trust_roots_for_tests(vec![cert_der]);
    let url = format!("https://127.0.0.1:{}{path}", http.addr.port());
    let started = rig
        .service
        .start(bandwidth_request(url.clone()))
        .await
        .unwrap();
    (finished(&rig, started.id).await, url)
}

#[tokio::test]
async fn a_provider_answering_429_fails_the_run_as_rate_limited() {
    let (run, url) = bandwidth_run_of("/limited", 0).await;

    assert_eq!(run.status, SpeedTestStatus::Failed);
    assert_eq!(
        run.error,
        Some(SpeedTestFailure::RateLimited { url: url.clone() })
    );
    let failure = run.error.unwrap();
    assert_eq!(
        failure.to_string(),
        format!("GET {url} answered HTTP 429 Too Many Requests")
    );
    assert_eq!(failure.params().get("url"), Some(&serde_json::json!(url)));
    let result = run.result.unwrap();
    assert_eq!(result.bytes, 0);
    assert!(
        result.connections.iter().all(|c| c.error.as_deref()
            == Some(format!("GET {url} answered HTTP 429 Too Many Requests").as_str())),
        "{:?}",
        result.connections
    );
}

#[tokio::test]
async fn a_provider_answering_another_error_status_fails_the_run_with_that_status() {
    let (run, url) = bandwidth_run_of("/missing", 0).await;

    assert_eq!(run.status, SpeedTestStatus::Failed);
    let failure = run.error.unwrap();
    assert_eq!(
        failure,
        SpeedTestFailure::HttpStatus {
            url: url.clone(),
            status: 404
        }
    );
    assert_eq!(
        failure.to_string(),
        format!("GET {url} answered HTTP 404 Not Found")
    );
    assert_eq!(
        failure.params().get("status"),
        Some(&serde_json::json!(404))
    );
}

#[tokio::test]
async fn a_failure_with_no_kind_of_its_own_keeps_its_detail() {
    let (run, _) = bandwidth_run_of("/blob", 0).await;

    assert_eq!(run.status, SpeedTestStatus::Failed);
    assert_eq!(
        run.error,
        Some(SpeedTestFailure::Other(
            "no bytes were received: the response has no body".into()
        ))
    );
    assert!(run.error.unwrap().params().is_empty());
}
