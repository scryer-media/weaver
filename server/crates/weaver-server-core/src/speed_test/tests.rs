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
        },
        max_duration: Duration::from_secs(600),
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
async fn bandwidth_over_https_pulls_every_stream_through_the_route() {
    let rig = rig().await;
    let (acceptor, cert_der, _) = loopback_tls();
    let http = HttpFixture::start(String::new(), 200_000, Some(acceptor)).await;
    rig.service.trust_roots_for_tests(vec![cert_der]);

    let started = rig
        .service
        .start(SpeedTestRequest {
            consumer: Consumer::Rss(1),
            route: Some(direct_route()),
            overrides: PathOverride::default(),
            kind: SpeedTestKind::Bandwidth {
                provider: BandwidthProvider::Custom {
                    url: format!("https://127.0.0.1:{}/blob", http.addr.port())
                        .parse()
                        .unwrap(),
                },
                bytes: 200_000,
                streams: 3,
            },
            max_duration: Duration::from_secs(600),
        })
        .await
        .unwrap();
    let run = finished(&rig, started.id).await;

    assert_eq!(run.status, SpeedTestStatus::Completed, "{:?}", run.error);
    let result = run.result.unwrap();
    assert_eq!(result.bytes, 600_000);
    assert_eq!(result.bandwidth.unwrap().streams, 3);
    assert_eq!(*http.requests().borrow(), 3);
    assert!(result.usenet.is_none());
    assert_eq!(result.connections.len(), 3);
    assert!(
        result
            .connections
            .iter()
            .all(|c| c.error.is_none() && c.path.egress_id == 0)
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
