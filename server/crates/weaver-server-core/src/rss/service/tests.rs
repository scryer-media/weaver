use super::*;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use tempfile::TempDir;
use tokio::sync::{RwLock, mpsc};

use crate::settings::Config;
use crate::{JobSpec, PipelineMetrics, SchedulerCommand, SharedPipelineState};

#[derive(Clone)]
struct TestHttpState {
    feed_body: String,
    nzb_body: Vec<u8>,
    etag: Option<String>,
    require_auth: bool,
    feed_delay_ms: u64,
    feed_requests: Arc<AtomicUsize>,
    nzb_requests: Arc<AtomicUsize>,
    auth_failures: Arc<AtomicUsize>,
    conditional_hits: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CapturedSubmission {
    spec: JobSpec,
}

#[tokio::test]
async fn feed_item_parsing_prefers_enclosure_and_supports_direct_link_fallback() {
    let rss = r#"<?xml version="1.0"?>
            <rss version="2.0">
              <channel>
                <title>Example</title>
                <item>
                  <guid>guid-1</guid>
                  <title>Frieren - enclosure</title>
                  <link>https://example.com/detail</link>
                  <enclosure url="https://example.com/download.nzb" length="1234" type="application/x-nzb"/>
                  <category>TV</category>
                </item>
              </channel>
            </rss>"#
            .to_string();
    let feed = feed_rs::parser::parse(Cursor::new(rss.as_bytes())).unwrap();
    let item = feed_item_from_entry(feed.entries.into_iter().next().unwrap());
    assert_eq!(
        item.download_url.as_deref(),
        Some("https://example.com/download.nzb")
    );
    assert_eq!(item.categories, vec!["TV".to_string()]);

    let atom = r#"<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom">
              <title>Atom Example</title>
              <entry>
                <id>tag:example.com,2026:test</id>
                <title>Frieren - link fallback</title>
                <link href="https://example.com/api?t=get&id=1" />
              </entry>
            </feed>"#;
    let feed = feed_rs::parser::parse(Cursor::new(atom.as_bytes())).unwrap();
    let item = feed_item_from_entry(feed.entries.into_iter().next().unwrap());
    assert_eq!(
        item.download_url.as_deref(),
        Some("https://example.com/api?t=get&id=1")
    );
}

#[test]
fn first_matching_rule_wins() {
    let item = FeedItem {
        item_id: "guid-1".to_string(),
        title: "Frieren".to_string(),
        published_at: None,
        size_bytes: Some(500),
        categories: vec!["tv".to_string()],
        download_url: Some("https://example.com/download.nzb".to_string()),
        display_url: None,
    };
    let rules = compile_rules(vec![
        RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Reject,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        },
        RssRuleRow {
            id: 2,
            feed_id: 1,
            sort_order: 1,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        },
    ]);
    let decision = evaluate_item(&rules, &item).unwrap();
    assert_eq!(decision.row.action, RssRuleAction::Reject);
}

#[tokio::test]
async fn run_sync_submits_matching_items_and_dedupes_across_restart() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("rss.sqlite");
    let state = TestHttpState {
        feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
        nzb_body: sample_nzb_bytes(),
        etag: None,
        require_auth: false,
        feed_delay_ms: 0,
        feed_requests: Arc::new(AtomicUsize::new(0)),
        nzb_requests: Arc::new(AtomicUsize::new(0)),
        auth_failures: Arc::new(AtomicUsize::new(0)),
        conditional_hits: Arc::new(AtomicUsize::new(0)),
    };
    let (base_url, server_task) = start_test_server(state.clone()).await;

    let db = Database::open(&db_path).unwrap();
    let feed = RssFeedRow {
        id: 1,
        name: "Test Feed".to_string(),
        url: format!("{base_url}/feed"),
        enabled: true,
        poll_interval_secs: 900,
        username: None,
        password: None,
        default_category: Some("tv".to_string()),
        default_metadata: vec![("source".to_string(), "rss".to_string())],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    };
    db.insert_rss_feed(&feed).unwrap();
    db.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Frieren".to_string()),
        item_categories: vec!["TV".to_string()],
        min_size_bytes: Some(1),
        max_size_bytes: Some(10_000),
        category_override: Some("anime".to_string()),
        metadata: vec![("profile".to_string(), "hevc".to_string())],
    })
    .unwrap();

    let submissions = Arc::new(StdMutex::new(Vec::new()));
    let service = build_service(temp.path(), db.clone(), submissions.clone());
    let report = service.run_all_sync().await.unwrap();
    assert_eq!(report.feeds_polled, 1);
    assert_eq!(report.items_submitted, 1);
    assert_eq!(submissions.lock().unwrap().len(), 1);
    let first = submissions.lock().unwrap()[0].clone();
    assert_eq!(first.spec.category.as_deref(), Some("anime"));
    assert!(
        first
            .spec
            .metadata
            .iter()
            .any(|(k, v)| k == "rss.feed_name" && v == "Test Feed")
    );

    drop(service);
    drop(db);

    let db = Database::open(&db_path).unwrap();
    let submissions_restart = Arc::new(StdMutex::new(Vec::new()));
    let service_restart = build_service(temp.path(), db, submissions_restart.clone());
    let report = service_restart.run_all_sync().await.unwrap();
    assert_eq!(report.items_new, 0);
    assert!(submissions_restart.lock().unwrap().is_empty());

    server_task.abort();
}

#[tokio::test]
async fn run_sync_uses_conditional_get_and_basic_auth() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(&temp.path().join("rss-auth.sqlite")).unwrap();
    let state = TestHttpState {
        feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
        nzb_body: sample_nzb_bytes(),
        etag: Some("v1".to_string()),
        require_auth: true,
        feed_delay_ms: 0,
        feed_requests: Arc::new(AtomicUsize::new(0)),
        nzb_requests: Arc::new(AtomicUsize::new(0)),
        auth_failures: Arc::new(AtomicUsize::new(0)),
        conditional_hits: Arc::new(AtomicUsize::new(0)),
    };
    let (base_url, server_task) = start_test_server(state.clone()).await;

    db.insert_rss_feed(&RssFeedRow {
        id: 1,
        name: "Auth Feed".to_string(),
        url: format!("{base_url}/feed"),
        enabled: true,
        poll_interval_secs: 900,
        username: Some("user".to_string()),
        password: Some("pass".to_string()),
        default_category: None,
        default_metadata: vec![],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    db.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Frieren".to_string()),
        item_categories: vec![],
        min_size_bytes: None,
        max_size_bytes: None,
        category_override: None,
        metadata: vec![],
    })
    .unwrap();

    let submissions = Arc::new(StdMutex::new(Vec::new()));
    let service = build_service(temp.path(), db.clone(), submissions);
    let first = service.run_all_sync().await.unwrap();
    assert_eq!(first.items_submitted, 1);
    let second = service.run_all_sync().await.unwrap();
    assert_eq!(second.items_fetched, 0);
    assert_eq!(state.conditional_hits.load(Ordering::SeqCst), 1);
    assert_eq!(state.auth_failures.load(Ordering::SeqCst), 0);
    server_task.abort();
}

#[tokio::test]
async fn failed_fetch_is_marked_seen_and_not_retried_immediately() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(&temp.path().join("rss-failure.sqlite")).unwrap();
    let state = TestHttpState {
        feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
        nzb_body: b"not an nzb".to_vec(),
        etag: None,
        require_auth: false,
        feed_delay_ms: 0,
        feed_requests: Arc::new(AtomicUsize::new(0)),
        nzb_requests: Arc::new(AtomicUsize::new(0)),
        auth_failures: Arc::new(AtomicUsize::new(0)),
        conditional_hits: Arc::new(AtomicUsize::new(0)),
    };
    let (base_url, server_task) = start_test_server(state.clone()).await;
    db.insert_rss_feed(&RssFeedRow {
        id: 1,
        name: "Broken Feed".to_string(),
        url: format!("{base_url}/feed"),
        enabled: true,
        poll_interval_secs: 900,
        username: None,
        password: None,
        default_category: None,
        default_metadata: vec![],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    db.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Frieren".to_string()),
        item_categories: vec![],
        min_size_bytes: None,
        max_size_bytes: None,
        category_override: None,
        metadata: vec![],
    })
    .unwrap();

    let service = build_service(temp.path(), db, Arc::new(StdMutex::new(Vec::new())));
    let first = service.run_all_sync().await.unwrap();
    assert_eq!(first.items_submitted, 0);
    assert_eq!(first.items_new, 1);
    assert_eq!(first.errors.len(), 1);

    let second = service.run_all_sync().await.unwrap();
    assert_eq!(second.items_new, 0);
    assert_eq!(second.items_submitted, 0);
    assert_eq!(state.nzb_requests.load(Ordering::SeqCst), 1);
    server_task.abort();
}

#[tokio::test]
async fn background_due_sync_skips_when_manual_sync_is_active() {
    let temp = TempDir::new().unwrap();
    let db = Database::open(&temp.path().join("rss-due.sqlite")).unwrap();
    let state = TestHttpState {
        feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
        nzb_body: sample_nzb_bytes(),
        etag: None,
        require_auth: false,
        feed_delay_ms: 250,
        feed_requests: Arc::new(AtomicUsize::new(0)),
        nzb_requests: Arc::new(AtomicUsize::new(0)),
        auth_failures: Arc::new(AtomicUsize::new(0)),
        conditional_hits: Arc::new(AtomicUsize::new(0)),
    };
    let (base_url, server_task) = start_test_server(state).await;
    db.insert_rss_feed(&RssFeedRow {
        id: 1,
        name: "Due Feed".to_string(),
        url: format!("{base_url}/feed"),
        enabled: true,
        poll_interval_secs: 1,
        username: None,
        password: None,
        default_category: None,
        default_metadata: vec![],
        etag: None,
        last_modified: None,
        last_polled_at: Some(unix_now_secs() - 10),
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    db.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Frieren".to_string()),
        item_categories: vec![],
        min_size_bytes: None,
        max_size_bytes: None,
        category_override: None,
        metadata: vec![],
    })
    .unwrap();

    let service = build_service(temp.path(), db, Arc::new(StdMutex::new(Vec::new())));
    let running = {
        let service = service.clone();
        tokio::spawn(async move { service.run_all_sync().await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        service.try_run_due_sync().await.unwrap(),
        DueSyncOutcome::SkippedActiveSync
    ));
    let _ = running.await.unwrap();
    server_task.abort();
}

fn build_service(
    data_dir: &Path,
    db: Database,
    submissions: Arc<StdMutex<Vec<CapturedSubmission>>>,
) -> RssService {
    let config = Arc::new(RwLock::new(Config {
        data_dir: data_dir.display().to_string(),
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        tuner: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        isp_bandwidth_cap: None,
        cleanup_after_extract: Some(true),
        config_path: None,
    }));
    let handle = test_scheduler_handle(submissions);
    crate::ingest::init_job_counter(20_000);
    RssService::new(handle, config, db)
}

fn test_scheduler_handle(submissions: Arc<StdMutex<Vec<CapturedSubmission>>>) -> SchedulerHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
    let (event_tx, _) = tokio::sync::broadcast::channel(8);
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            if let SchedulerCommand::AddJob { spec, reply, .. } = command {
                submissions
                    .lock()
                    .unwrap()
                    .push(CapturedSubmission { spec });
                let _ = reply.send(Ok(()));
            }
        }
    });
    SchedulerHandle::new(cmd_tx, event_tx, state)
}

async fn start_test_server(state: TestHttpState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    let mut state = state;
    state.feed_body = state.feed_body.replace("http://localhost", &base_url);
    let app = Router::new()
        .route("/feed", get(feed_handler))
        .route("/download.nzb", get(nzb_handler))
        .route("/api", get(nzb_handler))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base_url, task)
}

async fn feed_handler(State(state): State<TestHttpState>, headers: HeaderMap) -> impl IntoResponse {
    state.feed_requests.fetch_add(1, Ordering::SeqCst);
    if state.require_auth && !auth_ok(&headers) {
        state.auth_failures.fetch_add(1, Ordering::SeqCst);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if state.feed_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(state.feed_delay_ms)).await;
    }
    if let Some(etag) = &state.etag
        && headers
            .get(IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            == Some(etag.as_str())
    {
        state.conditional_hits.fetch_add(1, Ordering::SeqCst);
        return StatusCode::NOT_MODIFIED.into_response();
    }
    let mut response_headers = HeaderMap::new();
    if let Some(etag) = &state.etag {
        response_headers.insert(ETAG, HeaderValue::from_str(etag).unwrap());
    }
    (response_headers, state.feed_body.clone()).into_response()
}

async fn nzb_handler(State(state): State<TestHttpState>, headers: HeaderMap) -> impl IntoResponse {
    state.nzb_requests.fetch_add(1, Ordering::SeqCst);
    if state.require_auth && !auth_ok(&headers) {
        state.auth_failures.fetch_add(1, Ordering::SeqCst);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    (
        [(axum::http::header::CONTENT_TYPE, "application/x-nzb")],
        state.nzb_body,
    )
        .into_response()
}

fn auth_ok(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some("Basic dXNlcjpwYXNz")
}

fn sample_rss_feed(guid: &str, title: &str, path: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
            <rss version="2.0">
              <channel>
                <title>Example</title>
                <item>
                  <guid>{guid}</guid>
                  <title>{title}</title>
                  <link>https://example.com/details</link>
                  <enclosure url="http://localhost{path}" length="321" type="application/x-nzb"/>
                  <category>TV</category>
                  <pubDate>Mon, 10 Mar 2025 12:00:00 GMT</pubDate>
                </item>
              </channel>
            </rss>"#
    )
}

fn sample_nzb_bytes() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Frieren.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
        .to_vec()
}
