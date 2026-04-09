use super::*;
use crate::categories::CategoryConfig;
use crate::servers::ServerConfig;
use crate::settings::{Config, RetryOverrides};
use crate::{
    HistoryFilter, JobEvent, JobHistoryRow, RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow,
};

fn sample_config() -> Config {
    Config {
        data_dir: "/old/data".into(),
        intermediate_dir: Some("/old/data/intermediate".into()),
        complete_dir: Some("/old/data/complete".into()),
        buffer_pool: None,
        tuner: None,
        servers: vec![ServerConfig {
            id: 1,
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            username: Some("user".into()),
            password: Some("pass".into()),
            connections: 20,
            active: true,
            supports_pipelining: true,
            priority: 0,
            tls_ca_cert: None,
        }],
        categories: vec![CategoryConfig {
            id: 1,
            name: "tv".into(),
            dest_dir: Some("/old/data/complete/tv".into()),
            aliases: "shows".into(),
        }],
        retry: Some(RetryOverrides {
            max_retries: Some(4),
            base_delay_secs: Some(1.0),
            multiplier: Some(2.0),
        }),
        max_download_speed: Some(42),
        isp_bandwidth_cap: None,
        cleanup_after_extract: Some(true),
        config_path: None,
    }
}

#[test]
fn export_and_import_stable_state_roundtrip() {
    let src = Database::open_in_memory().unwrap();
    src.save_config(&sample_config()).unwrap();
    let hash = [7u8; 32];
    src.insert_api_key("integration", &hash, "integration")
        .unwrap();
    src.insert_job_history(&JobHistoryRow {
        job_id: 77,
        name: "test".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 45,
        optional_recovery_downloaded_bytes: 12,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".into()),
        output_dir: Some("/old/data/complete/tv/test".into()),
        nzb_path: Some("/old/data/.weaver-nzbs/77.nzb".into()),
        created_at: 1,
        completed_at: 2,
        metadata: Some("[[\"k\",\"v\"]]".into()),
    })
    .unwrap();
    src.insert_job_events(&[JobEvent {
        job_id: 77,
        timestamp: 2,
        kind: "JOB_COMPLETED".into(),
        message: "done".into(),
        file_id: None,
    }])
    .unwrap();
    src.insert_rss_feed(&RssFeedRow {
        id: 1,
        name: "feed".into(),
        url: "https://example.com/rss".into(),
        enabled: true,
        poll_interval_secs: 900,
        username: None,
        password: None,
        default_category: Some("tv".into()),
        default_metadata: vec![("source".into(), "rss".into())],
        etag: Some("etag".into()),
        last_modified: None,
        last_polled_at: Some(1),
        last_success_at: Some(1),
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    src.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some(".*".into()),
        item_categories: vec!["tv".into()],
        min_size_bytes: None,
        max_size_bytes: None,
        category_override: Some("tv".into()),
        metadata: vec![("tag".into(), "value".into())],
    })
    .unwrap();
    src.insert_rss_seen_item(&RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".into(),
        item_title: "release".into(),
        published_at: Some(5),
        size_bytes: Some(10),
        decision: "accepted".into(),
        seen_at: 6,
        job_id: Some(77),
        item_url: Some("https://example.com/file.nzb".into()),
        error: None,
    })
    .unwrap();

    let temp = tempfile::NamedTempFile::new().unwrap();
    let info = src.export_stable_state(temp.path()).unwrap();
    assert_eq!(info.schema_version, src.schema_version().unwrap());
    assert!(info.included_tables.iter().any(|t| t == "settings"));

    let dest = Database::open_in_memory().unwrap();
    assert!(dest.restore_target_is_pristine().unwrap());
    dest.import_stable_state(temp.path()).unwrap();

    let restored = dest.load_config().unwrap();
    assert_eq!(restored.data_dir, "/old/data");
    assert_eq!(restored.servers.len(), 1);
    assert_eq!(restored.categories.len(), 1);
    assert_eq!(dest.list_api_keys().unwrap().len(), 1);
    let history = dest.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].optional_recovery_bytes, 45);
    assert_eq!(history[0].optional_recovery_downloaded_bytes, 12);
    assert_eq!(dest.get_job_events(77).unwrap().len(), 1);
    assert_eq!(dest.list_rss_feeds().unwrap().len(), 1);
    assert_eq!(dest.list_rss_rules(1).unwrap().len(), 1);
    assert!(dest.rss_seen_item_exists(1, "guid-1").unwrap());
}

#[test]
fn restore_target_is_not_pristine_with_history() {
    let db = Database::open_in_memory().unwrap();
    assert!(db.restore_target_is_pristine().unwrap());
    db.insert_job_history(&JobHistoryRow {
        job_id: 1,
        name: "x".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 1,
        metadata: None,
    })
    .unwrap();
    assert!(!db.restore_target_is_pristine().unwrap());
}
