use std::path::Path;

use rusqlite::Connection;

use crate::StateError;

use super::Database;

const STABLE_TABLES: &[&str] = &[
    "schema_version",
    "settings",
    "servers",
    "categories",
    "api_keys",
    "job_history",
    "job_events",
    "rss_feeds",
    "rss_rules",
    "rss_seen_items",
];

const RESTORE_PRISTINE_TABLES: &[&str] = &[
    "job_history",
    "job_events",
    "active_jobs",
    "active_segments",
    "active_files",
    "active_par2",
    "active_extracted",
    "active_extraction_chunks",
    "active_archive_headers",
    "active_rar_volume_facts",
    "active_volume_status",
];

const CLEAR_IMPORT_TABLES: &[&str] = &[
    "rss_seen_items",
    "rss_rules",
    "rss_feeds",
    "job_events",
    "job_history",
    "api_keys",
    "categories",
    "servers",
    "settings",
];

#[derive(Debug, Clone)]
pub struct StableStateExport {
    pub schema_version: i64,
    pub included_tables: Vec<String>,
    pub max_job_id: u64,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

impl Database {
    pub fn schema_version(&self) -> Result<i64, StateError> {
        let conn = self.conn();
        conn.query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
            row.get(0)
        })
        .map_err(db_err)
    }

    pub fn restore_target_is_pristine(&self) -> Result<bool, StateError> {
        let conn = self.conn();
        for table in RESTORE_PRISTINE_TABLES {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = conn
                .query_row(&query, [], |row| row.get(0))
                .map_err(db_err)?;
            if count > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn export_stable_state(&self, dest: &Path) -> Result<StableStateExport, StateError> {
        let snapshot_dir = tempfile::tempdir().map_err(db_err)?;
        let snapshot_path = snapshot_dir.path().join("snapshot.db");
        let snapshot_path_str = snapshot_path.to_string_lossy().to_string();

        let schema_version = self.schema_version()?;
        let max_job_id = self.max_job_id_all()?;

        {
            let conn = self.conn();
            conn.execute("VACUUM INTO ?1", [&snapshot_path_str])
                .map_err(db_err)?;
        }

        let export_conn = Connection::open(dest).map_err(db_err)?;
        export_conn
            .execute("ATTACH DATABASE ?1 AS src", [&snapshot_path_str])
            .map_err(db_err)?;

        for table in STABLE_TABLES {
            let sql = format!("CREATE TABLE {table} AS SELECT * FROM src.{table}");
            export_conn.execute_batch(&sql).map_err(db_err)?;
        }

        export_conn
            .execute_batch("DETACH DATABASE src")
            .map_err(db_err)?;

        Ok(StableStateExport {
            schema_version,
            included_tables: STABLE_TABLES.iter().map(|t| (*t).to_string()).collect(),
            max_job_id,
        })
    }

    pub fn import_stable_state(&self, src: &Path) -> Result<(), StateError> {
        let src_path = src.to_string_lossy().to_string();
        let conn = self.conn();
        conn.execute("ATTACH DATABASE ?1 AS src", [&src_path])
            .map_err(db_err)?;
        let tx = conn.unchecked_transaction().map_err(db_err)?;

        for table in CLEAR_IMPORT_TABLES {
            let sql = format!("DELETE FROM {table}");
            tx.execute(&sql, []).map_err(db_err)?;
        }

        tx.execute_batch(
            "INSERT INTO settings (key, value)
                 SELECT key, value FROM src.settings;
             INSERT INTO servers (id, host, port, tls, username, password, connections, active, supports_pipelining, priority)
                 SELECT id, host, port, tls, username, password, connections, active, supports_pipelining, priority FROM src.servers;
             INSERT INTO categories (id, name, dest_dir, aliases)
                 SELECT id, name, dest_dir, aliases FROM src.categories;
             INSERT INTO api_keys (id, name, key_hash, scope, created_at, last_used_at)
                 SELECT id, name, key_hash, scope, created_at, last_used_at FROM src.api_keys;
             INSERT INTO job_history
                 (job_id, name, status, error_message, total_bytes, downloaded_bytes, failed_bytes, health, category, output_dir, nzb_path, created_at, completed_at, metadata)
                 SELECT job_id, name, status, error_message, total_bytes, downloaded_bytes, failed_bytes, health, category, output_dir, nzb_path, created_at, completed_at, metadata
                 FROM src.job_history;
             INSERT INTO job_events (id, job_id, timestamp, kind, message, file_id)
                 SELECT id, job_id, timestamp, kind, message, file_id FROM src.job_events;
             INSERT INTO rss_feeds
                 (id, name, url, enabled, poll_interval_secs, username, password, default_category, default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error, consecutive_failures)
                 SELECT id, name, url, enabled, poll_interval_secs, username, password, default_category, default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error, consecutive_failures
                 FROM src.rss_feeds;
             INSERT INTO rss_rules
                 (id, feed_id, sort_order, enabled, action, title_regex, item_categories, min_size_bytes, max_size_bytes, category_override, metadata)
                 SELECT id, feed_id, sort_order, enabled, action, title_regex, item_categories, min_size_bytes, max_size_bytes, category_override, metadata
                 FROM src.rss_rules;
             INSERT INTO rss_seen_items
                 (feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at, job_id, item_url, error)
                 SELECT feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at, job_id, item_url, error
                 FROM src.rss_seen_items;
             DELETE FROM sqlite_sequence WHERE name IN ('api_keys', 'job_events');
             INSERT INTO sqlite_sequence (name, seq)
                 SELECT 'api_keys', COALESCE(MAX(id), 0) FROM api_keys;
             INSERT INTO sqlite_sequence (name, seq)
                 SELECT 'job_events', COALESCE(MAX(id), 0) FROM job_events;
             ",
        )
        .map_err(db_err)?;

        tx.commit().map_err(db_err)?;
        conn.execute_batch("DETACH DATABASE src").map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        HistoryFilter, JobEvent, JobHistoryRow, RssFeedRow, RssRuleAction, RssRuleRow,
    };
    use weaver_core::config::{CategoryConfig, Config, RetryOverrides, ServerConfig};

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
        src.insert_rss_seen_item(&crate::db::RssSeenItemRow {
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
        assert_eq!(
            dest.list_job_history(&HistoryFilter::default())
                .unwrap()
                .len(),
            1
        );
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
}
