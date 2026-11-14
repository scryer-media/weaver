// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 92, upgrade step: a server list an older version stored past the
// server limit is brought down to it, once.
//
// Weaver takes at most 50 servers. One pass deletes servers until 50 remain:
// primary servers (the fill servers of the top priority level) and
// backfill-only servers are kept; of the others, the disabled go first, then
// the enabled, each lowest priority first and newest first inside a level.
// Only when every other server is gone and more than 50 are still stored do
// kept servers go: the first 50 in stored order stay.
//
// Each server goes through the rows a user's delete removes, and is named in
// a warning. A list of 50 or fewer writes nothing.

use std::cmp::Reverse;

use crate::StateError;
use crate::persistence::sql_runtime::SqlConn;
use crate::servers::persistence::delete_server_rows;

pub(crate) const HOOK_ID: &str = "trim_servers_to_limit_v92";
pub(crate) const SCHEMA_VERSION: i64 = 92;

struct Stored {
    id: u32,
    host: String,
    port: i64,
    priority: i64,
    backfill: bool,
    active: bool,
}

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn trim_servers_to_limit(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let stored = conn
        .fetch_all(
            "SELECT id, host, port, priority, backfill, active FROM servers ORDER BY priority, id",
            &[],
        )
        .await?
        .iter()
        .map(|row| {
            Ok(Stored {
                id: u32::try_from(row.i64("id")?)
                    .map_err(|error| StateError::Database(error.to_string()))?,
                host: row.text("host")?,
                port: row.i64("port")?,
                priority: row.i64("priority")?,
                backfill: row.bool("backfill")?,
                active: row.bool("active")?,
            })
        })
        .collect::<Result<Vec<_>, StateError>>()?;
    for server in past_limit(&stored, weaver_nntp::MAX_PROVIDERS) {
        delete_server_rows(conn, server.id).await?;
        tracing::warn!(
            server_id = server.id,
            host = %server.host,
            port = server.port,
            priority = server.priority,
            backfill = server.backfill,
            enabled = server.active,
            "server deleted: the server list held more than {} servers",
            weaver_nntp::MAX_PROVIDERS
        );
    }
    Ok(())
}

// The servers a list in stored order gives up so that `max` remain, first to
// go first.
fn past_limit(stored: &[Stored], max: usize) -> Vec<&Stored> {
    let excess = stored.len().saturating_sub(max);
    if excess == 0 {
        return Vec::new();
    }
    let top = stored
        .iter()
        .filter(|server| !server.backfill)
        .map(|server| server.priority)
        .min();
    let (kept, mut gone): (Vec<&Stored>, Vec<&Stored>) = stored
        .iter()
        .partition(|server| server.backfill || Some(server.priority) == top);
    gone.sort_by_key(|server| (server.active, Reverse(server.priority), Reverse(server.id)));
    gone.truncate(excess);
    let still_over = excess - gone.len();
    gone.extend(&kept[kept.len() - still_over..]);
    gone
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::Database;
    use crate::persistence::sql_runtime::SqlRuntime;
    use crate::servers::ServerConfig;

    fn placed(id: u32, priority: u32, backfill: bool, active: bool) -> ServerConfig {
        serde_json::from_value(serde_json::json!({
            "id": id, "host": format!("news-{id}.example.test"), "port": 119, "tls": false,
            "connections": 1, "active": active, "priority": priority, "backfill": backfill
        }))
        .unwrap()
    }

    fn stored(servers: &[ServerConfig]) -> Database {
        let db = Database::open_in_memory().unwrap();
        for server in servers {
            db.insert_server_past_limit(server).unwrap();
        }
        db
    }

    fn trim(db: &Database) {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "trim_servers_to_limit_test", |tx| {
                Box::pin(async move { trim_servers_to_limit(&mut tx.conn()).await })
            })
            .await
        })
        .unwrap();
    }

    // The real catalog, replayed to 91 with 53 servers stored and upgraded
    // to 92: the manifest entry reaches the step, and the step deletes on
    // the migration's own connection, a deleted server's route with it.
    // Server 1 is the primary; the others are enabled, on level 1.
    #[tokio::test]
    async fn the_upgrade_from_91_trims_a_stored_list_to_the_limit() {
        upgrade_trims_from(SCHEMA_VERSION - 1).await;
    }

    // The same from 90: the step runs after migration 91 in one upgrade.
    #[tokio::test]
    async fn the_upgrade_from_90_trims_a_stored_list_to_the_limit() {
        upgrade_trims_from(SCHEMA_VERSION - 2).await;
    }

    async fn upgrade_trims_from(from: i64) {
        use super::super::{
            apply_version_range, embedded_catalog, embedded_payload_bytes,
            replay_catalog_into_fresh_db,
        };
        use crate::migration_assets::MigrationInstallKind;

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let catalog = embedded_catalog().unwrap();
        let payload = embedded_payload_bytes().unwrap();
        replay_catalog_into_fresh_db(&pool, &catalog, &payload, Some(from), true)
            .await
            .unwrap();
        for id in 1..=53i64 {
            sqlx::query(
                "INSERT INTO servers (id, host, port, tls, connections, active, priority, backfill)
                 VALUES (?, ?, 119, 0, 1, 1, ?, 0)",
            )
            .bind(id)
            .bind(format!("news-{id}.example.test"))
            .bind(i64::from(id != 1))
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO proxy_routes (consumer, policy) VALUES (?, '{}')")
                .bind(format!("server:{id}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        apply_version_range(
            &pool,
            &catalog,
            &payload,
            MigrationInstallKind::Upgrade,
            from + 1,
            SCHEMA_VERSION,
        )
        .await
        .unwrap();

        let kept: Vec<i64> = sqlx::query_scalar("SELECT id FROM servers ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(kept, (1..=50).collect::<Vec<_>>());
        let routes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_routes")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(routes, 50);
    }

    fn ids(db: &Database) -> BTreeSet<u32> {
        db.list_servers()
            .unwrap()
            .into_iter()
            .map(|server| server.id)
            .collect()
    }

    fn rule_for(id: u32) -> crate::bandwidth::ScheduleEntry {
        crate::bandwidth::ScheduleEntry {
            id: format!("server-{id}-off"),
            enabled: true,
            label: String::new(),
            days: vec![],
            time: "08:00".into(),
            times: vec![],
            every_hour_at_minute: None,
            action: crate::bandwidth::ScheduleAction::SetServerActive {
                server_id: id,
                active: false,
            },
        }
    }

    // 55 servers: two primaries on level 0, three backfill-only servers on
    // the lowest level, and 50 others on levels 1 to 5, the disabled ones on
    // the levels that outrank the enabled ones. Five go: the disabled others,
    // lowest priority first, though enabled others sit below them. Every
    // primary and backfill-only server stays, and nothing keyed by a deleted
    // server is left.
    #[test]
    fn disabled_servers_go_before_lower_priority_ones_and_leave_no_rows() {
        let mut servers = vec![placed(1, 0, false, true), placed(2, 0, false, false)];
        servers.extend((3..=5).map(|id| placed(id, 9, true, id != 4)));
        // Disabled: 6 and 7 on level 1, 8 on level 2. Enabled: 9 to 55, on
        // levels 3 to 5.
        servers.extend((6..=8).map(|id| placed(id, if id == 8 { 2 } else { 1 }, false, false)));
        servers.extend((9..=55).map(|id| placed(id, 3 + id % 3, false, true)));
        let db = stored(&servers);
        let at = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for id in 1..=55u32 {
            db.upsert_server_download_usage(&crate::servers::ServerDownloadUsage {
                server_id: id,
                lifetime_bytes: 1,
                quota_baseline_bytes: 0,
                window_start: None,
                window_end: None,
                updated_at: at,
            })
            .unwrap();
            db.upsert_server_tls_diagnostics(&crate::servers::ServerTlsDiagnostics {
                server_id: id,
                cipher_suite: "TLS_AES_128_GCM_SHA256".into(),
                honors_client_cipher_order: None,
                probed_at: at,
            })
            .unwrap();
            db.save_proxy_routing_policy(
                crate::proxies::Consumer::Server(id),
                &crate::proxies::RoutingPolicy::default(),
            )
            .unwrap();
        }
        db.save_schedules(&(1..=55).map(rule_for).collect::<Vec<_>>())
            .unwrap();

        trim(&db);

        // The three disabled others, then the two newest enabled ones of the
        // lowest level (5: ids of the form 3n + 2).
        let gone = BTreeSet::from([6, 7, 8, 53, 50]);
        let kept: BTreeSet<u32> = (1..=55).filter(|id| !gone.contains(id)).collect();
        assert_eq!(ids(&db), kept);
        let usage: BTreeSet<u32> = db
            .list_server_download_usage()
            .unwrap()
            .into_iter()
            .map(|usage| usage.server_id)
            .collect();
        assert_eq!(usage, kept);
        let tls: BTreeSet<u32> = db
            .list_server_tls_diagnostics()
            .unwrap()
            .into_iter()
            .map(|diagnostics| diagnostics.server_id)
            .collect();
        assert_eq!(tls, kept);
        let routes: BTreeSet<String> = db
            .list_proxy_routes()
            .unwrap()
            .into_iter()
            .map(|(consumer, _)| consumer)
            .collect();
        assert_eq!(
            routes,
            kept.iter()
                .map(|id| crate::proxies::Consumer::Server(*id).key())
                .collect()
        );
        let ruled: BTreeSet<u32> = db
            .list_schedules()
            .unwrap()
            .into_iter()
            .filter_map(|entry| match entry.action {
                crate::bandwidth::ScheduleAction::SetServerActive { server_id, .. } => {
                    Some(server_id)
                }
                _ => None,
            })
            .collect();
        assert_eq!(ruled, kept);
    }

    // 30 primaries and 23 backfill-only servers, and four others: the four
    // go, then the last three of the stored order (priority, then id), which
    // here are the three newest backfill-only servers of the lowest level.
    #[test]
    fn kept_servers_past_the_limit_keep_the_first_fifty_in_stored_order() {
        let mut servers: Vec<ServerConfig> =
            (1..=30).map(|id| placed(id, 0, false, true)).collect();
        servers.extend((31..=53).map(|id| placed(id, if id <= 45 { 1 } else { 2 }, true, true)));
        servers.extend((54..=57).map(|id| placed(id, 5, false, id.is_multiple_of(2))));
        let db = stored(&servers);

        trim(&db);

        assert_eq!(ids(&db), (1..=50).collect());
    }

    // A list of 50 or fewer is left as it is and nothing is written, and a
    // list already trimmed is not trimmed again.
    #[test]
    fn a_list_within_the_limit_writes_nothing() {
        for count in [3u32, 50] {
            let servers: Vec<ServerConfig> = (1..=count)
                .map(|id| placed(id, id % 10, id % 7 == 0, id % 2 == 0))
                .collect();
            let db = stored(&servers);
            db.save_schedules(&[rule_for(1)]).unwrap();
            let before = db.list_setting_records().unwrap().len();

            trim(&db);
            trim(&db);

            assert_eq!(ids(&db), (1..=count).collect());
            assert_eq!(db.list_schedules().unwrap(), vec![rule_for(1)]);
            assert_eq!(db.list_setting_records().unwrap().len(), before);
        }
    }
}
