// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        self.insert_server_with_routing(server, None)
    }

    pub fn insert_server_with_routing(
        &self,
        server: &ServerConfig,
        routing: Option<&crate::proxies::RoutingPolicy>,
    ) -> Result<(), StateError> {
        self.insert_server_row(server, routing, true)
    }

    // A stored list past the limit, as an older version could leave one, so
    // the trim on load can be tested.
    #[cfg(test)]
    pub(crate) fn insert_server_past_limit(&self, server: &ServerConfig) -> Result<(), StateError> {
        self.insert_server_row(server, None, false)
    }

    fn insert_server_row(
        &self,
        server: &ServerConfig,
        routing: Option<&crate::proxies::RoutingPolicy>,
        within_limit: bool,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &record.password)
            .map_err(StateError::Database)?;
        let args = server_args(record, encrypted_password)?;
        let routing = routing.cloned();
        let consumer = crate::proxies::Consumer::Server(server.id);
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_consumer_routing", |tx| {
                let args = args.clone(); let routing = routing.clone();
                Box::pin(async move {
            if within_limit {
                refuse_past_server_limit(tx).await?;
            }
            tx.execute(
                "INSERT INTO servers
                    (id, host, port, tls, username, password, connections, active, supports_pipelining, pipelining_depth, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert, tls_name_mismatch_certificate_der)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            crate::proxies::persistence::write_routing(tx, consumer, routing.as_ref()).await?;
            Ok(())
                })
            }).await
        })
    }

    pub fn update_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        self.update_server_with_routing(server, None)
    }

    pub fn update_server_with_routing(
        &self,
        server: &ServerConfig,
        routing: Option<&crate::proxies::RoutingPolicy>,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &record.password)
            .map_err(StateError::Database)?;
        let mut args = server_args(record, encrypted_password)?;
        let id = args.remove(0);
        args.push(id);
        let routing = routing.cloned();
        let consumer = crate::proxies::Consumer::Server(server.id);
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_consumer_routing", |tx| {
                let args = args.clone();
                let routing = routing.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE servers
                    SET host = {}, port = {}, tls = {}, username = {}, password = {},
                        connections = {}, active = {}, supports_pipelining = {},
                        pipelining_depth = {}, priority = {},
                        backfill = {}, retention_days = {}, max_download_speed = {},
                        download_quota_enabled = {}, download_quota_limit_bytes = {},
                        download_quota_period = {}, download_quota_reset_time_minutes_local = {},
                        download_quota_weekly_reset_weekday = {},
                        download_quota_monthly_reset_day = {}, tls_ca_cert = {},
                        tls_name_mismatch_certificate_der = {}
                  WHERE id = {}",
                        &args,
                    )
                    .await?;
                    crate::proxies::persistence::write_routing(tx, consumer, routing.as_ref())
                        .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // Write through one proven BODY pipelining depth. Narrow on purpose: the
    // download runtime learns this while a user may be editing the same row,
    // so it must not carry the rest of the record with it.
    pub fn update_server_pipelining_depth(
        &self,
        id: u32,
        depth: Option<u8>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::OptI64(depth.map(i64::from)),
            SqlArg::I64(i64::from(id)),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE servers SET pipelining_depth = {} WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_server(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_routed_consumer", |tx| {
                Box::pin(async move { delete_server_rows(tx, id).await })
            })
            .await
        })
    }

    // Bring a stored server list past `MAX_PROVIDERS` down to it, deleting
    // the servers [`servers_past_limit`] picks through the same rows a user
    // delete removes, all in one transaction. `servers` is the list as just
    // read; returns how many went. A list within the cap writes nothing.
    pub(crate) fn trim_servers_past_limit(
        &self,
        servers: &[ServerConfig],
    ) -> Result<usize, StateError> {
        let doomed = servers_past_limit(servers, weaver_nntp::MAX_PROVIDERS);
        if doomed.is_empty() {
            return Ok(0);
        }
        let ids: Vec<u32> = doomed.iter().map(|server| server.id).collect();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_routed_consumer", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    for id in ids {
                        delete_server_rows(tx, id).await?;
                    }
                    Ok(())
                })
            })
            .await
        })?;
        log_trimmed(&doomed, servers.len());
        Ok(doomed.len())
    }
}

// A whole server list about to be written (an imported settings file, an
// environment seed, a saved configuration), brought within
// `MAX_PROVIDERS` by the same rule the trim on load applies. Returns the
// servers to write and the ids given up, with one warning per server.
pub(crate) fn keep_within_server_limit(servers: &[ServerConfig]) -> (Vec<ServerConfig>, Vec<u32>) {
    let doomed = servers_past_limit(servers, weaver_nntp::MAX_PROVIDERS);
    if doomed.is_empty() {
        return (servers.to_vec(), Vec::new());
    }
    log_trimmed(&doomed, servers.len());
    let gone: std::collections::HashSet<u32> = doomed.iter().map(|server| server.id).collect();
    let kept = servers
        .iter()
        .filter(|server| !gone.contains(&server.id))
        .cloned()
        .collect();
    (kept, gone.into_iter().collect())
}

// One warning per server the limit gave up (never its credentials), and one
// for the list.
fn log_trimmed(doomed: &[&ServerConfig], stored: usize) {
    for server in doomed {
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
    tracing::warn!(
        deleted = doomed.len(),
        stored,
        max = weaver_nntp::MAX_PROVIDERS,
        "server list trimmed to the server limit"
    );
}

// Every row a server delete removes: the server's schedule rules, the server,
// and its proxy route.
pub(crate) async fn delete_server_rows(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    id: u32,
) -> Result<bool, StateError> {
    Database::remove_server_schedules(tx, id).await?;
    let changed = tx
        .execute(
            "DELETE FROM servers WHERE id = {}",
            &[SqlArg::I64(i64::from(id))],
        )
        .await?;
    tx.execute(
        "DELETE FROM proxy_routes WHERE consumer = {}",
        &[SqlArg::Text(crate::proxies::Consumer::Server(id).key())],
    )
    .await?;
    Ok(changed > 0)
}

// The servers a stored list past `max` gives up, first to go first, so that
// `max` remain, or as many as can go.
//
// Primary servers (the fill servers of the top priority level) and
// backfill-only servers are never given up. Of the rest, the lowest priority
// (the highest priority number) goes first; inside one level a disabled
// server goes before an enabled one, then the newest (highest id). When the
// kept servers alone pass `max`, every other server goes and the list stays
// past the limit: the pool then refuses it in words (see
// `runtime::reload::server_list_past_limit`).
pub(crate) fn servers_past_limit(servers: &[ServerConfig], max: usize) -> Vec<&ServerConfig> {
    let excess = servers.len().saturating_sub(max);
    if excess == 0 {
        return Vec::new();
    }
    let top = servers
        .iter()
        .filter(|server| !server.backfill)
        .map(|server| server.priority)
        .min();
    let mut deletable: Vec<&ServerConfig> = servers
        .iter()
        .filter(|server| !server.backfill && Some(server.priority) != top)
        .collect();
    deletable.sort_by_key(|server| {
        (
            std::cmp::Reverse(server.priority),
            server.active,
            std::cmp::Reverse(server.id),
        )
    });
    deletable.truncate(excess);
    deletable
}

// Weaver takes at most `MAX_PROVIDERS` news servers, counted active or not:
// enabling one is an edit, never an insert, so the cap holds there too.
async fn refuse_past_server_limit(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
) -> Result<(), StateError> {
    let count = match tx
        .fetch_optional("SELECT COUNT(*) AS count FROM servers", &[])
        .await?
    {
        Some(row) => row.i64("count")?,
        None => 0,
    };
    let max = weaver_nntp::MAX_PROVIDERS;
    if count >= max as i64 {
        let mut params = serde_json::Map::new();
        params.insert("max".to_string(), serde_json::Value::from(max));
        return Err(StateError::Refused(crate::persistence::Refusal {
            code: "SERVER_LIMIT",
            message: format!("Weaver takes at most {max} servers"),
            params,
        }));
    }
    Ok(())
}

pub(crate) fn server_args(
    record: ServerRecord,
    password: Option<String>,
) -> Result<Vec<SqlArg>, StateError> {
    let max_download_speed = i64::try_from(record.max_download_speed).map_err(|_| {
        StateError::Database("server max download speed exceeds database range".to_string())
    })?;
    let quota_limit_bytes = i64::try_from(record.download_quota_limit_bytes).map_err(|_| {
        StateError::Database("server download quota limit exceeds database range".to_string())
    })?;

    Ok(vec![
        SqlArg::I64(i64::from(record.id)),
        SqlArg::Text(record.host),
        SqlArg::I64(i64::from(record.port)),
        SqlArg::Bool(record.tls),
        SqlArg::OptText(record.username),
        SqlArg::OptText(password),
        SqlArg::I64(i64::from(record.connections)),
        SqlArg::Bool(record.active),
        SqlArg::Bool(record.supports_pipelining),
        SqlArg::OptI64(record.pipelining_depth.map(i64::from)),
        SqlArg::I64(i64::from(record.priority)),
        SqlArg::Bool(record.backfill),
        SqlArg::I64(i64::from(record.retention_days)),
        SqlArg::I64(max_download_speed),
        SqlArg::Bool(record.download_quota_enabled),
        SqlArg::I64(quota_limit_bytes),
        SqlArg::Text(record.download_quota_period.as_str().to_string()),
        SqlArg::I64(i64::from(record.download_quota_reset_time_minutes_local)),
        SqlArg::Text(
            crate::servers::record::quota_weekday_str(record.download_quota_weekly_reset_weekday)
                .to_string(),
        ),
        SqlArg::I64(i64::from(record.download_quota_monthly_reset_day)),
        SqlArg::OptText(record.tls_ca_cert),
        SqlArg::OptBytes(record.tls_name_mismatch_certificate_der),
    ])
}

// Sixty servers over ten levels, for every way a list past the limit can
// arrive: three primaries (level 0), four backfill-only servers (two on the
// lowest level), and 53 others, every second one disabled. The limit keeps
// 50: every primary and backfill-only server stays, level 9's five others
// go, then five of level 8's six, disabled first, newest first.
#[cfg(test)]
pub(crate) mod past_limit_fixture {
    use crate::servers::{ServerConfig, ServerDownloadQuotaConfig};

    pub(crate) const GONE: [u32; 10] = [16, 25, 34, 43, 52, 51, 33, 15, 60, 42];

    pub(crate) fn placed(id: u32, priority: u32, backfill: bool, active: bool) -> ServerConfig {
        ServerConfig {
            id,
            host: format!("news-{id}.example.com"),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 2,
            active,
            supports_pipelining: true,
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority,
            backfill,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig::default(),
            tls_ca_cert: None,
        }
    }

    pub(crate) fn sixty_on_ten_levels() -> Vec<ServerConfig> {
        let mut servers: Vec<ServerConfig> =
            (1..=3).map(|id| placed(id, 0, false, id != 3)).collect();
        for (id, priority) in [(4, 9), (5, 9), (6, 3), (7, 3)] {
            servers.push(placed(id, priority, true, id != 4));
        }
        for id in 8..=60 {
            servers.push(placed(id, 1 + (id - 8) % 9, false, id.is_multiple_of(2)));
        }
        servers
    }

    // The ids of `servers`, as a set.
    pub(crate) fn ids(servers: &[ServerConfig]) -> std::collections::BTreeSet<u32> {
        servers.iter().map(|server| server.id).collect()
    }

    // The ids the limit keeps of [`sixty_on_ten_levels`].
    pub(crate) fn kept() -> std::collections::BTreeSet<u32> {
        (1..=60).filter(|id| !GONE.contains(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    fn server(id: u32) -> crate::servers::ServerConfig {
        serde_json::from_value(serde_json::json!({
            "id": id, "host": format!("news-{id}.example.test"), "port": 119, "tls": false,
            "connections": 1, "active": id.is_multiple_of(2)
        }))
        .unwrap()
    }

    // Fifty servers are taken, active or not; the fifty-first is refused in
    // words, and nothing of it is stored.
    #[test]
    fn a_fifty_first_server_is_refused() {
        let db = crate::Database::open_in_memory().unwrap();
        for id in 1..=50 {
            db.insert_server(&server(id)).unwrap();
        }
        let error = db.insert_server(&server(51)).unwrap_err();
        assert_eq!(
            error.refusal().map(|refusal| refusal.code),
            Some("SERVER_LIMIT")
        );
        assert_eq!(error.to_string(), "Weaver takes at most 50 servers");
        let stored = db.load_config().unwrap().servers;
        assert_eq!(stored.len(), 50);
        assert!(stored.iter().all(|server| server.id != 51));
    }

    fn placed(
        id: u32,
        priority: u32,
        backfill: bool,
        active: bool,
    ) -> crate::servers::ServerConfig {
        crate::servers::ServerConfig {
            priority,
            backfill,
            active,
            ..server(id)
        }
    }

    fn stored_ids(db: &crate::Database) -> std::collections::BTreeSet<u32> {
        db.list_servers()
            .unwrap()
            .into_iter()
            .map(|server| server.id)
            .collect()
    }

    // `past_limit_fixture`'s sixty servers, stored by an older version:
    // loading keeps the 50 the limit keeps.
    #[test]
    fn a_stored_list_past_the_limit_is_trimmed_to_it_on_load() {
        let db = crate::Database::open_in_memory().unwrap();
        for server in super::past_limit_fixture::sixty_on_ten_levels() {
            db.insert_server_past_limit(&server).unwrap();
        }
        assert_eq!(stored_ids(&db).len(), 60);

        let config = db.load_config().unwrap();

        let gone: std::collections::BTreeSet<u32> = (1..=60)
            .filter(|id| !stored_ids(&db).contains(id))
            .collect();
        let expected: std::collections::BTreeSet<u32> = super::past_limit_fixture::GONE.into();
        assert_eq!(gone, expected);
        assert_eq!(config.servers.len(), 50);
        assert!(
            config
                .servers
                .iter()
                .all(|server| !gone.contains(&server.id))
        );
        // Loading again finds nothing past the limit and changes nothing.
        assert_eq!(db.trim_servers_past_limit(&config.servers).unwrap(), 0);
        assert_eq!(stored_ids(&db).len(), 50);
    }

    // A list within the limit is left as it is.
    #[test]
    fn a_stored_list_within_the_limit_is_untouched() {
        for count in [3u32, 50] {
            let db = crate::Database::open_in_memory().unwrap();
            for id in 1..=count {
                db.insert_server(&placed(id, id % 10, id % 7 == 0, id % 2 == 0))
                    .unwrap();
            }
            let before = db.list_servers().unwrap();
            assert!(super::servers_past_limit(&before, weaver_nntp::MAX_PROVIDERS).is_empty());
            assert_eq!(db.trim_servers_past_limit(&before).unwrap(), 0);
            let config = db.load_config().unwrap();
            assert_eq!(config.servers.len(), count as usize);
            assert_eq!(stored_ids(&db), (1..=count).collect());
        }
    }

    // Thirty primaries, 25 backfill-only servers and two others: the two
    // others go, and nothing more. The 55 kept are past the limit, so the
    // pool for them is refused in words, never built and never a panic.
    #[test]
    fn kept_servers_alone_past_the_limit_are_left_and_refused_by_the_pool() {
        let db = crate::Database::open_in_memory().unwrap();
        for id in 1..=30 {
            db.insert_server_past_limit(&placed(id, 0, false, true))
                .unwrap();
        }
        for id in 31..=55 {
            let priority = if id <= 45 { 1 } else { 2 };
            db.insert_server_past_limit(&placed(id, priority, true, id.is_multiple_of(2)))
                .unwrap();
        }
        for id in 56..=57 {
            db.insert_server_past_limit(&placed(id, 5, false, true))
                .unwrap();
        }
        let config = db.load_config().unwrap();
        assert_eq!(stored_ids(&db), (1..=55).collect());
        assert_eq!(config.servers.len(), 55);
        assert_eq!(
            crate::runtime::reload::server_list_past_limit(&config.servers).as_deref(),
            Some(
                "55 servers are stored and Weaver takes at most 50; primary and \
                 backfill-only servers are never deleted for you, so delete 5 of them"
            )
        );
        // Loading again deletes nothing more.
        assert_eq!(db.trim_servers_past_limit(&config.servers).unwrap(), 0);
        assert_eq!(stored_ids(&db).len(), 55);
    }

    // A whole list written past the limit (a saved configuration, which a
    // settings-file import is) is written trimmed by the same rule. A server
    // already stored that the trim gives up loses its schedule rules too, as
    // a user delete removes them.
    #[test]
    fn a_whole_list_written_past_the_limit_is_trimmed_by_the_same_rule() {
        use super::past_limit_fixture::{kept, sixty_on_ten_levels};
        let db = crate::Database::open_in_memory().unwrap();
        db.insert_server(&placed(60, 9, false, false)).unwrap();
        db.save_schedules(&[crate::bandwidth::ScheduleEntry {
            id: "server-60-off".into(),
            enabled: true,
            label: String::new(),
            days: vec![],
            time: "08:00".into(),
            times: vec![],
            every_hour_at_minute: None,
            action: crate::bandwidth::ScheduleAction::SetServerActive {
                server_id: 60,
                active: false,
            },
        }])
        .unwrap();

        let mut config = db.load_config().unwrap();
        config.servers = sixty_on_ten_levels();
        db.save_config(&config).unwrap();

        assert_eq!(stored_ids(&db), kept());
        assert!(db.list_schedules().unwrap().is_empty());
        let written = db.replace_servers(&sixty_on_ten_levels()).unwrap();
        assert_eq!(super::past_limit_fixture::ids(&written), kept());
        assert_eq!(stored_ids(&db), kept());
    }
}
