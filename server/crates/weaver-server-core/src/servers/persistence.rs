// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use chrono::{DateTime, Local, Utc};

use crate::StateError;
use crate::operations::REMOVED_SERVER_ID_PREFIX;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlResult, SqlRuntime, SqlTx};
use crate::servers::{
    ServerConfig, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod, record::ServerRecord,
};

impl Database {
    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        self.insert_server_with_routing(server, None)
    }

    pub fn insert_server_with_routing(
        &self,
        server: &ServerConfig,
        routing: Option<&crate::proxies::RoutingPolicy>,
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
            let before = crate::proxies::lock_network_asks(tx).await?;
            tx.execute(
                "INSERT INTO servers
                    (id, host, port, tls, username, password, connections, active, supports_pipelining, pipelining_depth, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert, tls_name_mismatch_certificate_der)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            crate::proxies::persistence::write_routing(tx, consumer, routing.as_ref(), &before)
                .await?;
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
                    let before = crate::proxies::lock_network_asks(tx).await?;
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
                    crate::proxies::persistence::write_routing(
                        tx,
                        consumer,
                        routing.as_ref(),
                        &before,
                    )
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
        self.delete_server_at(id, Local::now())
            .map(|removal| removal.removed)
    }

    /// Delete a server as of `now`.
    ///
    /// A new server can take this id, so the removed server's data used
    /// never stays under it. A server whose quota resets on a calendar keeps
    /// its rows, moved to a consumer id no live server can have, until that
    /// quota's next reset after `now`; housekeeping then drops them. Any
    /// other server's rows go at once.
    pub fn delete_server_at(
        &self,
        id: u32,
        now: DateTime<Local>,
    ) -> Result<ServerRemoval, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_routed_consumer", |tx| {
                Box::pin(async move {
                    let kept_usage = keep_removed_usage(tx, id, now).await?;
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
                    // A new server can take this id; it must not inherit the
                    // removed server's data used. Kept rows moved above.
                    if kept_usage.is_none() {
                        tx.execute(
                            "DELETE FROM usage_daily WHERE consumer_kind = {} AND consumer_id = {}",
                            &[server_kind(), SqlArg::Text(id.to_string())],
                        )
                        .await?;
                    }
                    Ok(ServerRemoval {
                        removed: changed > 0,
                        kept_usage,
                    })
                })
            })
            .await
        })
    }
}

/// What deleting a server did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRemoval {
    /// Whether a server had the id.
    pub removed: bool,
    /// The server consumer id its data used is kept under until its quota
    /// resets, or `None` when that was deleted with it.
    pub kept_usage: Option<String>,
}

/// When a removed server's data used goes: its quota's next reset after
/// `now`. `None` for no quota, or one that never resets by itself.
pub fn removed_usage_kept_until(
    quota: &ServerDownloadQuotaConfig,
    now: DateTime<Local>,
) -> Option<DateTime<Utc>> {
    if !quota.enabled {
        return None;
    }
    crate::servers::transfer_policy::quota_window(now, quota.into()).map(|window| window.end)
}

fn server_kind() -> SqlArg {
    SqlArg::Text(
        crate::operations::UsageConsumerKind::Server
            .as_str()
            .to_string(),
    )
}

// Move a server's rows under a fresh removed-server consumer id with a
// record of its name and removal time, when its quota has a next reset.
async fn keep_removed_usage(
    tx: &mut SqlTx<'_>,
    id: u32,
    now: DateTime<Local>,
) -> SqlResult<Option<String>> {
    let Some(row) = tx
        .fetch_optional(
            "SELECT host, download_quota_enabled, download_quota_period,
                    download_quota_reset_time_minutes_local,
                    download_quota_weekly_reset_weekday,
                    download_quota_monthly_reset_day
               FROM servers WHERE id = {}",
            &[SqlArg::I64(i64::from(id))],
        )
        .await?
    else {
        return Ok(None);
    };
    // A quota this cannot read has no reset to wait for.
    let period = ServerDownloadQuotaPeriod::parse(&row.text("download_quota_period")?);
    let weekday = crate::servers::record::parse_quota_weekday(
        &row.text("download_quota_weekly_reset_weekday")?,
    );
    let (Some(period), Some(weekly_reset_weekday)) = (period, weekday) else {
        return Ok(None);
    };
    let quota = ServerDownloadQuotaConfig {
        enabled: row.bool("download_quota_enabled")?,
        limit_bytes: 0,
        period,
        reset_time_minutes_local: u16::try_from(
            row.i64("download_quota_reset_time_minutes_local")?,
        )
        .unwrap_or(0),
        weekly_reset_weekday,
        monthly_reset_day: u8::try_from(row.i64("download_quota_monthly_reset_day")?).unwrap_or(1),
    };
    let Some(until) = removed_usage_kept_until(&quota, now) else {
        return Ok(None);
    };

    let base = format!("{REMOVED_SERVER_ID_PREFIX}{id}-{}", now.timestamp());
    let mut consumer_id = base.clone();
    let mut attempt = 1u32;
    while tx
        .fetch_optional(
            "SELECT consumer_id FROM usage_removed_servers WHERE consumer_id = {}",
            &[SqlArg::Text(consumer_id.clone())],
        )
        .await?
        .is_some()
    {
        attempt += 1;
        consumer_id = format!("{base}-{attempt}");
    }
    tx.execute(
        "INSERT INTO usage_removed_servers (consumer_id, server_id, host, remove_at)
         VALUES ({}, {}, {}, {})",
        &[
            SqlArg::Text(consumer_id.clone()),
            SqlArg::I64(i64::from(id)),
            SqlArg::Text(row.text("host")?),
            SqlArg::I64(until.timestamp()),
        ],
    )
    .await?;
    tx.execute(
        "UPDATE usage_daily SET consumer_id = {} WHERE consumer_kind = {} AND consumer_id = {}",
        &[
            SqlArg::Text(consumer_id.clone()),
            server_kind(),
            SqlArg::Text(id.to_string()),
        ],
    )
    .await?;
    Ok(Some(consumer_id))
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
