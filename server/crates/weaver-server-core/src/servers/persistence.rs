use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &record.password)
            .map_err(StateError::Database)?;
        let args = server_args(record, encrypted_password)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO servers
                    (id, host, port, tls, username, password, connections, active, supports_pipelining, priority, backfill, retention_days, max_download_speed, download_quota_enabled, download_quota_limit_bytes, download_quota_period, download_quota_reset_time_minutes_local, download_quota_weekly_reset_weekday, download_quota_monthly_reset_day, tls_ca_cert)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &record.password)
            .map_err(StateError::Database)?;
        let mut args = server_args(record, encrypted_password)?;
        let id = args.remove(0);
        args.push(id);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE servers
                    SET host = {}, port = {}, tls = {}, username = {}, password = {},
                        connections = {}, active = {}, supports_pipelining = {}, priority = {},
                        backfill = {}, retention_days = {}, max_download_speed = {},
                        download_quota_enabled = {}, download_quota_limit_bytes = {},
                        download_quota_period = {}, download_quota_reset_time_minutes_local = {},
                        download_quota_weekly_reset_weekday = {},
                        download_quota_monthly_reset_day = {}, tls_ca_cert = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_server(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM servers WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
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
    ])
}
