use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn list_servers(&self) -> Result<Vec<ServerConfig>, StateError> {
        use crate::persistence::encryption::maybe_decrypt;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, host, port, tls, username, password, connections, active, supports_pipelining, priority, tls_ca_cert
                   FROM servers ORDER BY priority, id",
                &[],
            )
            .await?;

            rows.into_iter()
                .map(|row| {
                    let mut record = ServerRecord {
                        id: row.i32("id")? as u32,
                        host: row.text("host")?,
                        port: row.i32("port")? as u16,
                        tls: row.bool("tls")?,
                        username: row.opt_text("username")?,
                        password: row.opt_text("password")?,
                        connections: row.i32("connections")? as u16,
                        active: row.bool("active")?,
                        supports_pipelining: row.bool("supports_pipelining")?,
                        priority: row.i32("priority")? as u32,
                        tls_ca_cert: row.opt_text("tls_ca_cert")?,
                    };
                    record.password = maybe_decrypt(encryption_key.as_ref(), record.password);
                    Ok(record.into_config())
                })
                .collect()
        })
    }

    pub fn next_server_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM servers",
                &[],
            )
            .await?;
            let max = row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0);
            Ok(max as u32 + 1)
        })
    }
}
