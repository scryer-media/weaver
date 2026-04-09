use crate::StateError;
use crate::persistence::Database;
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn list_servers(&self) -> Result<Vec<ServerConfig>, StateError> {
        use crate::persistence::encryption::maybe_decrypt;

        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, host, port, tls, username, password, connections, active, supports_pipelining, priority, tls_ca_cert
                 FROM servers ORDER BY priority, id",
            )
            .map_err(|e| StateError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(ServerRecord {
                    id: row.get::<_, u32>(0)?,
                    host: row.get(1)?,
                    port: row.get(2)?,
                    tls: row.get::<_, bool>(3)?,
                    username: row.get(4)?,
                    password: row.get(5)?,
                    connections: row.get(6)?,
                    active: row.get::<_, bool>(7)?,
                    supports_pipelining: row.get::<_, bool>(8)?,
                    priority: row.get::<_, u32>(9)?,
                    tls_ca_cert: row.get(10)?,
                })
            })
            .map_err(|e| StateError::Database(e.to_string()))?;

        let mut servers = Vec::new();
        for row in rows {
            let mut record = row.map_err(|e| StateError::Database(e.to_string()))?;
            record.password = maybe_decrypt(self.encryption_key(), record.password);
            servers.push(record.into_config());
        }
        Ok(servers)
    }

    pub fn next_server_id(&self) -> Result<u32, StateError> {
        let conn = self.conn();
        let max: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM servers", [], |row| row.get(0))
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(max.unwrap_or(0) + 1)
    }
}
