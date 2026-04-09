use crate::StateError;
use crate::persistence::Database;
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let conn = self.conn();
        let record = ServerRecord::from_config(server);
        let encrypted_password = maybe_encrypt(self.encryption_key(), &record.password);
        conn.execute(
            "INSERT INTO servers (id, host, port, tls, username, password, connections, active, supports_pipelining, priority, tls_ca_cert)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                record.id,
                record.host,
                record.port,
                record.tls,
                record.username,
                encrypted_password,
                record.connections,
                record.active,
                record.supports_pipelining,
                record.priority,
                record.tls_ca_cert,
            ],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn update_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let conn = self.conn();
        let record = ServerRecord::from_config(server);
        let encrypted_password = maybe_encrypt(self.encryption_key(), &record.password);
        conn.execute(
            "UPDATE servers SET host=?2, port=?3, tls=?4, username=?5, password=?6,
             connections=?7, active=?8, supports_pipelining=?9, priority=?10, tls_ca_cert=?11
             WHERE id=?1",
            rusqlite::params![
                record.id,
                record.host,
                record.port,
                record.tls,
                record.username,
                encrypted_password,
                record.connections,
                record.active,
                record.supports_pipelining,
                record.priority,
                record.tls_ca_cert,
            ],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn delete_server(&self, id: u32) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM servers WHERE id = ?1", [id])
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(changed > 0)
    }
}
