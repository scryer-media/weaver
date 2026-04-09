use crate::StateError;
use crate::persistence::Database;
use crate::servers::ServerConfig;

impl Database {
    pub(crate) fn replace_servers(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        {
            let conn = self.conn();
            conn.execute("DELETE FROM servers", [])
                .map_err(|e| StateError::Database(e.to_string()))?;
        }
        for server in servers {
            self.insert_server(server)?;
        }
        Ok(())
    }
}
