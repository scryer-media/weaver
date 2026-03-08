use weaver_core::config::{
    BufferPoolOverrides, Config, RetryOverrides, ServerConfig, TunerOverrides,
};

use crate::StateError;

use super::Database;

impl Database {
    // ── Settings ──────────────────────────────────────────────────────

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached("SELECT value FROM settings WHERE key = ?1")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let result = stmt
            .query_row([key], |row| row.get(0))
            .ok();
        Ok(result)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![key, value],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    /// Load a full `Config` from the settings and servers tables.
    pub fn load_config(&self) -> Result<Config, StateError> {
        let conn = self.conn();

        // Load all settings into a map.
        let mut stmt = conn
            .prepare_cached("SELECT key, value FROM settings")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let settings: std::collections::HashMap<String, String> = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .map_err(|e| StateError::Database(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        // Load servers.
        drop(stmt);
        drop(conn);
        let servers = self.list_servers()?;

        let data_dir = settings
            .get("data_dir")
            .cloned()
            .unwrap_or_default();
        // Backward compat: if old "output_dir" exists but "intermediate_dir" doesn't, use it.
        let intermediate_dir = settings
            .get("intermediate_dir")
            .cloned()
            .or_else(|| settings.get("output_dir").cloned());
        let complete_dir = settings.get("complete_dir").cloned();
        let max_download_speed = settings
            .get("max_download_speed")
            .and_then(|v| v.parse().ok());
        let cleanup_after_extract = settings
            .get("cleanup_after_extract")
            .and_then(|v| v.parse().ok());

        let buffer_pool = {
            let small = settings.get("buffer_pool.small_count").and_then(|v| v.parse().ok());
            let medium = settings.get("buffer_pool.medium_count").and_then(|v| v.parse().ok());
            let large = settings.get("buffer_pool.large_count").and_then(|v| v.parse().ok());
            if small.is_some() || medium.is_some() || large.is_some() {
                Some(BufferPoolOverrides {
                    small_count: small,
                    medium_count: medium,
                    large_count: large,
                })
            } else {
                None
            }
        };

        let tuner = {
            let max_dl = settings.get("tuner.max_concurrent_downloads").and_then(|v| v.parse().ok());
            let max_dq = settings.get("tuner.max_decode_queue").and_then(|v| v.parse().ok());
            let decode_threads = settings.get("tuner.decode_thread_count").and_then(|v| v.parse().ok());
            if max_dl.is_some() || max_dq.is_some() || decode_threads.is_some() {
                Some(TunerOverrides {
                    max_concurrent_downloads: max_dl,
                    max_decode_queue: max_dq,
                    decode_thread_count: decode_threads,
                })
            } else {
                None
            }
        };

        let retry = {
            let max_retries = settings.get("retry.max_retries").and_then(|v| v.parse().ok());
            let base_delay = settings.get("retry.base_delay_secs").and_then(|v| v.parse().ok());
            let multiplier = settings.get("retry.multiplier").and_then(|v| v.parse().ok());
            if max_retries.is_some() || base_delay.is_some() || multiplier.is_some() {
                Some(RetryOverrides {
                    max_retries,
                    base_delay_secs: base_delay,
                    multiplier,
                })
            } else {
                None
            }
        };

        Ok(Config {
            data_dir,
            intermediate_dir,
            complete_dir,
            buffer_pool,
            tuner,
            servers,
            retry,
            max_download_speed,
            cleanup_after_extract,
            config_path: None,
        })
    }

    /// Save a full `Config` to the settings and servers tables.
    pub fn save_config(&self, config: &Config) -> Result<(), StateError> {
        self.set_setting("data_dir", &config.data_dir)?;
        if let Some(ref intermediate_dir) = config.intermediate_dir {
            self.set_setting("intermediate_dir", intermediate_dir)?;
        }
        if let Some(ref complete_dir) = config.complete_dir {
            self.set_setting("complete_dir", complete_dir)?;
        }
        if let Some(speed) = config.max_download_speed {
            self.set_setting("max_download_speed", &speed.to_string())?;
        }
        if let Some(cleanup) = config.cleanup_after_extract {
            self.set_setting("cleanup_after_extract", &cleanup.to_string())?;
        }

        if let Some(ref bp) = config.buffer_pool {
            if let Some(v) = bp.small_count {
                self.set_setting("buffer_pool.small_count", &v.to_string())?;
            }
            if let Some(v) = bp.medium_count {
                self.set_setting("buffer_pool.medium_count", &v.to_string())?;
            }
            if let Some(v) = bp.large_count {
                self.set_setting("buffer_pool.large_count", &v.to_string())?;
            }
        }

        if let Some(ref t) = config.tuner {
            if let Some(v) = t.max_concurrent_downloads {
                self.set_setting("tuner.max_concurrent_downloads", &v.to_string())?;
            }
            if let Some(v) = t.max_decode_queue {
                self.set_setting("tuner.max_decode_queue", &v.to_string())?;
            }
            if let Some(v) = t.decode_thread_count {
                self.set_setting("tuner.decode_thread_count", &v.to_string())?;
            }
        }

        if let Some(ref r) = config.retry {
            if let Some(v) = r.max_retries {
                self.set_setting("retry.max_retries", &v.to_string())?;
            }
            if let Some(v) = r.base_delay_secs {
                self.set_setting("retry.base_delay_secs", &v.to_string())?;
            }
            if let Some(v) = r.multiplier {
                self.set_setting("retry.multiplier", &v.to_string())?;
            }
        }

        // Sync servers: delete all, re-insert.
        {
            let conn = self.conn();
            conn.execute("DELETE FROM servers", [])
                .map_err(|e| StateError::Database(e.to_string()))?;
        }
        for server in &config.servers {
            self.insert_server(server)?;
        }

        Ok(())
    }

    // ── Servers ───────────────────────────────────────────────────────

    pub fn list_servers(&self) -> Result<Vec<ServerConfig>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, host, port, tls, username, password, connections, active, supports_pipelining
                 FROM servers ORDER BY id",
            )
            .map_err(|e| StateError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(ServerConfig {
                    id: row.get::<_, u32>(0)?,
                    host: row.get(1)?,
                    port: row.get(2)?,
                    tls: row.get::<_, bool>(3)?,
                    username: row.get(4)?,
                    password: row.get(5)?,
                    connections: row.get(6)?,
                    active: row.get::<_, bool>(7)?,
                    supports_pipelining: row.get::<_, bool>(8)?,
                })
            })
            .map_err(|e| StateError::Database(e.to_string()))?;

        let mut servers = Vec::new();
        for row in rows {
            servers.push(row.map_err(|e| StateError::Database(e.to_string()))?);
        }
        Ok(servers)
    }

    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO servers (id, host, port, tls, username, password, connections, active, supports_pipelining)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                server.id,
                server.host,
                server.port,
                server.tls,
                server.username,
                server.password,
                server.connections,
                server.active,
                server.supports_pipelining,
            ],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn update_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE servers SET host=?2, port=?3, tls=?4, username=?5, password=?6,
             connections=?7, active=?8, supports_pipelining=?9
             WHERE id=?1",
            rusqlite::params![
                server.id,
                server.host,
                server.port,
                server.tls,
                server.username,
                server.password,
                server.connections,
                server.active,
                server.supports_pipelining,
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

    pub fn next_server_id(&self) -> Result<u32, StateError> {
        let conn = self.conn();
        let max: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM servers", [], |row| row.get(0))
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(max.unwrap_or(0) + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting("foo", "bar").unwrap();
        assert_eq!(db.get_setting("foo").unwrap(), Some("bar".to_string()));
        assert_eq!(db.get_setting("missing").unwrap(), None);
    }

    #[test]
    fn settings_upsert() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting("key", "v1").unwrap();
        db.set_setting("key", "v2").unwrap();
        assert_eq!(db.get_setting("key").unwrap(), Some("v2".to_string()));
    }

    #[test]
    fn server_crud() {
        let db = Database::open_in_memory().unwrap();

        let server = ServerConfig {
            id: 1,
            host: "news.example.com".to_string(),
            port: 443,
            tls: true,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            connections: 10,
            active: true,
            supports_pipelining: false,
        };

        db.insert_server(&server).unwrap();
        let servers = db.list_servers().unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].host, "news.example.com");
        assert_eq!(servers[0].port, 443);

        let mut updated = server.clone();
        updated.connections = 20;
        db.update_server(&updated).unwrap();
        let servers = db.list_servers().unwrap();
        assert_eq!(servers[0].connections, 20);

        assert!(db.delete_server(1).unwrap());
        assert!(!db.delete_server(1).unwrap());
        assert!(db.list_servers().unwrap().is_empty());
    }

    #[test]
    fn next_server_id_empty() {
        let db = Database::open_in_memory().unwrap();
        assert_eq!(db.next_server_id().unwrap(), 1);
    }

    #[test]
    fn config_roundtrip() {
        let db = Database::open_in_memory().unwrap();

        let config = Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: Some("/tmp/intermediate".to_string()),
            complete_dir: Some("/tmp/complete".to_string()),
            buffer_pool: None,
            tuner: None,
            servers: vec![ServerConfig {
                id: 1,
                host: "news.test.com".to_string(),
                port: 563,
                tls: true,
                username: None,
                password: None,
                connections: 5,
                active: true,
                supports_pipelining: true,
            }],
            retry: None,
            max_download_speed: Some(1_000_000),
            cleanup_after_extract: Some(false),
            config_path: None,
        };

        db.save_config(&config).unwrap();
        let loaded = db.load_config().unwrap();

        assert_eq!(loaded.data_dir, "/tmp/weaver");
        assert_eq!(loaded.intermediate_dir, Some("/tmp/intermediate".to_string()));
        assert_eq!(loaded.complete_dir, Some("/tmp/complete".to_string()));
        assert_eq!(loaded.max_download_speed, Some(1_000_000));
        assert_eq!(loaded.cleanup_after_extract, Some(false));
        assert_eq!(loaded.servers.len(), 1);
        assert_eq!(loaded.servers[0].host, "news.test.com");
        assert!(loaded.servers[0].supports_pipelining);
    }
}
