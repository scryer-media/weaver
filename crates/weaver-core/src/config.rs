use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Shared config handle for runtime reads and writes.
pub type SharedConfig = Arc<RwLock<Config>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    /// Directory for active downloads (per-job subdirectories).
    /// Defaults to `{data_dir}/intermediate`.
    pub intermediate_dir: Option<String>,
    /// Directory for completed downloads (category subdirectories).
    /// Defaults to `{data_dir}/complete`.
    pub complete_dir: Option<String>,
    #[serde(default)]
    pub buffer_pool: Option<BufferPoolOverrides>,
    #[serde(default)]
    pub tuner: Option<TunerOverrides>,
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    #[serde(default)]
    pub retry: Option<RetryOverrides>,
    /// Maximum download speed in bytes/sec. 0 or absent means unlimited.
    #[serde(default)]
    pub max_download_speed: Option<u64>,
    /// Whether to delete intermediate files (NZB articles, PAR2, RAR volumes)
    /// after successful extraction. Defaults to true.
    #[serde(default)]
    pub cleanup_after_extract: Option<bool>,

    /// Path to the config file on disk. Not serialized to TOML.
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

impl Config {
    /// Returns the intermediate directory for active downloads.
    /// Defaults to `{data_dir}/intermediate`.
    pub fn intermediate_dir(&self) -> String {
        self.intermediate_dir
            .clone()
            .unwrap_or_else(|| format!("{}/intermediate", self.data_dir))
    }

    /// Returns the complete directory for finished downloads.
    /// Defaults to `{data_dir}/complete`.
    pub fn complete_dir(&self) -> String {
        self.complete_dir
            .clone()
            .unwrap_or_else(|| format!("{}/complete", self.data_dir))
    }

    /// Whether to clean up intermediate files after successful extraction.
    /// Defaults to `true` when not explicitly configured.
    pub fn cleanup_after_extract(&self) -> bool {
        self.cleanup_after_extract.unwrap_or(true)
    }

    /// Validate the configuration, returning any issues found.
    /// Empty server list is allowed (users add servers via UI).
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for (i, server) in self.servers.iter().enumerate() {
            if server.port == 0 {
                errors.push(format!("server[{i}] ({}) port must be > 0", server.host));
            }
            if server.connections == 0 {
                errors.push(format!(
                    "server[{i}] ({}) connections must be > 0",
                    server.host
                ));
            }
        }

        if self.data_dir.is_empty() {
            errors.push("data_dir must not be empty".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Save the config back to disk.
    pub fn save(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| std::io::Error::other("no config path set"))?;
        let toml_str = toml::to_string_pretty(self)
            .map_err(std::io::Error::other)?;
        std::fs::write(path, toml_str)
    }

    /// Return the next available server ID.
    pub fn next_server_id(&self) -> u32 {
        self.servers.iter().map(|s| s.id).max().unwrap_or(0) + 1
    }

    /// Assign IDs to any servers that have id == 0 (backward compat with old TOML files).
    pub fn assign_server_ids(&mut self) {
        let mut next = self.next_server_id();
        for server in &mut self.servers {
            if server.id == 0 {
                server.id = next;
                next += 1;
            }
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    /// Whether this server is enabled. Defaults to true.
    #[serde(default = "default_true")]
    pub active: bool,
    /// Whether the server supports NNTP command pipelining (RFC 4644).
    /// Auto-detected when the server is added or tested.
    #[serde(default)]
    pub supports_pipelining: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BufferPoolOverrides {
    pub small_count: Option<usize>,
    pub medium_count: Option<usize>,
    pub large_count: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetryOverrides {
    pub max_retries: Option<u32>,
    pub base_delay_secs: Option<f64>,
    pub multiplier: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TunerOverrides {
    pub max_concurrent_downloads: Option<usize>,
    pub max_decode_queue: Option<usize>,
    pub decode_thread_count: Option<usize>,
}
