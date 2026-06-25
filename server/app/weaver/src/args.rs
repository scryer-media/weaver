use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

pub(crate) const DEFAULT_CONFIG_FILE: &str = "weaver.toml";
pub(crate) const DEFAULT_SERVE_PORT: u16 = 9090;
pub(crate) const DEFAULT_SERVE_BASE_URL: &str = "/";

#[derive(Parser)]
#[command(name = "weaver", about = "Usenet binary downloader")]
pub(crate) struct Cli {
    /// Path to configuration file.
    #[arg(short, long)]
    pub(crate) config: Option<PathBuf>,

    /// Write service logs to the given file.
    #[arg(long, value_name = "PATH", global = true)]
    pub(crate) log_file: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

impl Cli {
    pub(crate) fn resolved_config_path(&self) -> PathBuf {
        self.config
            .clone()
            .unwrap_or_else(default_implicit_config_path)
    }
}

fn default_implicit_config_path() -> PathBuf {
    #[cfg(windows)]
    {
        return windows_implicit_config_path(
            PathBuf::from(DEFAULT_CONFIG_FILE).exists(),
            PathBuf::from("weaver.db").exists(),
            std::env::var_os("LOCALAPPDATA").map(PathBuf::from),
            std::env::var_os("APPDATA").map(PathBuf::from),
        );
    }

    PathBuf::from(DEFAULT_CONFIG_FILE)
}

#[cfg(windows)]
fn windows_implicit_config_path(
    local_config_exists: bool,
    local_db_exists: bool,
    local_app_data: Option<PathBuf>,
    app_data: Option<PathBuf>,
) -> PathBuf {
    if local_config_exists || local_db_exists {
        return PathBuf::from(DEFAULT_CONFIG_FILE);
    }

    if let Some(base) = local_app_data {
        return base.join("weaver");
    }

    if let Some(base) = app_data {
        return base.join("weaver");
    }

    PathBuf::from(DEFAULT_CONFIG_FILE)
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Download an NZB file.
    Download {
        /// Path to the NZB file.
        nzb: PathBuf,

        /// Output directory (overrides config).
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Start the HTTP server with GraphQL API.
    Serve {
        /// Port to listen on (default: 9090).
        #[arg(short, long, default_value_t = DEFAULT_SERVE_PORT)]
        port: u16,

        /// Base URL path for reverse proxy hosting (e.g. "/weaver").
        #[arg(long, default_value = DEFAULT_SERVE_BASE_URL)]
        base_url: String,
    },

    /// Local PAR2 verification and repair.
    Par2 {
        #[command(subcommand)]
        command: Par2Command,
    },
}

impl Command {
    pub(crate) fn default_serve() -> Self {
        Self::Serve {
            port: DEFAULT_SERVE_PORT,
            base_url: DEFAULT_SERVE_BASE_URL.to_string(),
        }
    }
}

#[derive(Args, Clone)]
pub(crate) struct Par2Args {
    /// PAR2 index/volume file, or a directory containing PAR2 files.
    #[arg(value_name = "PAR2")]
    pub(crate) input: PathBuf,

    /// Primary directory for reading and writing repaired files.
    #[arg(short = 'C', long, value_name = "DIR")]
    pub(crate) working_dir: Option<PathBuf>,

    /// Additional directories to search for data files.
    #[arg(value_name = "SEARCH_DIR")]
    pub(crate) search_dirs: Vec<PathBuf>,
}

#[derive(Subcommand, Clone)]
pub(crate) enum Par2Command {
    /// Verify files against a PAR2 set.
    #[command(alias = "v")]
    Verify(Par2Args),

    /// Repair files using a PAR2 set.
    #[command(alias = "r")]
    Repair {
        #[command(flatten)]
        args: Par2Args,
    },
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{Cli, Command, DEFAULT_CONFIG_FILE};

    #[test]
    fn argless_invocation_defaults_to_serve() {
        let cli = Cli::parse_from(["weaver"]);

        match cli.command.unwrap_or_else(Command::default_serve) {
            Command::Serve { port, base_url } => {
                assert_eq!(port, 9090);
                assert_eq!(base_url, "/");
            }
            _ => panic!("argless invocation should default to serve"),
        }
    }

    #[test]
    fn explicit_serve_arguments_are_preserved() {
        let cli = Cli::parse_from(["weaver", "serve", "--port", "9191", "--base-url", "/weaver"]);

        match cli.command.unwrap_or_else(Command::default_serve) {
            Command::Serve { port, base_url } => {
                assert_eq!(port, 9191);
                assert_eq!(base_url, "/weaver");
            }
            _ => panic!("explicit serve invocation should parse as serve"),
        }
    }

    #[test]
    fn explicit_config_path_is_preserved() {
        let cli = Cli::parse_from(["weaver", "--config", "custom-weaver.toml"]);

        assert_eq!(
            cli.resolved_config_path(),
            PathBuf::from("custom-weaver.toml")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn implicit_non_windows_config_path_stays_current_directory_toml() {
        let cli = Cli::parse_from(["weaver"]);

        assert_eq!(
            cli.resolved_config_path(),
            PathBuf::from(DEFAULT_CONFIG_FILE)
        );
    }

    #[cfg(windows)]
    #[test]
    fn implicit_windows_config_prefers_existing_current_directory_state() {
        assert_eq!(
            super::windows_implicit_config_path(
                true,
                false,
                Some(PathBuf::from("LocalAppData")),
                None,
            ),
            PathBuf::from(DEFAULT_CONFIG_FILE),
        );
        assert_eq!(
            super::windows_implicit_config_path(
                false,
                true,
                Some(PathBuf::from("LocalAppData")),
                None,
            ),
            PathBuf::from(DEFAULT_CONFIG_FILE),
        );
    }

    #[cfg(windows)]
    #[test]
    fn implicit_windows_config_uses_writable_app_data_for_new_portable_installs() {
        assert_eq!(
            super::windows_implicit_config_path(
                false,
                false,
                Some(PathBuf::from("LocalAppData")),
                Some(PathBuf::from("RoamingAppData")),
            ),
            PathBuf::from("LocalAppData").join("weaver"),
        );
        assert_eq!(
            super::windows_implicit_config_path(
                false,
                false,
                None,
                Some(PathBuf::from("RoamingAppData")),
            ),
            PathBuf::from("RoamingAppData").join("weaver"),
        );
    }
}
