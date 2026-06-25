use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

pub(crate) const DEFAULT_SERVE_PORT: u16 = 9090;
pub(crate) const DEFAULT_SERVE_BASE_URL: &str = "/";

#[derive(Parser)]
#[command(name = "weaver", about = "Usenet binary downloader")]
pub(crate) struct Cli {
    /// Path to configuration file.
    #[arg(short, long, default_value = "weaver.toml")]
    pub(crate) config: PathBuf,

    /// Write service logs to the given file.
    #[arg(long, value_name = "PATH", global = true)]
    pub(crate) log_file: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Option<Command>,
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
    use clap::Parser;

    use super::{Cli, Command};

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
}
