use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "weaver", about = "Usenet binary downloader")]
pub(crate) struct Cli {
    /// Path to configuration file.
    #[arg(short, long, default_value = "weaver.toml")]
    pub(crate) config: PathBuf,

    #[command(subcommand)]
    pub(crate) command: Command,
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
        #[arg(short, long, default_value = "9090")]
        port: u16,

        /// Base URL path for reverse proxy hosting (e.g. "/weaver").
        #[arg(long, default_value = "/")]
        base_url: String,
    },

    /// Local PAR2 verification and repair.
    Par2 {
        #[command(subcommand)]
        command: Par2Command,
    },
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
