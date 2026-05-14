use anyhow::{Context, Result, bail};
use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("payload exec runner: {error:#}");
        std::process::exit(91);
    }
}

fn run() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let payload_path = PathBuf::from(args.next().context("payload path is required")?);
    let exec_argv: Vec<OsString> = args.collect();
    if exec_argv.is_empty() {
        bail!("at least one exec argv entry is required");
    }

    let env_pairs: Vec<(OsString, OsString)> = env::vars_os().collect();
    weaver_docker_launcher::launch_compressed_payload(&payload_path, &exec_argv, &env_pairs)
}
