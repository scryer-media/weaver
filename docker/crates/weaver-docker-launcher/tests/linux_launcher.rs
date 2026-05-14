#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use anyhow::{Context, Result};
use tempfile::tempdir;

#[test]
fn launcher_can_decompress_and_exec_from_memfd_with_argv_and_env_passthrough() -> Result<()> {
    let helper_binary = fs::read(env!("CARGO_BIN_EXE_payload_probe"))
        .context("failed to read payload_probe test binary")?;
    let temp_dir = tempdir()?;
    let payload_path = temp_dir.path().join("payload_probe.zst");
    let created_path = temp_dir.path().join("created.txt");
    fs::write(&payload_path, zstd::bulk::compress(&helper_binary, 3)?)?;

    let output = Command::new(env!("CARGO_BIN_EXE_payload_exec_runner"))
        .arg(&payload_path)
        .arg("weaver-cortex-a76")
        .arg("download")
        .arg("--config")
        .arg("/config/weaver.toml")
        .env("WEAVER_LAUNCHER_PROBE_ENV", "round-trip")
        .env("WEAVER_LAUNCHER_PROBE_WRITE_PATH", &created_path)
        .env("TZ", "America/Denver")
        .env("LOG", "weaver=debug,info")
        .env("UMASK", "027")
        .output()
        .context("failed to run payload_exec_runner")?;
    let stdout = String::from_utf8(output.stdout).context("payload stdout was not valid UTF-8")?;
    let stderr = String::from_utf8(output.stderr).context("payload stderr was not valid UTF-8")?;

    assert!(
        output.status.success(),
        "payload exited with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout,
        stderr
    );
    assert!(stdout.contains("argv0=weaver-cortex-a76"));
    assert!(stdout.contains("argv1=download"));
    assert!(stdout.contains("argv2=--config"));
    assert!(stdout.contains("argv3=/config/weaver.toml"));
    assert!(stdout.contains("probe_env=round-trip"));
    assert!(stdout.contains("probe_tz=America/Denver"));
    assert!(stdout.contains("probe_rust_log=weaver=debug,info"));
    assert!(stdout.contains("probe_umask=27"));
    let created_mode = fs::metadata(&created_path)?.permissions().mode() & 0o777;
    assert_eq!(0o000, created_mode & 0o027);
    Ok(())
}
