// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct ValidationTools {
    temp: tempfile::TempDir,
    path: std::ffi::OsString,
}

impl ValidationTools {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        for (name, body) in [
            (
                "cargo",
                r#"printf '%s\n' "$*" >> "$VALIDATION_COMMAND_LOG"
if [ "$1" = "${FAIL_VALIDATION_STAGE:-}" ]; then exit 51; fi
"#,
            ),
            ("cargo-xwin", "exit 0\n"),
            (
                "docker",
                "printf '%s\\n' \"$*\" >> \"$DOCKER_COMMAND_LOG\"\nexit 0\n",
            ),
            ("podman", "exit 52\n"),
        ] {
            let file = temp.path().join(name);
            fs::write(&file, format!("#!/bin/sh\n{body}")).unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let path = std::env::join_paths(
            std::iter::once(temp.path().to_path_buf())
                .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
        )
        .unwrap();
        Self { temp, path }
    }

    fn command(&self, program: impl AsRef<std::ffi::OsStr>) -> Command {
        let mut command = Command::new(program);
        command
            .current_dir(repo_root())
            .env("PATH", &self.path)
            .env("VALIDATION_COMMAND_LOG", self.temp.path().join("cargo.log"))
            .env("DOCKER_COMMAND_LOG", self.temp.path().join("docker.log"))
            .env_remove("FAIL_VALIDATION_STAGE");
        command
    }

    fn cargo_calls(&self) -> Vec<String> {
        fs::read_to_string(self.temp.path().join("cargo.log"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn assert_no_docker(&self) {
        assert!(!self.temp.path().join("docker.log").exists());
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn assert_passed(output: &Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn clippy_runs_host_and_windows_without_a_container_engine() {
    let tools = ValidationTools::new();
    let output = tools
        .command(env!("CARGO_BIN_EXE_xtask"))
        .args(["ci", "clippy"])
        .output()
        .unwrap();
    assert_passed(&output);
    let calls = tools.cargo_calls();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].starts_with("clippy --workspace --locked "));
    assert!(calls[1].starts_with("xwin clippy --target x86_64-pc-windows-msvc "));
    for call in calls {
        for flag in [
            "--workspace",
            "--locked",
            "--lib",
            "--bins",
            "--tests",
            "--examples",
            "--benches",
        ] {
            assert!(call.split_whitespace().any(|arg| arg == flag), "{call}");
        }
        assert!(call.ends_with("-- -D warnings"), "{call}");
    }
    tools.assert_no_docker();
}

#[test]
fn a_host_clippy_failure_stops_validation_before_windows() {
    let tools = ValidationTools::new();
    let output = tools
        .command(env!("CARGO_BIN_EXE_xtask"))
        .args(["ci", "clippy"])
        .env("FAIL_VALIDATION_STAGE", "clippy")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(tools.cargo_calls().len(), 1);
    tools.assert_no_docker();
}

#[test]
fn a_windows_clippy_failure_fails_validation() {
    let tools = ValidationTools::new();
    let output = tools
        .command(env!("CARGO_BIN_EXE_xtask"))
        .args(["ci", "clippy"])
        .env("FAIL_VALIDATION_STAGE", "xwin")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(tools.cargo_calls().len(), 2);
    tools.assert_no_docker();
}

#[test]
fn publish_configuration_is_checked_without_docker() {
    let tools = ValidationTools::new();
    let output = tools
        .command("sh")
        .args(["docker/validate-release-build-config.sh", "--config-only"])
        .output()
        .unwrap();
    assert_passed(&output);
    tools.assert_no_docker();
}

#[test]
fn static_publish_validation_still_rejects_missing_platforms() {
    let tools = ValidationTools::new();
    let workflow = fs::read_to_string(repo_root().join(".github/workflows/deploy.yml")).unwrap();
    let invalid = workflow.replace(
        "platforms: linux/amd64,linux/arm64",
        "platforms: linux/amd64",
    );
    assert_ne!(invalid, workflow);
    let workflow_path = tools.temp.path().join("deploy.yml");
    fs::write(&workflow_path, invalid).unwrap();
    let output = tools
        .command("sh")
        .args(["docker/validate-release-build-config.sh", "--config-only"])
        .arg(workflow_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("must cover linux/amd64 and linux/arm64")
    );
    tools.assert_no_docker();
}

#[test]
fn full_publish_validation_keeps_the_docker_build_check() {
    let tools = ValidationTools::new();
    let output = tools
        .command("sh")
        .arg("docker/validate-release-build-config.sh")
        .output()
        .unwrap();
    assert_passed(&output);
    let call = fs::read_to_string(tools.temp.path().join("docker.log")).unwrap();
    assert!(call.starts_with("buildx build --check "), "{call}");
    assert!(
        call.contains("--platform linux/amd64,linux/arm64"),
        "{call}"
    );
}
