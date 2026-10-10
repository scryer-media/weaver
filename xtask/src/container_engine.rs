// The container engine the tooling runs its Linux checks in.
//
// Docker and Podman take the same `run` arguments for what the tooling
// does, so the engine is a choice of binary plus the two places they differ:
// how an image without a registry is named, and what a bind mount needs on a
// host that labels files.

use anyhow::{Result, anyhow, bail};
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContainerEngine {
    Docker,
    Podman,
}

impl ContainerEngine {
    pub(crate) fn binary(self) -> &'static str {
        match self {
            ContainerEngine::Docker => "docker",
            ContainerEngine::Podman => "podman",
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            ContainerEngine::Docker => "Docker",
            ContainerEngine::Podman => "Podman",
        }
    }

    // `image` as this engine can pull it without being asked anything.
    //
    // Docker reads a name without a registry as one on Docker Hub. Podman
    // looks it up in the host's list of registries, and with more than one
    // listed it asks which, which a run without a terminal cannot answer.
    pub(crate) fn image_reference(self, image: &str) -> String {
        if self == ContainerEngine::Docker {
            return image.to_string();
        }
        match image.split_once('/') {
            None => format!("docker.io/library/{image}"),
            Some((first, _)) if first.contains(['.', ':']) || first == "localhost" => {
                image.to_string()
            }
            Some(_) => format!("docker.io/{image}"),
        }
    }

    // Arguments a `run` that bind-mounts the checkout needs beyond Docker's.
    //
    // Podman on a host that labels files refuses a container access to a
    // mount that does not carry the container's label. Relabelling the
    // checkout would change the operator's files, so the container runs
    // unconfined by labels instead.
    pub(crate) fn bind_mount_run_args(self) -> &'static [&'static str] {
        match self {
            ContainerEngine::Docker => &[],
            ContainerEngine::Podman => &["--security-opt", "label=disable"],
        }
    }
}

// What resolution asks of the host.
pub(crate) trait EngineProbe {
    fn on_path(&self, program: &str) -> bool;

    // The command's standard output when it succeeds, and why it did not
    // otherwise.
    fn output(&self, program: &str, args: &[&str]) -> std::result::Result<String, String>;
}

// The real PATH and processes.
pub(crate) struct HostProbe;

impl EngineProbe for HostProbe {
    fn on_path(&self, program: &str) -> bool {
        crate::command_available(program).unwrap_or(false)
    }

    fn output(&self, program: &str, args: &[&str]) -> std::result::Result<String, String> {
        let output = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .map_err(|error| error.to_string())?;
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(stderr
            .lines()
            .find(|line| !line.trim().is_empty())
            .unwrap_or("it exited with an error")
            .trim()
            .to_string())
    }
}

// The engine that is running: Docker when its daemon answers, otherwise
// Podman when it does. An installed engine that is not running is passed
// over, and the error says what each one lacked.
pub(crate) fn running_engine(probe: &dyn EngineProbe) -> Result<ContainerEngine> {
    let docker = match running_docker(probe) {
        Ok(engine) => return Ok(engine),
        Err(error) => error,
    };
    let podman = match running_podman(probe) {
        Ok(engine) => return Ok(engine),
        Err(error) => error,
    };
    Err(anyhow!(
        "no container engine is running: docker: {docker}; podman: {podman}"
    ))
}

fn running_docker(probe: &dyn EngineProbe) -> Result<ContainerEngine> {
    if !probe.on_path("docker") {
        bail!("the docker CLI is not on PATH");
    }
    // A `docker` that is Podman's compatibility wrapper is Podman, and is
    // found as Podman below.
    if probe
        .output("docker", &["--version"])
        .is_ok_and(|version| version.trim().to_ascii_lowercase().starts_with("podman"))
    {
        bail!("the docker CLI on PATH is Podman's docker wrapper");
    }
    probe
        .output("docker", &["version", "--format", "{{.Server.Version}}"])
        .map_err(|error| anyhow!("the Docker daemon is not answering ({error})"))?;
    Ok(ContainerEngine::Docker)
}

fn running_podman(probe: &dyn EngineProbe) -> Result<ContainerEngine> {
    if !probe.on_path("podman") {
        bail!("the podman CLI is not on PATH");
    }
    probe
        .output("podman", &["info", "--format", "{{.Version.Version}}"])
        .map_err(|error| {
            anyhow!(
                "podman is not answering ({error}); on macOS or Windows start the machine with `podman machine start`"
            )
        })?;
    Ok(ContainerEngine::Podman)
}
