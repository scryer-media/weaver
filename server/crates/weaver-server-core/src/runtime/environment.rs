//! Safe runtime-environment facts for troubleshooting surfaces.

const DEPLOYMENT_ENV: &str = "WEAVER_DEPLOYMENT_ENV";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentEnvironment {
    Native,
    Docker,
    Container,
}

impl DeploymentEnvironment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Docker => "docker",
            Self::Container => "container",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatingSystem {
    Linux,
    Macos,
    Windows,
    Unknown,
}

impl OperatingSystem {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Macos => "macos",
            Self::Windows => "windows",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvironment {
    pub deployment: DeploymentEnvironment,
    pub operating_system: OperatingSystem,
    pub architecture: &'static str,
}

pub fn detect_runtime_environment() -> RuntimeEnvironment {
    let marker = std::env::var(DEPLOYMENT_ENV).ok();
    let docker_env_present =
        cfg!(target_os = "linux") && std::path::Path::new("/.dockerenv").exists();
    let cgroup = if cfg!(target_os = "linux") {
        std::fs::read_to_string("/proc/self/cgroup")
            .or_else(|_| std::fs::read_to_string("/proc/1/cgroup"))
            .ok()
    } else {
        None
    };

    RuntimeEnvironment {
        deployment: detect_deployment(marker.as_deref(), docker_env_present, cgroup.as_deref()),
        operating_system: current_operating_system(),
        architecture: std::env::consts::ARCH,
    }
}

fn detect_deployment(
    marker: Option<&str>,
    docker_env_present: bool,
    cgroup: Option<&str>,
) -> DeploymentEnvironment {
    match marker
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("docker") => return DeploymentEnvironment::Docker,
        Some("container") => return DeploymentEnvironment::Container,
        // The heuristics below read cgroup strings; "native" is the operator's
        // override when they misfire, because everything a container may not
        // do (self-restart, the wizard's bind question) hangs off this answer.
        Some("native") => return DeploymentEnvironment::Native,
        _ => {}
    }
    if docker_env_present {
        return DeploymentEnvironment::Docker;
    }

    let cgroup = cgroup.unwrap_or_default().to_ascii_lowercase();
    if cgroup.contains("docker") {
        DeploymentEnvironment::Docker
    } else if ["containerd", "kubepods", "libpod", "podman", "lxc"]
        .iter()
        .any(|signal| cgroup.contains(signal))
    {
        DeploymentEnvironment::Container
    } else {
        DeploymentEnvironment::Native
    }
}

const fn current_operating_system() -> OperatingSystem {
    if cfg!(target_os = "linux") {
        OperatingSystem::Linux
    } else if cfg!(target_os = "macos") {
        OperatingSystem::Macos
    } else if cfg!(target_os = "windows") {
        OperatingSystem::Windows
    } else {
        OperatingSystem::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_marker_wins() {
        assert_eq!(
            detect_deployment(Some("docker"), false, None),
            DeploymentEnvironment::Docker
        );
        assert_eq!(
            detect_deployment(Some("container"), false, None),
            DeploymentEnvironment::Container
        );
        // The escape hatch for misfiring heuristics: "native" overrides even
        // hard container evidence.
        assert_eq!(
            detect_deployment(Some("native"), true, Some("0::/docker/abc")),
            DeploymentEnvironment::Native
        );
    }

    #[test]
    fn docker_and_other_container_signals_are_distinct() {
        assert_eq!(
            detect_deployment(None, true, None),
            DeploymentEnvironment::Docker
        );
        assert_eq!(
            detect_deployment(None, false, Some("0::/docker/abc")),
            DeploymentEnvironment::Docker
        );
        assert_eq!(
            detect_deployment(None, false, Some("0::/kubepods/containerd/abc")),
            DeploymentEnvironment::Container
        );
        assert_eq!(
            detect_deployment(None, false, Some("0::/user.slice")),
            DeploymentEnvironment::Native
        );
    }

    #[test]
    fn platform_mapping_is_known_for_supported_targets() {
        assert_ne!(current_operating_system(), OperatingSystem::Unknown);
        assert!(!std::env::consts::ARCH.is_empty());
    }
}
