//! Whether this deployment may restart Weaver from its own UI, and the
//! in-process channel that asks the serve loop to do it.
//!
//! The mechanism is platform-specific and has to run after graceful teardown,
//! so it lives in the binary. What lives here is the decision and the
//! plumbing, so the REST and GraphQL surfaces answer from one rule.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Notify;

use super::environment::{
    DeploymentEnvironment, OperatingSystem, RuntimeEnvironment, detect_runtime_environment,
};

/// Escape hatch: set to a false value to remove the restart button and refuse
/// the restart endpoint on an otherwise-supported install.
///
/// This exists for supervisors Weaver cannot see — a Windows service wrapper
/// around `weaver.exe`, say, which would read the restart's exit as a crash
/// and start a second copy. The operator knows their supervisor; Weaver can
/// only know the deployment.
pub const ENV_UI_RESTART: &str = "WEAVER_UI_RESTART";

/// Whether the operator has left UI-driven restarts enabled.
///
/// Unset and every truthy spelling mean enabled. A false value — or an
/// unparsable one, because a mangled attempt to disable the hatch must not
/// quietly re-enable it — means disabled.
pub fn ui_restart_enabled() -> bool {
    crate::security::parse_bool_env(ENV_UI_RESTART, true).unwrap_or(false)
}

/// Whether the running process can replace itself, and what to do instead
/// when it cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartCapability {
    pub supported: bool,
    /// Set only when unsupported: the refusal, in the operator's terms.
    pub reason: Option<String>,
}

impl RestartCapability {
    pub fn supported() -> Self {
        Self {
            supported: true,
            reason: None,
        }
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            supported: false,
            reason: Some(reason.into()),
        }
    }
}

/// The running program file, when it can still be started again: resolved and
/// present on disk.
///
/// `None` when either is untrue. An upgrade that replaced the binary under the
/// running process must not cost the operator that process.
pub fn resolvable_executable() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?;
    executable.is_file().then_some(executable)
}

/// Settle the restart rules for one deployment.
///
/// Pure over its inputs so both HTTP surfaces and the tests decide identically
/// without depending on the process the test happens to run in.
pub fn restart_capability(
    environment: &RuntimeEnvironment,
    executable: Option<&Path>,
    ui_restart_enabled: bool,
) -> RestartCapability {
    // The operator's word beats every rule below: they may know a supervisor
    // Weaver cannot detect.
    if !ui_restart_enabled {
        return RestartCapability::unsupported(format!(
            "restarting from the UI is disabled by {ENV_UI_RESTART} in this deployment's \
             environment. Restart Weaver the way you normally start it.",
        ));
    }

    // A container that exits without a restart policy leaves the operator with
    // nothing, and the runtime — not Weaver — owns that decision.
    match environment.deployment {
        DeploymentEnvironment::Docker => {
            return RestartCapability::unsupported(
                "Weaver is running in a Docker container, where the container runtime decides \
                 restarts. Restart the container instead.",
            );
        }
        DeploymentEnvironment::Container => {
            return RestartCapability::unsupported(
                "Weaver is running in a container, where the container runtime decides restarts. \
                 Restart the container instead.",
            );
        }
        DeploymentEnvironment::Native => {}
    }

    if executable.is_none() {
        return RestartCapability::unsupported(
            "Weaver cannot find its own program file, so it cannot start itself again. Restart it \
             the way you normally start it.",
        );
    }

    match environment.operating_system {
        OperatingSystem::Linux | OperatingSystem::Macos | OperatingSystem::Windows => {
            RestartCapability::supported()
        }
        OperatingSystem::Unknown => RestartCapability::unsupported(
            "Weaver does not know how to restart itself on this operating system. Restart it the \
             way you normally start it.",
        ),
    }
}

/// The restart rules as they stand for the running process.
pub fn current_restart_capability() -> RestartCapability {
    restart_capability(
        &detect_runtime_environment(),
        resolvable_executable().as_deref(),
        ui_restart_enabled(),
    )
}

/// The restart surface the HTTP layer holds: whether a restart is allowed
/// here, and the request that reaches the serve loop.
///
/// A `Notify` rather than a flag because the request is a one-shot edge, and
/// `notify_one` stores a permit when the loop is not parked on
/// [`RestartController::requested`] yet, so a request cannot be lost to timing.
#[derive(Clone)]
pub struct RestartController {
    requested: Arc<Notify>,
    capability: Arc<dyn Fn() -> RestartCapability + Send + Sync>,
}

impl RestartController {
    /// The controller a running server holds. The capability is settled on
    /// every ask rather than cached, so a program file an upgrade replaced is
    /// noticed before the process is torn down for it.
    pub fn new() -> Self {
        Self::with_capability_source(current_restart_capability)
    }

    /// A controller whose capability comes from `source` rather than from this
    /// process — how a caller presents a deployment it is not running in.
    pub fn with_capability_source(
        source: impl Fn() -> RestartCapability + Send + Sync + 'static,
    ) -> Self {
        Self {
            requested: Arc::new(Notify::new()),
            capability: Arc::new(source),
        }
    }

    /// Whether this deployment may restart Weaver at all.
    pub fn capability(&self) -> RestartCapability {
        (self.capability)()
    }

    /// Ask for a restart. Never blocks and never fails: the serve loop decides
    /// when the process is safe to replace.
    pub fn request_restart(&self) {
        self.requested.notify_one();
    }

    /// Resolves once a restart has been requested.
    pub async fn requested(&self) {
        self.requested.notified().await;
    }
}

impl Default for RestartController {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RestartController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RestartController").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(
        deployment: DeploymentEnvironment,
        operating_system: OperatingSystem,
    ) -> RuntimeEnvironment {
        RuntimeEnvironment {
            deployment,
            operating_system,
            architecture: "x86_64",
        }
    }

    fn executable() -> PathBuf {
        PathBuf::from("/opt/weaver/weaver")
    }

    #[test]
    fn container_deployments_are_never_restarted_from_the_ui() {
        for deployment in [
            DeploymentEnvironment::Docker,
            DeploymentEnvironment::Container,
        ] {
            let capability = restart_capability(
                &environment(deployment, OperatingSystem::Linux),
                Some(&executable()),
                true,
            );
            assert!(!capability.supported, "{deployment:?}");
            let reason = capability.reason.expect("a refusal explains itself");
            assert!(reason.contains("container"), "{reason}");
        }
    }

    #[test]
    fn native_installs_restart_on_every_supported_platform() {
        for operating_system in [
            OperatingSystem::Linux,
            OperatingSystem::Macos,
            OperatingSystem::Windows,
        ] {
            let capability = restart_capability(
                &environment(DeploymentEnvironment::Native, operating_system),
                Some(&executable()),
                true,
            );
            assert!(capability.supported, "{operating_system:?}");
            assert_eq!(capability.reason, None);
        }
    }

    #[test]
    fn an_unresolvable_program_file_refuses_the_restart() {
        let capability = restart_capability(
            &environment(DeploymentEnvironment::Native, OperatingSystem::Linux),
            None,
            true,
        );
        assert!(!capability.supported);
        assert!(
            capability
                .reason
                .expect("a refusal explains itself")
                .contains("program file")
        );
    }

    #[test]
    fn the_operator_hatch_refuses_restart_on_any_deployment() {
        // WEAVER_UI_RESTART=0 wins over an otherwise fully supported install:
        // the operator may know a supervisor Weaver cannot detect.
        let capability = restart_capability(
            &environment(DeploymentEnvironment::Native, OperatingSystem::Linux),
            Some(&executable()),
            false,
        );
        assert!(!capability.supported);
        assert!(
            capability
                .reason
                .expect("a refusal explains itself")
                .contains(ENV_UI_RESTART)
        );
    }

    #[test]
    fn an_unknown_operating_system_refuses_the_restart() {
        let capability = restart_capability(
            &environment(DeploymentEnvironment::Native, OperatingSystem::Unknown),
            Some(&executable()),
            true,
        );
        assert!(!capability.supported);
    }

    #[test]
    fn an_injected_capability_source_answers_instead_of_this_process() {
        let controller = RestartController::with_capability_source(|| {
            restart_capability(
                &environment(DeploymentEnvironment::Docker, OperatingSystem::Linux),
                Some(Path::new("/opt/weaver/weaver")),
                true,
            )
        });
        assert!(!controller.capability().supported);
    }

    #[tokio::test]
    async fn a_restart_request_wakes_the_waiting_serve_loop() {
        let controller = RestartController::new();
        let waiter = controller.clone();
        let serve_loop = tokio::spawn(async move { waiter.requested().await });

        controller.request_restart();

        tokio::time::timeout(std::time::Duration::from_secs(5), serve_loop)
            .await
            .expect("a requested restart wakes the serve loop")
            .expect("the waiting task completes");
    }

    #[tokio::test]
    async fn a_request_made_before_the_loop_waits_is_not_lost() {
        let controller = RestartController::new();
        controller.request_restart();

        tokio::time::timeout(std::time::Duration::from_secs(5), controller.requested())
            .await
            .expect("a stored request is delivered to the first waiter");
    }
}
