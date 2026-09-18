//! In-application upgrades.
//!
//! The upgrade mechanics — manifest validation, installation classification,
//! download, verification, extraction, promotion, rollback, the durable journal
//! and the Windows helper handoff — live in the shared `application-updater`
//! crate. This module owns everything Weaver-specific: its product identity,
//! the service that orchestrates a run, and the names the rest of the workspace
//! imports from here.

mod error;
pub mod manifest;
mod product;
mod service;
pub(crate) mod trust;

pub use error::{
    ApplicationUpgradeError, ApplicationUpgradeResult, map_updater_error, to_updater_error,
};
pub use product::{
    APPLICATION_UPGRADE_HELPER_PLAN_SCHEMA, JOURNAL_SCHEMA, UPGRADE_MANIFEST_SCHEMA_VERSION,
    UPGRADE_MANIFEST_V2_SCHEMA_VERSION, WEAVER_PRODUCT,
};
pub use trust::{install_default_rustls_provider, spawn_sigstore_trust_root_priming};

pub use service::{
    ApplicationUpgradeRun, ApplicationUpgradeRunStatus, ApplicationUpgradeService,
    ApplicationUpgradeSnapshot, ApplicationUpgradeStartRequest,
    application_upgrade_helper_update_journal, phases,
};

/// Classify this installation from live startup evidence.
///
/// The observation and the judgement both live in the shared crate; this binds
/// them to Weaver's environment markers, registry key and write-probe prefix.
pub fn collect_installation_assessment() -> InstallationAssessment {
    application_updater::evidence::collect_installation_assessment(&WEAVER_PRODUCT)
}

pub use application_updater::helper_plan::{
    APPLICATION_UPGRADE_HELPER_WAIT_BUDGET, ApplicationUpgradeHelperMode,
    ApplicationUpgradeHelperOwner, ApplicationUpgradeHelperPlan, ApplicationUpgradeHelperRelaunch,
    ApplicationUpgradeHelperReplacement, MsiHelperJournalTransition, PortableReplacementOperations,
    WRITE_PROBE_PERMISSION_DENIED, WriteProbeOutcome, classify_write_probe_error,
    helper_wait_remaining, helper_write_probe_required, msi_exit_code_transition,
    msi_install_succeeded, open_process_failure_means_exited, path_is_within,
    portable_replacement_operations, portable_replacement_rollback_operations,
    reboot_required_completion_allowed, should_restore_tray_startup,
};
pub use application_updater::installation::{
    EligibilityReason, InstallationAssessment, InstallationEvidence, InstallationKind,
    InstallationOs, ManagementOwner, classify_installation, macos_app_bundle_path,
};
pub use application_updater::journal::ApplicationUpgradeJournal;
