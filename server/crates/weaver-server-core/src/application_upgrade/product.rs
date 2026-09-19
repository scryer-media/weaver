//! Weaver's product identity for in-application upgrades.
//!
//! Every string here appears in a signed manifest, on disk, or in a process
//! argument, so changing one is a compatibility break: an upgrade helper from
//! one version must still read a plan and a journal written by another. The
//! tests below assert each value literally for that reason — a rename has to be
//! a deliberate edit in two places, not a refactor that carried.

use std::time::Duration;

use application_updater::ProductDescriptor;

/// Schema identifier of the frozen v1 upgrade manifest.
pub const UPGRADE_MANIFEST_SCHEMA_VERSION: &str = "weaver.upgrade.manifest.v1";

/// Schema identifier of the forward-tolerant v2 upgrade manifest.
pub const UPGRADE_MANIFEST_V2_SCHEMA_VERSION: &str = "weaver.upgrade.manifest.v2";

/// Schema identifier written into the durable upgrade journal.
pub const JOURNAL_SCHEMA: &str = "weaver.upgrade.journal.v1";

/// Schema identifier written into the Windows helper plan.
pub const APPLICATION_UPGRADE_HELPER_PLAN_SCHEMA: &str = "weaver.upgrade.helper-plan.v1";

/// Bounded budget for a single upgrade HTTP operation.
///
/// Release artifacts are tens of megabytes and the check runs on whatever link
/// the operator has, so this is deliberately far longer than the API-call
/// timeouts elsewhere in Weaver.
const LONG_RUNNING_HTTP_OPERATION_TIMEOUT: Duration = Duration::from_secs(600);

/// Weaver's identity as the shared upgrade core sees it.
pub const WEAVER_PRODUCT: ProductDescriptor = ProductDescriptor {
    display_name: "Weaver",
    release_repository: "scryer-media/weaver",
    release_workflow: ".github/workflows/deploy.yml",
    manifest_asset_name: "weaver-upgrade-manifest.json",
    manifest_signature_asset_name: "weaver-upgrade-manifest.json.sigstore.json",
    manifest_schema: UPGRADE_MANIFEST_SCHEMA_VERSION,
    manifest_v2_asset_name: "weaver-upgrade-manifest.v2.json",
    manifest_v2_signature_asset_name: "weaver-upgrade-manifest.v2.json.sigstore.json",
    manifest_v2_schema: UPGRADE_MANIFEST_V2_SCHEMA_VERSION,
    macos_bundle_name: "Weaver.app",
    journal_schema: JOURNAL_SCHEMA,
    helper_plan_schema: APPLICATION_UPGRADE_HELPER_PLAN_SCHEMA,
    windows_server_executable: "weaver.exe",
    windows_tray_executable: "weaver-tray.exe",
    windows_helper_executable: "weaver-upgrade-helper.exe",
    staged_replacement_prefix: ".weaver-upgrade-new-",
    disable_self_upgrade_env: "WEAVER_DISABLE_SELF_UPGRADE",
    package_env: "WEAVER_PACKAGE",
    tray_supervised_env: "WEAVER_TRAY_SUPERVISED",
    windows_registry_key: "Software\\Scryer Media\\Weaver",
    write_probe_prefix: ".weaver-write-probe-",
    http_operation_timeout: LONG_RUNNING_HTTP_OPERATION_TIMEOUT,
};

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire identities are a compatibility contract, so they are asserted
    /// as literals rather than against the constants that define them.
    #[test]
    fn the_product_descriptor_names_weavers_frozen_wire_identities() {
        assert_eq!(WEAVER_PRODUCT.display_name, "Weaver");
        assert_eq!(WEAVER_PRODUCT.release_repository, "scryer-media/weaver");
        assert_eq!(
            WEAVER_PRODUCT.release_workflow,
            ".github/workflows/deploy.yml"
        );
        assert_eq!(
            WEAVER_PRODUCT.manifest_asset_name,
            "weaver-upgrade-manifest.json"
        );
        assert_eq!(
            WEAVER_PRODUCT.manifest_signature_asset_name,
            "weaver-upgrade-manifest.json.sigstore.json"
        );
        assert_eq!(WEAVER_PRODUCT.manifest_schema, "weaver.upgrade.manifest.v1");
        assert_eq!(
            WEAVER_PRODUCT.manifest_v2_asset_name,
            "weaver-upgrade-manifest.v2.json"
        );
        assert_eq!(
            WEAVER_PRODUCT.manifest_v2_signature_asset_name,
            "weaver-upgrade-manifest.v2.json.sigstore.json"
        );
        assert_eq!(
            WEAVER_PRODUCT.manifest_v2_schema,
            "weaver.upgrade.manifest.v2"
        );
        assert_eq!(WEAVER_PRODUCT.macos_bundle_name, "Weaver.app");
        assert_eq!(WEAVER_PRODUCT.journal_schema, "weaver.upgrade.journal.v1");
        assert_eq!(
            WEAVER_PRODUCT.helper_plan_schema,
            "weaver.upgrade.helper-plan.v1"
        );
        assert_eq!(WEAVER_PRODUCT.windows_server_executable, "weaver.exe");
        assert_eq!(WEAVER_PRODUCT.windows_tray_executable, "weaver-tray.exe");
        assert_eq!(
            WEAVER_PRODUCT.windows_helper_executable,
            "weaver-upgrade-helper.exe"
        );
        assert_eq!(
            WEAVER_PRODUCT.staged_replacement_prefix,
            ".weaver-upgrade-new-"
        );
        assert_eq!(
            WEAVER_PRODUCT.disable_self_upgrade_env,
            "WEAVER_DISABLE_SELF_UPGRADE"
        );
        assert_eq!(WEAVER_PRODUCT.package_env, "WEAVER_PACKAGE");
        assert_eq!(WEAVER_PRODUCT.tray_supervised_env, "WEAVER_TRAY_SUPERVISED");
        assert_eq!(
            WEAVER_PRODUCT.windows_registry_key,
            "Software\\Scryer Media\\Weaver"
        );
        assert_eq!(WEAVER_PRODUCT.write_probe_prefix, ".weaver-write-probe-");
    }

    /// The two Windows executables a portable upgrade swaps, in plan order.
    #[test]
    fn the_windows_replacements_are_the_server_then_the_tray() {
        assert_eq!(
            WEAVER_PRODUCT.windows_replacement_executables(),
            ["weaver.exe", "weaver-tray.exe"]
        );
    }

    #[test]
    fn release_signer_is_pinned_to_the_release_workflow_and_tag() {
        let signer = WEAVER_PRODUCT.release_required_signer("weaver-v0.12.6");
        assert_eq!(signer.github_repository, "scryer-media/weaver");
        assert_eq!(
            signer.github_workflow.as_deref(),
            Some(".github/workflows/deploy.yml")
        );
        assert_eq!(
            signer.github_ref.as_deref(),
            Some("refs/tags/weaver-v0.12.6")
        );
    }

    #[test]
    fn every_manifest_artifact_must_come_from_this_releases_download_prefix() {
        assert_eq!(
            WEAVER_PRODUCT.release_download_prefix("weaver-v0.12.6"),
            "https://github.com/scryer-media/weaver/releases/download/weaver-v0.12.6/"
        );
    }
}
