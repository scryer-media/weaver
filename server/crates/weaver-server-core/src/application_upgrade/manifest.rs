//! Signed release-manifest validation for in-application upgrades.
//!
//! The schema and its validation live in the shared `application-updater`
//! crate; this module binds them to Weaver's product identity and maps the
//! shared error type onto [`ApplicationUpgradeError`].

use artifact_trust::RequiredSigner;

use super::error::{ApplicationUpgradeResult, map_updater_error};
use super::product::WEAVER_PRODUCT;

pub use application_updater::manifest::{
    UPGRADE_MANIFEST_MAX_BYTES, UpgradeArchitecture, UpgradeArchive, UpgradeArtifact,
    UpgradeArtifactMember, UpgradeChannel, UpgradeManifest, UpgradeManifestV2, UpgradePlatform,
    ValidatedUpgradeManifestV2,
};

pub use super::product::{UPGRADE_MANIFEST_SCHEMA_VERSION, UPGRADE_MANIFEST_V2_SCHEMA_VERSION};

/// Parses and validates a signed upgrade manifest payload.
pub fn parse_and_validate_upgrade_manifest(
    raw: &[u8],
) -> ApplicationUpgradeResult<UpgradeManifest> {
    application_updater::manifest::parse_and_validate_upgrade_manifest(&WEAVER_PRODUCT, raw)
        .map_err(map_updater_error)
}

/// Parses and validates a signed v2 upgrade manifest payload.
///
/// v2 is forward-tolerant: artifacts naming a platform, architecture, channel
/// or archive this build has never heard of are retained but never selected,
/// and unknown JSON fields are ignored. Everything this build *does* understand
/// is validated exactly as strictly as v1.
pub fn parse_and_validate_upgrade_manifest_v2(
    raw: &[u8],
) -> ApplicationUpgradeResult<ValidatedUpgradeManifestV2> {
    application_updater::manifest::parse_and_validate_upgrade_manifest_v2(&WEAVER_PRODUCT, raw)
        .map_err(map_updater_error)
}

/// The Sigstore identity required of Weaver's release workflow for a tag.
pub fn weaver_release_required_signer(release_tag: &str) -> RequiredSigner {
    WEAVER_PRODUCT.release_required_signer(release_tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published example is the fixture: a change that makes the shipped
    /// example unparseable is a release break, not a test break.
    #[test]
    fn accepts_the_published_v1_example() {
        let raw = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../api/upgrade/manifest.v1.example.json"
        ));
        let manifest = parse_and_validate_upgrade_manifest(raw).expect("v1 example is valid");
        assert_eq!(manifest.schema, UPGRADE_MANIFEST_SCHEMA_VERSION);
        assert!(!manifest.artifacts.is_empty());
    }

    #[test]
    fn accepts_the_published_v2_example() {
        let raw = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../api/upgrade/manifest.v2.example.json"
        ));
        let validated = parse_and_validate_upgrade_manifest_v2(raw).expect("v2 example is valid");
        assert_eq!(
            validated.published.schema,
            UPGRADE_MANIFEST_V2_SCHEMA_VERSION
        );
        assert!(!validated.understood.artifacts.is_empty());
    }

    /// A v1 parser must refuse a v2 document outright rather than read the part
    /// of it that happens to look familiar.
    #[test]
    fn the_v1_parser_refuses_a_v2_manifest() {
        let raw = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../api/upgrade/manifest.v2.example.json"
        ));
        parse_and_validate_upgrade_manifest(raw).expect_err("v2 is not a v1 manifest");
    }
}
