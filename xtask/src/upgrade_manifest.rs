//! Signed upgrade-manifest generation for a release.
//!
//! The manifest is what an installed Weaver reads to decide what to download
//! and what to trust: for every supported layout it names the release asset, its
//! exact byte length, its BLAKE3 hash, and — for archives — every regular file
//! inside it with its size and executable bit. The running build verifies all of
//! that before anything is promoted, so a manifest that disagrees with the
//! assets is a release that cannot install itself.
//!
//! Two generations are published for every release. v1 is frozen and its parser
//! rejects anything it does not recognise, so it can never grow a field; v2 is
//! forward-tolerant and is where new platforms, channels and archive kinds go.
//! Clients read v2 first and fall back to v1 only when the v2 asset is absent,
//! which is what a release published before v2 existed looks like. Both are
//! generated from the same assets in the same pass, so they cannot disagree
//! about a release.
//!
//! Generation is deterministic: artifacts and their members are sorted, so two
//! runs over the same assets produce byte-identical output and the signature
//! covers something reproducible.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use serde::Serialize;

/// Schema identifier of the frozen v1 manifest. Must match the running build's
/// `weaver_server_core::application_upgrade::UPGRADE_MANIFEST_SCHEMA_VERSION`.
const UPGRADE_MANIFEST_SCHEMA_V1: &str = "weaver.upgrade.manifest.v1";

/// Schema identifier of the forward-tolerant v2 manifest.
const UPGRADE_MANIFEST_SCHEMA_V2: &str = "weaver.upgrade.manifest.v2";

/// Cap on a single archive member we are willing to hash and describe. Far above
/// anything Weaver ships; a release that trips it is a packaging bug.
const MAX_ARCHIVE_MEMBER_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Args)]
pub(crate) struct UpgradeManifestArgs {
    #[arg(
        long,
        value_enum,
        default_value_t = UpgradeManifestGeneration::V1,
        help = "Which manifest generation to write"
    )]
    schema: UpgradeManifestGeneration,
    #[arg(long, help = "Weaver version without the weaver-v prefix")]
    version: String,
    #[arg(
        long,
        help = "Release tag that owns the assets; defaults to weaver-v<version>"
    )]
    tag: Option<String>,
    #[arg(long, default_value = "scryer-media/weaver")]
    repository: String,
    #[arg(long, default_value = "release-artifacts")]
    artifacts_dir: PathBuf,
    #[arg(long, help = "Where to write the manifest")]
    output: PathBuf,
}

/// Which generation a run writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum UpgradeManifestGeneration {
    V1,
    V2,
}

/// One release asset the manifest describes, and how to classify it.
struct UpgradeManifestAssetSpec {
    platform: &'static str,
    arch: &'static str,
    channel: &'static str,
    archive: &'static str,
    /// The asset filename for an architecture, e.g. `weaver-linux-arm64-portable.tar.gz`.
    asset_name: fn(&str) -> String,
}

/// The assets both generations describe.
///
/// Every layout an installed Weaver can replace in place appears here: the
/// portable tarballs, the Windows portable tarball the helper swaps executables
/// from, and the MSI a direct Windows install upgrades through.
const UPGRADE_MANIFEST_ASSETS: &[UpgradeManifestAssetSpec] = &[
    UpgradeManifestAssetSpec {
        platform: "linux",
        arch: "x86_64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-linux-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "linux",
        arch: "arm64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-linux-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "darwin",
        arch: "x86_64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-darwin-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "darwin",
        arch: "arm64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-darwin-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "windows",
        arch: "x86_64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-windows-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "windows",
        arch: "arm64",
        channel: "portable",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-windows-{arch}-portable.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "windows",
        arch: "x86_64",
        channel: "msi",
        archive: "msi",
        asset_name: |arch| format!("weaver-windows-{arch}.msi"),
    },
    UpgradeManifestAssetSpec {
        platform: "windows",
        arch: "arm64",
        channel: "msi",
        archive: "msi",
        asset_name: |arch| format!("weaver-windows-{arch}.msi"),
    },
];

/// Assets only v2 describes.
///
/// The macOS application bundle is upgraded by replacing the whole `.app`, which
/// is a channel v1's frozen parser has never heard of — so it appears only here.
/// A v1 manifest that carried one would be rejected outright by every shipped
/// build, which is what the `the_v1_manifest_never_carries_v2_only_artifacts`
/// test guards.
const UPGRADE_MANIFEST_V2_ONLY_ASSETS: &[UpgradeManifestAssetSpec] = &[
    UpgradeManifestAssetSpec {
        platform: "darwin",
        arch: "x86_64",
        channel: "app",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-darwin-{arch}.app.tar.gz"),
    },
    UpgradeManifestAssetSpec {
        platform: "darwin",
        arch: "arm64",
        channel: "app",
        archive: "tar.gz",
        asset_name: |arch| format!("weaver-darwin-{arch}.app.tar.gz"),
    },
];

#[derive(Debug, Serialize)]
struct UpgradeManifest {
    schema: String,
    tag: String,
    version: String,
    artifacts: Vec<UpgradeArtifact>,
}

#[derive(Debug, Serialize)]
struct UpgradeArtifact {
    platform: String,
    arch: String,
    channel: String,
    asset_name: String,
    url: String,
    size: u64,
    blake3: String,
    archive: String,
    members: Vec<UpgradeArtifactMember>,
}

#[derive(Debug, Serialize)]
struct UpgradeArtifactMember {
    path: String,
    size: u64,
    executable: bool,
}

pub(crate) fn run_upgrade_manifest(args: UpgradeManifestArgs) -> Result<()> {
    let tag = args
        .tag
        .clone()
        .unwrap_or_else(|| format!("weaver-v{}", args.version));
    let manifest = generate_upgrade_manifest(
        args.schema,
        &args.version,
        &tag,
        &args.repository,
        &args.artifacts_dir,
    )?;
    let mut encoded =
        serde_json::to_string_pretty(&manifest).context("failed to encode the upgrade manifest")?;
    encoded.push('\n');
    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create manifest directory {}", parent.display()))?;
    }
    std::fs::write(&args.output, encoded)
        .with_context(|| format!("failed to write {}", args.output.display()))?;
    println!(
        "wrote {} ({} artifacts) to {}",
        manifest.schema,
        manifest.artifacts.len(),
        args.output.display()
    );
    Ok(())
}

/// Build one generation's manifest from the assets on disk.
fn generate_upgrade_manifest(
    generation: UpgradeManifestGeneration,
    version: &str,
    tag: &str,
    repository: &str,
    artifacts_dir: &Path,
) -> Result<UpgradeManifest> {
    let specs = UPGRADE_MANIFEST_ASSETS.iter().chain(
        // v1 is frozen: it never carries an artifact naming a channel its parser
        // does not know.
        match generation {
            UpgradeManifestGeneration::V1 => [].iter(),
            UpgradeManifestGeneration::V2 => UPGRADE_MANIFEST_V2_ONLY_ASSETS.iter(),
        },
    );
    let mut artifacts = Vec::new();
    for spec in specs {
        artifacts.push(collect_upgrade_manifest_artifact(
            spec,
            tag,
            repository,
            artifacts_dir,
        )?);
    }
    artifacts.sort_by(|left, right| {
        upgrade_manifest_artifact_sort_key(left).cmp(&upgrade_manifest_artifact_sort_key(right))
    });
    Ok(UpgradeManifest {
        schema: match generation {
            UpgradeManifestGeneration::V1 => UPGRADE_MANIFEST_SCHEMA_V1,
            UpgradeManifestGeneration::V2 => UPGRADE_MANIFEST_SCHEMA_V2,
        }
        .to_string(),
        tag: tag.to_string(),
        version: version.to_string(),
        artifacts,
    })
}

/// Total order over artifacts, so generation is deterministic.
fn upgrade_manifest_artifact_sort_key(artifact: &UpgradeArtifact) -> (&str, &str, &str) {
    (
        artifact.platform.as_str(),
        artifact.arch.as_str(),
        artifact.channel.as_str(),
    )
}

fn collect_upgrade_manifest_artifact(
    spec: &UpgradeManifestAssetSpec,
    tag: &str,
    repository: &str,
    artifacts_dir: &Path,
) -> Result<UpgradeArtifact> {
    let asset_name = (spec.asset_name)(spec.arch);
    let path = artifacts_dir.join(&asset_name);
    let metadata = std::fs::metadata(&path).with_context(|| {
        format!(
            "release asset {} is missing from {}",
            asset_name,
            artifacts_dir.display()
        )
    })?;
    if !metadata.is_file() {
        bail!("release asset {asset_name} is not a regular file");
    }
    let members = if spec.archive == "tar.gz" {
        collect_tar_gz_members(&path)?
    } else {
        Vec::new()
    };
    Ok(UpgradeArtifact {
        platform: spec.platform.to_string(),
        arch: spec.arch.to_string(),
        channel: spec.channel.to_string(),
        url: format!("https://github.com/{repository}/releases/download/{tag}/{asset_name}"),
        asset_name,
        size: metadata.len(),
        blake3: blake3_of(&path)?,
        archive: spec.archive.to_string(),
        members,
    })
}

fn blake3_of(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Every regular file in a `.tar.gz`, sorted by path.
///
/// Links, devices, fifos and sockets are refused rather than skipped: the
/// installed build extracts this archive over its own installation, and a member
/// it cannot describe is a member it must not be asked to trust. Directory
/// entries carry no content and are not described.
fn collect_tar_gz_members(path: &Path) -> Result<Vec<UpgradeArtifactMember>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let decoder = flate2::read::GzDecoder::new(BufReader::new(file));
    let mut archive = tar::Archive::new(decoder);
    let mut members = Vec::new();
    for entry in archive
        .entries()
        .with_context(|| format!("failed to read {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read an entry of {}", path.display()))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            continue;
        }
        if !entry_type.is_file() {
            bail!(
                "{} contains a non-regular member ({entry_type:?}); upgrade archives must contain \
                 only regular files and directories",
                path.display()
            );
        }
        let member_path = archive_member_path(&entry.path().with_context(|| {
            format!(
                "{} contains a member with an unreadable path",
                path.display()
            )
        })?)?;
        let size = entry.header().size().with_context(|| {
            format!(
                "{} contains a member with an unreadable size",
                path.display()
            )
        })?;
        if size > MAX_ARCHIVE_MEMBER_BYTES {
            bail!(
                "{} contains a member larger than the manifest cap",
                path.display()
            );
        }
        let mode = entry.header().mode().unwrap_or(0o644);
        members.push(UpgradeArtifactMember {
            path: member_path,
            size,
            executable: mode & 0o111 != 0,
        });
    }
    sort_and_validate_archive_members(members, path)
}

/// Sort members by path and refuse a duplicate.
///
/// A duplicate path means the extraction order decides what is installed, which
/// is not something a signed manifest may leave open.
fn sort_and_validate_archive_members(
    mut members: Vec<UpgradeArtifactMember>,
    path: &Path,
) -> Result<Vec<UpgradeArtifactMember>> {
    members.sort_by(|left, right| left.path.cmp(&right.path));
    let mut seen = BTreeSet::new();
    for member in &members {
        if !seen.insert(member.path.clone()) {
            bail!(
                "{} contains the member '{}' more than once",
                path.display(),
                member.path
            );
        }
    }
    Ok(members)
}

/// Normalize an archive member path the way the installed build does.
///
/// The running build refuses an absolute, escaping or non-UTF-8 member path, so
/// this refuses the same shapes rather than publishing a manifest describing an
/// archive that can never be extracted. `./x` normalizes to `x`, which is what
/// `tar -C dir .` writes.
fn archive_member_path(path: &Path) -> Result<String> {
    let raw = path.to_string_lossy();
    let windows_drive_prefix = raw.as_bytes().get(1) == Some(&b':')
        && raw
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_alphabetic());
    if path.is_absolute() || raw.contains('\\') || windows_drive_prefix {
        bail!("upgrade archive contains an absolute member path '{raw}'");
    }
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => {
                let component = component
                    .to_str()
                    .with_context(|| format!("member path '{raw}' is not valid UTF-8"))?;
                components.push(component.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("upgrade archive contains an unsafe member path '{raw}'");
            }
        }
    }
    if components.is_empty() {
        bail!("upgrade archive contains an empty member path");
    }
    Ok(components.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic release assets: byte lengths differ per asset so a manifest
    /// that mixed two up could not pass.
    fn write_fixture_assets(dir: &Path) {
        std::fs::create_dir_all(dir).expect("create fixture directory");
        for spec in UPGRADE_MANIFEST_ASSETS
            .iter()
            .chain(UPGRADE_MANIFEST_V2_ONLY_ASSETS.iter())
        {
            let asset_name = (spec.asset_name)(spec.arch);
            let path = dir.join(&asset_name);
            if spec.archive == "msi" {
                std::fs::write(&path, format!("msi payload for {asset_name}"))
                    .expect("write msi fixture");
                continue;
            }
            let members = fixture_members(spec, &asset_name);
            write_tar_gz(&path, &members);
        }
    }

    /// The archive layout each channel actually ships.
    fn fixture_members(
        spec: &UpgradeManifestAssetSpec,
        asset_name: &str,
    ) -> Vec<(String, Vec<u8>, u32, bool)> {
        let body = |name: &str| format!("{name} content for {asset_name}").into_bytes();
        match (spec.platform, spec.channel) {
            ("windows", "portable") => vec![
                ("weaver.exe".to_string(), body("weaver.exe"), 0o755, false),
                (
                    "weaver-tray.exe".to_string(),
                    body("weaver-tray.exe"),
                    0o755,
                    false,
                ),
            ],
            ("darwin", "app") => vec![
                ("Weaver.app/".to_string(), Vec::new(), 0o755, true),
                ("Weaver.app/Contents/".to_string(), Vec::new(), 0o755, true),
                (
                    "Weaver.app/Contents/Info.plist".to_string(),
                    body("Info.plist"),
                    0o644,
                    false,
                ),
                (
                    "Weaver.app/Contents/MacOS/".to_string(),
                    Vec::new(),
                    0o755,
                    true,
                ),
                (
                    "Weaver.app/Contents/MacOS/weaver".to_string(),
                    body("weaver"),
                    0o755,
                    false,
                ),
                (
                    "Weaver.app/Contents/MacOS/weaver-tray".to_string(),
                    body("weaver-tray"),
                    0o755,
                    false,
                ),
            ],
            _ => vec![("weaver".to_string(), body("weaver"), 0o755, false)],
        }
    }

    fn write_tar_gz(path: &Path, members: &[(String, Vec<u8>, u32, bool)]) {
        let file = File::create(path).expect("create archive");
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        for (member_path, bytes, mode, is_dir) in members {
            let mut header = tar::Header::new_gnu();
            header.set_path(member_path).expect("set member path");
            header.set_mode(*mode);
            header.set_size(bytes.len() as u64);
            header.set_entry_type(if *is_dir {
                tar::EntryType::Directory
            } else {
                tar::EntryType::Regular
            });
            header.set_cksum();
            builder
                .append(&header, bytes.as_slice())
                .expect("append member");
        }
        builder
            .into_inner()
            .expect("finish archive")
            .finish()
            .expect("finish gzip");
    }

    fn generate(generation: UpgradeManifestGeneration, dir: &Path) -> UpgradeManifest {
        generate_upgrade_manifest(
            generation,
            "9.8.7",
            "weaver-v9.8.7",
            "scryer-media/weaver",
            dir,
        )
        .expect("generate manifest")
    }

    fn encode(manifest: &UpgradeManifest) -> String {
        let mut encoded = serde_json::to_string_pretty(manifest).expect("encode manifest");
        encoded.push('\n');
        encoded
    }

    /// Set to `1` to rewrite the committed examples from the fixtures instead of
    /// comparing against them. The examples are generated artefacts, but they are
    /// committed because the running build's manifest parser test reads them as
    /// its own fixture — so regenerating is a deliberate, reviewable edit.
    const REGENERATE_ENV: &str = "WEAVER_REGENERATE_UPGRADE_MANIFEST_EXAMPLES";

    fn example_path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("api")
            .join("upgrade")
            .join(name)
    }

    /// The committed example, or the generated one after rewriting it.
    fn golden(name: &str, generated: &str) -> String {
        let path = example_path(name);
        if std::env::var(REGENERATE_ENV).as_deref() == Ok("1") {
            std::fs::create_dir_all(path.parent().expect("example directory"))
                .expect("create example directory");
            std::fs::write(&path, generated).expect("rewrite example");
            return generated.to_string();
        }
        std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("read {}: {error}", path.display());
        })
    }

    /// The committed example is the contract the running build's parser test
    /// reads. Regenerate it with:
    /// `cargo run -p xtask -- ci upgrade-manifest --schema v1 …`
    #[test]
    fn v1_generation_is_deterministic_and_matches_the_committed_example() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        let first = encode(&generate(UpgradeManifestGeneration::V1, temp.path()));
        let second = encode(&generate(UpgradeManifestGeneration::V1, temp.path()));
        assert_eq!(first, second, "generation must be deterministic");
        assert_eq!(first, golden("manifest.v1.example.json", &first));
    }

    #[test]
    fn v2_generation_is_deterministic_and_matches_the_committed_example() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        let first = encode(&generate(UpgradeManifestGeneration::V2, temp.path()));
        let second = encode(&generate(UpgradeManifestGeneration::V2, temp.path()));
        assert_eq!(first, second, "generation must be deterministic");
        assert_eq!(first, golden("manifest.v2.example.json", &first));
    }

    /// v1's parser is frozen and rejects an unknown channel outright, so a v1
    /// manifest carrying the bundle artifact would make every shipped build
    /// unable to read the release at all.
    #[test]
    fn the_v1_manifest_never_carries_v2_only_artifacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        let v1 = generate(UpgradeManifestGeneration::V1, temp.path());
        assert!(
            v1.artifacts
                .iter()
                .all(|artifact| artifact.channel != "app"),
            "v1 must not describe the application-bundle channel"
        );
        let v2 = generate(UpgradeManifestGeneration::V2, temp.path());
        assert_eq!(v1.artifacts.len() + 2, v2.artifacts.len());
    }

    /// The bundle archive is rooted at `Weaver.app/`, because the installed
    /// build promotes the staged bundle by that exact name.
    #[test]
    fn the_bundle_archive_is_described_rooted_at_the_app_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        let v2 = generate(UpgradeManifestGeneration::V2, temp.path());
        let bundle = v2
            .artifacts
            .iter()
            .find(|artifact| artifact.channel == "app")
            .expect("v2 describes the bundle channel");
        assert!(!bundle.members.is_empty());
        for member in &bundle.members {
            assert!(
                member.path.starts_with("Weaver.app/"),
                "{} is not inside the bundle",
                member.path
            );
        }
        assert!(
            bundle.members.iter().any(|member| member.path
                == "Weaver.app/Contents/MacOS/weaver-tray"
                && member.executable),
            "the wrapper that relaunches the replaced bundle must be in it"
        );
    }

    /// A symlink in an upgrade archive is refused at generation time rather than
    /// published for an installed build to discover.
    #[test]
    fn a_link_member_is_refused_rather_than_described() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        let path = temp.path().join("weaver-linux-x86_64-portable.tar.gz");
        let file = File::create(&path).expect("create archive");
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path("weaver").expect("set member path");
        header.set_link_name("/bin/sh").expect("set link target");
        header.set_size(0);
        header.set_mode(0o777);
        header.set_cksum();
        builder.append(&header, &[][..]).expect("append link");
        builder
            .into_inner()
            .expect("finish archive")
            .finish()
            .expect("finish gzip");

        let error = generate_upgrade_manifest(
            UpgradeManifestGeneration::V1,
            "9.8.7",
            "weaver-v9.8.7",
            "scryer-media/weaver",
            temp.path(),
        )
        .expect_err("a link member is refused");
        assert!(error.to_string().contains("non-regular member"), "{error}");
    }

    /// Escaping and absolute member paths are refused, so a signed manifest can
    /// never describe an extraction outside the install directory.
    #[test]
    fn unsafe_member_paths_are_refused() {
        for raw in ["/etc/passwd", "../weaver", "C:\\weaver.exe", "dir\\weaver"] {
            archive_member_path(Path::new(raw)).expect_err(&format!("{raw} must be refused"));
        }
        assert_eq!(
            archive_member_path(Path::new("./weaver")).expect("./x normalizes"),
            "weaver"
        );
        assert_eq!(
            archive_member_path(Path::new("Weaver.app/Contents/MacOS/weaver"))
                .expect("nested paths are kept"),
            "Weaver.app/Contents/MacOS/weaver"
        );
    }

    /// A missing asset fails the release rather than publishing a manifest that
    /// silently omits a platform.
    #[test]
    fn a_missing_asset_fails_generation() {
        let temp = tempfile::tempdir().expect("tempdir");
        write_fixture_assets(temp.path());
        std::fs::remove_file(temp.path().join("weaver-windows-arm64.msi"))
            .expect("remove one asset");
        let error = generate_upgrade_manifest(
            UpgradeManifestGeneration::V1,
            "9.8.7",
            "weaver-v9.8.7",
            "scryer-media/weaver",
            temp.path(),
        )
        .expect_err("a missing asset is fatal");
        assert!(
            error.to_string().contains("weaver-windows-arm64.msi"),
            "{error}"
        );
    }

    /// Every asset the manifests name is distinct, so no two artifacts can be
    /// hashed from the same file.
    #[test]
    fn every_described_asset_name_is_distinct() {
        let mut seen = BTreeSet::new();
        for spec in UPGRADE_MANIFEST_ASSETS
            .iter()
            .chain(UPGRADE_MANIFEST_V2_ONLY_ASSETS.iter())
        {
            let asset_name = (spec.asset_name)(spec.arch);
            assert!(
                seen.insert(asset_name.clone()),
                "duplicate asset {asset_name}"
            );
        }
        assert_eq!(seen.len(), 10);
    }
}
