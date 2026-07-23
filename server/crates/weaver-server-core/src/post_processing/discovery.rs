use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::manifest::{
    BareScriptAdapter, ManifestError, detect_bare_script_adapter, parse_native_manifest,
    parse_nzbget_manifest,
};
use super::model::{
    DeclaredExtensionVersion, ExtensionAdapter, ExtensionDigest, ExtensionId, ExtensionManifest,
    ExtensionRevision, NzbgetCompatibilityName, VerifiedExtensionDigest,
};
use crate::persistence::Database;

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
pub(crate) const MAX_PACKAGE_ENTRIES: usize = 10_000;
pub(crate) const MAX_PACKAGE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub struct DiscoveryOptions {
    pub enabled: bool,
    pub bare_script_adapter: Option<BareScriptAdapter>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveredExtension {
    pub manifest: ExtensionManifest,
    pub source_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("post-processing extension discovery is disabled")]
    Disabled,
    #[error("extension package was not found")]
    NotFound,
    #[error("extension package changed after discovery")]
    PackageChanged,
    #[error("extension package contains a symbolic link or unsupported file type")]
    UnsafePackageEntry,
    #[error("extension package exceeds the discovery limit")]
    PackageTooLarge,
    #[error("extension package path is not valid UTF-8")]
    InvalidPath,
    #[error("extension manifest is invalid: {0}")]
    Manifest(#[from] ManifestError),
    #[error("extension package is invalid: {0}")]
    Validation(#[from] super::model::PostProcessingValidationError),
    #[error("extension discovery I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("extension persistence failed: {0}")]
    Persistence(#[from] crate::StateError),
}

pub fn discover_extensions(
    data_dir: &Path,
    options: DiscoveryOptions,
) -> Result<Vec<DiscoveredExtension>, DiscoveryError> {
    if !options.enabled {
        return Err(DiscoveryError::Disabled);
    }
    let scripts_dir = data_dir.join("scripts");
    if !scripts_dir.exists() {
        fs::create_dir_all(&scripts_dir)?;
        return Ok(vec![]);
    }
    let mut entries = fs::read_dir(&scripts_dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut discovered = Vec::new();
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            if let Some(extension) = discover_manifest_package(&path)? {
                discovered.push(extension);
            }
        } else if metadata.is_file() && is_bare_script_candidate(&path) {
            discovered.push(discover_bare_script(&path, options.bare_script_adapter)?);
        }
    }
    Ok(discovered)
}

pub fn discover_and_record_extensions(
    db: &Database,
    data_dir: &Path,
    options: DiscoveryOptions,
    now_epoch_ms: i64,
) -> Result<Vec<DiscoveredExtension>, DiscoveryError> {
    let extensions = discover_extensions(data_dir, options)?;
    for extension in &extensions {
        db.upsert_discovered_extension(
            &extension.manifest,
            extension.source_path.to_str(),
            now_epoch_ms,
        )?;
    }
    Ok(extensions)
}

pub fn approve_discovered_extension(
    db: &Database,
    data_dir: &Path,
    extension_id: &ExtensionId,
    revision_id: &super::model::ExtensionRevisionId,
    now_epoch_ms: i64,
) -> Result<PathBuf, DiscoveryError> {
    let record = db
        .extension_revision(extension_id, revision_id)?
        .ok_or(DiscoveryError::NotFound)?;
    let source = PathBuf::from(record.source_path.ok_or(DiscoveryError::NotFound)?);
    let expected = record.manifest.revision().digest();
    if hash_package(&source)?.as_str() != expected.as_str() {
        return Err(DiscoveryError::PackageChanged);
    }

    let (_, digest_hex) = expected
        .as_str()
        .split_once(':')
        .ok_or(DiscoveryError::PackageChanged)?;
    let managed = data_dir
        .join("managed-extensions")
        .join("blake3")
        .join(digest_hex);
    if managed.exists() {
        if hash_package(&managed)?.as_str() != expected.as_str() {
            return Err(DiscoveryError::PackageChanged);
        }
    } else {
        let parent = managed.parent().ok_or(DiscoveryError::InvalidPath)?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".{digest_hex}.tmp-{}", std::process::id()));
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;
        let copy_result = copy_package(&source, &temporary);
        if let Err(error) = copy_result {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
        if hash_package(&temporary)?.as_str() != expected.as_str() {
            let _ = fs::remove_dir_all(&temporary);
            return Err(DiscoveryError::PackageChanged);
        }
        match fs::rename(&temporary, &managed) {
            Ok(()) => {}
            Err(error) if managed.exists() => {
                let _ = fs::remove_dir_all(&temporary);
                if hash_package(&managed)?.as_str() != expected.as_str() {
                    return Err(DiscoveryError::Io(error));
                }
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&temporary);
                return Err(DiscoveryError::Io(error));
            }
        }
    }

    let managed_text = managed.to_str().ok_or(DiscoveryError::InvalidPath)?;
    if !db.approve_extension_revision(extension_id, revision_id, managed_text, now_epoch_ms)? {
        return Err(DiscoveryError::NotFound);
    }
    Ok(managed)
}

fn discover_manifest_package(path: &Path) -> Result<Option<DiscoveredExtension>, DiscoveryError> {
    let native_path = path.join("weaver-extension.json");
    let nzbget_path = path.join("manifest.json");
    let (manifest_path, native) = match (native_path.is_file(), nzbget_path.is_file()) {
        (true, false) => (native_path, true),
        (false, true) => (nzbget_path, false),
        (false, false) => return Ok(None),
        (true, true) => return Err(ManifestError::ShapeConflict.into()),
    };
    reject_symlinks(path)?;
    let digest = hash_package(path)?;
    let input = read_utf8_limited(&manifest_path, MAX_MANIFEST_BYTES)?;
    let verified = VerifiedExtensionDigest::from_verified_package_digest(digest);
    let manifest = if native {
        parse_native_manifest(&input, verified)?
    } else {
        parse_nzbget_manifest(&input, verified)?
    };
    Ok(Some(DiscoveredExtension {
        manifest,
        source_path: path.to_path_buf(),
    }))
}

fn discover_bare_script(
    path: &Path,
    adapter_override: Option<BareScriptAdapter>,
) -> Result<DiscoveredExtension, DiscoveryError> {
    let digest = hash_package(path)?;
    let preamble = read_utf8_prefix(path, 8 * 1024)?;
    let adapter = detect_bare_script_adapter(&preamble, adapter_override);
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(DiscoveryError::InvalidPath)?;
    let readable = normalized_name(
        path.file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("script"),
    );
    let digest_hex = digest.as_str().split_once(':').expect("validated digest").1;
    let namespace = match adapter {
        ExtensionAdapter::Nzbget => "nzbget",
        ExtensionAdapter::Sabnzbd => "sabnzbd",
        ExtensionAdapter::Native | ExtensionAdapter::Webhook => {
            unreachable!("bare scripts only use SABnzbd or NZBGet adapters")
        }
    };
    let extension_id = ExtensionId::new(format!("{namespace}.{readable}-{}", &digest_hex[..12]))?;
    let compatibility_name = if adapter == ExtensionAdapter::Nzbget {
        Some(NzbgetCompatibilityName::new(filename.to_string())?)
    } else {
        None
    };
    let revision = ExtensionRevision::from_verified(
        extension_id,
        DeclaredExtensionVersion::new("bare-script")?,
        VerifiedExtensionDigest::from_verified_package_digest(digest),
    );
    let manifest = ExtensionManifest::new(
        adapter,
        compatibility_name,
        filename.to_string(),
        revision,
        filename.to_string(),
        vec![],
        vec![],
        vec![],
    )?;
    Ok(DiscoveredExtension {
        manifest,
        source_path: path.to_path_buf(),
    })
}

fn is_bare_script_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "sh" | "bash" | "py" | "pl" | "rb" | "ps1" | "bat" | "cmd" | "exe"
            )
        })
}

fn normalized_name(value: &str) -> String {
    let mut normalized = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            normalized.push(character.to_ascii_lowercase());
            separator = false;
        } else if !separator && !normalized.is_empty() {
            normalized.push('-');
            separator = true;
        }
    }
    let normalized = normalized.trim_matches('-');
    if normalized.is_empty() {
        "script".into()
    } else {
        normalized.chars().take(48).collect()
    }
}

fn read_utf8_limited(path: &Path, limit: u64) -> Result<String, DiscoveryError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > limit {
        return Err(DiscoveryError::PackageTooLarge);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(DiscoveryError::PackageTooLarge);
    }
    String::from_utf8(bytes).map_err(|_| DiscoveryError::InvalidPath)
}

fn read_utf8_prefix(path: &Path, limit: u64) -> Result<String, DiscoveryError> {
    let mut bytes = Vec::with_capacity(limit as usize);
    File::open(path)?.take(limit).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

pub(crate) fn verify_managed_extension(
    path: &Path,
    expected: &ExtensionDigest,
) -> Result<(), DiscoveryError> {
    if hash_package(path)?.as_str() != expected.as_str() {
        return Err(DiscoveryError::PackageChanged);
    }
    Ok(())
}

pub(crate) fn hash_package(path: &Path) -> Result<ExtensionDigest, DiscoveryError> {
    let mut entries = package_files(path)?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = blake3::Hasher::new();
    let mut total_bytes = 0_u64;
    for (relative, absolute) in entries {
        let relative = relative.to_str().ok_or(DiscoveryError::InvalidPath)?;
        let metadata = fs::metadata(&absolute)?;
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .ok_or(DiscoveryError::PackageTooLarge)?;
        if total_bytes > MAX_PACKAGE_BYTES {
            return Err(DiscoveryError::PackageTooLarge);
        }
        hasher.update(&(relative.len() as u64).to_le_bytes());
        hasher.update(relative.as_bytes());
        hasher.update(&metadata.len().to_le_bytes());
        let mut file = File::open(absolute)?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    ExtensionDigest::new(format!("blake3:{}", hasher.finalize().to_hex())).map_err(Into::into)
}

pub(crate) fn package_files(path: &Path) -> Result<Vec<(PathBuf, PathBuf)>, DiscoveryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(DiscoveryError::UnsafePackageEntry);
    }
    if metadata.is_file() {
        let name = path.file_name().ok_or(DiscoveryError::InvalidPath)?;
        return Ok(vec![(PathBuf::from(name), path.to_path_buf())]);
    }
    if !metadata.is_dir() {
        return Err(DiscoveryError::UnsafePackageEntry);
    }
    let mut out = Vec::new();
    collect_files(path, path, &mut out)?;
    Ok(out)
}

fn collect_files(
    root: &Path,
    directory: &Path,
    out: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), DiscoveryError> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        if out.len() >= MAX_PACKAGE_ENTRIES {
            return Err(DiscoveryError::PackageTooLarge);
        }
        let absolute = entry.path();
        let metadata = fs::symlink_metadata(&absolute)?;
        if metadata.file_type().is_symlink() {
            return Err(DiscoveryError::UnsafePackageEntry);
        }
        if metadata.is_dir() {
            collect_files(root, &absolute, out)?;
        } else if metadata.is_file() {
            let relative = absolute
                .strip_prefix(root)
                .map_err(|_| DiscoveryError::InvalidPath)?
                .to_path_buf();
            out.push((relative, absolute));
        } else {
            return Err(DiscoveryError::UnsafePackageEntry);
        }
    }
    Ok(())
}

fn reject_symlinks(path: &Path) -> Result<(), DiscoveryError> {
    package_files(path).map(|_| ())
}

pub(crate) fn copy_package(source: &Path, destination: &Path) -> Result<(), DiscoveryError> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.is_file() {
        let name = source.file_name().ok_or(DiscoveryError::InvalidPath)?;
        fs::copy(source, destination.join(name))?;
        return Ok(());
    }
    for (relative, absolute) in package_files(source)? {
        let target = destination.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(absolute, target)?;
    }
    Ok(())
}
