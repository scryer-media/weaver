//! Live listing of `data_dir/scripts`.
//!
//! Nothing here is persisted: a script is whatever is in the directory when the
//! listing runs, which is also when execution resolves it. Editing or renaming a
//! script is editing or renaming a script.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::manifest::{
    ManifestError, NZBGET_MANIFEST_FILE, detect_bare_script_adapter, parse_nzbget_manifest,
};
use super::model::{PostProcessingValidationError, ScriptAdapter, ScriptManifest, ScriptName};

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const SHEBANG_PREFIX_BYTES: u64 = 8 * 1024;

/// The scripts directory beneath the configured data directory.
pub fn scripts_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("scripts")
}

/// A script that is present and parseable right now.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DiscoveredScript {
    pub name: ScriptName,
    /// Package directory for a manifest package, or the scripts directory for a bare script.
    pub root: PathBuf,
    pub manifest: ScriptManifest,
}

/// Something in the scripts directory that could not be listed, surfaced instead of hidden.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptProblem {
    pub name: String,
    pub message: String,
}

/// Everything the scripts directory currently offers.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ScriptListing {
    pub scripts: Vec<DiscoveredScript>,
    pub problems: Vec<ScriptProblem>,
}

#[derive(Debug, thiserror::Error)]
pub enum ListingError {
    #[error("script '{0}' was not found in the scripts directory")]
    NotFound(String),
    #[error("script manifest is invalid: {0}")]
    Manifest(#[from] ManifestError),
    #[error("script is invalid: {0}")]
    Validation(#[from] PostProcessingValidationError),
    #[error("scripts directory could not be read: {0}")]
    Io(#[from] io::Error),
}

/// List every script under `data_dir/scripts`, creating the directory when absent.
pub fn list_scripts(data_dir: &Path) -> Result<ScriptListing, ListingError> {
    let root = scripts_dir(data_dir);
    if !root.exists() {
        fs::create_dir_all(&root)?;
        return Ok(ScriptListing::default());
    }
    let mut entries = fs::read_dir(&root)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    let mut listing = ScriptListing::default();
    for entry in entries {
        let path = entry.path();
        let raw_name = entry.file_name().to_string_lossy().into_owned();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        let name = match ScriptName::new(raw_name.clone()) {
            Ok(name) => name,
            // Dotfiles and editor droppings are not scripts; only report names
            // that look intentional.
            Err(_) if raw_name.starts_with('.') => continue,
            Err(error) => {
                listing.problems.push(ScriptProblem {
                    name: raw_name,
                    message: error.to_string(),
                });
                continue;
            }
        };
        if metadata.is_dir() {
            match read_manifest_package(&path, &name) {
                Ok(Some(script)) => listing.scripts.push(script),
                Ok(None) => {}
                Err(error) => listing.problems.push(ScriptProblem {
                    name: raw_name,
                    message: error.to_string(),
                }),
            }
        } else if metadata.is_file() && is_bare_script_candidate(&path, &metadata) {
            match read_bare_script(&root, &path, &name) {
                Ok(script) => listing.scripts.push(script),
                Err(error) => listing.problems.push(ScriptProblem {
                    name: raw_name,
                    message: error.to_string(),
                }),
            }
        }
    }
    Ok(listing)
}

/// Resolve one script by name at execution time.
pub fn resolve_script(
    data_dir: &Path,
    name: &ScriptName,
) -> Result<DiscoveredScript, ListingError> {
    let root = scripts_dir(data_dir);
    let path = root.join(name.as_str());
    let metadata =
        fs::symlink_metadata(&path).map_err(|_| ListingError::NotFound(name.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(ListingError::NotFound(name.to_string()));
    }
    if metadata.is_dir() {
        return read_manifest_package(&path, name)?
            .ok_or_else(|| ListingError::NotFound(name.to_string()));
    }
    if metadata.is_file() {
        return read_bare_script(&root, &path, name);
    }
    Err(ListingError::NotFound(name.to_string()))
}

fn read_manifest_package(
    path: &Path,
    name: &ScriptName,
) -> Result<Option<DiscoveredScript>, ListingError> {
    let manifest_path = path.join(NZBGET_MANIFEST_FILE);
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let input = read_utf8_limited(&manifest_path, MAX_MANIFEST_BYTES)?;
    let manifest = parse_nzbget_manifest(&input)?;
    Ok(Some(DiscoveredScript {
        name: name.clone(),
        root: path.to_path_buf(),
        manifest,
    }))
}

fn read_bare_script(
    root: &Path,
    path: &Path,
    name: &ScriptName,
) -> Result<DiscoveredScript, ListingError> {
    let preamble = read_utf8_prefix(path, SHEBANG_PREFIX_BYTES)?;
    let adapter = detect_bare_script_adapter(&preamble);
    let compatibility_name = match adapter {
        ScriptAdapter::Nzbget => Some(super::model::NzbgetCompatibilityName::new(
            name.as_str().to_string(),
        )?),
        ScriptAdapter::Sabnzbd => None,
    };
    let manifest = ScriptManifest::new(
        adapter,
        compatibility_name,
        name.as_str().to_string(),
        None,
        name.as_str().to_string(),
        vec![],
        vec![],
    )?;
    Ok(DiscoveredScript {
        name: name.clone(),
        root: root.to_path_buf(),
        manifest,
    })
}

/// A regular file counts as a script when it carries a known script extension or
/// the executable bit, which is what both oracles list.
fn is_bare_script_candidate(path: &Path, metadata: &fs::Metadata) -> bool {
    let known_extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "sh" | "bash" | "py" | "pl" | "rb" | "ps1" | "bat" | "cmd" | "exe"
            )
        });
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        known_extension || metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        known_extension
    }
}

fn read_utf8_limited(path: &Path, limit: u64) -> Result<String, ListingError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > limit {
        return Err(ListingError::Io(io::Error::other(
            "script manifest exceeds the 1 MiB limit",
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    String::from_utf8(bytes)
        .map_err(|_| ListingError::Io(io::Error::other("script manifest is not valid UTF-8")))
}

fn read_utf8_prefix(path: &Path, limit: u64) -> Result<String, ListingError> {
    let mut bytes = Vec::with_capacity(limit as usize);
    File::open(path)?.take(limit).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
