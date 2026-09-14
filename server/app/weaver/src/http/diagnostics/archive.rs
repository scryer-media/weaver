//! The tar + zstd writer and the manifest that describes what went in.
//!
//! Components are collected before the archive is written, each one carrying
//! either its bytes or the error that stopped it. A component that failed is
//! still *named* in the manifest with its error, because "the pipeline snapshot
//! is missing" and "the pipeline snapshot was never asked for" are different
//! bug reports, and the archive is the only place that difference survives.

use std::io::Write;

use serde::{Deserialize, Serialize};

/// zstd level 10: roughly an order of magnitude smaller than the raw log text
/// for a few hundred milliseconds of CPU on the sizes this package carries.
const ZSTD_LEVEL: i32 = 10;

/// One collected file, or the reason there is no file.
pub(super) struct Component {
    pub(super) name: String,
    pub(super) outcome: Result<Vec<u8>, String>,
}

impl Component {
    pub(super) fn bytes(name: impl Into<String>, data: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            outcome: Ok(data),
        }
    }

    pub(super) fn text(name: impl Into<String>, data: impl Into<String>) -> Self {
        Self::bytes(name, data.into().into_bytes())
    }

    pub(super) fn failed(name: impl Into<String>, error: impl std::fmt::Display) -> Self {
        Self {
            name: name.into(),
            outcome: Err(error.to_string()),
        }
    }

    /// Serializes a value, turning a serialization failure into the component's
    /// error rather than into a failure of the whole package.
    pub(super) fn json<T: Serialize>(name: impl Into<String>, value: &T) -> Self {
        let name = name.into();
        match serde_json::to_vec_pretty(value) {
            Ok(data) => Self::bytes(name, data),
            Err(error) => Self::failed(name, error),
        }
    }
}

/// What the archive says about itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) weaver_version: String,
    pub(super) generated_at_utc: String,
    pub(super) files: Vec<ManifestFile>,
    pub(super) errors: Vec<ManifestError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ManifestFile {
    pub(super) name: String,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ManifestError {
    pub(super) name: String,
    pub(super) error: String,
}

/// The archive name, which is also the download filename.
pub(super) fn archive_filename(generated_at_utc: &str) -> String {
    // `:` is not a legal filename character on Windows, and the desktop
    // wrapper's save dialog is the primary destination for this download.
    let stamp = generated_at_utc.replace([':'], "-");
    format!("weaver-diagnostics-{stamp}.tar.zst")
}

/// Writes every component into one zstd-compressed tar stream, appending a
/// manifest that lists what made it in and what did not.
///
/// The manifest is written last and describes itself as well, so a reader can
/// check the listing against the entries without knowing the write order.
pub(super) fn build_archive(
    components: Vec<Component>,
    weaver_version: &str,
    generated_at_utc: &str,
) -> std::io::Result<Vec<u8>> {
    let mut files = Vec::new();
    let mut errors = Vec::new();
    let mut payloads = Vec::new();

    for component in components {
        match component.outcome {
            Ok(data) => {
                files.push(ManifestFile {
                    name: component.name.clone(),
                    bytes: data.len() as u64,
                });
                payloads.push((component.name, data));
            }
            Err(error) => errors.push(ManifestError {
                name: component.name,
                error,
            }),
        }
    }

    let mut manifest = Manifest {
        weaver_version: weaver_version.to_string(),
        generated_at_utc: generated_at_utc.to_string(),
        files,
        errors,
    };
    // The manifest lists itself, so its own size has to be known before it is
    // serialized. Reserve the entry with a placeholder, serialize, then correct
    // the size in a second pass — the listing is short and the second
    // serialization cannot change its own length, because only a numeric field
    // moves and it is rewritten to the value that makes it true.
    manifest.files.push(ManifestFile {
        name: MANIFEST_NAME.to_string(),
        bytes: 0,
    });
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        std::io::Error::other(format!("manifest serialization failed: {error}"))
    })?;
    for _ in 0..4 {
        let measured = manifest_bytes.len() as u64;
        if let Some(entry) = manifest
            .files
            .iter_mut()
            .find(|entry| entry.name == MANIFEST_NAME)
        {
            if entry.bytes == measured {
                break;
            }
            entry.bytes = measured;
        }
        manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            std::io::Error::other(format!("manifest serialization failed: {error}"))
        })?;
    }
    payloads.push((MANIFEST_NAME.to_string(), manifest_bytes));

    let encoder = zstd::Encoder::new(Vec::new(), ZSTD_LEVEL)?;
    let mut tar = tar::Builder::new(encoder);
    for (name, data) in payloads {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_cksum();
        tar.append_data(&mut header, &name, data.as_slice())?;
    }
    let encoder = tar.into_inner()?;
    let mut buffer = encoder.finish()?;
    buffer.flush()?;
    Ok(buffer)
}

pub(super) const MANIFEST_NAME: &str = "manifest.json";

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn unpack(archive: &[u8]) -> Vec<(String, Vec<u8>)> {
        let decoded = zstd::decode_all(archive).expect("archive decompresses");
        let mut reader = tar::Archive::new(decoded.as_slice());
        reader
            .entries()
            .expect("entries")
            .map(|entry| {
                let mut entry = entry.expect("entry");
                let name = entry.path().expect("path").display().to_string();
                let mut data = Vec::new();
                entry.read_to_end(&mut data).expect("entry body");
                (name, data)
            })
            .collect()
    }

    #[test]
    fn archive_round_trips_components_and_manifest() {
        let archive = build_archive(
            vec![
                Component::text("metrics-1.txt", "weaver_build_info 1\n"),
                Component::json("host.json", &serde_json::json!({ "arch": "test-arch" })),
                Component::failed("pipeline-internals.json", "scheduler channel closed"),
            ],
            "0.0.0-test",
            "2000-01-01T00:00:00Z",
        )
        .expect("archive builds");

        let entries = unpack(&archive);
        let names: Vec<&str> = entries.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec!["metrics-1.txt", "host.json", "manifest.json"],
            "a failed component contributes no entry"
        );

        let manifest_bytes = &entries
            .iter()
            .find(|(name, _)| name == MANIFEST_NAME)
            .expect("manifest entry")
            .1;
        let manifest: Manifest = serde_json::from_slice(manifest_bytes).expect("manifest parses");

        assert_eq!(manifest.weaver_version, "0.0.0-test");
        assert_eq!(manifest.generated_at_utc, "2000-01-01T00:00:00Z");
        assert_eq!(manifest.errors.len(), 1);
        assert_eq!(manifest.errors[0].name, "pipeline-internals.json");
        assert!(manifest.errors[0].error.contains("channel closed"));

        for (name, data) in &entries {
            let listed = manifest
                .files
                .iter()
                .find(|entry| &entry.name == name)
                .unwrap_or_else(|| panic!("{name} is listed in the manifest"));
            assert_eq!(listed.bytes, data.len() as u64, "{name}");
        }
        assert_eq!(manifest.files.len(), entries.len());
    }

    #[test]
    fn archive_filename_is_safe_on_every_platform() {
        let filename = archive_filename("2026-09-11T04:05:06Z");
        assert_eq!(filename, "weaver-diagnostics-2026-09-11T04-05-06Z.tar.zst");
        assert!(!filename.contains(':'));
    }
}
