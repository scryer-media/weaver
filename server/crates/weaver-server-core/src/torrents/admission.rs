// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Whether a torrent's metainfo may become a job, and what it is called.

use crate::post_processing::model::PostProcessingSettings;

/// A torrent as admission reads it: its name, whether it is one file, and
/// the paths of its files, padding left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub name: String,
    pub single_file: bool,
    pub paths: Vec<String>,
}

/// The torrent's job may not be made, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("{path} has an unwanted extension ({pattern})")]
    Unwanted { path: String, pattern: String },
    #[error("{0} is an executable")]
    Executable(String),
    #[error("the torrent has no name")]
    Unnamed,
}

fn extension(name: &str) -> Option<&str> {
    let base = name.rsplit(['/', '\\']).next()?;
    let (stem, extension) = base.rsplit_once('.')?;
    (!stem.is_empty() && !extension.is_empty()).then_some(extension)
}

/// Refuses a torrent whose name or any file carries an extension the
/// operator does not want, or that would be named after an executable
/// whatever the policy says.
pub fn admit(policy: &PostProcessingSettings, manifest: &Manifest) -> Result<(), Refusal> {
    if manifest.name.trim().is_empty() {
        return Err(Refusal::Unnamed);
    }
    for path in std::iter::once(&manifest.name).chain(&manifest.paths) {
        if let Some(pattern) = policy.unacceptable_extension_match(path) {
            return Err(Refusal::Unwanted {
                path: path.clone(),
                pattern: pattern.to_owned(),
            });
        }
    }
    if let Some(extension) = extension(&manifest.name)
        && weaver_nzb::delivery_rename::EXECUTABLE_EXTENSIONS
            .iter()
            .any(|floor| floor.eq_ignore_ascii_case(extension))
    {
        return Err(Refusal::Executable(manifest.name.clone()));
    }
    Ok(())
}

/// The directory a torrent's files land in: the torrent's name, without
/// its extension when it is one file, as one safe path component.
pub fn output_name(manifest: &Manifest) -> String {
    let name = manifest.name.trim();
    let name = match (manifest.single_file, name.rsplit_once('.')) {
        (true, Some((stem, _))) if !stem.is_empty() => stem,
        _ => name,
    };
    weaver_model::files::sanitize_path_component(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(name: &str, single_file: bool, paths: &[&str]) -> Manifest {
        Manifest {
            name: name.to_owned(),
            single_file,
            paths: paths.iter().map(|path| (*path).to_owned()).collect(),
        }
    }

    fn policy(extensions: &[&str]) -> PostProcessingSettings {
        PostProcessingSettings {
            unacceptable_extensions: extensions.iter().map(|e| (*e).to_owned()).collect(),
            ..PostProcessingSettings::default()
        }
    }

    #[test]
    fn an_unwanted_extension_on_the_name_or_any_path_refuses() {
        let policy = policy(&["nfo", "sc?"]);
        assert_eq!(
            admit(
                &policy,
                &manifest("set", false, &["a.bin", "sub/readme.NFO"])
            ),
            Err(Refusal::Unwanted {
                path: "sub/readme.NFO".into(),
                pattern: "nfo".into()
            })
        );
        assert_eq!(
            admit(&policy, &manifest("set.scr", true, &["set.scr"])),
            Err(Refusal::Unwanted {
                path: "set.scr".into(),
                pattern: "sc?".into()
            })
        );
        admit(&policy, &manifest("set", false, &["a.bin", "b.mkv"])).unwrap();
    }

    #[test]
    fn an_executable_name_is_refused_with_no_policy() {
        let policy = policy(&[]);
        assert_eq!(
            admit(&policy, &manifest("setup.EXE", true, &["setup.EXE"])),
            Err(Refusal::Executable("setup.EXE".into()))
        );
        // A file inside a directory is the policy's to judge.
        admit(&policy, &manifest("set", false, &["setup.exe"])).unwrap();
        assert_eq!(
            admit(&policy, &manifest("  ", false, &[])),
            Err(Refusal::Unnamed)
        );
    }

    #[test]
    fn the_output_name_drops_a_single_files_extension() {
        assert_eq!(output_name(&manifest("one.mkv", true, &[])), "one");
        assert_eq!(output_name(&manifest("set.v2", false, &[])), "set.v2");
        assert_eq!(output_name(&manifest(".hidden", true, &[])), ".hidden");
    }
}
