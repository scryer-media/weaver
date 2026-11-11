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
    #[error("{path} has an unwanted extension ({pattern}), as every file of the torrent does")]
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

/// Admits a torrent, with the files among its `paths`, by their index
/// there, whose extension the operator does not want: those are left out
/// of its download. Refuses one that would leave no file wanted, one with
/// no name, and one that would be named after an executable whatever the
/// policy says.
pub fn admit(policy: &PostProcessingSettings, manifest: &Manifest) -> Result<Vec<usize>, Refusal> {
    if manifest.name.trim().is_empty() {
        return Err(Refusal::Unnamed);
    }
    let mut first = None;
    let mut unwanted = Vec::new();
    for (index, path) in manifest.paths.iter().enumerate() {
        if let Some(pattern) = policy.unacceptable_extension_match(path) {
            first.get_or_insert((path, pattern));
            unwanted.push(index);
        }
    }
    if let Some((path, pattern)) = first
        && unwanted.len() == manifest.paths.len()
    {
        return Err(Refusal::Unwanted {
            path: path.clone(),
            pattern: pattern.to_owned(),
        });
    }
    if let Some(extension) = extension(&manifest.name)
        && weaver_nzb::delivery_rename::EXECUTABLE_EXTENSIONS
            .iter()
            .any(|floor| floor.eq_ignore_ascii_case(extension))
    {
        return Err(Refusal::Executable(manifest.name.clone()));
    }
    Ok(unwanted)
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

    // A file with an unwanted extension is left out of the download; the
    // torrent is refused only when that leaves no file wanted.
    #[test]
    fn files_with_an_unwanted_extension_are_left_out_and_none_left_refuses() {
        let policy = policy(&["nfo", "sc?"]);
        assert_eq!(
            admit(
                &policy,
                &manifest("set", false, &["a.bin", "sub/readme.NFO", "b.scr"])
            ),
            Ok(vec![1, 2])
        );
        assert_eq!(
            admit(&policy, &manifest("set.scr", true, &["set.scr"])),
            Err(Refusal::Unwanted {
                path: "set.scr".into(),
                pattern: "sc?".into()
            })
        );
        assert_eq!(
            admit(&policy, &manifest("set", false, &["a.nfo", "b.NFO"])),
            Err(Refusal::Unwanted {
                path: "a.nfo".into(),
                pattern: "nfo".into()
            })
        );
        assert_eq!(
            admit(&policy, &manifest("set", false, &["a.bin", "b.mkv"])),
            Ok(Vec::new())
        );
    }

    #[test]
    fn an_executable_name_is_refused_with_no_policy() {
        let policy = policy(&[]);
        assert_eq!(
            admit(&policy, &manifest("setup.EXE", true, &["setup.EXE"])),
            Err(Refusal::Executable("setup.EXE".into()))
        );
        // A file inside a directory is the policy's to judge.
        assert_eq!(
            admit(&policy, &manifest("set", false, &["setup.exe"])),
            Ok(Vec::new())
        );
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
