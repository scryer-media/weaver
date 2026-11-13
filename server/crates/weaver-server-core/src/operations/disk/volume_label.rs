// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The name a volume is shown by: where the OS mounts it, such as `D:\` or
// `/mnt/nas`.
//
// Read once, when a volume is first seen, through the same mount reads the
// kind of storage uses ([`super::storage_kind`]). Never read for a write.

use std::path::Path;

// The mount path or drive root of the volume `path` lies on, read at the
// nearest of its ancestors that exists. What cannot be read is shown by the
// path's own root.
pub(crate) fn volume_label(path: &Path) -> String {
    path.ancestors()
        .find(|at| at.exists())
        .and_then(mount_path)
        .filter(|label| !label.is_empty())
        .unwrap_or_else(|| root_of(path))
}

// The root `path` starts from: `/`, or a drive or share with its separator.
// A path with none is shown as it is.
fn root_of(path: &Path) -> String {
    use std::path::Component;

    let root: std::path::PathBuf = path
        .components()
        .take_while(|part| matches!(part, Component::Prefix(_) | Component::RootDir))
        .collect();
    if root.as_os_str().is_empty() {
        path.display().to_string()
    } else {
        root.display().to_string()
    }
}

// The filesystem's own record of where it is mounted.
#[cfg(target_vendor = "apple")]
fn mount_path(existing: &Path) -> Option<String> {
    let stat = super::storage_kind::mount_stat(existing)?;
    // SAFETY: `f_mntonname` is a NUL-terminated array the call filled.
    let name = unsafe { std::ffi::CStr::from_ptr(stat.f_mntonname.as_ptr()) };
    Some(name.to_string_lossy().into_owned())
}

#[cfg(windows)]
fn mount_path(existing: &Path) -> Option<String> {
    let root = super::storage_kind::volume_root(existing)?;
    let length = root.iter().position(|unit| *unit == 0).unwrap_or(root.len());
    Some(String::from_utf16_lossy(&root[..length]))
}

// The highest folder above `existing` still on its device: a mount begins
// where the device changes.
#[cfg(all(unix, not(target_vendor = "apple")))]
fn mount_path(existing: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    let existing = std::fs::canonicalize(existing).ok()?;
    let device = std::fs::metadata(&existing).ok()?.dev();
    Some(
        top_on_device(&existing, |at| {
            std::fs::metadata(at).ok().map(|metadata| metadata.dev())
        }, device)
        .display()
        .to_string(),
    )
}

#[cfg(not(any(unix, windows)))]
fn mount_path(_existing: &Path) -> Option<String> {
    None
}

// The highest of `path` and its ancestors that `device_of` still finds on
// `device`, climbing until one is on another.
#[cfg(any(test, all(unix, not(target_vendor = "apple"))))]
fn top_on_device<'a>(
    path: &'a Path,
    device_of: impl Fn(&Path) -> Option<u64>,
    device: u64,
) -> &'a Path {
    let mut top = path;
    for above in path.ancestors().skip(1) {
        if device_of(above) != Some(device) {
            break;
        }
        top = above;
    }
    top
}

#[cfg(test)]
mod tests {
    use super::*;

    // A folder not made yet is shown by the volume it will be made on.
    #[test]
    fn a_folder_not_yet_made_takes_its_nearest_ancestors_label() {
        let root = tempfile::tempdir().expect("temp dir");
        let label = volume_label(root.path());
        assert!(!label.is_empty());
        assert_eq!(volume_label(&root.path().join("not/yet/made")), label);
    }

    // The label is a mount the folder lies under, not the folder itself.
    #[cfg(unix)]
    #[test]
    fn the_label_is_a_mount_above_the_folder() {
        let root = tempfile::tempdir().expect("temp dir");
        let inside = root.path().join("inside");
        std::fs::create_dir(&inside).expect("a folder");
        let label = volume_label(&inside);
        assert!(label.starts_with('/'), "{label}");
        let inside = std::fs::canonicalize(&inside).expect("a real path");
        assert_ne!(Path::new(&label), inside);
    }

    // The climb stops at the first folder on another device.
    #[test]
    fn the_mount_is_the_highest_folder_on_the_device() {
        let device_of = |at: &Path| match at.to_str()? {
            "/" | "/mnt" => Some(1),
            "/mnt/nas" | "/mnt/nas/downloads" | "/mnt/nas/downloads/job" => Some(2),
            _ => None,
        };
        assert_eq!(
            top_on_device(Path::new("/mnt/nas/downloads/job"), device_of, 2),
            Path::new("/mnt/nas")
        );
        assert_eq!(top_on_device(Path::new("/mnt"), device_of, 1), Path::new("/"));
        assert_eq!(
            top_on_device(Path::new("/mnt/nas"), device_of, 2),
            Path::new("/mnt/nas")
        );
    }

    // What has no mount to read is shown by its root, and a path with no
    // root as it is.
    #[test]
    fn a_path_falls_back_to_its_root() {
        #[cfg(unix)]
        assert_eq!(root_of(Path::new("/mnt/nas/downloads")), "/");
        #[cfg(windows)]
        assert_eq!(root_of(Path::new(r"D:\downloads\job")), r"D:\");
        assert_eq!(root_of(Path::new("downloads/job")), "downloads/job");
    }
}
