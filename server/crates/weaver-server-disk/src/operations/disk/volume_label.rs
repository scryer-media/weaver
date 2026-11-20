// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The name a volume is shown by: where the OS mounts it, such as `D:\` or
// `/mnt/nas`.
//
// Read once, when a volume is first seen, through the same mount reads the
// kind of storage uses ([`super::storage_kind`]). Never read for a write.

use std::path::Path;

// The mount path or drive root of the volume `path` lies on, read at the
// nearest of its ancestors that exists.
//
// A mount that cannot be read is shown by the path's own root with its
// device beside it, as `/ (device 1a2b)`: the bare root is the label of the
// volume really mounted there, and two volumes must never share a label, as
// each label is one series in the metrics and one row in the interface.
pub(crate) fn volume_label(path: &Path) -> String {
    let mount = path.ancestors().find(|at| at.exists()).and_then(mount_path);
    label_from(mount, path, || super::nearest_device(path))
}

// The label from what was read: the mount where there is one, and the
// path's root marked with its device where there is not. The device is only
// looked for when the mount could not be read.
fn label_from(mount: Option<String>, path: &Path, device: impl FnOnce() -> Option<u64>) -> String {
    if let Some(mount) = mount.filter(|mount| !mount.is_empty()) {
        return mount;
    }
    match device() {
        Some(device) => format!("{} (device {device:x})", root_of(path)),
        None => unread_label(path),
    }
}

// The label of a volume whose device is not known: the path's own root,
// marked so. Made from the path alone; no disk is looked at.
pub(super) fn unread_label(path: &Path) -> String {
    format!("{} (device unknown)", root_of(path))
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
    let length = root
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(root.len());
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
        top_on_device(
            &existing,
            |at| std::fs::metadata(at).ok().map(|metadata| metadata.dev()),
            device,
        )
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
fn top_on_device(path: &Path, device_of: impl Fn(&Path) -> Option<u64>, device: u64) -> &Path {
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
        assert_eq!(
            top_on_device(Path::new("/mnt"), device_of, 1),
            Path::new("/")
        );
        assert_eq!(
            top_on_device(Path::new("/mnt/nas"), device_of, 2),
            Path::new("/mnt/nas")
        );
    }

    // What has no mount to read starts from its root, and a path with no
    // root is taken as it is.
    #[test]
    fn a_path_falls_back_to_its_root() {
        #[cfg(unix)]
        assert_eq!(root_of(Path::new("/mnt/nas/downloads")), "/");
        #[cfg(windows)]
        assert_eq!(root_of(Path::new(r"D:\downloads\job")), r"D:\");
        assert_eq!(root_of(Path::new("downloads/job")), "downloads/job");
    }

    // A mount that was read is the label as it stands, and no device is
    // looked for.
    #[test]
    fn a_mount_that_was_read_is_the_label_unchanged() {
        let unasked = || -> Option<u64> { panic!("a read mount needs no device") };
        assert_eq!(
            label_from(Some("/mnt/nas".into()), Path::new("/mnt/nas/job"), unasked),
            "/mnt/nas"
        );
        assert_eq!(
            label_from(Some("/".into()), Path::new("/srv/job"), unasked),
            "/"
        );
    }

    // Two volumes whose mounts cannot be read share a root. Neither takes
    // the other's label, and neither takes the label of the volume really
    // mounted at that root.
    #[test]
    fn volumes_with_unread_mounts_never_share_a_label() {
        let (first, second) = (Path::new("/mnt/first/job"), Path::new("/mnt/second/job"));
        let real_root = label_from(Some(root_of(first)), first, || Some(1));
        let first_label = label_from(None, first, || Some(0x1a2b));
        let second_label = label_from(None, second, || Some(0x3c4d));
        // A mount read as empty is not a mount.
        let empty_label = label_from(Some(String::new()), second, || Some(0x5e6f));
        let unknown_label = label_from(None, second, || None);

        let labels = [
            real_root.as_str(),
            first_label.as_str(),
            second_label.as_str(),
            empty_label.as_str(),
            unknown_label.as_str(),
        ];
        let distinct: std::collections::HashSet<&str> = labels.into_iter().collect();
        assert_eq!(distinct.len(), labels.len(), "{labels:?}");

        let root = root_of(first);
        assert_eq!(real_root, root);
        assert_eq!(first_label, format!("{root} (device 1a2b)"));
        assert_eq!(second_label, format!("{root} (device 3c4d)"));
        // No device either: the label says so.
        assert_eq!(unknown_label, format!("{root} (device unknown)"));
    }
}
