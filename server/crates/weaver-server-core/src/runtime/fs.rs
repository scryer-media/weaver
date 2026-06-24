use std::io;
use std::path::{Component, Path, PathBuf};

use crate::runtime::file_cache;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryFingerprint {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(windows)]
    volume_serial_number: u32,
    #[cfg(windows)]
    file_index: u64,
}

pub(crate) fn paths_equivalent_for_placement(left: &Path, right: &Path) -> bool {
    path_equivalence_key(left) == path_equivalence_key(right)
}

pub(crate) fn rename_no_overwrite(src: &Path, dst: &Path) -> io::Result<()> {
    if paths_equivalent_for_placement(src, dst) {
        if src == dst {
            return Ok(());
        }
        return rename_equivalent_path(src, dst);
    }

    let metadata = std::fs::symlink_metadata(src)?;
    if metadata.file_type().is_symlink() {
        return move_symlink_no_overwrite(src, dst);
    }

    if metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "directory moves require copy fallback for no-overwrite placement",
        ));
    }

    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("source is not a regular file: {}", src.display()),
        ));
    }

    match std::fs::hard_link(src, dst) {
        Ok(()) => {
            std::fs::remove_file(src)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("destination already exists: {}", dst.display()),
        )),
        Err(_) => copy_file_no_overwrite_then_remove_source(src, dst),
    }
}

fn copy_file_no_overwrite_then_remove_source(src: &Path, dst: &Path) -> io::Result<()> {
    let parent_fingerprint = prepare_destination_parent(dst)?;
    match file_cache::copy_large_file(src, dst) {
        Ok(_) => {}
        Err(error) => {
            if error.kind() != io::ErrorKind::AlreadyExists {
                cleanup_copy_destination_if_parent_matches(dst, &parent_fingerprint);
            }
            return Err(error);
        }
    }
    if !destination_parent_matches(dst, &parent_fingerprint)? {
        return Err(io::Error::other(format!(
            "destination parent changed during copy: {}",
            dst.display()
        )));
    }
    std::fs::remove_file(src)
}

fn cleanup_copy_destination_if_parent_matches(
    dst: &Path,
    parent_fingerprint: &DirectoryFingerprint,
) {
    if destination_parent_matches(dst, parent_fingerprint).unwrap_or(false) {
        let _ = std::fs::remove_file(dst);
    }
}

fn move_symlink_no_overwrite(src: &Path, dst: &Path) -> io::Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let target = std::fs::read_link(src)?;
    create_symlink_no_overwrite(src, &target, dst)?;
    std::fs::remove_file(src)
}

#[cfg(unix)]
fn create_symlink_no_overwrite(_src: &Path, target: &Path, dst: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, dst).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            )
        } else {
            error
        }
    })
}

#[cfg(windows)]
fn create_symlink_no_overwrite(src: &Path, target: &Path, dst: &Path) -> io::Result<()> {
    let target_is_dir = std::fs::metadata(src)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    let result = if target_is_dir {
        std::os::windows::fs::symlink_dir(target, dst)
    } else {
        std::os::windows::fs::symlink_file(target, dst)
    };
    result.map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            )
        } else {
            error
        }
    })
}

#[cfg(not(any(unix, windows)))]
fn create_symlink_no_overwrite(_src: &Path, _target: &Path, dst: &Path) -> io::Result<()> {
    let _ = dst;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "symlink placement is unsupported on this platform",
    ))
}

pub(crate) fn prepare_destination_parent(dst: &Path) -> io::Result<DirectoryFingerprint> {
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    directory_fingerprint_from_path(parent)
}

pub(crate) fn destination_parent_matches(
    dst: &Path,
    expected: &DirectoryFingerprint,
) -> io::Result<bool> {
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    directory_fingerprint_from_path(parent).map(|actual| &actual == expected)
}

fn path_equivalence_key(path: &Path) -> String {
    let normalized = lexical_normalize(path);
    let key = normalized.to_string_lossy();
    #[cfg(windows)]
    {
        key.replace('/', "\\").to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        key.into_owned()
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(_) | Component::RootDir | Component::Prefix(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

#[cfg(windows)]
fn rename_equivalent_path(src: &Path, dst: &Path) -> io::Result<()> {
    let parent = src.parent().unwrap_or_else(|| Path::new("."));
    let pid = std::process::id();
    for attempt in 0..100u32 {
        let tmp = parent.join(format!(".weaver-rename-{pid}-{attempt}.tmp"));
        if tmp.try_exists()? {
            continue;
        }
        std::fs::rename(src, &tmp)?;
        match std::fs::rename(&tmp, dst) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let _ = std::fs::rename(&tmp, src);
                return Err(error);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate temporary path for case-only rename",
    ))
}

#[cfg(not(windows))]
fn rename_equivalent_path(_src: &Path, _dst: &Path) -> io::Result<()> {
    Ok(())
}

fn directory_fingerprint_from_path(path: &Path) -> io::Result<DirectoryFingerprint> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("destination parent is not a directory: {}", path.display()),
        ));
    }

    #[cfg(unix)]
    {
        Ok(DirectoryFingerprint {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?;
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        let result =
            unsafe { GetFileInformationByHandle(directory.as_raw_handle(), info.as_mut_ptr()) };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        let info = unsafe { info.assume_init() };
        let file_index = ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64;
        Ok(DirectoryFingerprint {
            volume_serial_number: info.dwVolumeSerialNumber,
            file_index,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = metadata;
        Ok(DirectoryFingerprint {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_equivalence_matches_platform_case_rules() {
        let left = Path::new("A/B.RAR");
        let right = Path::new("a\\b.rar");

        #[cfg(windows)]
        assert!(paths_equivalent_for_placement(left, right));
        #[cfg(not(windows))]
        assert!(!paths_equivalent_for_placement(left, right));
    }

    #[test]
    fn rename_no_overwrite_rejects_existing_destination() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src.bin");
        let dst = temp.path().join("dst.bin");
        std::fs::write(&src, b"source").unwrap();
        std::fs::write(&dst, b"dest").unwrap();

        let error = rename_no_overwrite(&src, &dst).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"source");
        assert_eq!(std::fs::read(&dst).unwrap(), b"dest");
    }

    #[test]
    fn rename_no_overwrite_moves_regular_file() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src.bin");
        let dst = temp.path().join("dst.bin");
        std::fs::write(&src, b"source").unwrap();

        rename_no_overwrite(&src, &dst).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    #[cfg(unix)]
    #[test]
    fn rename_no_overwrite_moves_symlink_without_following_target() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target.bin");
        let src = temp.path().join("link-src");
        let dst = temp.path().join("link-dst");
        std::fs::write(&target, b"target").unwrap();
        std::os::unix::fs::symlink(&target, &src).unwrap();

        rename_no_overwrite(&src, &dst).unwrap();

        assert!(!src.exists());
        assert!(
            std::fs::symlink_metadata(&dst)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read_link(&dst).unwrap(), target);
    }

    #[cfg(unix)]
    #[test]
    fn rename_no_overwrite_rejects_existing_symlink_destination() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target.bin");
        let src = temp.path().join("link-src");
        let dst = temp.path().join("link-dst");
        std::fs::write(&target, b"target").unwrap();
        std::os::unix::fs::symlink(&target, &src).unwrap();
        std::fs::write(&dst, b"existing").unwrap();

        let error = rename_no_overwrite(&src, &dst).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            std::fs::symlink_metadata(&src)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(&dst).unwrap(), b"existing");
    }
}
