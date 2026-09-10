use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};

use crate::jobs::ids::JobId;

pub const WORKING_DIR_MARKER: &str = ".weaver-job-dir";
pub const OUTPUT_DIR_MARKER: &str = ".weaver-output-dir";

pub fn sanitize_dirname(name: &str) -> String {
    weaver_model::files::sanitize_path_component(name)
}

pub fn compute_working_dir(intermediate_dir: &Path, job_id: JobId, name: &str) -> PathBuf {
    let dir_name = sanitize_dirname(name);
    let candidate = intermediate_dir.join(&dir_name);
    if !candidate.exists() {
        candidate
    } else {
        intermediate_dir.join(weaver_model::files::path_component_with_suffix(
            &dir_name,
            &format!(".#{}", job_id.0),
        ))
    }
}

pub fn working_dir_marker_path(dir: &Path) -> PathBuf {
    dir.join(WORKING_DIR_MARKER)
}

pub fn is_weaver_owned_working_dir(dir: &Path) -> bool {
    dir.parent()
        .is_some_and(|root| owned_working_directory(root, dir).is_ok())
}

fn open_working_directory(root: &Path, path: &Path) -> std::io::Result<Dir> {
    let relative = path.strip_prefix(root).map_err(std::io::Error::other)?;
    let mut components = relative.components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(std::io::Error::other(
            "working directory must be a direct child of its configured root",
        ));
    }
    Dir::open_ambient_dir(root, cap_std::ambient_authority())?.open_dir_nofollow(relative)
}

fn working_marker_value(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<String> {
    let metadata = dir.dir_metadata()?;
    let mut hash = blake3::Hasher::new();
    hash.update(path.as_os_str().as_encoded_bytes());
    hash.update(&metadata.dev().to_le_bytes());
    hash.update(&metadata.ino().to_le_bytes());
    hash.update(&job_id.0.to_le_bytes());
    Ok(format!(
        "weaver-job-v1:{}:{}\n",
        job_id.0,
        hash.finalize().to_hex()
    ))
}

fn read_working_marker(dir: &Dir) -> std::io::Result<String> {
    if !dir.symlink_metadata(WORKING_DIR_MARKER)?.is_file() {
        return Err(std::io::Error::other(
            "working directory marker is not a regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = dir.open_with(WORKING_DIR_MARKER, &options)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other(
            "working directory marker is not a regular file",
        ));
    }
    let mut value = String::new();
    file.take(256).read_to_string(&mut value)?;
    Ok(value)
}

fn owned_working_directory(root: &Path, path: &Path) -> std::io::Result<Dir> {
    let dir = open_working_directory(root, path)?;
    let stored = read_working_marker(&dir)?;
    let job_id = stored
        .strip_prefix("weaver-job-v1:")
        .and_then(|value| value.split(':').next())
        .and_then(|value| value.parse().ok())
        .map(JobId)
        .ok_or_else(|| std::io::Error::other("working directory has no valid ownership marker"))?;
    if stored != working_marker_value(&dir, path, job_id)? {
        return Err(std::io::Error::other(
            "working directory ownership does not match its identity",
        ));
    }
    Ok(dir)
}

pub fn mark_weaver_owned_working_dir(
    root: &Path,
    path: &Path,
    job_id: JobId,
) -> std::io::Result<()> {
    let dir = open_working_directory(root, path)?;
    let expected = working_marker_value(&dir, path, job_id)?;
    match read_working_marker(&dir) {
        Ok(stored) if stored == expected => return Ok(()),
        // Only trusted active-job restoration calls this migration path.
        Ok(stored) if stored.is_empty() => dir.remove_file(WORKING_DIR_MARKER)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => {
            return Err(std::io::Error::other(
                "refusing to replace a foreign working directory marker",
            ));
        }
    }
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    dir.open_with(WORKING_DIR_MARKER, &options)?
        .write_all(expected.as_bytes())
}

pub fn remove_weaver_owned_working_dir(root: &Path, path: &Path) -> std::io::Result<()> {
    owned_working_directory(root, path)?.remove_open_dir_all()
}

pub async fn stamp_working_dir(root: &Path, path: &Path, job_id: JobId) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || mark_weaver_owned_working_dir(&root, &path, job_id))
        .await
        .map_err(std::io::Error::other)?
}

pub async fn prepare_history_working_dir(
    root: &Path,
    path: &Path,
    job_id: JobId,
) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let dir = match open_working_directory(&root, &path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            result => result?,
        };
        let stored = read_working_marker(&dir)?;
        if stored.is_empty() {
            // The durable history row supplies the legacy directory's job identity.
            mark_weaver_owned_working_dir(&root, &path, job_id)
        } else if stored == working_marker_value(&dir, &path, job_id)? {
            Ok(())
        } else {
            Err(std::io::Error::other(
                "historical working directory ownership mismatch",
            ))
        }
    })
    .await
    .map_err(std::io::Error::other)?
}

pub fn mark_weaver_owned_output_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::write(dir.join(OUTPUT_DIR_MARKER), output_marker_value(dir)?)
}

pub fn is_weaver_owned_output_dir(dir: &Path) -> bool {
    let Ok(directory_metadata) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        return false;
    }
    let marker = dir.join(OUTPUT_DIR_MARKER);
    let Ok(marker_metadata) = std::fs::symlink_metadata(&marker) else {
        return false;
    };
    if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
        return false;
    }
    let Ok(expected) = output_marker_value(dir) else {
        return false;
    };
    std::fs::read(&marker).is_ok_and(|stored| stored == expected)
}

fn output_marker_value(dir: &Path) -> std::io::Result<Vec<u8>> {
    let canonical = std::fs::canonicalize(dir)?;
    let digest = blake3::hash(canonical.as_os_str().as_encoded_bytes());
    Ok(format!("weaver-output-v1:{}\n", digest.to_hex()).into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn working_directory_ownership_is_bound_to_job_and_directory() {
        let temp = tempfile::tempdir().unwrap();
        let owned = temp.path().join("owned");
        let other = temp.path().join("other");
        std::fs::create_dir(&owned).unwrap();
        std::fs::create_dir(&other).unwrap();
        mark_weaver_owned_working_dir(temp.path(), &owned, JobId(7)).unwrap();
        assert!(is_weaver_owned_working_dir(&owned));
        assert!(mark_weaver_owned_working_dir(temp.path(), &owned, JobId(8)).is_err());
        std::fs::copy(
            working_dir_marker_path(&owned),
            working_dir_marker_path(&other),
        )
        .unwrap();
        assert!(!is_weaver_owned_working_dir(&other));
        assert!(remove_weaver_owned_working_dir(temp.path(), &other).is_err());
        assert!(other.exists());
        remove_weaver_owned_working_dir(temp.path(), &owned).unwrap();
        assert!(!owned.exists());
    }

    #[test]
    fn sanitize_dirname_bounds_long_names() {
        let sanitized = sanitize_dirname(&"a".repeat(400));

        assert!(sanitized.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
    }

    #[test]
    fn sanitize_dirname_disarms_windows_device_names() {
        assert_eq!(sanitize_dirname("CON"), "_CON");
    }

    #[test]
    fn compute_working_dir_bounds_collision_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let long_name = "a".repeat(400);
        let original = compute_working_dir(temp.path(), JobId(42), &long_name);
        std::fs::create_dir(&original).unwrap();

        let suffixed = compute_working_dir(temp.path(), JobId(42), &long_name);
        let file_name = suffixed.file_name().unwrap().to_string_lossy();

        assert!(file_name.ends_with(".#42"));
        assert!(file_name.len() <= weaver_model::files::DOWNLOAD_FILENAME_MAX_BYTES);
    }

    #[test]
    fn output_ownership_marker_is_bound_to_its_directory() {
        let temp = tempfile::tempdir().unwrap();
        let owned = temp.path().join("owned");
        let copied = temp.path().join("copied");
        std::fs::create_dir(&owned).unwrap();
        std::fs::create_dir(&copied).unwrap();
        mark_weaver_owned_output_dir(&owned).unwrap();

        assert!(is_weaver_owned_output_dir(&owned));
        std::fs::copy(
            owned.join(OUTPUT_DIR_MARKER),
            copied.join(OUTPUT_DIR_MARKER),
        )
        .unwrap();
        assert!(!is_weaver_owned_output_dir(&copied));
    }
}
