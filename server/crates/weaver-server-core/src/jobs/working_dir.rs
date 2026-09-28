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

const MARKER_V1_PREFIX: &str = "weaver-job-v1:";
const MARKER_V2_PREFIX: &str = "weaver-job-v2:";

/// The marker's identity hash.
///
/// v2 binds the directory's path, inode and owning job. v1 also bound the
/// device number, which is not stable on every filesystem: pooled and layered
/// filesystems renumber it across reboots or dataset recreation, and every
/// marker written before the renumbering then stopped matching its own
/// directory. `dev` is only passed when recomputing a v1 marker.
fn marker_hash(path: &Path, dev: Option<u64>, ino: u64, job_id: JobId) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(path.as_os_str().as_encoded_bytes());
    if let Some(dev) = dev {
        hash.update(&dev.to_le_bytes());
    }
    hash.update(&ino.to_le_bytes());
    hash.update(&job_id.0.to_le_bytes());
    hash.finalize().to_hex().to_string()
}

fn working_marker_value(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<String> {
    let metadata = dir.dir_metadata()?;
    Ok(format!(
        "{MARKER_V2_PREFIX}{}:{}\n",
        job_id.0,
        marker_hash(path, None, metadata.ino(), job_id)
    ))
}

/// The v1 markers this directory could legitimately carry for `job_id`: the
/// hash with the device number as it reads now, and with none at all.
fn legacy_marker_values(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<[String; 2]> {
    let metadata = dir.dir_metadata()?;
    let line = |dev| {
        format!(
            "{MARKER_V1_PREFIX}{}:{}\n",
            job_id.0,
            marker_hash(path, dev, metadata.ino(), job_id)
        )
    };
    Ok([line(Some(metadata.dev())), line(None)])
}

/// The job a marker names, from either marker version.
fn marker_job_id(stored: &str) -> Option<JobId> {
    stored
        .strip_prefix(MARKER_V2_PREFIX)
        .or_else(|| stored.strip_prefix(MARKER_V1_PREFIX))
        .and_then(|value| value.split(':').next())
        .and_then(|value| value.parse().ok())
        .map(JobId)
}

/// How a stored marker compares with what this directory should carry for
/// the job it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerMatch {
    /// A v2 marker for this directory and job.
    Current,
    /// A v1 marker for this directory and job, to be rewritten as v2.
    Legacy,
    /// The marker names this job but its hash does not match the directory.
    Mismatch,
}

fn match_marker(
    dir: &Dir,
    path: &Path,
    stored: &str,
    job_id: JobId,
) -> std::io::Result<MarkerMatch> {
    if stored == working_marker_value(dir, path, job_id)? {
        return Ok(MarkerMatch::Current);
    }
    if legacy_marker_values(dir, path, job_id)?
        .iter()
        .any(|legacy| legacy == stored)
    {
        return Ok(MarkerMatch::Legacy);
    }
    Ok(MarkerMatch::Mismatch)
}

fn write_working_marker(dir: &Dir, value: &str) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    dir.open_with(WORKING_DIR_MARKER, &options)?
        .write_all(value.as_bytes())
}

/// Replaces a verified v1 marker with its v2 form, so the directory stops
/// depending on a device number that may change.
fn upgrade_legacy_marker(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<()> {
    let current = working_marker_value(dir, path, job_id)?;
    dir.remove_file(WORKING_DIR_MARKER)?;
    write_working_marker(dir, &current)
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
    let job_id = marker_job_id(&stored)
        .ok_or_else(|| std::io::Error::other("working directory has no valid ownership marker"))?;
    match match_marker(&dir, path, &stored, job_id)? {
        MarkerMatch::Current => {}
        MarkerMatch::Legacy => upgrade_legacy_marker(&dir, path, job_id)?,
        MarkerMatch::Mismatch => {
            return Err(std::io::Error::other(
                "working directory ownership does not match its identity",
            ));
        }
    }
    Ok(dir)
}

pub fn mark_weaver_owned_working_dir(
    root: &Path,
    path: &Path,
    job_id: JobId,
) -> std::io::Result<()> {
    let dir = open_working_directory(root, path)?;
    match read_working_marker(&dir) {
        Ok(stored) if marker_job_id(&stored) == Some(job_id) => {
            match match_marker(&dir, path, &stored, job_id)? {
                MarkerMatch::Current => return Ok(()),
                MarkerMatch::Legacy => return upgrade_legacy_marker(&dir, path, job_id),
                MarkerMatch::Mismatch => {
                    return Err(std::io::Error::other(
                        "refusing to replace a foreign working directory marker",
                    ));
                }
            }
        }
        // Only trusted active-job restoration calls this migration path.
        Ok(stored) if stored.is_empty() => dir.remove_file(WORKING_DIR_MARKER)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => {
            return Err(std::io::Error::other(
                "refusing to replace a foreign working directory marker",
            ));
        }
    }
    write_working_marker(&dir, &working_marker_value(&dir, path, job_id)?)
}

pub fn remove_weaver_owned_working_dir(root: &Path, path: &Path) -> std::io::Result<()> {
    owned_working_directory(root, path)?.remove_open_dir_all()
}

/// What a history cleanup did with one job's working directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryWorkingDir {
    /// The directory is the job's (or is already gone) and may be removed.
    Owned,
    /// The marker names this job but no longer matches the directory, so the
    /// directory is left on disk. The history record can still go: nothing
    /// about the mismatch makes the directory anyone else's.
    LeftInPlace,
}

fn judge_history_marker(
    dir: &Dir,
    path: &Path,
    stored: &str,
    job_id: JobId,
) -> std::io::Result<HistoryWorkingDir> {
    // A marker naming another job is never this job's to delete.
    if marker_job_id(stored) != Some(job_id) {
        return Err(std::io::Error::other(
            "working directory no longer belongs to the expected job",
        ));
    }
    match match_marker(dir, path, stored, job_id)? {
        MarkerMatch::Current => Ok(HistoryWorkingDir::Owned),
        MarkerMatch::Legacy => {
            upgrade_legacy_marker(dir, path, job_id)?;
            Ok(HistoryWorkingDir::Owned)
        }
        MarkerMatch::Mismatch => {
            tracing::warn!(
                job_id = job_id.0,
                dir = %path.display(),
                "working directory marker names this job but no longer matches the \
                 directory; leaving the directory in place"
            );
            Ok(HistoryWorkingDir::LeftInPlace)
        }
    }
}

/// Delete only the directory still owned by the job selected for cleanup.
/// Validation and removal use the same opened directory capability.
pub fn remove_job_working_dir(
    root: &Path,
    path: &Path,
    job_id: JobId,
) -> std::io::Result<HistoryWorkingDir> {
    let dir = open_working_directory(root, path)?;
    let stored = read_working_marker(&dir)?;
    match judge_history_marker(&dir, path, &stored, job_id)? {
        HistoryWorkingDir::Owned => {
            dir.remove_open_dir_all()?;
            Ok(HistoryWorkingDir::Owned)
        }
        HistoryWorkingDir::LeftInPlace => Ok(HistoryWorkingDir::LeftInPlace),
    }
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
) -> std::io::Result<HistoryWorkingDir> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let dir = match open_working_directory(&root, &path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HistoryWorkingDir::Owned);
            }
            result => result?,
        };
        let stored = read_working_marker(&dir)?;
        if stored.is_empty() {
            // The durable history row supplies the legacy directory's job identity.
            mark_weaver_owned_working_dir(&root, &path, job_id)?;
            return Ok(HistoryWorkingDir::Owned);
        }
        judge_history_marker(&dir, &path, &stored, job_id)
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

    #[tokio::test]
    async fn history_cleanup_rejects_a_valid_marker_for_a_replacement_job() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("job");
        std::fs::create_dir(&path).unwrap();
        mark_weaver_owned_working_dir(temp.path(), &path, JobId(7)).unwrap();
        assert_eq!(
            prepare_history_working_dir(temp.path(), &path, JobId(7))
                .await
                .unwrap(),
            HistoryWorkingDir::Owned
        );
        std::fs::rename(&path, temp.path().join("previous")).unwrap();
        std::fs::create_dir(&path).unwrap();
        mark_weaver_owned_working_dir(temp.path(), &path, JobId(8)).unwrap();
        std::fs::write(path.join("payload"), b"replacement").unwrap();
        assert!(remove_job_working_dir(temp.path(), &path, JobId(7)).is_err());
        assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"replacement");
        remove_job_working_dir(temp.path(), &path, JobId(8)).unwrap();
        assert!(!path.exists());
    }

    #[cfg(unix)]
    /// Writes a v1 marker for `path` computed with `dev`, the way a binary that
    /// still hashed the device number would have.
    fn write_v1_marker(path: &Path, dev: Option<u64>, job_id: JobId) {
        let ino = std::fs::metadata(path).unwrap().ino();
        std::fs::write(
            working_dir_marker_path(path),
            format!(
                "{MARKER_V1_PREFIX}{}:{}\n",
                job_id.0,
                marker_hash(path, dev, ino, job_id)
            ),
        )
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_v1_marker_for_the_same_directory_is_accepted_and_rewritten_as_v2() {
        let temp = tempfile::tempdir().unwrap();
        for (name, with_dev) in [("current-dev", true), ("no-dev", false)] {
            let path = temp.path().join(name);
            std::fs::create_dir(&path).unwrap();
            let dev = with_dev.then(|| std::fs::metadata(&path).unwrap().dev());
            write_v1_marker(&path, dev, JobId(21));

            assert_eq!(
                prepare_history_working_dir(temp.path(), &path, JobId(21))
                    .await
                    .unwrap(),
                HistoryWorkingDir::Owned,
                "{name}: a v1 marker this directory could have written is its owner"
            );
            let rewritten = std::fs::read_to_string(working_dir_marker_path(&path)).unwrap();
            assert!(
                rewritten.starts_with(MARKER_V2_PREFIX),
                "{name}: an accepted v1 marker is rewritten as v2, got {rewritten:?}"
            );
            assert!(is_weaver_owned_working_dir(&path));
            assert_eq!(
                remove_job_working_dir(temp.path(), &path, JobId(21)).unwrap(),
                HistoryWorkingDir::Owned
            );
            assert!(!path.exists());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_marker_for_this_job_that_no_longer_matches_leaves_the_directory_in_place() {
        // A v1 marker written under a device number the filesystem has since
        // renumbered: it names the right job, and its hash cannot be
        // reproduced from the directory as it is now.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("renumbered");
        std::fs::create_dir(&path).unwrap();
        let dev = std::fs::metadata(&path).unwrap().dev();
        write_v1_marker(&path, Some(dev.wrapping_add(1)), JobId(22));
        std::fs::write(path.join("payload"), b"kept").unwrap();

        assert_eq!(
            prepare_history_working_dir(temp.path(), &path, JobId(22))
                .await
                .unwrap(),
            HistoryWorkingDir::LeftInPlace
        );
        assert_eq!(
            remove_job_working_dir(temp.path(), &path, JobId(22)).unwrap(),
            HistoryWorkingDir::LeftInPlace
        );
        assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"kept");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_marker_naming_another_job_is_still_refused() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("foreign");
        std::fs::create_dir(&path).unwrap();
        write_v1_marker(&path, None, JobId(23));
        std::fs::write(path.join("payload"), b"not yours").unwrap();

        assert!(
            prepare_history_working_dir(temp.path(), &path, JobId(24))
                .await
                .is_err()
        );
        assert!(remove_job_working_dir(temp.path(), &path, JobId(24)).is_err());
        assert_eq!(std::fs::read(path.join("payload")).unwrap(), b"not yours");
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
