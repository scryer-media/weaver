// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};

use weaver_server_base::jobs::ids::JobId;

pub use crate::jobs::dir_markers::{OUTPUT_DIR_MARKER, WORKING_DIR_MARKER};
// The folder under a destination that holds each job's staging directory,
// named by the job's id.
pub const STAGING_DIR_NAME: &str = ".weaver-staging";

pub fn sanitize_dirname(name: &str) -> String {
    weaver_model::files::sanitize_path_component(name)
}

pub fn compute_working_dir(intermediate_dir: &Path, job_id: JobId, name: &str) -> PathBuf {
    let dir_name = sanitize_dirname(name);
    let candidate = intermediate_dir.join(&dir_name);
    if !candidate.exists() {
        candidate
    } else {
        suffixed_working_dir(intermediate_dir, &dir_name, job_id)
    }
}

// The job-specific name a job falls back to when its plain name is taken.
pub fn suffixed_working_dir(intermediate_dir: &Path, dir_name: &str, job_id: JobId) -> PathBuf {
    intermediate_dir.join(weaver_model::files::path_component_with_suffix(
        dir_name,
        &format!(".#{}", job_id.0),
    ))
}

// A working directory claimed for a job at admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedWorkingDir {
    pub path: PathBuf,
    // The directory was already on disk under the job's own suffixed name,
    // left by an earlier admission of this same job.
    pub existed: bool,
}

// One claim or adoption of a working directory at a time. A claim makes the
// directory and writes its marker under this lock, and an adoption reads
// the marker and writes its own under it, so no adoption ever sees a
// directory made for a job being added before that job's marker is in it.
// The lock is held for those two steps alone: the root is made, and an
// adoption's directory opened, before it is taken, so a place that hangs
// stalls its own caller and no other.
static CLAIMS: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn claims() -> std::sync::MutexGuard<'static, ()> {
    CLAIMS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// Claims a job's working directory with the file system as the arbiter: the
// plain name is created, not checked, so two admissions sanitizing to one
// name cannot both take it. The one that finds the plain name taken takes
// its job-suffixed name instead. The directory is marked the job's in the
// same step, before anything else can judge it by its name.
pub fn claim_working_dir(
    intermediate_dir: &Path,
    job_id: JobId,
    name: &str,
) -> std::io::Result<ClaimedWorkingDir> {
    std::fs::create_dir_all(intermediate_dir)?;
    claim_under_root(intermediate_dir, job_id, name)
}

// The claim itself, under a root that is there: nothing here makes the root.
fn claim_under_root(
    intermediate_dir: &Path,
    job_id: JobId,
    name: &str,
) -> std::io::Result<ClaimedWorkingDir> {
    let _claims = claims();
    let claimed = create_working_dir(intermediate_dir, job_id, name)?;
    if let Err(error) = mark_weaver_owned_working_dir(intermediate_dir, &claimed.path, job_id) {
        if !claimed.existed {
            let _ = std::fs::remove_dir(&claimed.path);
        }
        return Err(error);
    }
    Ok(claimed)
}

// A job added to work in the directory it names, as a reprocess is: the
// directory is made, if it is not there, and marked the job's in one step
// under the claim lock when it stands under `root`. Whether it was there.
pub fn claim_named_working_dir(root: &Path, path: &Path, job_id: JobId) -> std::io::Result<bool> {
    let existed = match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    if !path.starts_with(root) {
        std::fs::create_dir_all(path)?;
        return Ok(existed);
    }
    std::fs::create_dir_all(root)?;
    let _claims = claims();
    std::fs::create_dir_all(path)?;
    mark_weaver_owned_working_dir(root, path, job_id)?;
    Ok(existed)
}

// The directory under `root` a job names for itself, as a torrent's
// unpacking does: the one already carrying its marker, the suffixed name
// first, as a claim falls back to it; else `name`, unless another job's
// marker holds it, and then the suffixed name. Asked again, it names the
// same directory while that one is there, whatever became of the other: at
// most two marker reads, no scan of the root.
pub fn own_named_dir(root: &Path, name: &str, job_id: JobId) -> PathBuf {
    let dir_name = sanitize_dirname(name);
    let suffixed = suffixed_working_dir(root, &dir_name, job_id);
    if matches!(working_dir_owner(root, &suffixed), Ok(Some(owner)) if owner == job_id) {
        return suffixed;
    }
    let plain = root.join(&dir_name);
    match working_dir_owner(root, &plain) {
        Ok(Some(owner)) if owner != job_id => suffixed,
        _ => plain,
    }
}

fn create_working_dir(
    intermediate_dir: &Path,
    job_id: JobId,
    name: &str,
) -> std::io::Result<ClaimedWorkingDir> {
    let dir_name = sanitize_dirname(name);
    let candidate = intermediate_dir.join(&dir_name);
    match std::fs::create_dir(&candidate) {
        Ok(()) => {
            return Ok(ClaimedWorkingDir {
                path: candidate,
                existed: false,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let suffixed = suffixed_working_dir(intermediate_dir, &dir_name, job_id);
    match std::fs::create_dir(&suffixed) {
        Ok(()) => Ok(ClaimedWorkingDir {
            path: suffixed,
            existed: false,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(ClaimedWorkingDir {
            path: suffixed,
            existed: true,
        }),
        Err(error) => Err(error),
    }
}

pub fn working_dir_marker_path(dir: &Path) -> PathBuf {
    dir.join(WORKING_DIR_MARKER)
}

pub fn is_weaver_owned_working_dir(dir: &Path) -> bool {
    dir.parent()
        .is_some_and(|root| owned_working_directory(root, dir).is_ok())
}

pub fn check_script_directory_owner(path: &Path, job_id: JobId) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("directory has no parent"))?;
    let dir = open_working_directory(parent, path)?;
    match read_working_marker(&dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
        Ok(stored)
            if marker_job_id(&stored) == Some(job_id)
                && !matches!(
                    match_marker(&dir, path, &stored, job_id)?,
                    MarkerMatch::Mismatch
                ) =>
        {
            Ok(())
        }
        Ok(_) => Err(std::io::Error::other(
            "script directory has a foreign ownership marker",
        )),
    }
}

// The working directory under `root` whose ownership marker is this job's.
// A moving job's row names where its delivery goes; what is still to move
// is found here by the job it belongs to, whatever the directory is named.
pub fn find_job_working_dir(root: &Path, job_id: JobId) -> std::io::Result<Option<PathBuf>> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if is_job_working_dir(root, &path, job_id)? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

// Whether `path` is a working directory under `root` that carries this
// job's ownership marker: the job's own working directory, by identity,
// never by where it sits.
pub fn is_job_working_dir(root: &Path, path: &Path, job_id: JobId) -> std::io::Result<bool> {
    Ok(working_dir_owner(root, path)? == Some(job_id))
}

// The job whose ownership marker `path`, a working directory under `root`,
// carries, if it carries one that matches it.
pub fn working_dir_owner(root: &Path, path: &Path) -> std::io::Result<Option<JobId>> {
    let Ok(dir) = open_working_directory(root, path) else {
        return Ok(None);
    };
    let Ok(stored) = read_working_marker(&dir) else {
        return Ok(None);
    };
    let Some(job_id) = marker_job_id(&stored) else {
        return Ok(None);
    };
    Ok((!matches!(
        match_marker(&dir, path, &stored, job_id)?,
        MarkerMatch::Mismatch
    ))
    .then_some(job_id))
}

// Every working directory under `root` whose ownership marker is one of
// `jobs`. A finished job's directory keeps its marker while it still holds
// something the job could not remove, and is found here by its job.
pub fn job_working_dirs(
    root: &Path,
    jobs: &std::collections::BTreeSet<JobId>,
) -> std::io::Result<std::collections::BTreeMap<JobId, Vec<PathBuf>>> {
    marked_working_dirs(root, |owner| jobs.contains(&owner))
}

// One reading of an intermediate root for housekeeping: every working
// directory in it whose ownership marker matches it, with the job the
// marker names, and how many entries could not be read. An entry that
// cannot be read is counted, never taken for a directory that is not
// there: whoever uses the reading knows it missed something.
pub struct RootScan {
    pub marked: Vec<(JobId, PathBuf)>,
    pub unreadable: usize,
}

// Whether an entry that failed this way is no working directory of ours at
// all, so a reading that passed it missed nothing: a folder the service may
// not open (it made every working directory itself, and could not remove
// this one either), and a marker that is not a regular file or does not
// read as text. Any other failure, such as an I/O error or a path that
// stops answering, may hide a job's folder.
pub fn not_ours_to_judge(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::InvalidData
    )
}

// A root that cannot be read, missing or not, is an error: nothing under it
// can be judged now. An entry that is not ours to judge is passed; any other
// that cannot be read is passed and counted.
pub fn scan_working_dirs(root: &Path) -> std::io::Result<RootScan> {
    let mut scan = RootScan {
        marked: Vec::new(),
        unreadable: 0,
    };
    for entry in std::fs::read_dir(root)? {
        let Ok(entry) = entry else {
            scan.unreadable += 1;
            continue;
        };
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => {}
            Ok(_) => continue,
            Err(_) => {
                scan.unreadable += 1;
                continue;
            }
        }
        let path = entry.path();
        let dir = match open_working_directory(root, &path) {
            Ok(dir) => dir,
            Err(error) => {
                scan.unreadable += usize::from(!not_ours_to_judge(error.kind()));
                continue;
            }
        };
        let stored = match read_working_marker(&dir) {
            Ok(stored) => stored,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                scan.unreadable += usize::from(!not_ours_to_judge(error.kind()));
                continue;
            }
        };
        let Some(owner) = marker_job_id(&stored) else {
            continue;
        };
        match match_marker(&dir, &path, &stored, owner) {
            Ok(MarkerMatch::Mismatch) => {}
            Ok(_) => scan.marked.push((owner, path)),
            Err(_) => scan.unreadable += 1,
        }
    }
    Ok(scan)
}

fn marked_working_dirs(
    root: &Path,
    wanted: impl Fn(JobId) -> bool,
) -> std::io::Result<std::collections::BTreeMap<JobId, Vec<PathBuf>>> {
    let mut found = std::collections::BTreeMap::<JobId, Vec<PathBuf>>::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(error) => return Err(error),
    };
    // An entry that cannot be read is passed and counted, so one bad entry
    // never stops the callers, who only remove what is found here.
    let mut unreadable = 0usize;
    for entry in entries {
        let Ok(entry) = entry else {
            unreadable += 1;
            continue;
        };
        let path = entry.path();
        match working_dir_owner(root, &path) {
            Ok(Some(owner)) if wanted(owner) => found.entry(owner).or_default().push(path),
            Ok(_) => {}
            Err(_) => unreadable += 1,
        }
    }
    if unreadable > 0 {
        tracing::warn!(
            root = %root.display(),
            entries = unreadable,
            "entries of the intermediate directory could not be read; they are passed"
        );
    }
    Ok(found)
}

pub fn open_working_directory(root: &Path, path: &Path) -> std::io::Result<Dir> {
    let relative = path.strip_prefix(root).map_err(|_| {
        std::io::Error::other(format!(
            "working directory {} is not inside the intermediate folder {}",
            path.display(),
            root.display()
        ))
    })?;
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

pub const MARKER_V1_PREFIX: &str = "weaver-job-v1:";
pub const MARKER_V2_PREFIX: &str = "weaver-job-v2:";

// The marker's identity hash.
//
// v2 binds the directory's path, inode and owning job. v1 also bound the
// device number, which is not stable on every filesystem: pooled and layered
// filesystems renumber it across reboots or dataset recreation, and every
// marker written before the renumbering then stopped matching its own
// directory. `dev` is only passed when recomputing a v1 marker.
pub fn marker_hash(path: &Path, dev: Option<u64>, ino: u64, job_id: JobId) -> String {
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

// The v1 markers this directory could legitimately carry for `job_id`: the
// hash with the device number as it reads now, and with none at all.
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

// The job a marker names, from either marker version.
pub fn marker_job_id(stored: &str) -> Option<JobId> {
    stored
        .strip_prefix(MARKER_V2_PREFIX)
        .or_else(|| stored.strip_prefix(MARKER_V1_PREFIX))
        .and_then(|value| value.split(':').next())
        .and_then(|value| value.parse().ok())
        .map(JobId)
}

// How a stored marker compares with what this directory should carry for
// the job it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerMatch {
    // A v2 marker for this directory and job.
    Current,
    // A v1 marker for this directory and job, to be rewritten as v2.
    Legacy,
    // The marker names this job but its hash does not match the directory.
    Mismatch,
}

pub fn match_marker(
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

// Where the v2 form of a marker is written before it replaces the v1 one.
// Never read as a marker.
pub const WORKING_DIR_MARKER_UPGRADE: &str = ".weaver-job-dir.upgrade";

// Replaces a verified v1 marker with its v2 form, so the directory stops
// depending on a device number that may change.
//
// The v2 value is written and synced under another name and then renamed
// over the marker, so a crash or a failed write leaves the v1 marker in
// place rather than a directory with no marker at all. A temporary left by
// an earlier attempt is removed first.
pub fn upgrade_legacy_marker(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<()> {
    let current = working_marker_value(dir, path, job_id)?;
    match dir.remove_file(WORKING_DIR_MARKER_UPGRADE) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    let mut file = dir.open_with(WORKING_DIR_MARKER_UPGRADE, &options)?;
    file.write_all(current.as_bytes())?;
    file.sync_all()?;
    drop(file);
    dir.rename(WORKING_DIR_MARKER_UPGRADE, dir, WORKING_DIR_MARKER)
}

pub fn read_working_marker(dir: &Dir) -> std::io::Result<String> {
    if !dir.symlink_metadata(WORKING_DIR_MARKER)?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
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
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "working directory marker is not a regular file",
        ));
    }
    let mut value = String::new();
    file.take(256).read_to_string(&mut value)?;
    Ok(value)
}

pub fn owned_working_directory(root: &Path, path: &Path) -> std::io::Result<Dir> {
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
    mark_open_working_dir(&open_working_directory(root, path)?, path, job_id)
}

fn mark_open_working_dir(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<()> {
    match read_working_marker(dir) {
        Ok(stored) if marker_job_id(&stored) == Some(job_id) => {
            match match_marker(dir, path, &stored, job_id)? {
                MarkerMatch::Current => return Ok(()),
                MarkerMatch::Legacy => return upgrade_legacy_marker(dir, path, job_id),
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
    write_working_marker(dir, &working_marker_value(dir, path, job_id)?)
}

// A job's staging directory is `<destination>/.weaver-staging/<job id>`, and
// the destination may be a folder other users can write to. Nothing under it
// is reached through a symbolic link: each of the two steps down from the
// destination is opened without following one, and what is then made, marked
// or removed is the directory that was opened, never a path looked up again.
fn staging_destination(staging: &Path, job_id: JobId) -> std::io::Result<&Path> {
    let name = job_id.0.to_string();
    staging
        .parent()
        .filter(|parent| {
            staging.file_name() == Some(name.as_ref())
                && parent.file_name() == Some(STAGING_DIR_NAME.as_ref())
        })
        .and_then(Path::parent)
        .ok_or_else(|| std::io::Error::other("not a job's staging directory"))
}

// The directory `name` directly inside `parent`, or None when nothing is
// there. A symbolic link in its place is an error, never followed.
fn open_child_dir(parent: &Dir, name: &str) -> std::io::Result<Option<Dir>> {
    match parent.symlink_metadata(name) {
        Ok(meta) if meta.file_type().is_symlink() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the job's staging directory is reached through a symbolic link",
        )),
        Ok(_) => parent.open_dir_nofollow(name).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn make_child_dir(parent: &Dir, name: &str) -> std::io::Result<Dir> {
    match parent.create_dir(name) {
        Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => return Err(error),
        _ => {}
    }
    open_child_dir(parent, name)?.ok_or_else(|| std::io::ErrorKind::NotFound.into())
}

// Make the job's staging directory and mark it the job's, as its working
// directory is.
pub fn make_job_staging_dir(staging: &Path, job_id: JobId) -> std::io::Result<()> {
    let destination = staging_destination(staging, job_id)?;
    std::fs::create_dir_all(destination)?;
    let destination = Dir::open_ambient_dir(destination, cap_std::ambient_authority())?;
    let parent = make_child_dir(&destination, STAGING_DIR_NAME)?;
    let dir = make_child_dir(&parent, &job_id.0.to_string())?;
    mark_open_working_dir(&dir, staging, job_id)
}

// Whether the job's staging directory holds anything besides its ownership
// marker. A directory that is missing holds nothing; one reached through a
// symbolic link is an error, and its target is never listed.
pub fn job_staging_dir_holds_files(staging: &Path, job_id: JobId) -> std::io::Result<bool> {
    let destination = staging_destination(staging, job_id)?;
    let destination = match Dir::open_ambient_dir(destination, cap_std::ambient_authority()) {
        Ok(destination) => destination,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let Some(parent) = open_child_dir(&destination, STAGING_DIR_NAME)? else {
        return Ok(false);
    };
    let Some(dir) = open_child_dir(&parent, &job_id.0.to_string())? else {
        return Ok(false);
    };
    for entry in dir.entries()? {
        let name = entry?.file_name();
        if name != WORKING_DIR_MARKER && name != WORKING_DIR_MARKER_UPGRADE {
            return Ok(true);
        }
    }
    Ok(false)
}

// Remove the job's staging directory, whatever it still holds, when it
// carries this job's marker. One that was never marked goes only when it is
// empty. A directory already gone is removed.
pub fn remove_job_staging_dir(staging: &Path, job_id: JobId) -> std::io::Result<HistoryWorkingDir> {
    let name = job_id.0.to_string();
    let destination = staging_destination(staging, job_id)?;
    let destination = match Dir::open_ambient_dir(destination, cap_std::ambient_authority()) {
        Ok(destination) => destination,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HistoryWorkingDir::Owned);
        }
        Err(error) => return Err(error),
    };
    let Some(parent) = open_child_dir(&destination, STAGING_DIR_NAME)? else {
        return Ok(HistoryWorkingDir::Owned);
    };
    let Some(dir) = open_child_dir(&parent, &name)? else {
        return Ok(HistoryWorkingDir::Owned);
    };
    let stored = match read_working_marker(&dir) {
        Ok(stored) => stored,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            drop(dir);
            return Ok(match parent.remove_dir(&name) {
                Ok(()) => HistoryWorkingDir::Owned,
                Err(error) => {
                    tracing::warn!(
                        job_id = job_id.0,
                        dir = %staging.display(),
                        %error,
                        "a staging directory with no ownership marker is not empty; leaving it in place"
                    );
                    HistoryWorkingDir::LeftInPlace
                }
            });
        }
        Err(error) => return Err(error),
    };
    match judge_history_marker(&dir, staging, &stored, job_id)? {
        HistoryWorkingDir::Owned => {
            dir.remove_open_dir_all()?;
            Ok(HistoryWorkingDir::Owned)
        }
        HistoryWorkingDir::LeftInPlace => Ok(HistoryWorkingDir::LeftInPlace),
    }
}

pub fn remove_weaver_owned_working_dir(root: &Path, path: &Path) -> std::io::Result<()> {
    owned_working_directory(root, path)?.remove_open_dir_all()
}

// Every working directory under `root` whose ownership marker matches it,
// with the job the marker names, that `keep` does not claim. A directory
// without a marker that matches is never one of them.
pub fn unclaimed_working_dirs(
    root: &Path,
    keep: impl Fn(JobId, &Path) -> bool,
) -> std::io::Result<Vec<(JobId, PathBuf)>> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut unclaimed = Vec::new();
    // An entry that cannot be read is passed and counted: it is never
    // removed, and never stops the rest.
    let mut unreadable = 0usize;
    for entry in entries {
        let Ok(entry) = entry else {
            unreadable += 1;
            continue;
        };
        let path = entry.path();
        let Some(owner) = working_dir_owner(root, &path).ok().flatten() else {
            continue;
        };
        if !keep(owner, &path) {
            unclaimed.push((owner, path));
        }
    }
    if unreadable > 0 {
        tracing::warn!(
            root = %root.display(),
            entries = unreadable,
            "entries of the intermediate directory could not be read; they are passed"
        );
    }
    Ok(unclaimed)
}

// Removes one directory found unclaimed, when its marker still names
// `owner`, the job it was judged by: a directory that has changed hands
// since is left. One that still holds what it cannot give up stays for a
// later cleanup.
pub fn remove_unclaimed_working_dir(root: &Path, path: &Path, owner: JobId) -> bool {
    match remove_job_working_dir(root, path, owner) {
        Ok(HistoryWorkingDir::LeftInPlace) => false,
        Ok(HistoryWorkingDir::Owned) => {
            tracing::info!(dir = %path.display(), "removed an unreferenced intermediate directory");
            true
        }
        Err(error) => {
            tracing::warn!(
                dir = %path.display(),
                error = %error,
                "could not remove an unreferenced intermediate directory; it stays for a later cleanup"
            );
            false
        }
    }
}

// What a history cleanup did with one job's working directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryWorkingDir {
    // The directory is the job's (or is already gone) and may be removed.
    Owned,
    // The marker names this job but no longer matches the directory, so the
    // directory is left on disk. The history record can still go: nothing
    // about the mismatch makes the directory anyone else's.
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

// Delete only the directory still owned by the job selected for cleanup.
// Validation and removal use the same opened directory capability.
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

// `remove_job_working_dir`, for a remover that may come after another of
// the same directory: a directory that is gone is removed, and so owned.
pub fn remove_job_working_dir_if_there(
    root: &Path,
    path: &Path,
    job_id: JobId,
) -> std::io::Result<HistoryWorkingDir> {
    match remove_job_working_dir(root, path, job_id) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && matches!(std::fs::symlink_metadata(path), Err(gone) if gone.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(HistoryWorkingDir::Owned)
        }
        result => result,
    }
}

// A failed job's working directory keeps only what the job delivered. The
// graph's internals and the run's caches go: their checkpoint leaves with
// the job, so nothing could read them again. A directory left holding nothing but its ownership
// marker goes whole. A directory this job does not own is left as it is.
pub fn release_failed_working_dir(root: &Path, path: &Path, job_id: JobId) -> std::io::Result<()> {
    let dir = match open_working_directory(root, path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        result => result?,
    };
    let stored = read_working_marker(&dir)?;
    if judge_history_marker(&dir, path, &stored, job_id)? != HistoryWorkingDir::Owned {
        return Ok(());
    }
    for scratch in [
        weaver_graph::names::INTERNAL_DIR,
        weaver_graph::names::CACHE_DIR,
    ] {
        match dir.remove_dir_all(scratch) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
    }
    let mut delivered = false;
    for entry in dir.entries()? {
        delivered |= entry?.file_name() != WORKING_DIR_MARKER;
    }
    if !delivered {
        dir.remove_open_dir_all()?;
    }
    Ok(())
}

// The working directory a job that never started was given under the
// intermediate folder set now, and the one it had, still on disk until the
// job's row names the new one.
pub struct FollowedDir {
    pub path: PathBuf,
    root: PathBuf,
    job_id: JobId,
    // The claim found the directory already there, under the job's own
    // suffixed name: giving the claim up leaves it.
    existed: bool,
    // The directory the job had, when it was there: this job's own, holding
    // nothing but its marker. Only its path is kept: on Windows a folder held
    // open cannot be removed by anyone else, and a cancel's cleanup may
    // remove it while the row is written.
    old: Option<PathBuf>,
}

impl FollowedDir {
    // The job's row names the new directory: the old one goes, if it still
    // holds nothing but the marker.
    pub fn leave_old(self) {
        let Some(path) = self.old else {
            return;
        };
        let Some(old_root) = path.parent() else {
            return;
        };
        // Opened again by its path and judged as at the claim: gone
        // meanwhile, or no longer the job's own with nothing but its marker,
        // and it is left.
        let emptied = match open_working_directory(old_root, &path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
            Ok(dir) => holds_only_its_marker(&dir, &path, self.job_id).and_then(|only| {
                if only {
                    dir.remove_open_dir_all().map(|()| true)
                } else {
                    Ok(false)
                }
            }),
        };
        if let Err(error) = emptied {
            tracing::warn!(job_id = self.job_id.0, dir = %path.display(), %error, "could not remove the working directory a job moved out of");
        }
    }

    // The new directory is not the job's after all: what the claim made
    // goes, and whatever the job had stays as it is. It goes while it holds
    // nothing but its marker, or not even that, as a removal of it cut short
    // leaves it: anything else in it is not the claim's to remove.
    pub fn abandon(self) {
        if self.existed {
            return;
        }
        let removed = (|| {
            let dir = match open_working_directory(&self.root, &self.path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                result => result?,
            };
            let mut entries = 0;
            for entry in dir.entries()? {
                if entry?.file_name() != WORKING_DIR_MARKER {
                    return Ok(());
                }
                entries += 1;
            }
            if entries == 0 {
                // Judged under the claim lock, so a claim of the same name
                // that has made the directory and not yet marked it is not
                // taken for one cut short.
                let _claims = claims();
                if dir.entries()?.next().is_none() {
                    return dir.remove_open_dir_all();
                }
                return Ok(());
            }
            if holds_only_its_marker(&dir, &self.path, self.job_id)? {
                dir.remove_open_dir_all()?;
            }
            Ok::<_, std::io::Error>(())
        })();
        if let Err(error) = removed
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(job_id = self.job_id.0, dir = %self.path.display(), %error, "could not remove a working directory claimed for a job that did not take it");
        }
    }
}

// Whether the working directory `dir`, at `path`, is `job_id`'s own and holds
// nothing but its marker. A marker that cannot be read proves nothing.
fn holds_only_its_marker(dir: &Dir, path: &Path, job_id: JobId) -> std::io::Result<bool> {
    let Ok(stored) = read_working_marker(dir) else {
        return Ok(false);
    };
    if marker_job_id(&stored) != Some(job_id)
        || !matches!(
            match_marker(dir, path, &stored, job_id)?,
            MarkerMatch::Current | MarkerMatch::Legacy
        )
    {
        return Ok(false);
    }
    for entry in dir.entries()? {
        if entry?.file_name() != WORKING_DIR_MARKER {
            return Ok(false);
        }
    }
    Ok(true)
}

// A job that never started works under the intermediate folder set now. Its
// working directory elsewhere is claimed again, marked, under `root`, when
// the one it has is this job's own and holds nothing but its marker, or is
// gone, folder or root: a job that never started left nothing there. One
// that is another's or holds anything else is left as it is, and the job
// keeps it: None. A directory already under `root` is kept while it is
// there, and claimed again when it is gone. Nothing is removed here: the
// caller records the new directory, then leaves the old.
pub fn follow_intermediate_dir(
    root: &Path,
    current: &Path,
    job_id: JobId,
    name: &str,
) -> std::io::Result<Option<FollowedDir>> {
    let Some(old_root) = current.parent() else {
        return Ok(None);
    };
    // Under `root` already: one existence check, and the directory is kept
    // unless it is gone. Gone with the root itself not there, as under a
    // mount that is not up yet, the job keeps what it has: a claim under
    // the root the job already had never makes that root.
    let same_root = old_root == root;
    if same_root
        && (!matches!(std::fs::symlink_metadata(current), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            || std::fs::symlink_metadata(root).is_err())
    {
        return Ok(None);
    }
    let old = match open_working_directory(old_root, current) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
        // The handle is dropped here, before the claim: see `FollowedDir::old`.
        Ok(dir) => {
            if !holds_only_its_marker(&dir, current, job_id)? {
                return Ok(None);
            }
            Some(current.to_path_buf())
        }
    };
    let claimed = if same_root {
        claim_under_root(root, job_id, name)?
    } else {
        claim_working_dir(root, job_id, name)?
    };
    Ok(Some(FollowedDir {
        path: claimed.path,
        root: root.to_path_buf(),
        job_id,
        existed: claimed.existed,
        old,
    }))
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
        // An adoption never reads or writes a marker beside a claim: a
        // directory made for a job being added has that job's marker before
        // it is judged here, whenever it was opened.
        let stored = {
            let _claims = claims();
            // The caller names this as the job's own working directory, so
            // one with no marker at all is adopted as one with an empty
            // marker is.
            let stored = match read_working_marker(&dir) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                result => result?,
            };
            if stored.is_empty() {
                // The durable history row supplies the legacy directory's job identity.
                mark_open_working_dir(&dir, &path, job_id)?;
                return Ok(HistoryWorkingDir::Owned);
            }
            stored
        };
        judge_history_marker(&dir, &path, &stored, job_id)
    })
    .await
    .map_err(std::io::Error::other)?
}

// Who an output directory's ownership marker says delivered it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputDirOwner {
    // The job the marker names.
    Job(JobId),
    // A marker written before markers named their job: it proves the
    // directory is one Weaver delivered, not which job delivered it.
    Legacy,
}

impl OutputDirOwner {
    // Whether the marker lets `job_id` remove the directory: one naming that
    // job, or a legacy one when `legacy_ok` says no other job records the
    // same directory.
    pub fn admits(self, job_id: JobId, legacy_ok: bool) -> bool {
        match self {
            Self::Job(owner) => owner == job_id,
            Self::Legacy => legacy_ok,
        }
    }
}

const OUTPUT_MARKER_V1_PREFIX: &str = "weaver-output-v1:";
const OUTPUT_MARKER_V2_PREFIX: &str = "weaver-output-v2:";

// Marks `dir` as the output `job_id` delivered.
pub fn mark_weaver_owned_output_dir(dir: &Path, job_id: JobId) -> std::io::Result<()> {
    std::fs::write(
        dir.join(OUTPUT_DIR_MARKER),
        output_marker_value(dir, job_id)?,
    )
}

// The owner `dir`'s marker names, when `dir` is a real directory holding a
// marker made for this very directory; `None` otherwise.
pub fn output_dir_owner(dir: &Path) -> Option<OutputDirOwner> {
    let directory_metadata = std::fs::symlink_metadata(dir).ok()?;
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        return None;
    }
    let marker = dir.join(OUTPUT_DIR_MARKER);
    let marker_metadata = std::fs::symlink_metadata(&marker).ok()?;
    if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
        return None;
    }
    let canonical = std::fs::canonicalize(dir).ok()?;
    parse_output_marker(&std::fs::read(&marker).ok()?, &canonical)
}

// Whether `dir` carries a valid marker of any owner.
pub fn is_weaver_owned_output_dir(dir: &Path) -> bool {
    output_dir_owner(dir).is_some()
}

fn output_path_digest(path: &Path) -> String {
    blake3::hash(path.as_os_str().as_encoded_bytes())
        .to_hex()
        .to_string()
}

// The owner a stored marker names when it was made for `path`.
fn parse_output_marker(stored: &[u8], path: &Path) -> Option<OutputDirOwner> {
    let text = std::str::from_utf8(stored).ok()?.strip_suffix('\n')?;
    let digest = output_path_digest(path);
    if let Some(rest) = text.strip_prefix(OUTPUT_MARKER_V2_PREFIX) {
        let (job_id, hash) = rest.split_once(':')?;
        let job_id = job_id.parse::<u64>().ok()?;
        return (hash == digest).then_some(OutputDirOwner::Job(JobId(job_id)));
    }
    let hash = text.strip_prefix(OUTPUT_MARKER_V1_PREFIX)?;
    (hash == digest).then_some(OutputDirOwner::Legacy)
}

// A marker as a build before job-named markers wrote it, for tests of how
// one is still read.
#[cfg(any(test, feature = "test-support"))]
pub fn mark_legacy_output_dir(dir: &Path) -> std::io::Result<()> {
    let canonical = std::fs::canonicalize(dir)?;
    std::fs::write(
        dir.join(OUTPUT_DIR_MARKER),
        format!(
            "{OUTPUT_MARKER_V1_PREFIX}{}\n",
            output_path_digest(&canonical)
        ),
    )
}

// Removes `dir` once it holds nothing but, at most, the marker naming
// `job_id`: the folder a torrent's job made for its files, which goes once
// they have, as an empty one did before it was marked. One holding anything
// else, or another job's marker, stays. Whether it went.
pub fn remove_emptied_output_dir(dir: &Path, job_id: JobId) -> std::io::Result<bool> {
    let mut marker = false;
    for entry in std::fs::read_dir(dir)? {
        if entry?.file_name() != OUTPUT_DIR_MARKER || marker {
            return Ok(false);
        }
        marker = true;
    }
    if marker {
        if output_dir_owner(dir) != Some(OutputDirOwner::Job(job_id)) {
            return Ok(false);
        }
        std::fs::remove_file(dir.join(OUTPUT_DIR_MARKER))?;
    }
    std::fs::remove_dir(dir).map(|()| true)
}

// Removes an output directory its marker proved Weaver's, the marker last:
// everything else in it first, then the marker, then the directory. A
// removal that fails part way leaves the marker, so a retry still proves the
// rest Weaver's and finishes. A link inside is removed, never followed.
pub fn remove_owned_output_dir(dir: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == OUTPUT_DIR_MARKER {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            match std::fs::remove_file(&path) {
                // A directory link on Windows goes as a directory.
                Err(error) if file_type.is_symlink() => {
                    std::fs::remove_dir(&path).map_err(|_| error)?;
                }
                result => result?,
            }
        }
        #[cfg(any(test, feature = "test-support"))]
        injected_removal_failure(dir)?;
    }
    match std::fs::remove_file(dir.join(OUTPUT_DIR_MARKER)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    std::fs::remove_dir(dir)
}

// Output directories whose removal fails once one entry is gone, for tests
// of a removal that fails part way. Keyed by directory, so tests in
// separate temporary directories do not meet.
#[cfg(any(test, feature = "test-support"))]
static INJECTED_REMOVAL_FAILURES: std::sync::Mutex<Vec<PathBuf>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(any(test, feature = "test-support"))]
pub fn inject_removal_failure(dir: &Path, fail: bool) {
    let mut failing = INJECTED_REMOVAL_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    failing.retain(|failing| failing != dir);
    if fail {
        failing.push(dir.to_path_buf());
    }
}

#[cfg(any(test, feature = "test-support"))]
fn injected_removal_failure(dir: &Path) -> std::io::Result<()> {
    let failing = INJECTED_REMOVAL_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if failing.iter().any(|failing| failing == dir) {
        return Err(std::io::Error::other("injected removal failure"));
    }
    Ok(())
}

// Whether `dir` is the output `job_id` delivered at `previous`, moved since
// by the job's own script: its marker names `previous` and the job, or is a
// legacy marker for `previous`, and nothing is left at `previous`.
pub fn is_relocated_output_dir(dir: &Path, previous: &Path, job_id: JobId) -> bool {
    if !matches!(std::fs::symlink_metadata(previous), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return false;
    }
    let Some(parent) = previous
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
    else {
        return false;
    };
    let Some(name) = previous.file_name() else {
        return false;
    };
    let previous = parent.join(name);
    let Ok(metadata) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return false;
    }
    let marker = dir.join(OUTPUT_DIR_MARKER);
    let Ok(metadata) = std::fs::symlink_metadata(&marker) else {
        return false;
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    std::fs::read(&marker).is_ok_and(|stored| {
        parse_output_marker(&stored, &previous).is_some_and(|owner| owner.admits(job_id, true))
    })
}

fn output_marker_value(dir: &Path, job_id: JobId) -> std::io::Result<Vec<u8>> {
    let canonical = std::fs::canonicalize(dir)?;
    Ok(format!(
        "{OUTPUT_MARKER_V2_PREFIX}{}:{}\n",
        job_id.0,
        output_path_digest(&canonical)
    )
    .into_bytes())
}

// Moved from the pipeline's queue: history's file removal below the pipeline
// reaches the same staging paths.
// Where a job writes the files it delivers when its intermediate directory
// and its destination lie on different devices: its staging directory on the
// destination's device, so finalize renames them into place there instead of
// copying every byte across. On one device nothing is staged. Whether they
// share a device is the disk owner's answer,
// [`crate::operations::StorageCapacity::same_filesystem`].
pub fn staging_dir_on(one_device: bool, destination_root: &Path, job_id: JobId) -> Option<PathBuf> {
    (!one_device).then(|| staging_dir_for(destination_root, job_id))
}

pub fn staging_dir_for(destination_root: &Path, job_id: JobId) -> PathBuf {
    destination_root
        .join(STAGING_DIR_NAME)
        .join(job_id.0.to_string())
}

// The job's staging directories under each of `roots`, whatever they still
// hold. Only a directory carrying the job's marker goes, and no symbolic link
// on the way down from a root is followed.
//
// `roots` is `destination_roots`: the complete folder first, then each
// category's folder. A job's staging directory is under the complete folder
// unless its category delivers elsewhere, and a history row does not say
// which, so every root is swept. The complete folder must be reachable. A
// category folder in which the job's staging directory cannot be looked up
// is passed over with a warning, so one unreachable category does not stop
// the cleanup of jobs that never used it; whatever is found under a folder
// that can be searched is removed or fails as before.
pub async fn remove_job_staging(
    roots: &[std::path::PathBuf],
    id: JobId,
) -> Result<(), crate::jobs::error::SchedulerError> {
    remove_job_staging_checked(roots, id).await.map(|_| ())
}

// `remove_job_staging`, saying whether every root was searched: false when a
// category folder was passed over, so the job's staging there is not known
// to be gone.
pub async fn remove_job_staging_checked(
    roots: &[std::path::PathBuf],
    id: JobId,
) -> Result<bool, crate::jobs::error::SchedulerError> {
    let mut checked = true;
    for (index, root) in roots.iter().enumerate() {
        let staging = staging_dir_for(root, id);
        if index > 0 && !staging_dir_reachable(root.clone(), staging.clone(), id).await? {
            checked = false;
            continue;
        }
        remove_staging_dir(staging, id).await?;
    }
    Ok(checked)
}

// Whether the job's staging directory under a category's folder can be looked
// up, which is what removing it needs. Opening the folder is not the test: on
// Linux a directory opens with `O_PATH` even when its mode lets nothing be
// looked up in it. A staging directory that does not exist is reachable, as
// there is nothing to remove.
async fn staging_dir_reachable(
    root: std::path::PathBuf,
    staging: std::path::PathBuf,
    id: JobId,
) -> Result<bool, crate::jobs::error::SchedulerError> {
    tokio::task::spawn_blocking(move || match std::fs::symlink_metadata(&staging) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::warn!(
                job_id = id.0,
                dir = %root.display(),
                %error,
                "a category folder could not be searched; its staging directory for the job is not checked"
            );
            false
        }
    })
    .await
    .map_err(|error| crate::jobs::error::SchedulerError::Internal(error.to_string()))
}

async fn remove_staging_dir(
    staging: std::path::PathBuf,
    id: JobId,
) -> Result<(), crate::jobs::error::SchedulerError> {
    let removed = tokio::task::spawn_blocking(move || remove_job_staging_dir(&staging, id))
        .await
        .map_err(|error| crate::jobs::error::SchedulerError::Internal(error.to_string()))?;
    match removed {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Err(
            crate::jobs::error::SchedulerError::Conflict(error.to_string()),
        ),
        Err(error) => Err(error.into()),
    }
}
