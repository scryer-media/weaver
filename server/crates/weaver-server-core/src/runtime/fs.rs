// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use std::sync::OnceLock;

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

// Moves a directory or a file and never replaces an existing target. Atomic
// wherever the mount takes the exclusive rename; see `rename_exclusive_with`
// for the mounts that refuse it.
pub(crate) fn rename_path_exclusive(src: &Path, dst: &Path) -> io::Result<()> {
    rename_path_exclusive_or(src, dst, Unlinkable::Copied)
}

// `rename_path_exclusive` that never copies: a move of names alone, however
// large the file. The same exclusive rename, then the same link and unlink.
// A symbolic link that neither took is made again at the target, which
// never replaces, and removed at the source.
//
// Anything else is looked for at the target and then renamed, as
// libtorrent moves a torrent's files (`storage_utils.cpp` `move_storage`:
// `exists`, then `move_file`; `path.cpp` `rename` is a plain rename(2)),
// whenever neither the exclusive rename nor the link moved it. Only two
// answers of those end the move: the target exists (`AlreadyExists`, for
// the caller to number or refuse), and another device (`CrossesDevices`,
// for the caller to copy). Any other, as network and FUSE mounts give
// (EINVAL, ENOTSUP, EPERM, EACCES, EMLINK and more), passes to the next
// step, and the plain rename's own answer is the move's.
//
// An entry made at the target between that look and that rename is
// replaced by a file: no move left there refuses it.
pub(crate) fn rename_path_exclusive_in_place(src: &Path, dst: &Path) -> io::Result<()> {
    rename_path_exclusive_or(src, dst, Unlinkable::Renamed)
}

// What becomes of a file on a mount with neither an exclusive rename nor
// hard links.
#[derive(Clone, Copy)]
enum Unlinkable {
    // Copied into a file made new, and its source removed.
    Copied,
    // Renamed after a look at the target.
    Renamed,
}

fn rename_path_exclusive_or(src: &Path, dst: &Path, unlinkable: Unlinkable) -> io::Result<()> {
    if src == dst {
        return Ok(());
    }
    // `MoveFileExW` below, without `MOVEFILE_COPY_ALLOWED` or
    // `MOVEFILE_REPLACE_EXISTING`, never copies and never replaces.
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let _ = unlinkable;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        rename_exclusive_with_link(src, dst, exclusive_rename_native, unlinkable, |src, dst| {
            std::fs::hard_link(src, dst)
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let path = |path: &Path| -> io::Result<Vec<u16>> {
            let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
            if wide.contains(&0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "path contains NUL",
                ));
            }
            wide.push(0);
            Ok(wide)
        };
        let src = path(src)?;
        let dst = path(dst)?;
        // No REPLACE_EXISTING or COPY_ALLOWED: the destination stays exclusive.
        let result = unsafe {
            windows_sys::Win32::Storage::FileSystem::MoveFileExW(
                src.as_ptr(),
                dst.as_ptr(),
                windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
            )
        };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic exclusive file moves are unsupported on this platform",
        ))
    }
}

// One job's file moves that never replace what is there, in one run of
// placing or moving its files.
//
// `rename_path_exclusive_in_place` is the move: an entry at the target,
// whenever it came, is answered `AlreadyExists` and stays as it was (but on
// a mount with no move that refuses, where it is looked for first). A file
// is copied only across devices, never on its own. Across devices,
// where no rename goes, the file is copied to a new hidden name in the
// target's folder and moved from there by the same never-replacing move, so
// the target is never a file half written. The copy is removed whenever the
// move does not end at the target.
//
// The hidden copy is named for the job (`move_copy_name`), one of
// `MOVE_COPY_TRIES` names. A run that died mid-copy leaves one behind; the
// job's next move into that folder removes it first (`sweep_move_copies`),
// and so does a removal of the torrent with its files.
pub(crate) struct JobMoves {
    job: u64,
    // The folders swept of the job's left copies in this run.
    swept: std::cell::RefCell<std::collections::HashSet<PathBuf>>,
}

// How many names a job's hidden copy may take in one folder. A move that
// finds every one taken, or cannot make any, fails with that error.
pub(crate) const MOVE_COPY_TRIES: u32 = 16;

// The `n`th name of `job`'s hidden copy.
pub(crate) fn move_copy_name(job: u64, n: u32) -> String {
    format!(".wvr-{job}-{n:x}")
}

// Removes from `folder` the hidden copies `job`'s moves left there: only a
// regular file under one of that job's own names, never another job's and
// never a folder or a link.
pub(crate) fn sweep_move_copies(job: u64, folder: &Path) {
    for n in 0..MOVE_COPY_TRIES {
        let copy = folder.join(move_copy_name(job, n));
        if std::fs::symlink_metadata(&copy).is_ok_and(|found| found.file_type().is_file())
            && let Err(error) = std::fs::remove_file(&copy)
        {
            tracing::warn!(path = %copy.display(), error = %error, "a left copy of a moved file could not be removed");
        }
    }
}

impl JobMoves {
    pub(crate) fn of(job: u64) -> Self {
        Self {
            job,
            swept: Default::default(),
        }
    }

    // Moves the file `src` to `dst`, never over what is there.
    pub(crate) fn move_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.move_file_using(src, dst, rename_path_exclusive_in_place)
    }

    pub(crate) fn move_file_using(
        &self,
        src: &Path,
        dst: &Path,
        rename: impl Fn(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let folder = dst.parent().unwrap_or_else(|| Path::new("."));
        if self.swept.borrow_mut().insert(folder.to_path_buf()) {
            sweep_move_copies(self.job, folder);
        }
        match rename(src, dst) {
            Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {}
            moved => return moved,
        }
        // No copy is made for a name that is taken already.
        if std::fs::symlink_metadata(dst).is_ok() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        let mut made = None;
        for n in 0..MOVE_COPY_TRIES {
            let copy = folder.join(move_copy_name(self.job, n));
            // Made new, so it is this move's own; a name something else
            // holds is passed over. The copy that failed removed what it
            // made.
            match file_cache::copy_large_file(src, &copy) {
                Ok(_) => {
                    made = Some(Ok(copy));
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    made = Some(Err(error));
                }
                Err(error) => return Err(error),
            }
        }
        // Every name was taken, or the folder itself cannot be made (a file
        // holds its name): the last answer, never another try.
        let copy = made.unwrap_or_else(|| Err(io::Error::from(io::ErrorKind::AlreadyExists)))?;
        if let Err(error) = rename(&copy, dst) {
            let _ = std::fs::remove_file(&copy);
            return Err(error);
        }
        // The file is at `dst`, whole: a source that will not go is only
        // left behind.
        if let Err(error) = std::fs::remove_file(src)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(path = %src.display(), error = %error, "a moved file was copied but could not be removed from where it was");
        }
        Ok(())
    }
}

// The one call that both moves and refuses an existing target, atomically.
#[cfg(any(target_os = "linux", target_os = "macos"))]
type ExclusiveRename = fn(&std::ffi::CStr, &std::ffi::CStr) -> io::Result<()>;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn exclusive_rename_native(src: &std::ffi::CStr, dst: &std::ffi::CStr) -> io::Result<()> {
    // The raw syscall rather than the libc wrapper: musl's static libc has
    // no renameat2 symbol, so the portable release binary cannot link it.
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            src.as_ptr(),
            libc::AT_FDCWD,
            dst.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renamex_np(src.as_ptr(), dst.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

// A mount that cannot refuse an existing target in the rename itself (NFS,
// CIFS, 9p, virtiofs, FUSE without the flag) answers the flag with one of
// these, as systemd's rename_noreplace() reads them (src/basic/fs-util.c).
// Files fall back to link/unlink, then an exclusively created copy when
// links are unavailable. A check followed by plain rename could overwrite
// a file created between the two. Directories retain their inode for the
// working-directory ownership journal; plain rename cannot replace a file
// or a nonempty directory with a directory.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn rename_exclusive_with(src: &Path, dst: &Path, exclusive: ExclusiveRename) -> io::Result<()> {
    rename_exclusive_with_link(src, dst, exclusive, Unlinkable::Copied, |src, dst| {
        std::fs::hard_link(src, dst)
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_exclusive_with_link(
    src: &Path,
    dst: &Path,
    exclusive: ExclusiveRename,
    unlinkable: Unlinkable,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = |path: &Path| {
        std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
    };
    // Whether an answer of the first two rungs passes to the next one. In
    // place, only "the target exists" and "another device" end the move
    // there: network and FUSE mounts answer these calls with many codes
    // where a plain rename still works, and the last rung's rename says
    // what is wrong when nothing works. A move that copies when no name
    // move goes passes on "not supported" alone, so a refusal never turns
    // into a copy on the same device.
    let passes = |error: &io::Error, also: &[i32]| match unlinkable {
        Unlinkable::Renamed => !matches!(error.raw_os_error(), Some(libc::EEXIST | libc::EXDEV)),
        Unlinkable::Copied => refused(error, also),
    };
    match exclusive(&path(src)?, &path(dst)?) {
        Err(error) if passes(&error, &[]) => {}
        moved => return moved,
    }
    // A directory has no hard links: link(2) answers it EPERM.
    match link(src, dst) {
        Ok(()) => {
            return std::fs::remove_file(src).inspect_err(|_| {
                let _ = std::fs::remove_file(dst);
            });
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return finish_or_refuse(src, dst);
        }
        // Another device: no move of names goes there, and the caller that
        // never copies in place is told so.
        Err(error)
            if matches!(unlinkable, Unlinkable::Renamed)
                && error.kind() == io::ErrorKind::CrossesDevices =>
        {
            return Err(error);
        }
        Err(error) if passes(&error, &[libc::EPERM, libc::EXDEV]) => {}
        Err(error) => return Err(error),
    }
    let source = std::fs::symlink_metadata(src)?;
    if let Some(settled) = settled_before_copy(src, dst) {
        return settled;
    }
    // A symbolic link is made again at the target, which never replaces:
    // neither a copy nor a rename that cannot refuse.
    if source.file_type().is_symlink() {
        return move_symlink_no_overwrite(src, dst);
    }
    if matches!(unlinkable, Unlinkable::Renamed) {
        // The limit `rename_path_exclusive_in_place` names: the look
        // above, then a rename that cannot refuse.
        return std::fs::rename(src, dst);
    }
    if source.is_file() {
        return copy_file_no_overwrite_then_remove_source(src, dst);
    }
    if !source.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported move source",
        ));
    }
    std::fs::rename(src, dst)
}

// The target exists. A file whose target is its own inode is a link and
// unlink cut off between the two: the move finishes by unlinking the
// source. Any other target is refused.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn finish_or_refuse(src: &Path, dst: &Path) -> io::Result<()> {
    let source = std::fs::symlink_metadata(src)?;
    let target = std::fs::symlink_metadata(dst)?;
    if !source.is_dir()
        && source.ino() != 0
        && (source.dev(), source.ino()) == (target.dev(), target.ino())
    {
        let src_parent = src.parent().unwrap_or_else(|| Path::new("."));
        let dst_parent = dst.parent().unwrap_or_else(|| Path::new("."));
        let from = std::fs::metadata(src_parent)?;
        let to = std::fs::metadata(dst_parent)?;
        if (from.dev(), from.ino()) == (to.dev(), to.ino()) {
            if src.file_name() == dst.file_name() {
                return Ok(());
            }
            // Case-folding and Unicode-normalizing mounts can resolve two
            // spellings to one entry. Only unlink a proven second link.
            let mut links = 0;
            let mut source_named = false;
            let mut target_named = false;
            for entry in std::fs::read_dir(src_parent)? {
                let entry = entry?;
                let metadata = std::fs::symlink_metadata(entry.path())?;
                if (metadata.dev(), metadata.ino()) == (source.dev(), source.ino()) {
                    links += 1;
                    source_named |= Some(entry.file_name().as_os_str()) == src.file_name();
                    target_named |= Some(entry.file_name().as_os_str()) == dst.file_name();
                }
            }
            if links == 1 {
                return Ok(());
            }
            if !source_named || !target_named {
                return Err(io::Error::from_raw_os_error(libc::EEXIST));
            }
        }
        return std::fs::remove_file(src);
    }
    Err(io::Error::from_raw_os_error(libc::EEXIST))
}

// The call is not supported here, as opposed to failed.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn refused(error: &io::Error, also: &[i32]) -> bool {
    const UNSUPPORTED: [i32; 5] = [
        libc::EINVAL,
        libc::ENOSYS,
        libc::ENOTTY,
        libc::EOPNOTSUPP,
        libc::ENOTSUP,
    ];
    error
        .raw_os_error()
        .is_some_and(|code| UNSUPPORTED.contains(&code) || also.contains(&code))
}

pub(crate) fn paths_equivalent_for_placement(left: &Path, right: &Path) -> bool {
    path_equivalence_key(left) == path_equivalence_key(right)
}

// Where a move by name alone left an entry.
#[derive(Debug)]
pub(crate) enum Placement {
    // At its destination.
    Placed,
    // Still at its source, the destination free when this was said: the
    // mount could not place it by name (a link or rename across devices,
    // or one it refuses), and only a copy can. The error says why. The
    // caller makes that copy where it may take its time, exclusively
    // created, never over an entry that appears meanwhile.
    Copy(io::Error),
}

// Moves a file and never replaces an existing target, copying it when the
// mount cannot move it by name. A caller that must not spend a copy's time
// where it stands uses `place_no_overwrite` and copies elsewhere.
pub(crate) fn rename_no_overwrite(src: &Path, dst: &Path) -> io::Result<()> {
    match place_no_overwrite(src, dst)? {
        Placement::Placed => Ok(()),
        Placement::Copy(_) => copy_file_no_overwrite_then_remove_source(src, dst),
    }
}

// `rename_no_overwrite` up to the copy: only metadata changes, a link and
// an unlink, a rename of the same entry or a symlink made again.
pub(crate) fn place_no_overwrite(src: &Path, dst: &Path) -> io::Result<Placement> {
    place_no_overwrite_with_link(src, dst, |src, dst| std::fs::hard_link(src, dst))
}

pub(crate) fn place_no_overwrite_with_link(
    src: &Path,
    dst: &Path,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<Placement> {
    if paths_equivalent_for_placement(src, dst) {
        if src == dst {
            return Ok(Placement::Placed);
        }
        return rename_equivalent_path(src, dst).map(|()| Placement::Placed);
    }

    let metadata = std::fs::symlink_metadata(src)?;
    if metadata.file_type().is_symlink() {
        return move_symlink_no_overwrite(src, dst).map(|()| Placement::Placed);
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

    match link(src, dst) {
        Ok(()) => {
            std::fs::remove_file(src)?;
            Ok(Placement::Placed)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("destination already exists: {}", dst.display()),
        )),
        Err(error) => match settled_before_copy(src, dst) {
            Some(settled) => settled.map(|()| Placement::Placed).map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("destination already exists: {}", dst.display()),
                    )
                } else {
                    error
                }
            }),
            None => Ok(Placement::Copy(error)),
        },
    }
}

// Some mounts reject a link or rename before checking whether the target
// exists. A target that is there is settled before any copy: another
// spelling or link of the source, which finishes the move without taking
// its only name, or another entry, which refuses it. None when the target
// is free. The copy itself still creates exclusively against a later
// collision.
fn settled_before_copy(src: &Path, dst: &Path) -> Option<io::Result<()>> {
    match std::fs::symlink_metadata(dst) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => return Some(Err(error)),
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        Some(finish_or_refuse(src, dst))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = src;
        Some(Err(io::Error::from(io::ErrorKind::AlreadyExists)))
    }
}

fn copy_file_no_overwrite_then_remove_source(src: &Path, dst: &Path) -> io::Result<()> {
    let parent_fingerprint = prepare_destination_parent(dst)?;
    file_cache::copy_large_file(src, dst)?;
    if !destination_parent_matches(dst, &parent_fingerprint)? {
        return Err(io::Error::other(format!(
            "destination parent changed during copy: {}",
            dst.display()
        )));
    }
    std::fs::remove_file(src)
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

// Devices a test gives the directories it makes, so one machine's disk
// reads as several: a path reads as the device of the longest directory it
// lies under. Each test names only its own temporary directories, so tests
// running together never read each other's.
#[cfg(test)]
pub(crate) mod test_devices {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static DEVICES: Mutex<Vec<(PathBuf, u64)>> = Mutex::new(Vec::new());

    pub(crate) fn assign(dir: &Path, device: u64) {
        DEVICES.lock().unwrap().push((dir.to_path_buf(), device));
    }

    // Directories whose device reads are held: each read announces itself
    // on the sender and waits until the hold is dropped.
    type Hold = (
        PathBuf,
        tokio::sync::mpsc::UnboundedSender<PathBuf>,
        std::sync::Arc<(Mutex<bool>, std::sync::Condvar)>,
    );
    static HELD: Mutex<Vec<Hold>> = Mutex::new(Vec::new());

    // Releases the device reads of its directory when dropped.
    pub(crate) struct HeldDevice(std::sync::Arc<(Mutex<bool>, std::sync::Condvar)>);

    impl Drop for HeldDevice {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }

    // Hold every device read of `dir` until the returned hold is dropped;
    // each read is announced on the receiver as it begins.
    pub(crate) fn hold(dir: &Path) -> (HeldDevice, tokio::sync::mpsc::UnboundedReceiver<PathBuf>) {
        let (entered, announced) = tokio::sync::mpsc::unbounded_channel();
        let gate = std::sync::Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        HELD.lock()
            .unwrap()
            .push((dir.to_path_buf(), entered, std::sync::Arc::clone(&gate)));
        (HeldDevice(gate), announced)
    }

    // Directories whose device cannot be read while the returned guard
    // lives, as a mount that fails its reads.
    static REFUSED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    pub(crate) struct RefusedDevice(PathBuf);

    impl Drop for RefusedDevice {
        fn drop(&mut self) {
            REFUSED.lock().unwrap().retain(|dir| *dir != self.0);
        }
    }

    pub(crate) fn refuse(dir: &Path) -> RefusedDevice {
        REFUSED.lock().unwrap().push(dir.to_path_buf());
        RefusedDevice(dir.to_path_buf())
    }

    pub(crate) fn refused(path: &Path) -> bool {
        REFUSED
            .lock()
            .unwrap()
            .iter()
            .any(|dir| path.starts_with(dir))
    }

    pub(crate) fn of(path: &Path) -> Option<u64> {
        let held = HELD
            .lock()
            .unwrap()
            .iter()
            .find(|(dir, _, _)| path == dir)
            .map(|(_, entered, gate)| (entered.clone(), std::sync::Arc::clone(gate)));
        if let Some((entered, gate)) = held {
            let _ = entered.send(path.to_path_buf());
            let (released, wake) = &*gate;
            drop(wake.wait_while(released.lock().unwrap(), |released| !*released));
        }
        DEVICES
            .lock()
            .unwrap()
            .iter()
            .filter(|(dir, _)| path.starts_with(dir))
            .max_by_key(|(dir, _)| dir.components().count())
            .map(|(_, device)| *device)
    }
}

// Whether two directories lie on one device, read on the spot: for tests
// only. Everything else asks the disk owner,
// [`crate::operations::StorageCapacity`].
#[cfg(test)]
pub(crate) use crate::operations::disk::same_device_now as same_device;

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

// Give a delivered tree to the user Weaver runs as: every entry under
// `path`, and `path` itself, owned by the effective uid and gid, and each
// file granting at least 0o666 and directory 0o777 less the process umask.
// An entry that already has that owner and grants that much is not written
// to. Links are owned over but never followed or given a mode. Unix only;
// elsewhere nothing.
pub(crate) fn normalize_extracted_tree(path: &Path) -> io::Result<()> {
    normalize_extracted_path(path, true)
}

fn normalize_extracted_path(path: &Path, recursive: bool) -> io::Result<()> {
    if let Err(error) = std::fs::symlink_metadata(path) {
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(error);
    }

    #[cfg(unix)]
    {
        normalize_extracted_tree_unix(path, recursive)
    }

    #[cfg(not(unix))]
    {
        let _ = (path, recursive);
        Ok(())
    }
}

#[cfg(unix)]
fn normalize_extracted_tree_unix(path: &Path, recursive: bool) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;

    fn normalize_one(
        path: &Path,
        uid: libc::uid_t,
        gid: libc::gid_t,
        umask: u32,
        recursive: bool,
    ) -> io::Result<()> {
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let path_c = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path contains NUL byte: {}", path.display()),
            )
        })?;

        if metadata.uid() != uid || metadata.gid() != gid {
            let chown_result = unsafe { libc::lchown(path_c.as_ptr(), uid, gid) };
            if chown_result != 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::NotFound {
                    return Ok(());
                }
                apply_metadata_change(path, "ownership", Err(error))?;
            }
        }

        if metadata.file_type().is_symlink() {
            return Ok(());
        }

        // Only an entry that withholds access is given a mode, and it keeps
        // the bits it has. Setting a mode on a share that manages access
        // itself (a Synology ACL share) drops the rules the entry inherited
        // from its folder, so an entry that already grants enough is left.
        let wanted = (if metadata.is_dir() { 0o777 } else { 0o666 }) & !umask;
        let current = metadata.permissions().mode();
        if current & wanted != wanted {
            let mut permissions = metadata.permissions();
            permissions.set_mode((current & 0o7777) | wanted);
            if let Err(error) = std::fs::set_permissions(path, permissions) {
                if error.kind() == io::ErrorKind::NotFound {
                    return Ok(());
                }
                apply_metadata_change(path, "mode", Err(error))?;
            }
        }

        if recursive && metadata.is_dir() {
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                normalize_one(&entry.path(), uid, gid, umask, true)?;
            }
        }

        Ok(())
    }

    normalize_one(
        path,
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() },
        current_umask(),
        recursive,
    )
}

// Remote and non-POSIX mounts may fix ownership and modes by mount policy.
// Those optional attributes must not turn successfully written data into a
// failed delivery. Other errors still fail, including missing files and I/O.
pub(super) fn apply_metadata_change(
    path: &Path,
    operation: &str,
    result: io::Result<()>,
) -> io::Result<()> {
    match result {
        Err(error) if metadata_is_mount_managed(&error) => {
            tracing::warn!(path = %path.display(), operation, %error, "filesystem retains its own file metadata");
            Ok(())
        }
        result => result,
    }
}

fn metadata_is_mount_managed(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
    ) {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error().is_some_and(|code| {
        [libc::ENOSYS, libc::ENOTSUP, libc::EOPNOTSUPP, libc::EROFS].contains(&code)
    }) {
        return true;
    }
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(1 | 50)) {
        // ERROR_INVALID_FUNCTION / ERROR_NOT_SUPPORTED.
        return true;
    }
    false
}

#[cfg(unix)]
fn current_umask() -> u32 {
    static UMASK: OnceLock<u32> = OnceLock::new();
    *UMASK.get_or_init(|| {
        let mask = unsafe { libc::umask(0o022) };
        unsafe {
            libc::umask(mask);
        }
        mask as u32
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn two_directories_on_one_device_are_one_device_even_before_they_exist() {
        let root = tempfile::tempdir().expect("temp dir");
        let made = root.path().join("made");
        std::fs::create_dir(&made).expect("dir");
        assert!(super::same_device(&made, &root.path().join("not/yet/made")));
    }

    #[cfg(unix)]
    #[test]
    fn directories_on_two_devices_are_told_apart() {
        let root = tempfile::tempdir().expect("temp dir");
        let ours = std::fs::metadata(root.path()).expect("meta");
        // `/dev` is its own filesystem wherever the tests run; when it is not,
        // there is nothing to tell apart and the case says nothing.
        use std::os::unix::fs::MetadataExt;
        let dev = std::fs::metadata("/dev").expect("meta");
        if dev.dev() != ours.dev() {
            assert!(!super::same_device(
                root.path(),
                std::path::Path::new("/dev")
            ));
        }
    }

    use super::*;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_mount_without_exclusive_rename_or_links_copies_without_overwriting() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        let dst = dir.path().join("destination.bin");
        std::fs::write(&src, b"source").unwrap();
        let no_links = |_: &Path, _: &Path| Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        rename_exclusive_with_link(
            &src,
            &dst,
            refuse::<{ libc::EINVAL }>,
            Unlinkable::Copied,
            no_links,
        )
        .unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
        assert!(!src.exists());

        std::fs::write(&src, b"second").unwrap();
        let error = rename_exclusive_with_link(
            &src,
            &dst,
            refuse::<{ libc::EINVAL }>,
            Unlinkable::Copied,
            no_links,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"second");
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    // A link refused the way `refuses` says, on a mount that refuses the
    // exclusive rename too, by the move that never copies.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn in_place_without_links(src: &Path, dst: &Path, refuses: i32) -> io::Result<()> {
        rename_exclusive_with_link(
            src,
            dst,
            refuse::<{ libc::EINVAL }>,
            Unlinkable::Renamed,
            |_, _| Err(io::Error::from_raw_os_error(refuses)),
        )
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_mount_without_exclusive_rename_or_links_renames_and_never_copies() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        let dst = dir.path().join("destination.bin");
        std::fs::write(&src, b"source").unwrap();
        let inode = std::fs::metadata(&src).unwrap().ino();
        in_place_without_links(&src, &dst, libc::EOPNOTSUPP).unwrap();
        // The very file under its new name: a copy would be another.
        assert_eq!(std::fs::metadata(&dst).unwrap().ino(), inode);
        assert!(!src.exists());

        // A name that is taken when it is looked at is still refused.
        std::fs::write(&src, b"second").unwrap();
        let error = in_place_without_links(&src, &dst, libc::EOPNOTSUPP).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"second");
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    // A symbolic link that no exclusive rename and no link took is made
    // again at the target, by a call that never replaces: it is not
    // renamed after a look.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_symlink_moved_in_place_without_links_is_made_again_not_renamed() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.link");
        let dst = dir.path().join("destination.link");
        std::os::unix::fs::symlink("pointed/at", &src).unwrap();
        let inode = std::fs::symlink_metadata(&src).unwrap().ino();
        in_place_without_links(&src, &dst, libc::EPERM).unwrap();
        let made = std::fs::symlink_metadata(&dst).unwrap();
        assert!(made.file_type().is_symlink());
        assert_eq!(std::fs::read_link(&dst).unwrap(), Path::new("pointed/at"));
        // Made while the source was still there: a rename would have kept
        // the source's own entry.
        assert_ne!(made.ino(), inode);
        assert!(std::fs::symlink_metadata(&src).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_link_across_devices_is_told_and_nothing_is_copied_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        let dst = dir.path().join("destination.bin");
        std::fs::write(&src, b"source").unwrap();
        let error = in_place_without_links(&src, &dst, libc::EXDEV).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::CrossesDevices);
        assert_eq!(std::fs::read(&src).unwrap(), b"source");
        assert!(!dst.exists());
    }

    // A move in place whose exclusive rename answers `exclusive` and whose
    // link answers `link`: the file is the very one, under its new name, so
    // the plain rename moved it and nothing copied it. The link was asked
    // once.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn renamed_in_place_after(exclusive: ExclusiveRename, link: i32) {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        let dst = dir.path().join("destination.bin");
        std::fs::write(&src, b"source").unwrap();
        let inode = std::fs::metadata(&src).unwrap().ino();
        let asked = std::cell::Cell::new(0);
        rename_exclusive_with_link(&src, &dst, exclusive, Unlinkable::Renamed, |_, _| {
            asked.set(asked.get() + 1);
            Err(io::Error::from_raw_os_error(link))
        })
        .unwrap();
        assert_eq!(asked.get(), 1);
        assert_eq!(std::fs::metadata(&dst).unwrap().ino(), inode);
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
        assert!(!src.exists());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_link_answered_eacces_in_place_still_moves_the_file_by_rename() {
        renamed_in_place_after(refuse::<{ libc::EINVAL }>, libc::EACCES);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn an_exclusive_rename_answered_eperm_in_place_still_moves_the_file_by_rename() {
        renamed_in_place_after(refuse::<{ libc::EPERM }>, libc::EOPNOTSUPP);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_link_answered_emlink_in_place_still_moves_the_file_by_rename() {
        renamed_in_place_after(refuse::<{ libc::EINVAL }>, libc::EMLINK);
    }

    // In place, the exclusive rename's "another device" and "the target
    // exists" still end the move there, the target untouched.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn in_place_only_another_device_or_a_taken_target_end_the_exclusive_rename() {
        for (errno, kind) in [
            (libc::EXDEV, io::ErrorKind::CrossesDevices),
            (libc::EEXIST, io::ErrorKind::AlreadyExists),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let src = dir.path().join("source.bin");
            let dst = dir.path().join("destination.bin");
            std::fs::write(&src, b"source").unwrap();
            let exclusive: ExclusiveRename = match errno {
                libc::EXDEV => refuse::<{ libc::EXDEV }>,
                _ => refuse::<{ libc::EEXIST }>,
            };
            let error =
                rename_exclusive_with_link(&src, &dst, exclusive, Unlinkable::Renamed, |_, _| {
                    panic!("no link is asked after {errno}")
                })
                .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(std::fs::read(&src).unwrap(), b"source");
            assert!(!dst.exists());
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_mount_without_links_recognizes_an_alias_before_copying() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        let src = dir.path().join("source.bin");
        let dst = parent.join("../source.bin");
        std::fs::write(&src, b"source").unwrap();
        let no_links = |_: &Path, _: &Path| Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        rename_exclusive_with_link(
            &src,
            &dst,
            refuse::<{ libc::EINVAL }>,
            Unlinkable::Copied,
            no_links,
        )
        .unwrap();
        assert_eq!(std::fs::read(&src).unwrap(), b"source");
    }

    #[test]
    fn a_copy_that_cannot_open_its_source_never_cleans_an_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("missing.bin");
        let dst = dir.path().join("destination.bin");
        std::fs::write(&dst, b"another owner").unwrap();
        assert!(copy_file_no_overwrite_then_remove_source(&src, &dst).is_err());
        assert_eq!(std::fs::read(&dst).unwrap(), b"another owner");
    }

    // Every entry of `dir`, by name, in order.
    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort_unstable();
        names
    }

    // A rename that will not cross from one folder to another, as one
    // across devices will not.
    fn across(src: &Path, dst: &Path) -> io::Result<()> {
        if src.parent() == dst.parent() {
            rename_path_exclusive(src, dst)
        } else {
            Err(io::Error::from(io::ErrorKind::CrossesDevices))
        }
    }

    #[test]
    fn a_file_moved_across_devices_arrives_whole_and_leaves_no_copy() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (dir.path().join("from"), dir.path().join("to"));
        std::fs::create_dir(&from).unwrap();
        std::fs::create_dir(&to).unwrap();
        std::fs::write(from.join("a.bin"), b"moved").unwrap();
        JobMoves::of(7)
            .move_file_using(&from.join("a.bin"), &to.join("a.bin"), across)
            .unwrap();
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"moved");
        assert_eq!(entries(&to), ["a.bin"]);
        assert!(entries(&from).is_empty());
    }

    // The name is taken while the copy is made: the file there stays as it
    // was, the source stays, and the hidden copy is gone.
    #[test]
    fn a_move_across_devices_never_replaces_and_leaves_no_copy_when_it_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (dir.path().join("from"), dir.path().join("to"));
        std::fs::create_dir(&from).unwrap();
        std::fs::create_dir(&to).unwrap();
        let (src, dst) = (from.join("a.bin"), to.join("a.bin"));
        std::fs::write(&src, b"moved").unwrap();
        let meets = |src: &Path, dst: &Path| {
            if src.parent() == dst.parent() {
                std::fs::write(dst, b"another's").unwrap();
            }
            across(src, dst)
        };
        let error = JobMoves::of(7)
            .move_file_using(&src, &dst, meets)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&dst).unwrap(), b"another's");
        assert_eq!(entries(&to), ["a.bin"]);
        assert_eq!(std::fs::read(&src).unwrap(), b"moved");

        // A name taken before the move makes no copy at all.
        let error = JobMoves::of(7)
            .move_file_using(&src, &dst, across)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(entries(&to), ["a.bin"]);

        // Any other failure of the last move removes the copy too.
        std::fs::remove_file(&dst).unwrap();
        let refused = |src: &Path, dst: &Path| {
            if src.parent() == dst.parent() {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            } else {
                across(src, dst)
            }
        };
        let error = JobMoves::of(7)
            .move_file_using(&src, &dst, refused)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(entries(&to).is_empty());
        assert_eq!(std::fs::read(&src).unwrap(), b"moved");
    }

    // A move across devices tries a bounded number of hidden names and then
    // answers. Where a file holds the name of the target's folder no copy
    // can ever be made: the move ends with that error, and the source stays.
    #[test]
    fn a_move_across_devices_gives_up_when_no_copy_can_be_made() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        std::fs::create_dir(&from).unwrap();
        let src = from.join("a.bin");
        std::fs::write(&src, b"moved").unwrap();
        std::fs::write(dir.path().join("to"), b"a file, not a folder").unwrap();
        let error = JobMoves::of(7)
            .move_file_using(&src, &dir.path().join("to/a.bin"), across)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"moved");
        assert_eq!(
            std::fs::read(dir.path().join("to")).unwrap(),
            b"a file, not a folder"
        );

        // Every hidden name held by something that is not the job's to
        // sweep: the move ends, and each of them stays.
        let to = dir.path().join("full");
        std::fs::create_dir(&to).unwrap();
        for n in 0..MOVE_COPY_TRIES {
            std::fs::create_dir(to.join(move_copy_name(7, n))).unwrap();
        }
        let error = JobMoves::of(7)
            .move_file_using(&src, &to.join("a.bin"), across)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(entries(&to).len(), MOVE_COPY_TRIES as usize);
        assert_eq!(std::fs::read(&src).unwrap(), b"moved");
    }

    // A copy a dead run left is the job's own, by its exact name: the job's
    // next move into the folder removes it, and nothing else. Another job's
    // copy, a name that only starts like one, and a folder under the job's
    // name all stay.
    #[test]
    fn a_jobs_left_copies_are_swept_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (dir.path().join("from"), dir.path().join("to"));
        std::fs::create_dir(&from).unwrap();
        std::fs::create_dir(&to).unwrap();
        std::fs::write(from.join("a.bin"), b"moved").unwrap();
        std::fs::write(to.join(move_copy_name(7, 0)), b"left").unwrap();
        std::fs::write(to.join(move_copy_name(7, 3)), b"left").unwrap();
        std::fs::write(to.join(move_copy_name(70, 0)), b"another job's").unwrap();
        std::fs::write(to.join(".wvr-7-0.txt"), b"not a copy").unwrap();
        std::fs::write(to.join(".wvr-7-10"), b"past the names").unwrap();
        std::fs::create_dir(to.join(move_copy_name(7, 1))).unwrap();
        JobMoves::of(7)
            .move_file_using(&from.join("a.bin"), &to.join("a.bin"), across)
            .unwrap();
        assert_eq!(
            entries(&to),
            [
                ".wvr-7-0.txt",
                ".wvr-7-1",
                ".wvr-7-10",
                ".wvr-70-0",
                "a.bin"
            ]
        );
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"moved");
    }

    #[test]
    fn metadata_fallback_accepts_mount_policy_but_not_data_errors() {
        let path = Path::new("payload.bin");
        for kind in [io::ErrorKind::Unsupported, io::ErrorKind::PermissionDenied] {
            apply_metadata_change(path, "mode", Err(io::Error::from(kind))).unwrap();
        }
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::StorageFull,
            io::ErrorKind::InvalidData,
        ] {
            let error =
                apply_metadata_change(path, "mode", Err(io::Error::from(kind))).unwrap_err();
            assert_eq!(error.kind(), kind);
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_mount_metadata_refusals_do_not_turn_delivery_into_failure() {
        for errno in [
            libc::EPERM,
            libc::EACCES,
            libc::EOPNOTSUPP,
            libc::ENOSYS,
            libc::EROFS,
        ] {
            apply_metadata_change(
                Path::new("payload.bin"),
                "ownership",
                Err(io::Error::from_raw_os_error(errno)),
            )
            .unwrap();
        }
        let error = apply_metadata_change(
            Path::new("payload.bin"),
            "ownership",
            Err(io::Error::from_raw_os_error(libc::EIO)),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

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

    // The exclusive call as a mount that refuses it answers.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn refuse<const ERRNO: i32>(_: &std::ffi::CStr, _: &std::ffi::CStr) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(ERRNO))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_refused_exclusive_rename_still_moves_a_file() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("graph-waiting-0-3");
        let dst = temp.path().join("movie.mkv");
        std::fs::write(&src, b"source").unwrap();

        rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_refused_exclusive_rename_still_moves_a_directory() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("old");
        let dst = temp.path().join("new");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("inside.bin"), b"inside").unwrap();

        rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(dst.join("inside.bin")).unwrap(), b"inside");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_refused_exclusive_rename_never_replaces_an_existing_file() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src.bin");
        let dst = temp.path().join("dst.bin");
        std::fs::write(&src, b"source").unwrap();
        std::fs::write(&dst, b"dest").unwrap();

        let error = rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"source");
        assert_eq!(std::fs::read(&dst).unwrap(), b"dest");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_refused_exclusive_rename_never_replaces_an_existing_directory() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("old");
        let dst = temp.path().join("new");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("inside.bin"), b"inside").unwrap();
        // Empty, so a plain rename(2) would replace it.
        std::fs::create_dir(&dst).unwrap();

        let error = rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(src.join("inside.bin")).unwrap(), b"inside");
        assert_eq!(std::fs::read_dir(&dst).unwrap().count(), 0);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_file_already_linked_at_both_names_finishes_its_move() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("graph-waiting-0-3");
        let dst = temp.path().join("movie.mkv");
        std::fs::write(&src, b"source").unwrap();
        std::fs::hard_link(&src, &dst).unwrap();

        rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_fallback_move_through_an_alias_never_unlinks_its_only_entry() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        let src = temp.path().join("source.bin");
        let dst = parent.join("../source.bin");
        std::fs::write(&src, b"source").unwrap();

        rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap();

        assert_eq!(std::fs::read(&src).unwrap(), b"source");
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_different_file_with_the_same_bytes_is_still_refused() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src.bin");
        let dst = temp.path().join("dst.bin");
        std::fs::write(&src, b"same").unwrap();
        std::fs::write(&dst, b"same").unwrap();

        let error = rename_exclusive_with(&src, &dst, refuse::<{ libc::EINVAL }>).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&src).unwrap(), b"same");
        assert_eq!(std::fs::read(&dst).unwrap(), b"same");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn an_unimplemented_exclusive_rename_falls_back_as_a_refused_one_does() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src.bin");
        let dst = temp.path().join("dst.bin");
        std::fs::write(&src, b"source").unwrap();

        rename_exclusive_with(&src, &dst, refuse::<{ libc::ENOSYS }>).unwrap();

        assert!(!src.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"source");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn any_other_exclusive_rename_error_is_returned_unchanged() {
        let failures: [ExclusiveRename; 3] = [
            refuse::<{ libc::EACCES }>,
            refuse::<{ libc::ENOENT }>,
            refuse::<{ libc::EXDEV }>,
        ];
        for (failure, errno) in failures
            .into_iter()
            .zip([libc::EACCES, libc::ENOENT, libc::EXDEV])
        {
            let temp = tempfile::tempdir().unwrap();
            let src = temp.path().join("src.bin");
            let dst = temp.path().join("dst.bin");
            std::fs::write(&src, b"source").unwrap();

            let error = rename_exclusive_with(&src, &dst, failure).unwrap_err();

            assert_eq!(error.raw_os_error(), Some(errno));
            assert_eq!(std::fs::read(&src).unwrap(), b"source");
            assert!(!dst.exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn normalize_extracted_tree_opens_entries_the_umask_would_have_left_open() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("out");
        let dir = root.join("nested");
        let file = dir.join("movie.mkv");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"media").unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();

        normalize_extracted_tree(&root).unwrap();

        let mask = current_umask();
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700 | (0o777 & !mask)
        );
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700 | (0o777 & !mask)
        );
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600 | (0o666 & !mask)
        );
    }

    // A share that manages access itself (a Synology ACL share shows every
    // entry as 0o777) drops its inherited rules from an entry the moment the
    // entry is given a mode. An entry that already grants everything the
    // umask allows is therefore left exactly as it is.
    #[cfg(unix)]
    #[test]
    fn normalize_extracted_tree_leaves_an_entry_that_already_grants_enough() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("out");
        let dir = root.join("nested");
        let file = dir.join("movie.mkv");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"media").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o777)).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();

        normalize_extracted_tree(&root).unwrap();

        for path in [&root, &dir, &file] {
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o777,
                "{} was given a mode it did not need",
                path.display()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn normalize_extracted_tree_treats_missing_paths_as_already_gone() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("vanished");

        normalize_extracted_tree(&root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn normalize_extracted_tree_does_not_follow_symlink_targets() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("out");
        let target = temp.path().join("target.bin");
        let link = root.join("link.bin");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(&target, b"target").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        normalize_extracted_tree(&root).unwrap();

        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn normalize_extracted_tree_gives_every_entry_to_the_effective_user() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("out");
        let dir = root.join("nested");
        let file = dir.join("movie.mkv");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"media").unwrap();
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        // Moved to another of the user's groups where it has one, so the
        // tree starts owned otherwise than it ends.
        let mut groups = vec![0 as libc::gid_t; 64];
        let count = unsafe { libc::getgroups(groups.len() as libc::c_int, groups.as_mut_ptr()) };
        groups.truncate(usize::try_from(count).unwrap_or(0));
        if let Some(other) = groups.into_iter().find(|group| *group != gid) {
            std::os::unix::fs::lchown(&file, None, Some(other)).unwrap();
            assert_eq!(std::fs::metadata(&file).unwrap().gid(), other);
        }

        normalize_extracted_tree(&root).unwrap();

        for path in [&root, &dir, &file] {
            let metadata = std::fs::symlink_metadata(path).unwrap();
            assert_eq!((metadata.uid(), metadata.gid()), (uid, gid), "{path:?}");
        }
    }
}
