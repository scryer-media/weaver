// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the mover of its own directory. A move is a disk job like any
// other, fenced behind the runs and the repair out when it is asked for
// (libtorrent `torrent::move_storage`, a fence job of the disk thread): it
// happens at the first step where nothing of the job is out on a thread,
// and in one piece, so no step ever sees a path under the old directory
// beside one under the new. Every open file is let go first, the whole
// directory is renamed, or copied and then removed where the destination is
// on another device, and every path the job holds is re-rooted
// (libtorrent `storage_utils::move_storage`).

use std::io;
use std::path::{Path, PathBuf};

use super::super::leaf::LeafError;
use super::super::root::Root;
use super::GraphJob;

// What of the job is to move, and where.
pub(crate) enum Relocation {
    // Its directory, to the directory claimed for it.
    Working(PathBuf),
    // The kept files placed outside its directory, each to its own name
    // under `to`, a file already there met as `existing` says.
    Kept { to: PathBuf, existing: Existing },
}

// What a move of placed files does with a file already at its destination
// (libtorrent `move_flags_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Existing {
    // The file there stays, and so does the source of the one that would
    // have taken its place; the torrent's pieces are checked again from the
    // new place, which now holds a file it did not write
    // (`move_flags_t::dont_replace`, `disk_status::need_full_check`).
    Keep,
    // The moved file takes its place (`move_flags_t::always_replace_files`).
    Replace,
}

impl<R: Root> GraphJob<R> {
    // Move the job's directory to `to`, the directory claimed for it, which
    // exists and is empty. Done now when nothing of the job is out on a
    // thread, else at the end of the first step where that holds; the
    // outcome, now or then, comes from [`Self::relocated`].
    pub(crate) fn relocate(&mut self, to: PathBuf) {
        self.moving = Some(Relocation::Working(to));
        self.try_relocate();
    }

    // Move the kept files placed outside the job's directory to the same
    // names under `to`, a directory that exists, fenced as
    // [`Self::relocate`] is (libtorrent `torrent::move_storage` moves a
    // torrent's files wherever they sit). The job's own directory stays. A
    // file already at a destination is met as `existing` says.
    pub(crate) fn relocate_kept(&mut self, to: PathBuf, existing: Existing) {
        self.moving = Some(Relocation::Kept { to, existing });
        self.try_relocate();
    }

    // Whether the last move of kept files left a file already at its
    // destination in place, so the files no longer vouch for the pieces had
    // and want a check. Told once.
    pub(crate) fn kept_existing(&mut self) -> bool {
        std::mem::take(&mut self.kept_existing)
    }

    // Whether a move is waiting or under way.
    #[cfg(test)]
    pub(crate) fn relocating(&self) -> bool {
        self.moving.is_some()
    }

    // The move asked for, once it happened or failed: the directory the job
    // now writes under, or why it still writes under the old one. Each
    // outcome is given once.
    pub(crate) fn relocated(&mut self) -> Option<Result<PathBuf, LeafError>> {
        self.relocated.take()
    }

    // Move now if a move is waiting and nothing of the job is out.
    pub(crate) fn try_relocate(&mut self) {
        if self.moving.is_none() || self.awaits_mail() {
            return;
        }
        let outcome = match self.moving.take().expect("checked above") {
            Relocation::Working(to) => {
                let from = self.writer.working_dir().to_path_buf();
                self.writer.release();
                move_directory(&from, &to).map(|()| {
                    self.writer.relocate(&from, &to);
                    to
                })
            }
            Relocation::Kept { to, existing } => {
                match self.writer.kept_dir().map(Path::to_path_buf) {
                    Some(from) => {
                        self.writer.release_kept();
                        let moves: Vec<(PathBuf, PathBuf)> = self
                            .writer
                            .kept_paths(&from)
                            .into_iter()
                            .zip(self.writer.kept_paths(&to))
                            .collect();
                        move_files(&from, &moves, existing).map(|kept| {
                            self.kept_existing = kept;
                            self.writer.rekeep(&to);
                            to
                        })
                    }
                    None => Err(LeafError::Io {
                        path: to,
                        source: io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "the job keeps no files outside its directory",
                        ),
                    }),
                }
            }
        };
        self.relocated = Some(outcome);
    }
}

// Move each file from its path to the other, every one or none
// (libtorrent `storage_utils::move_storage`): a file already at a
// destination is met as `existing` says, kept with its would-be replacement
// left at its source, or replaced; a file not there yet has nothing to
// move, a file on another device is copied and its source removed only once
// every file is in, and a move that fails partway puts back what it moved.
// A file it replaced stays replaced, and a file it kept stays kept, as
// libtorrent's do. The directories under `from` the move leaves empty go.
// True when a file already at a destination was kept.
fn move_files(
    from: &Path,
    moves: &[(PathBuf, PathBuf)],
    existing: Existing,
) -> Result<bool, LeafError> {
    move_files_using(from, moves, existing, |from, to| std::fs::rename(from, to))
}

fn move_files_using(
    from: &Path,
    moves: &[(PathBuf, PathBuf)],
    existing: Existing,
    rename: impl Fn(&Path, &Path) -> io::Result<()>,
) -> Result<bool, LeafError> {
    let mut kept = false;
    let mut renamed = Vec::new();
    let mut copied = Vec::new();
    let mut failed = None;
    for (source, target) in moves {
        // A dangling link there still counts as a file there, as in
        // libtorrent.
        if existing == Existing::Keep && std::fs::symlink_metadata(target).is_ok() {
            kept = true;
            continue;
        }
        if std::fs::symlink_metadata(source).is_err() {
            continue;
        }
        if let Some(parent) = target.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            failed = Some(LeafError::io(parent)(error));
            break;
        }
        match rename(source, target) {
            Ok(()) => renamed.push((source, target)),
            Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {
                match std::fs::copy(source, target) {
                    Ok(_) => copied.push((source, target)),
                    Err(error) => {
                        let _ = std::fs::remove_file(target);
                        failed = Some(LeafError::io(source)(error));
                        break;
                    }
                }
            }
            Err(error) => {
                failed = Some(LeafError::io(source)(error));
                break;
            }
        }
    }
    if let Some(error) = failed {
        for (source, target) in renamed {
            let _ = std::fs::rename(target, source);
        }
        for (_, target) in copied {
            let _ = std::fs::remove_file(target);
        }
        return Err(error);
    }
    for (source, _) in copied {
        let _ = std::fs::remove_file(source);
    }
    for (source, _) in moves {
        let mut dir = source.parent();
        while let Some(at) = dir.filter(|at| *at != from && at.starts_with(from)) {
            if std::fs::remove_dir(at).is_err() {
                break;
            }
            dir = at.parent();
        }
    }
    Ok(kept)
}

// Move the directory at `from` to `to`, an empty directory that exists:
// renamed where one device holds both, else copied whole and then removed,
// with nothing left at `to` when the copy fails partway.
fn move_directory(from: &Path, to: &Path) -> Result<(), LeafError> {
    move_directory_using(from, to, |from, to| std::fs::rename(from, to))
}

fn move_directory_using(
    from: &Path,
    to: &Path,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<(), LeafError> {
    // The claimed directory makes way for the rename, which on every
    // platform needs the destination absent.
    std::fs::remove_dir(to).map_err(LeafError::io(to))?;
    match rename(from, to) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {}
        Err(error) => {
            // The claim stands as it was.
            let _ = std::fs::create_dir(to);
            return Err(LeafError::Io {
                path: from.to_path_buf(),
                source: error,
            });
        }
    }
    if let Err(error) = copy_tree(from, to) {
        let _ = std::fs::remove_dir_all(to);
        let _ = std::fs::create_dir(to);
        return Err(error);
    }
    std::fs::remove_dir_all(from).map_err(LeafError::io(from))
}

// Copy the directory at `from`, as it is, to `to`, which need not exist.
fn copy_tree(from: &Path, to: &Path) -> Result<(), LeafError> {
    std::fs::create_dir_all(to).map_err(LeafError::io(to))?;
    for entry in std::fs::read_dir(from).map_err(LeafError::io(from))? {
        let entry = entry.map_err(LeafError::io(from))?;
        let source = entry.path();
        let target = to.join(entry.file_name());
        let kind = entry.file_type().map_err(LeafError::io(&source))?;
        if kind.is_dir() {
            copy_tree(&source, &target)?;
        } else {
            std::fs::copy(&source, &target).map_err(LeafError::io(&source))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(root: &Path) {
        std::fs::create_dir_all(root.join("sub/.weaver-chunks")).unwrap();
        std::fs::write(root.join("a.bin"), b"aaa").unwrap();
        std::fs::write(root.join("sub/b.bin"), b"bbbb").unwrap();
        std::fs::write(root.join("sub/.weaver-chunks/scratch"), b"s").unwrap();
    }

    fn contents(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut found = Vec::new();
        for entry in walkdir(root) {
            let rest = entry.strip_prefix(root).unwrap().to_path_buf();
            found.push((rest, std::fs::read(&entry).unwrap()));
        }
        found.sort();
        found
    }

    fn walkdir(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(walkdir(&path));
            } else {
                files.push(path);
            }
        }
        files
    }

    // libtorrent `storage_utils::move_storage`: on one device the directory
    // is renamed, and the claimed destination is what it becomes.
    #[test]
    fn a_directory_moves_whole_by_rename() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir(&to).unwrap();
        move_directory(&from, &to).unwrap();
        assert!(!from.exists());
        assert_eq!(contents(&to), expected);
    }

    // libtorrent `storage_utils::move_storage`: across devices the files
    // are copied and the source removed only once every copy is in.
    #[test]
    fn a_directory_on_another_device_is_copied_then_removed() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir(&to).unwrap();
        move_directory_using(&from, &to, |_, _| {
            Err(io::Error::from(io::ErrorKind::CrossesDevices))
        })
        .unwrap();
        assert!(!from.exists());
        assert_eq!(contents(&to), expected);
    }

    // libtorrent `storage_utils::move_storage`: a file that will not move
    // puts back every file moved before it, and leaves a copy made across
    // devices behind no trace, its source untouched.
    #[test]
    fn files_that_fail_to_move_partway_are_put_back() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        let names = ["a.bin", "sub/b.bin", "c.bin"];
        std::fs::write(from.join("c.bin"), b"cc").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = names
            .iter()
            .map(|name| (from.join(name), to.join(name)))
            .collect();
        let error = move_files_using(&from, &moves, Existing::Keep, |source, target| {
            if source.ends_with("c.bin") {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            } else if source.ends_with("b.bin") {
                Err(io::Error::from(io::ErrorKind::CrossesDevices))
            } else {
                std::fs::rename(source, target)
            }
        })
        .unwrap_err();
        assert!(matches!(error, LeafError::Io { ref path, .. } if path.ends_with("c.bin")));
        let mut expected = expected;
        expected.push((PathBuf::from("c.bin"), b"cc".to_vec()));
        expected.sort();
        assert_eq!(contents(&from), expected);
        assert!(contents(&to).is_empty());
    }

    // `dont_replace`, the mode qBittorrent's move of a user's asks for
    // (`MoveStorageMode::KeepExistingFiles`): a file already at a destination
    // stays, the one that would have taken its place stays at its source,
    // and every other file moves; the move tells that it kept one.
    #[test]
    fn files_moved_keeping_those_there_leave_their_own_behind() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir_all(to.join("sub")).unwrap();
        std::fs::write(to.join("sub/b.bin"), b"old").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = expected
            .iter()
            .map(|(name, _)| (from.join(name), to.join(name)))
            .collect();
        assert!(move_files(&from, &moves, Existing::Keep).unwrap());
        let moved = |name: &str, bytes: &[u8]| (PathBuf::from(name), bytes.to_vec());
        assert_eq!(
            contents(&to),
            [
                moved("a.bin", b"aaa"),
                moved("sub/.weaver-chunks/scratch", b"s"),
                moved("sub/b.bin", b"old"),
            ]
        );
        assert_eq!(contents(&from), [moved("sub/b.bin", b"bbbb")]);

        // Nothing there, nothing kept.
        let again = root.path().join("again");
        let moves: Vec<(PathBuf, PathBuf)> = vec![(to.join("a.bin"), again.join("a.bin"))];
        assert!(!move_files(&to, &moves, Existing::Keep).unwrap());
        assert_eq!(std::fs::read(again.join("a.bin")).unwrap(), b"aaa");
    }

    // `always_replace_files`, the mode qBittorrent's move of a finished
    // torrent asks for (`MoveStorageMode::Overwrite`): a file already at a
    // destination gives way to the moved one.
    #[test]
    fn files_moved_with_replace_take_the_place_of_those_there() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir_all(to.join("sub")).unwrap();
        std::fs::write(to.join("sub/b.bin"), b"old").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = expected
            .iter()
            .map(|(name, _)| (from.join(name), to.join(name)))
            .collect();
        assert!(!move_files(&from, &moves, Existing::Replace).unwrap());
        assert_eq!(contents(&to), expected);
        assert!(contents(&from).is_empty());
    }

    // A rename that fails for another reason leaves the job as it was: the
    // source untouched and the claimed destination standing, empty.
    #[test]
    fn a_failed_move_leaves_source_and_claim_as_they_were() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir(&to).unwrap();
        let error = move_directory_using(&from, &to, |_, _| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();
        assert!(matches!(error, LeafError::Io { ref path, .. } if *path == from));
        assert_eq!(contents(&from), expected);
        assert!(to.is_dir());
        assert!(contents(&to).is_empty());
    }
}
