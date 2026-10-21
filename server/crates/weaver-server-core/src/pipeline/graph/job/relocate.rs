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

impl<R: Root> GraphJob<R> {
    // Move the job's directory to `to`, the directory claimed for it, which
    // exists and is empty. Done now when nothing of the job is out on a
    // thread, else at the end of the first step where that holds; the
    // outcome, now or then, comes from [`Self::relocated`].
    pub(crate) fn relocate(&mut self, to: PathBuf) {
        self.moving = Some(to);
        self.try_relocate();
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
        let to = self.moving.take().expect("checked above");
        let from = self.writer.working_dir().to_path_buf();
        self.writer.release();
        let outcome = move_directory(&from, &to).map(|()| {
            self.writer.relocate(&from, &to);
            to
        });
        self.relocated = Some(outcome);
    }
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
