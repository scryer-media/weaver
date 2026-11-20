// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's files moving to another place (libtorrent
// `storage_utils::move_storage`): the whole directory claimed for it, or the
// files placed outside it each to its own name under a new directory. Every
// handle on the files is let go first, so the move happens whole and no
// write or read sees a path under the old place beside one under the new.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use weaver_server_disk::operations::disk::beneath;

pub fn key_of(path: &Path) -> String {
    weaver_graph::names::key(&path.to_string_lossy())
}

// What a move of placed files does with a file already at its destination
// (libtorrent `move_flags_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existing {
    // The file there stays, and so does the source of the one that would
    // have taken its place; the torrent's pieces are checked again from the
    // new place, which now holds a file it did not write
    // (`move_flags_t::dont_replace`, `disk_status::need_full_check`).
    Keep,
    // The moved file takes its place (`move_flags_t::always_replace_files`).
    Replace,
}

// A move that failed: the path it failed at, and why.
#[derive(Debug, thiserror::Error)]
#[error("{}: {source}", path.display())]
pub struct MoveError {
    pub path: PathBuf,
    #[source]
    pub source: io::Error,
}

// A move of files that failed, with what it had moved put back.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct Failed {
    #[source]
    pub error: MoveError,
    // The files, by their place in the list moved, that would not go back:
    // something took each one's old name since it left. Each stays where
    // the move put it, and the file at its old name is not the mover's.
    pub stranded: Vec<usize>,
}

impl From<MoveError> for Failed {
    fn from(error: MoveError) -> Self {
        Self {
            error,
            stranded: Vec::new(),
        }
    }
}

impl MoveError {
    fn at(path: &Path) -> impl FnOnce(io::Error) -> Self + '_ {
        move |source| Self {
            path: path.to_path_buf(),
            source,
        }
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
//
// Keeping is by a move that never replaces
// (`rename_path_exclusive_in_place`, and across devices a copy made new),
// not by a look before one that does: a file that takes the name after the
// look is kept as one found by it is. So is every move back (`put_back`).
// A file is copied only across devices; on one device a mount with no move
// that refuses is looked at and then renamed, which is that mount's limit.
//
// A destination that is itself a file of the batch still to move (a move
// into a folder of the torrent's own, or out of one) is not a file already
// there: the batch is ordered (`ordered`) so that file moves first, and
// each ends at its own name with none taking another's place. A batch that
// cannot be ordered so moves nothing.
//
// What it moved and what it kept, by their place in `moves`.
pub fn move_files(
    from: &Path,
    moves: &[(PathBuf, PathBuf)],
    existing: Existing,
) -> Result<Moved, Failed> {
    confined(from, moves)?;
    match existing {
        Existing::Keep => move_files_using(
            from,
            moves,
            existing,
            weaver_server_disk::runtime::fs::rename_path_exclusive_in_place,
        ),
        Existing::Replace => {
            move_files_using(from, moves, existing, |from, to| std::fs::rename(from, to))
        }
    }
}

// Every file of `moves` stays inside its folder through the links on its
// way, or nothing moves: a source that leads outside `from` (a link to a
// file outside, or a linked folder above it), or a destination whose folder
// leads outside the one it moves to. A move goes by path, so a file moved
// through a link would leave, or a copy across devices would take in, bytes
// not the torrent's. The destination's own name may be a link: a rename
// takes the place of the link, never what it points at, and one kept is
// read through the save folder's checks afterwards.
//
// Each move is `(from/name, to/name)`, so the folder it moves to is the
// destination less as many parts as the name has. Each folder is resolved
// once. A link swapped in after the look is not seen: a rename holds no
// folder open (`beneath::resolves_inside`).
fn confined(from: &Path, moves: &[(PathBuf, PathBuf)]) -> Result<(), MoveError> {
    let refused = |path: &Path, root: &Path| MoveError {
        path: path.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} leads outside {} through a link, so the torrent was not moved",
                path.display(),
                root.display()
            ),
        ),
    };
    let mut inside: HashMap<PathBuf, bool> = HashMap::new();
    let mut folder_inside = |root: &Path, folder: &Path| -> Result<bool, MoveError> {
        if let Some(known) = inside.get(folder) {
            return Ok(*known);
        }
        let found = folder == root
            || beneath::resolves_inside(root, folder).map_err(MoveError::at(folder))?;
        inside.insert(folder.to_path_buf(), found);
        Ok(found)
    };
    for (source, target) in moves {
        let Ok(name) = source.strip_prefix(from) else {
            return Err(refused(source, from));
        };
        if !beneath::resolves_inside(from, source).map_err(MoveError::at(source))? {
            return Err(refused(source, from));
        }
        let parts = name.components().count();
        let (Some(to), Some(folder)) = (target.ancestors().nth(parts), target.parent()) else {
            return Err(refused(target, target));
        };
        if !folder_inside(to, folder)? {
            return Err(refused(target, to));
        }
    }
    Ok(())
}

// What a move of files did, each file by its place in the list moved.
#[derive(Debug, Default)]
pub struct Moved {
    // The files whose destination held a file already, which was kept.
    pub kept: Vec<usize>,
    // The files that moved, in the order they did.
    pub moved: Vec<usize>,
}

impl Moved {
    // The files that moved go back where they were, the last first, over
    // nothing: a move ended whole that what came after it could not keep.
    // The folders under `to` that leaves empty go, but for `there`, the
    // ones that were there before the move. The files that would not go
    // back, by their place in `moves`: see `Failed::stranded`.
    pub fn put_back(
        &self,
        to: &Path,
        moves: &[(PathBuf, PathBuf)],
        there: &HashSet<PathBuf>,
    ) -> Vec<usize> {
        let stranded = put_back(moves, self.moved.iter().rev().copied());
        remove_emptied_but(to, moves.iter().map(|(_, target)| target.as_path()), there);
        stranded
    }
}

// Each of `moved`, files of `moves` now at their destinations, back at its
// source by a move that never replaces: a name something took since the
// file left it is theirs, and the file stays where it is. Those that stay.
fn put_back(moves: &[(PathBuf, PathBuf)], moved: impl Iterator<Item = usize>) -> Vec<usize> {
    let mut stranded = Vec::new();
    for at in moved {
        let (source, target) = &moves[at];
        if let Err(error) = move_exclusive(target, source) {
            tracing::warn!(from = %target.display(), to = %source.display(), error = %error, "a moved file could not be put back where it was; it stays where the move put it");
            stranded.push(at);
        }
    }
    stranded
}

// The file at `from` moved to `to`, never over what is there, its folder
// made when it is not there: renamed, or across devices copied into a file
// made new and then removed.
fn move_exclusive(from: &Path, to: &Path) -> io::Result<()> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match weaver_server_disk::runtime::fs::rename_path_exclusive_in_place(from, to) {
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {}
        moved => return moved,
    }
    weaver_server_disk::runtime::file_cache::copy_large_file(from, to)?;
    left_behind(from, std::fs::remove_file(from));
    Ok(())
}

// The order `moves` are made in: each file after the file of the batch that
// sits at its destination, so no file lands on one still to move. One
// lookup per file. An error when files would each take another's place in
// a ring, which no order moves without setting one aside. With it, which
// files another of the batch waits on to move out of its way.
//
// The file at a destination is the one of that very path, else one whose
// path differs from it only as a volume that folds names would not tell
// apart (`tee::key_of`, the key unpacked files are named by): on such a
// volume they are one name. On a volume that tells them apart the second
// only orders two files that never meet.
fn ordered(moves: &[(PathBuf, PathBuf)]) -> Result<(Vec<usize>, Vec<bool>), MoveError> {
    let by_source: HashMap<&Path, usize> = moves
        .iter()
        .enumerate()
        .map(|(at, (source, _))| (source.as_path(), at))
        .collect();
    let by_key: HashMap<String, usize> = moves
        .iter()
        .enumerate()
        .map(|(at, (source, _))| (key_of(source), at))
        .collect();
    let at_destination = |target: &Path| {
        by_source
            .get(target)
            .or_else(|| by_key.get(&key_of(target)))
            .copied()
    };
    // 0 not looked at, 1 waiting on the file at its destination, 2 in.
    let mut state = vec![0_u8; moves.len()];
    let mut order = Vec::with_capacity(moves.len());
    let mut waiting = Vec::new();
    let mut awaited = vec![false; moves.len()];
    for first in 0..moves.len() {
        let mut at = first;
        while state[at] == 0 {
            state[at] = 1;
            waiting.push(at);
            match at_destination(&moves[at].1) {
                Some(next) if next != at => {
                    awaited[next] = true;
                    at = next;
                }
                _ => break,
            }
        }
        if state[at] == 1 && waiting.last() != Some(&at) {
            return Err(MoveError {
                path: moves[at].0.clone(),
                source: io::Error::other(
                    "files of the torrent would each move onto another still to move",
                ),
            });
        }
        while let Some(done) = waiting.pop() {
            state[done] = 2;
            order.push(done);
        }
    }
    Ok((order, awaited))
}

fn move_files_using(
    from: &Path,
    moves: &[(PathBuf, PathBuf)],
    existing: Existing,
    rename: impl Fn(&Path, &Path) -> io::Result<()>,
) -> Result<Moved, Failed> {
    let mut kept = Vec::new();
    let mut renamed = Vec::new();
    let mut copied = Vec::new();
    let mut failed = None;
    let (order, awaited) = ordered(moves)?;
    for at in order {
        let (source, target) = &moves[at];
        // A dangling link there still counts as a file there, as in
        // libtorrent.
        if existing == Existing::Keep && std::fs::symlink_metadata(target).is_ok() {
            kept.push(at);
            continue;
        }
        if std::fs::symlink_metadata(source).is_err() {
            continue;
        }
        if let Some(parent) = target.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            failed = Some(MoveError::at(parent)(error));
            break;
        }
        match rename(source, target) {
            Ok(()) => renamed.push(at),
            // Taken since the look: kept, as one the look found is.
            Err(error)
                if existing == Existing::Keep && error.kind() == io::ErrorKind::AlreadyExists =>
            {
                kept.push(at);
            }
            // A copy leaves its source in place until every file is in, so
            // a file another waits on cannot go by one: the move fails
            // whole rather than land that other on a source still there.
            Err(error) if error.kind() == io::ErrorKind::CrossesDevices && awaited[at] => {
                failed = Some(MoveError::at(source)(error));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {
                let made = match existing {
                    // Into a file made new: a copy that fails removes what
                    // it made, and nothing else.
                    Existing::Keep => {
                        weaver_server_disk::runtime::file_cache::copy_large_file(source, target)
                            .map(drop)
                    }
                    // Over what is there. A copy that fails removes only a
                    // file it made: one that was there is not this move's
                    // to remove, whether or not the copy could open it.
                    // The copy writes into that file in place, so one that
                    // fails partway leaves it cut short or part written
                    // over: what it held before is not kept.
                    Existing::Replace => {
                        let made_new = std::fs::symlink_metadata(target).is_err();
                        std::fs::copy(source, target).map(drop).inspect_err(|_| {
                            if made_new {
                                let _ = std::fs::remove_file(target);
                            }
                        })
                    }
                };
                match made {
                    Ok(()) => copied.push(at),
                    Err(error)
                        if existing == Existing::Keep
                            && error.kind() == io::ErrorKind::AlreadyExists =>
                    {
                        kept.push(at);
                    }
                    Err(error) => {
                        failed = Some(MoveError::at(source)(error));
                        break;
                    }
                }
            }
            Err(error) => {
                failed = Some(MoveError::at(source)(error));
                break;
            }
        }
    }
    if let Some(error) = failed {
        // A copy's source never left: the copy goes. Then the last moved
        // first: a file goes back before the one that moved out of its way.
        for at in copied {
            let _ = std::fs::remove_file(&moves[at].1);
        }
        let stranded = put_back(moves, renamed.into_iter().rev());
        return Err(Failed { error, stranded });
    }
    // Every file is in: the move is done, and a source that will not go is
    // only left behind (libtorrent `storage_utils.cpp:314-320`).
    for at in &copied {
        let source = &moves[*at].0;
        left_behind(source, std::fs::remove_file(source));
    }
    remove_emptied(from, moves.iter().map(|(source, _)| source.as_path()));
    let mut moved = renamed;
    moved.extend(copied);
    Ok(Moved { kept, moved })
}

// The folders above each of `paths`, under `from`, that hold nothing now.
pub fn remove_emptied<'a>(from: &Path, paths: impl Iterator<Item = &'a Path>) {
    remove_emptied_but(from, paths, &HashSet::new());
}

// `remove_emptied`, but for the folders of `there` and those above them:
// the ones that were there before what emptied them came.
pub fn remove_emptied_but<'a>(
    from: &Path,
    paths: impl Iterator<Item = &'a Path>,
    there: &HashSet<PathBuf>,
) {
    for path in paths {
        let mut dir = path.parent();
        while let Some(at) = dir.filter(|at| *at != from && at.starts_with(from)) {
            if there.contains(at) || std::fs::remove_dir(at).is_err() {
                break;
            }
            dir = at.parent();
        }
    }
}

// Move the directory at `from` to `to`, an empty directory that exists:
// renamed where one device holds both, else copied whole and then removed,
// with nothing left at `to` when the copy fails partway.
pub fn move_directory(from: &Path, to: &Path) -> Result<(), MoveError> {
    move_directory_using(from, to, |from, to| std::fs::rename(from, to))
}

fn move_directory_using(
    from: &Path,
    to: &Path,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<(), MoveError> {
    // The claimed directory makes way for the rename, which on every
    // platform needs the destination absent.
    std::fs::remove_dir(to).map_err(MoveError::at(to))?;
    match rename(from, to) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => {}
        Err(error) => {
            // The claim stands as it was.
            let _ = std::fs::create_dir(to);
            return Err(MoveError {
                path: from.to_path_buf(),
                source: error,
            });
        }
    }
    if let Err(error) = copy_tree(from, from, to) {
        let _ = std::fs::remove_dir_all(to);
        let _ = std::fs::create_dir(to);
        return Err(error);
    }
    // The copy is whole, so the move is done: a source that will not go is
    // only left behind (libtorrent `storage_utils.cpp:295-335`).
    left_behind(from, std::fs::remove_dir_all(from));
    Ok(())
}

// A source a finished move could not remove, which stays where it was.
fn left_behind(path: &Path, removed: io::Result<()>) {
    if let Err(error) = removed
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %error, "a moved torrent's files were copied but could not be removed from where they were");
    }
}

// Copy the directory at `from`, as it is, to `to`, which need not exist. A
// link in it is copied as what it points at, which must be inside `root`,
// the folder moved: one that leads outside fails the move, so no bytes from
// outside are taken in as the torrent's.
fn copy_tree(root: &Path, from: &Path, to: &Path) -> Result<(), MoveError> {
    std::fs::create_dir_all(to).map_err(MoveError::at(to))?;
    for entry in std::fs::read_dir(from).map_err(MoveError::at(from))? {
        let entry = entry.map_err(MoveError::at(from))?;
        let source = entry.path();
        let target = to.join(entry.file_name());
        let mut kind = entry.file_type().map_err(MoveError::at(&source))?;
        if kind.is_symlink() {
            if !beneath::resolves_inside(root, &source).map_err(MoveError::at(&source))? {
                return Err(MoveError {
                    path: source.clone(),
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{} leads outside {} through a link, so the torrent was not moved",
                            source.display(),
                            root.display()
                        ),
                    ),
                });
            }
            kind = std::fs::metadata(&source)
                .map_err(MoveError::at(&source))?
                .file_type();
        }
        if kind.is_dir() {
            copy_tree(root, &source, &target)?;
        } else {
            std::fs::copy(&source, &target).map_err(MoveError::at(&source))?;
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
        assert!(matches!(error.error, MoveError { ref path, .. } if path.ends_with("c.bin")));
        assert!(error.stranded.is_empty());
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
        let kept = move_files(&from, &moves, Existing::Keep).unwrap().kept;
        assert_eq!(kept.len(), 1);
        assert!(moves[kept[0]].0.ends_with("sub/b.bin"));
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
        let moved = move_files(&to, &moves, Existing::Keep).unwrap();
        assert!(moved.kept.is_empty());
        assert_eq!(moved.moved, [0]);
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
        assert!(
            move_files(&from, &moves, Existing::Replace)
                .unwrap()
                .kept
                .is_empty()
        );
        assert_eq!(contents(&to), expected);
        assert!(contents(&from).is_empty());
    }

    // A move into a folder of the torrent's own, or out of one: a file's
    // destination is then another file of the batch still to move. Each
    // ends at its own name with its own bytes, in either mode, whatever
    // order the batch lists them in; none is replaced by another, and none
    // is taken for a file already there.
    #[test]
    fn a_file_never_moves_onto_one_of_the_batch_still_to_move() {
        for existing in [Existing::Replace, Existing::Keep] {
            for reversed in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let from = root.path().join("from");
                let to = from.join("sub");
                std::fs::create_dir_all(from.join("sub/sub")).unwrap();
                std::fs::write(from.join("a.bin"), b"top").unwrap();
                std::fs::write(from.join("sub/a.bin"), b"middle").unwrap();
                std::fs::write(from.join("sub/sub/a.bin"), b"deep").unwrap();
                let mut moves: Vec<(PathBuf, PathBuf)> = ["a.bin", "sub/a.bin", "sub/sub/a.bin"]
                    .iter()
                    .map(|name| (from.join(name), to.join(name)))
                    .collect();
                if reversed {
                    moves.reverse();
                }
                let moved = move_files(&from, &moves, existing).unwrap();
                assert!(moved.kept.is_empty(), "{existing:?} {reversed}");
                assert_eq!(moved.moved.len(), 3);
                let at = |name: &str| std::fs::read(to.join(name)).unwrap();
                assert_eq!(at("a.bin"), b"top", "{existing:?} {reversed}");
                assert_eq!(at("sub/a.bin"), b"middle", "{existing:?} {reversed}");
                assert_eq!(at("sub/sub/a.bin"), b"deep", "{existing:?} {reversed}");

                // And back out again, by the move that puts a move back.
                assert!(moved.put_back(&to, &moves, &HashSet::new()).is_empty());
                let at = |name: &str| std::fs::read(from.join(name)).unwrap();
                assert_eq!(at("a.bin"), b"top");
                assert_eq!(at("sub/a.bin"), b"middle");
                assert_eq!(at("sub/sub/a.bin"), b"deep");
                assert!(!from.join("sub/sub/sub").exists());
            }
        }
    }

    // A file that takes a destination's name after the look that found it
    // free is kept as one the look found is: the move never replaces it,
    // and the file that would have taken its place stays at its source.
    #[test]
    fn a_file_that_appears_after_the_look_is_kept_not_replaced() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        std::fs::create_dir_all(&from).unwrap();
        std::fs::write(from.join("a.bin"), b"ours").unwrap();
        std::fs::write(from.join("b.bin"), b"ours too").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = ["a.bin", "b.bin"]
            .iter()
            .map(|name| (from.join(name), to.join(name)))
            .collect();
        let moved = move_files_using(&from, &moves, Existing::Keep, |source, target| {
            if source.ends_with("b.bin") {
                std::fs::write(target, b"theirs").unwrap();
            }
            weaver_server_disk::runtime::fs::rename_path_exclusive(source, target)
        })
        .unwrap();
        assert_eq!((moved.kept, moved.moved), (vec![1], vec![0]));
        assert_eq!(std::fs::read(to.join("b.bin")).unwrap(), b"theirs");
        assert_eq!(std::fs::read(from.join("b.bin")).unwrap(), b"ours too");
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"ours");
    }

    // Putting files back never replaces: a file whose old name was taken
    // since it left stays where the move put it, the file at its old name
    // is as it was put there, and the put-back names it. So does the undo
    // of a batch that failed partway.
    #[test]
    fn a_file_put_back_never_replaces_what_took_its_name() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        std::fs::create_dir_all(&from).unwrap();
        std::fs::write(from.join("a.bin"), b"ours").unwrap();
        std::fs::write(from.join("b.bin"), b"ours too").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = ["a.bin", "b.bin"]
            .iter()
            .map(|name| (from.join(name), to.join(name)))
            .collect();
        let moved = move_files(&from, &moves, Existing::Replace).unwrap();
        std::fs::create_dir_all(&from).unwrap();
        std::fs::write(from.join("a.bin"), b"theirs").unwrap();
        assert_eq!(moved.put_back(&to, &moves, &HashSet::new()), [0]);
        assert_eq!(std::fs::read(from.join("a.bin")).unwrap(), b"theirs");
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"ours");
        assert_eq!(std::fs::read(from.join("b.bin")).unwrap(), b"ours too");
        assert!(!to.join("b.bin").exists());

        // A batch that fails at its second file, the first one's old name
        // taken by then.
        let (from, to) = (root.path().join("from2"), root.path().join("to2"));
        std::fs::create_dir_all(&from).unwrap();
        std::fs::write(from.join("a.bin"), b"ours").unwrap();
        std::fs::write(from.join("b.bin"), b"ours too").unwrap();
        let moves: Vec<(PathBuf, PathBuf)> = ["a.bin", "b.bin"]
            .iter()
            .map(|name| (from.join(name), to.join(name)))
            .collect();
        let failed = move_files_using(&from, &moves, Existing::Replace, |source, target| {
            if source.ends_with("b.bin") {
                std::fs::write(from.join("a.bin"), b"theirs").unwrap();
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
            std::fs::rename(source, target)
        })
        .unwrap_err();
        assert_eq!(failed.stranded, [0]);
        assert_eq!(std::fs::read(from.join("a.bin")).unwrap(), b"theirs");
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"ours");
        assert_eq!(std::fs::read(from.join("b.bin")).unwrap(), b"ours too");
    }

    // A replacing copy across devices that fails leaves the file that was
    // at the destination: it is not one this move made.
    #[cfg(unix)]
    #[test]
    fn a_failed_copy_over_a_file_there_does_not_remove_it() {
        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        std::fs::create_dir_all(&from).unwrap();
        std::fs::create_dir_all(&to).unwrap();
        // A source the copy cannot open.
        std::os::unix::fs::symlink(from.join("gone"), from.join("a.bin")).unwrap();
        std::fs::write(to.join("a.bin"), b"theirs").unwrap();
        let moves = vec![(from.join("a.bin"), to.join("a.bin"))];
        move_files_using(&from, &moves, Existing::Replace, |_, _| {
            Err(io::Error::from(io::ErrorKind::CrossesDevices))
        })
        .unwrap_err();
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"theirs");
    }

    // A file's destination that differs from a file of the batch still to
    // move only as a volume that folds names would not tell apart: that
    // file moves first, as one at the very path does, so on such a volume
    // neither lands on the other. On a volume that tells them apart the
    // two never meet, and each ends at its own name all the same.
    #[test]
    fn a_file_waits_for_one_at_its_destination_under_another_case() {
        for reversed in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let from = root.path().join("from");
            let to = from.join("sub");
            std::fs::create_dir_all(&to).unwrap();
            std::fs::write(from.join("a.bin"), b"top").unwrap();
            std::fs::write(from.join("sub/A.BIN"), b"middle").unwrap();
            let mut moves: Vec<(PathBuf, PathBuf)> = ["a.bin", "sub/A.BIN"]
                .iter()
                .map(|name| (from.join(name), to.join(name)))
                .collect();
            if reversed {
                moves.reverse();
            }
            let moved = move_files(&from, &moves, Existing::Replace).unwrap();
            assert_eq!(moved.moved.len(), 2, "{reversed}");
            assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"top");
            assert_eq!(std::fs::read(to.join("sub/A.BIN")).unwrap(), b"middle");
        }
    }

    // Files that would each take another's place in a ring move not at
    // all, and neither do the rest of the batch.
    #[test]
    fn files_that_would_swap_places_do_not_move() {
        let root = tempfile::tempdir().unwrap();
        let from = root.path().to_path_buf();
        std::fs::write(from.join("a.bin"), b"a").unwrap();
        std::fs::write(from.join("b.bin"), b"b").unwrap();
        std::fs::write(from.join("c.bin"), b"c").unwrap();
        let moves = vec![
            (from.join("c.bin"), from.join("moved/c.bin")),
            (from.join("a.bin"), from.join("b.bin")),
            (from.join("b.bin"), from.join("a.bin")),
        ];
        move_files(&from, &moves, Existing::Replace).unwrap_err();
        assert_eq!(std::fs::read(from.join("a.bin")).unwrap(), b"a");
        assert_eq!(std::fs::read(from.join("b.bin")).unwrap(), b"b");
        assert_eq!(std::fs::read(from.join("c.bin")).unwrap(), b"c");
        assert!(!from.join("moved").exists());
    }

    // libtorrent `storage_utils::move_storage` (`storage_utils.cpp:295-335`):
    // once every copy is in, the move is done; a source that will not go
    // after it is left behind, and the move stands.
    #[cfg(unix)]
    #[test]
    fn a_copied_move_stands_when_its_source_will_not_go() {
        use std::os::unix::fs::PermissionsExt;
        let locked = |dir: &Path, mode| {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        let across = |_: &Path, _: &Path| Err(io::Error::from(io::ErrorKind::CrossesDevices));

        let root = tempfile::tempdir().unwrap();
        let (from, to) = (root.path().join("from"), root.path().join("to"));
        tree(&from);
        let expected = contents(&from);
        std::fs::create_dir(&to).unwrap();
        locked(&from.join("sub"), 0o555);
        let moved = move_directory_using(&from, &to, across);
        locked(&from.join("sub"), 0o755);
        moved.unwrap();
        assert_eq!(contents(&to), expected);
        assert!(from.join("sub/b.bin").exists());

        let (from, to) = (root.path().join("files"), root.path().join("moved"));
        tree(&from);
        let moves: Vec<(PathBuf, PathBuf)> = expected
            .iter()
            .map(|(name, _)| (from.join(name), to.join(name)))
            .collect();
        locked(&from.join("sub"), 0o555);
        let moved = move_files_using(&from, &moves, Existing::Replace, across);
        locked(&from.join("sub"), 0o755);
        assert!(moved.unwrap().kept.is_empty());
        assert_eq!(contents(&to), expected);
        assert!(from.join("sub/b.bin").exists());
        assert!(!from.join("a.bin").exists());
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
        assert!(matches!(error, MoveError { ref path, .. } if *path == from));
        assert_eq!(contents(&from), expected);
        assert!(to.is_dir());
        assert!(contents(&to).is_empty());
    }

    fn leads_outside(error: &MoveError) -> bool {
        error.source.kind() == io::ErrorKind::InvalidInput
            && error.source.to_string().contains("leads outside")
    }

    // A move of files through a link that leads out of either folder moves
    // nothing: a file that is a link to one outside, a linked folder above a
    // file, and a destination folder linked outside. Links that stay inside
    // move as they always did.
    #[cfg(unix)]
    #[test]
    fn a_move_through_a_link_that_leads_outside_moves_nothing() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let (from, to, away) = (
            root.path().join("from"),
            root.path().join("to"),
            root.path().join("away"),
        );
        tree(&from);
        std::fs::create_dir_all(&away).unwrap();
        std::fs::create_dir_all(&to).unwrap();
        std::fs::write(away.join("secret"), b"theirs").unwrap();
        let expected = contents(&from);
        let moves_of = |names: &[&str]| -> Vec<(PathBuf, PathBuf)> {
            names
                .iter()
                .map(|name| (from.join(name), to.join(name)))
                .collect()
        };
        for existing in [Existing::Keep, Existing::Replace] {
            // A file of the torrent's that is a link to one outside.
            symlink(away.join("secret"), from.join("linked.bin")).unwrap();
            let failed =
                move_files(&from, &moves_of(&["a.bin", "linked.bin"]), existing).unwrap_err();
            assert!(leads_outside(&failed.error), "{}", failed.error);
            std::fs::remove_file(from.join("linked.bin")).unwrap();
            // A folder of the torrent's linked outside.
            symlink(&away, from.join("out")).unwrap();
            let failed =
                move_files(&from, &moves_of(&["a.bin", "out/secret"]), existing).unwrap_err();
            assert!(leads_outside(&failed.error), "{}", failed.error);
            std::fs::remove_file(from.join("out")).unwrap();
            // A folder at the new place linked outside.
            symlink(&away, to.join("sub")).unwrap();
            let failed =
                move_files(&from, &moves_of(&["a.bin", "sub/b.bin"]), existing).unwrap_err();
            assert!(leads_outside(&failed.error), "{}", failed.error);
            std::fs::remove_file(to.join("sub")).unwrap();
            assert_eq!(contents(&from), expected);
            assert!(contents(&to).is_empty());
            assert_eq!(
                contents(&away),
                vec![(PathBuf::from("secret"), b"theirs".to_vec())]
            );
        }
        // A folder at the new place linked to another inside it moves.
        std::fs::create_dir_all(to.join("real")).unwrap();
        symlink(to.join("real"), to.join("sub")).unwrap();
        move_files(&from, &moves_of(&["a.bin", "sub/b.bin"]), Existing::Keep).unwrap();
        assert_eq!(std::fs::read(to.join("real/b.bin")).unwrap(), b"bbbb");
        assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), b"aaa");
    }

    // A folder copied across devices with a link in it to a file outside
    // fails whole, nothing left at the new place; one to a file inside is
    // copied as the file.
    #[cfg(unix)]
    #[test]
    fn a_directory_copy_refuses_a_link_that_leads_outside() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let (from, to, away) = (
            root.path().join("from"),
            root.path().join("to"),
            root.path().join("away"),
        );
        tree(&from);
        std::fs::create_dir_all(&away).unwrap();
        std::fs::write(away.join("secret"), b"theirs").unwrap();
        let across = |_: &Path, _: &Path| Err(io::Error::from(io::ErrorKind::CrossesDevices));
        symlink(away.join("secret"), from.join("sub/linked.bin")).unwrap();
        std::fs::create_dir(&to).unwrap();
        let error = move_directory_using(&from, &to, across).unwrap_err();
        assert!(leads_outside(&error), "{error}");
        assert!(to.is_dir());
        assert!(contents(&to).is_empty());
        assert!(from.join("a.bin").exists());
        std::fs::remove_file(from.join("sub/linked.bin")).unwrap();
        symlink("../a.bin", from.join("sub/inside.bin")).unwrap();
        move_directory_using(&from, &to, across).unwrap();
        assert_eq!(std::fs::read(to.join("sub/inside.bin")).unwrap(), b"aaa");
        assert!(!from.exists());
    }
}
