// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file opened, made or looked at by its name under a folder, never through
// a link that leads out of that folder. A link that stays inside is followed;
// one at any part of the name that leads outside is refused, with why.
//
// The name is resolved from a handle on the folder, one part at a time
// (`openat2` with `RESOLVE_BENEATH` where the system has it, a walk of
// `openat` calls elsewhere), so no link swapped in between a look and the
// open can take it outside: there is no look apart from the open. A link
// with an absolute target is refused by that walk even when it lands
// inside; for it alone the name is resolved by path, and opened again from
// the folder's handle along the path it resolved to, which holds no link,
// so a link swapped in meanwhile is refused all the same.
//
// The cost is the folder's handle, once per open or look: a file kept open
// is read and written with no further call.

use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};

// How a file is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Access {
    Read,
    // To read and write, made with the folders it is in when it is not
    // there, the folder itself among them; never cut.
    Write,
}

// What a look at a file found, through the links that stay inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Look {
    pub(crate) is_file: bool,
    pub(crate) len: u64,
    // None where the file system keeps no time for it.
    pub(crate) modified: Option<SystemTime>,
}

// The file `name`, a relative path, under `root`.
pub(crate) fn open(root: &Path, name: &Path, access: Access) -> io::Result<File> {
    let dir = folder(root, access)?;
    beneath(root, name, |name| open_in(&dir, name, access))
}

// The file `name` under `root` looked at, following the links that stay
// inside: not there is `NotFound`.
pub(crate) fn look(root: &Path, name: &Path) -> io::Result<Look> {
    let dir = folder(root, Access::Read)?;
    let found = beneath(root, name, |name| dir.metadata(name))?;
    Ok(Look {
        is_file: found.is_file(),
        len: found.len(),
        modified: found
            .modified()
            .ok()
            .map(cap_std::time::SystemTime::into_std),
    })
}

// Whether `path`, a path under `root` by name, resolves inside `root` as far
// as it is there: a part not there yet stays where its folder is. For a step
// that works by path (a rename), which this cannot hold to the answer: a
// link swapped in after it is not seen.
pub(crate) fn resolves_inside(root: &Path, path: &Path) -> io::Result<bool> {
    let Ok(name) = path.strip_prefix(root) else {
        return Ok(false);
    };
    // A folder not there yet holds no link.
    if std::fs::symlink_metadata(root).is_err_and(|error| error.kind() == io::ErrorKind::NotFound) {
        return Ok(true);
    }
    Ok(resolved(root, name)?.is_some())
}

// Whether `error` is the refusal of a name that leads outside its folder.
pub(crate) fn is_outside(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput
        && error
            .get_ref()
            .is_some_and(|inner| inner.to_string().contains(OUTSIDE))
}

const OUTSIDE: &str = "through a link, so it is not opened";

// Why a name was refused.
fn outside(root: &Path, name: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "{} leads outside {} {OUTSIDE}",
            name.display(),
            root.display()
        ),
    )
}

// The folder, made first for a write when it is not there.
fn folder(root: &Path, access: Access) -> io::Result<Dir> {
    match Dir::open_ambient_dir(root, ambient_authority()) {
        Err(error) if error.kind() == io::ErrorKind::NotFound && access == Access::Write => {
            std::fs::create_dir_all(root)?;
            Dir::open_ambient_dir(root, ambient_authority())
        }
        opened => opened,
    }
}

// `run` on `name` from the folder's handle; on a refusal to leave it, again
// on the path `name` resolves to, when that is inside.
fn beneath<T>(root: &Path, name: &Path, run: impl Fn(&Path) -> io::Result<T>) -> io::Result<T> {
    if !name
        .components()
        .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(outside(root, name));
    }
    match run(name) {
        Err(error) if refused(&error) => {
            let Some(inside) = resolved(root, name)? else {
                return Err(outside(root, name));
            };
            run(&inside).map_err(|error| {
                if escaped(&error) {
                    outside(root, name)
                } else {
                    error
                }
            })
        }
        done => done,
    }
}

// An error that may be the walk's refusal to leave the folder.
fn refused(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::CrossesDevices
    )
}

// The walk's refusal to leave the folder, and no other permission error.
fn escaped(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::CrossesDevices
        || (error.kind() == io::ErrorKind::PermissionDenied
            && error.to_string().contains("outside of the filesystem"))
}

// `name` under `root` resolved by path as far as it is there, the rest as
// given, relative to `root` resolved; None when it is not inside.
fn resolved(root: &Path, name: &Path) -> io::Result<Option<PathBuf>> {
    let base = std::fs::canonicalize(root)?;
    let mut rest = Vec::new();
    let mut at = root.join(name);
    let found = loop {
        match std::fs::canonicalize(&at) {
            Ok(found) => break found,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let (Some(part), Some(parent)) = (at.file_name(), at.parent()) else {
                    return Err(error);
                };
                rest.push(part.to_owned());
                at = parent.to_path_buf();
            }
            Err(error) => return Err(error),
        }
    };
    let Ok(inside) = found.strip_prefix(&base) else {
        return Ok(None);
    };
    let mut inside = inside.to_path_buf();
    inside.extend(rest.into_iter().rev());
    Ok((!inside.as_os_str().is_empty()).then_some(inside))
}

fn open_in(dir: &Dir, name: &Path, access: Access) -> io::Result<File> {
    let opened = match access {
        Access::Read => dir.open(name),
        Access::Write => {
            let mut options = OpenOptions::new();
            options.read(true).write(true).create(true).truncate(false);
            match dir.open_with(name, &options) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if let Some(parent) = name.parent()
                        && !parent.as_os_str().is_empty()
                    {
                        dir.create_dir_all(parent)?;
                    }
                    dir.open_with(name, &options)
                }
                opened => opened,
            }
        }
    };
    opened.map(cap_std::fs::File::into_std)
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::fs::symlink;

    use super::*;

    // A save folder beside a folder outside it.
    fn places() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let top = tempfile::tempdir().unwrap();
        let save = top.path().join("save");
        let away = top.path().join("away");
        std::fs::create_dir_all(&save).unwrap();
        std::fs::create_dir_all(&away).unwrap();
        (top, save, away)
    }

    fn refused_outside(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::InvalidInput && error.to_string().contains("leads outside")
    }

    fn write(root: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
        open(root, Path::new(name), Access::Write)?.write_all(bytes)
    }

    fn read(root: &Path, name: &str) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        open(root, Path::new(name), Access::Read)?.read_to_end(&mut out)?;
        Ok(out)
    }

    // A link to a file outside, a link to a folder outside, and a link in
    // the middle of a name to a folder outside: none is written or read
    // through, and nothing is made outside.
    #[test]
    fn a_link_that_leads_outside_is_refused_for_write_and_read() {
        let (_top, save, away) = places();
        std::fs::write(away.join("secret"), b"theirs").unwrap();
        std::fs::create_dir_all(away.join("deep")).unwrap();
        symlink(away.join("secret"), save.join("file.bin")).unwrap();
        symlink(&away, save.join("dir")).unwrap();
        std::fs::create_dir_all(save.join("mid")).unwrap();
        symlink(&away, save.join("mid/link")).unwrap();
        // Relative targets too: the walk sees them for what they are.
        symlink("../../away", save.join("mid/relative")).unwrap();
        for name in [
            "file.bin",
            "dir/payload.bin",
            "mid/link/deep/payload.bin",
            "mid/relative/payload.bin",
        ] {
            let wrote = write(&save, name, b"ours").unwrap_err();
            assert!(refused_outside(&wrote), "{name}: {wrote}");
            let read = read(&save, name).unwrap_err();
            assert!(refused_outside(&read), "{name}: {read}");
            let looked = look(&save, Path::new(name)).unwrap_err();
            assert!(refused_outside(&looked), "{name}: {looked}");
        }
        assert_eq!(std::fs::read(away.join("secret")).unwrap(), b"theirs");
        assert!(!away.join("payload.bin").exists());
        assert!(!away.join("deep/payload.bin").exists());
    }

    // Links that stay inside, relative or absolute, to a file or a folder,
    // are written and read through, and so is a name with none.
    #[test]
    fn a_link_that_stays_inside_works() {
        let (_top, save, _away) = places();
        std::fs::create_dir_all(save.join("real")).unwrap();
        symlink("real", save.join("in")).unwrap();
        symlink(save.join("real"), save.join("absolute")).unwrap();
        write(&save, "in/a.bin", b"aaaa").unwrap();
        write(&save, "absolute/b.bin", b"bb").unwrap();
        write(&save, "plain/c.bin", b"c").unwrap();
        assert_eq!(std::fs::read(save.join("real/a.bin")).unwrap(), b"aaaa");
        assert_eq!(std::fs::read(save.join("real/b.bin")).unwrap(), b"bb");
        assert_eq!(read(&save, "in/a.bin").unwrap(), b"aaaa");
        assert_eq!(read(&save, "absolute/b.bin").unwrap(), b"bb");
        assert_eq!(read(&save, "plain/c.bin").unwrap(), b"c");
        symlink("real/a.bin", save.join("to_file")).unwrap();
        assert_eq!(read(&save, "to_file").unwrap(), b"aaaa");
        let found = look(&save, Path::new("to_file")).unwrap();
        assert!(found.is_file);
        assert_eq!(found.len, 4);
        assert!(found.modified.is_some());
    }

    // A save folder not there yet is made by the first write.
    #[test]
    fn a_write_makes_the_folder_and_a_read_does_not() {
        let (_top, save, _away) = places();
        let fresh = save.join("fresh");
        assert_eq!(
            read(&fresh, "a.bin").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(!fresh.exists());
        write(&fresh, "sub/a.bin", b"x").unwrap();
        assert_eq!(std::fs::read(fresh.join("sub/a.bin")).unwrap(), b"x");
    }

    // A name that climbs or roots itself is refused before anything is
    // opened.
    #[test]
    fn a_name_that_climbs_is_refused() {
        let (_top, save, away) = places();
        for name in ["../away/x", "/etc/passwd", "a/../../away/x"] {
            assert!(
                refused_outside(&write(&save, name, b"x").unwrap_err()),
                "{name}"
            );
        }
        assert!(!away.join("x").exists());
    }

    // What a move asks: a path that resolves inside, through a link or not,
    // and one that leaves.
    #[test]
    fn a_path_is_judged_where_it_resolves() {
        let (_top, save, away) = places();
        std::fs::create_dir_all(save.join("real")).unwrap();
        symlink(save.join("real"), save.join("in")).unwrap();
        symlink(&away, save.join("out")).unwrap();
        assert!(resolves_inside(&save, &save.join("in/a.bin")).unwrap());
        assert!(resolves_inside(&save, &save.join("new/deeper/a.bin")).unwrap());
        assert!(!resolves_inside(&save, &save.join("out/a.bin")).unwrap());
        assert!(!resolves_inside(&save, &away.join("a.bin")).unwrap());
    }
}
