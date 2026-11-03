// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The restore, lifted from the classic engine's
// `pipeline/completion/finalize/rar.rs` `try_restore_rar_recovery_volumes`:
// unrar-rs finds the recovery volumes beside the set's volumes, checks every
// volume against them, and rebuilds the ones missing or bad. It works on
// files, so the set is staged into the namespace's scratch directory for it,
// and what it rebuilt is written back into the members.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use weaver_graph::recovery::MemberAccess;
use weaver_graph::{ByteRange, MemberId, VolumeIndex};

// How much is copied at once, so a volume never sits in memory whole.
const CHUNK: u64 = 1 << 20;

// A file of the set to stage: a data volume, a member of the namespace, or a
// recovery volume, one of the object's own.
pub(super) enum Source {
    Member(MemberId),
    Volume(VolumeIndex),
}

pub(super) struct Staged {
    pub(super) name: String,
    pub(super) source: Source,
    pub(super) length: u64,
}

// The data volumes rebuilt, by their index in the set, each with the file
// that holds it; `None` when the recovery volumes cannot rebuild the set.
pub(super) fn restore(
    dir: &Path,
    files: &[Staged],
    access: &mut dyn MemberAccess,
) -> Option<Vec<(u32, PathBuf)>> {
    let paths: Vec<PathBuf> = files
        .iter()
        .filter_map(|file| stage(dir, file, access))
        .collect();
    let report =
        unrar_rs::restore_volumes_from_paths(&paths, &unrar_rs::RecoveryOptions::default()).ok()?;
    report
        .missing_volume_numbers
        .into_iter()
        .zip(report.restored_paths)
        .map(|(index, path)| Some((u32::try_from(index).ok()?, path)))
        .collect()
}

// Copy one file of the set into `dir` under its own name. A file whose bytes
// cannot all be read is left out, which the restore counts as missing.
fn stage(dir: &Path, file: &Staged, access: &mut dyn MemberAccess) -> Option<PathBuf> {
    let name = Path::new(&file.name).file_name()?;
    let path = dir.join(name);
    let copied = File::create(&path).ok().and_then(|mut out| {
        let mut buffer = Vec::new();
        let mut at = 0;
        while at < file.length {
            let range = ByteRange::new(at, (at + CHUNK).min(file.length));
            buffer.clear();
            let read = match file.source {
                Source::Member(member) => access.read(member, range, &mut buffer),
                Source::Volume(volume) => access.read_volume(volume, range, &mut buffer),
            };
            if !read || buffer.len() as u64 != range.len() {
                return None;
            }
            out.write_all(&buffer).ok()?;
            at = range.end;
        }
        Some(())
    });
    if copied.is_none() {
        let _ = std::fs::remove_file(&path);
    }
    copied.map(|()| path)
}

// A rebuilt volume's file and its length, for the volume to be judged by.
pub(super) fn open(path: &Path) -> Option<(File, u64)> {
    let file = File::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    Some((file, length))
}

// The file's bytes over `range` onto the end of `into`.
pub(super) fn read_at(file: &mut File, range: ByteRange, into: &mut Vec<u8>) -> bool {
    let start = into.len();
    into.resize(start + range.len() as usize, 0);
    file.seek(SeekFrom::Start(range.start)).is_ok() && file.read_exact(&mut into[start..]).is_ok()
}

// Write a rebuilt volume into its member, a piece at a time, as far as the
// member reaches: a RAR3 restore pads a volume to the widest of its set and
// leaves the recovery footer's width of zeros past it.
pub(super) fn write_back(path: &Path, member: MemberId, access: &mut dyn MemberAccess) -> bool {
    let (Ok(file), Some(length)) = (File::open(path), access.length(member)) else {
        return false;
    };
    let mut file = file.take(length);
    let mut buffer = vec![0; CHUNK as usize];
    let mut offset = 0;
    loop {
        match file.read(&mut buffer) {
            Ok(0) => return true,
            Ok(read) => {
                access.write_repaired(member, offset, &buffer[..read]);
                offset += read as u64;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}
