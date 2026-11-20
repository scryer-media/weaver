// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a removed torrent leaves behind in history: the files a deletion
//! of it removes, what live torrents still hold, and the retiring rows a
//! start finishes. The torrent client lives in weaver-server-torrents.

use crate::StateError;
use crate::jobs::JobId;
use crate::persistence::Database;
use crate::torrents::files;
use crate::torrents::files::Files;
use crate::torrents::model::Adopted;
use crate::torrents::persistence::HistoryFile;
use crate::torrents::persistence::TorrentsPersistenceLiveHeldPathsInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceMarkTorrentRetiringInternalQueries;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use weaver_server_torrent_store::torrents::metainfo;

// What a deletion of a torrent's files removes: see `written_paths`.
#[derive(Debug, Default)]
pub struct Deletion {
    // The job whose marker on `dir` or `claimed` lets that folder go once
    // the marker is all it holds.
    pub owner: Option<JobId>,
    // The torrent's own files Weaver made, each removed only as a file.
    pub paths: Vec<PathBuf>,
    // What its job made of them: unpacked files, each removed only while
    // its name holds a regular file. A folder or a link found there is not
    // what the job placed, and stays.
    pub made: Vec<PathBuf>,
    // The directory the files are in.
    pub dir: PathBuf,
    // Whether that directory is the torrent's own, to go once empty.
    pub own: bool,
    // The folders that were there before the torrent, which stay.
    pub kept_dirs: HashSet<PathBuf>,
    // The folder claimed for the torrent beside a shared one, to go once
    // empty.
    pub claimed: Option<PathBuf>,
    // The torrent's unpacking directory, under its root, removed whole only
    // while its marker names the torrent's job.
    pub work: Option<(PathBuf, PathBuf, JobId)>,
}

// What a deletion of the files takes: each listed file not among those
// adopted, and each of `outputs`, in the directory the files are in. With a
// folder `claimed` beside it, that directory is a shared one, and stays.
pub fn deletion(
    files: &Files,
    adopted: &Adopted,
    outputs: &[String],
    claimed: Option<PathBuf>,
) -> Deletion {
    let dir = files.dir();
    let kept = files::adopted_paths(adopted);
    let listed = files
        .names()
        .flatten()
        .filter(|name| !adopted.all_files && !kept.contains(*name))
        .map(|name| dir.join(name));
    let paths = listed.collect();
    let made = outputs.iter().map(|output| dir.join(output)).collect();
    let kept_dirs = adopted.dirs.iter().map(|folder| dir.join(folder)).collect();
    Deletion {
        owner: None,
        paths,
        made,
        own: claimed.is_none(),
        dir,
        kept_dirs,
        claimed,
        work: None,
    }
}

// [`history_files`], on the calling thread.
pub fn listed_files(deletion: &Deletion) -> Vec<HistoryFile> {
    {
        let listed = deletion.paths.iter().map(|path| (path.clone(), false));
        // An unpacked file is listed only while its name holds a regular
        // file: a folder or a link there is someone else's.
        let made = deletion
            .made
            .iter()
            .filter(|path| is_file(path))
            .map(|path| (path.clone(), false));
        let work = deletion.work.as_ref().and_then(|(root, work, job_id)| {
            let owned = crate::jobs::working_dir::working_dir_owner(root, work)
                .is_ok_and(|owner| owner == Some(*job_id));
            owned.then(|| (work.clone(), true))
        });
        listed
            .chain(made)
            .chain(work)
            .filter(|(path, _)| std::fs::symlink_metadata(path).is_ok())
            .map(|(path, whole)| HistoryFile {
                path: path.to_string_lossy().into_owned(),
                whole,
            })
            .collect()
    }
}

// Start-up, and the retry of what it left: each torrent a removal left
// retiring finishes its removal from its row. One whose removal asked for
// its files has them deleted as a live removal deletes them, from its
// metainfo, what was there before it and what unpacking placed, under the
// same rules a removal from history keeps (`spare_held`); one that kept
// them lists them, its kept unpacking folder with them, so the sweep leaves
// it. Each comes back with its history row, built from its torrents row
// (`RetiringRow`), and the files that row lists. What live torrents hold
// is read once for all of them; when that read fails, those that would
// delete stay retiring for the next try. Reads and deletes on the calling
// thread: run off the runtime.
pub fn finish_retiring(
    db: &Database,
    work: &Path,
) -> Result<Vec<(JobId, crate::history::JobHistoryRow, Vec<HistoryFile>)>, StateError> {
    let rows = db.retiring_torrents()?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let planned: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let deletion = retiring_deletion(&row, work);
            (row, deletion)
        })
        .collect();
    let dirs: Vec<PathBuf> = planned
        .iter()
        .filter(|(row, _)| row.delete_files)
        .map(|(_, deletion)| deletion.dir.clone())
        .filter(|dir| !dir.as_os_str().is_empty())
        .collect();
    let held = LiveHeld::under(db, &dirs);
    if let Err(error) = &held {
        tracing::warn!(error = %error, "could not read what live torrents hold; removed torrents whose files go stay retiring");
    }
    let mut finished = Vec::with_capacity(planned.len());
    for (row, mut deletion) in planned {
        if row.delete_files {
            let Ok(held) = &held else {
                continue;
            };
            spare_held(&mut deletion, held, row.job_id);
            delete_written(&deletion);
        }
        finished.push((row.job_id, row.history, listed_files(&deletion)));
    }
    Ok(finished)
}

// What a retiring torrent's removal takes, from its row (`written_paths`
// for a live one): its files by its metainfo under the folder they are in,
// less what was there before it, and what unpacking placed; its unpacking
// folder by its marker. One whose metainfo or what was there before it does
// not read takes no file, and one with no folder takes only its unpacking
// folder.
fn retiring_deletion(row: &crate::torrents::persistence::RetiringRow, work: &Path) -> Deletion {
    let job_id = row.job_id;
    let claimed = row
        .files_dir
        .as_ref()
        .and(row.storage_dir.as_deref())
        .map(PathBuf::from);
    let base = row.files_dir.as_deref().or(row.storage_dir.as_deref());
    let files = match (row.metainfo.as_deref(), base) {
        (Some(bytes), Some(base)) => match metainfo::parse(bytes) {
            Ok(parsed) => Files::new(&parsed.layout, PathBuf::from(base))
                .map_err(|error| tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's files do not read; they stay"))
                .ok(),
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's metainfo does not read; its files stay");
                None
            }
        },
        _ => None,
    };
    let mut deletion = match (files, &row.adopted, &row.outputs) {
        (Some(files), Some(adopted), Some(outputs)) => deletion(&files, adopted, outputs, claimed),
        (files, ..) => Deletion {
            dir: files
                .map(|files| files.dir())
                .or_else(|| base.map(PathBuf::from))
                .unwrap_or_default(),
            claimed,
            ..Deletion::default()
        },
    };
    deletion.owner = Some(job_id);
    deletion.work = Some((work.to_path_buf(), unpacking_dir(work, job_id), job_id));
    deletion
}

// Leaves out of `deletion` what a removal from history would leave: what a
// live torrent holds (`held`), what lies under a link inside its folder or
// in a folder that is a link, and, in a folder of its own, everything while
// that folder's marker does not name its job.
fn spare_held(deletion: &mut Deletion, held: &LiveHeld, job_id: JobId) {
    use crate::jobs::working_dir::{OutputDirOwner, output_dir_owner};
    let dir = deletion.dir.clone();
    let linked = std::fs::symlink_metadata(&dir).is_ok_and(|found| found.file_type().is_symlink());
    let unowned = deletion.own && output_dir_owner(&dir) != Some(OutputDirOwner::Job(job_id));
    if linked || unowned {
        deletion.paths.clear();
        deletion.made.clear();
        deletion.own = false;
        return;
    }
    let mut links = Links::under(&dir);
    let mut spared = |path: &Path| held.holds(path, false) || links.above(path);
    deletion.paths.retain(|path| !spared(path));
    deletion.made.retain(|path| !spared(path));
}

// The one check that a removal under a torrent's folder does not leave it
// through a link (`SECURITY.md`, "Deletes are narrow"): whether a folder
// between `dir` and a path is a link. Each folder is read once, never
// followed. `dir` itself is not judged here.
pub struct Links<'a> {
    dir: &'a Path,
    seen: HashMap<PathBuf, bool>,
}

impl<'a> Links<'a> {
    pub fn under(dir: &'a Path) -> Self {
        Self {
            dir,
            seen: HashMap::new(),
        }
    }

    // Whether a folder above `path`, inside the folder, is a link.
    pub fn above(&mut self, path: &Path) -> bool {
        let dir = self.dir;
        let seen = &mut self.seen;
        path.ancestors()
            .skip(1)
            .take_while(|folder| *folder != dir && folder.starts_with(dir))
            .any(|folder| {
                if let Some(linked) = seen.get(folder) {
                    return *linked;
                }
                let linked = std::fs::symlink_metadata(folder)
                    .is_ok_and(|found| found.file_type().is_symlink());
                seen.insert(folder.to_path_buf(), linked);
                linked
            })
    }
}

// Whether `path` holds a regular file, never following a link.
fn is_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|found| found.file_type().is_file())
}

// `path` as the filesystem spells it, so two spellings of one place (through
// a link, or in another case where the filesystem ignores case) are one:
// the whole of it resolved when it is there and not a link; a link at its
// end kept, not followed, under its folder resolved; and a part that is not
// there yet as given, under the deepest folder of it that is.
pub fn held_key(path: &Path) -> PathBuf {
    if std::fs::symlink_metadata(path).is_ok_and(|found| !found.file_type().is_symlink())
        && let Ok(found) = std::fs::canonicalize(path)
    {
        return found;
    }
    under_resolved(path)
}

// `path` resolved, links and all, as far as it is there, and the rest as
// given: a folder's own spelling, a link to it included.
pub fn resolved(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| under_resolved(path))
}

// `path`'s folder resolved, links and all, as far as it is there, and the
// rest as given.
fn under_resolved(path: &Path) -> PathBuf {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => resolved(parent).join(name),
        _ => path.to_path_buf(),
    }
}

// The first part of each of `names`, paths relative to a torrent's folder;
// None when one names the folder itself.
pub fn held_tops<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<BTreeSet<String>> {
    names
        .into_iter()
        .map(|name| {
            Path::new(name).components().find_map(|part| match part {
                std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                _ => None,
            })
        })
        .collect()
}

// What a torrent holds where its files are (`torrent_held_paths`), through
// any link to its folders: under the folder its files are in, the first
// part of each of its files' and outputs' paths (`tops`), or that whole
// folder while they are not known, and the folder claimed for it when its
// files lie elsewhere. A part there now is held as the filesystem spells
// it ([`held_key`]: its case and form on disk, which an adopted folder may
// spell otherwise than the metainfo); one not there yet is held as the
// metainfo writes it, the name the torrent creates it by. Reads the
// filesystem: run off the runtime.
pub fn held_of(
    storage_dir: Option<&str>,
    files_dir: Option<&str>,
    tops: Option<&BTreeSet<String>>,
) -> Vec<String> {
    let Some(base) = files_dir.or(storage_dir) else {
        return Vec::new();
    };
    // The folders resolved whole: a save path given through a link holds
    // what lies where it leads.
    let base = resolved(Path::new(base));
    let mut held: Vec<PathBuf> = match tops {
        Some(tops) => tops.iter().map(|top| held_key(&base.join(top))).collect(),
        None => vec![base],
    };
    if let (Some(storage), Some(files)) = (storage_dir, files_dir)
        && storage != files
    {
        held.push(resolved(Path::new(storage)));
    }
    held.into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

// The paths live torrents hold under the folders history removes from, as
// [`held_key`] spells them, with every folder above each, so a listed
// folder that goes whole is known to hold one.
#[derive(Debug, Default)]
pub struct LiveHeld {
    pub paths: HashSet<PathBuf>,
    pub above: HashSet<PathBuf>,
}

impl LiveHeld {
    // What the live torrents hold under `dirs`, in one read for every
    // folder of the call (`live_held_paths`), never a torrent's metainfo.
    // Each folder is asked for as the filesystem spells it and as given:
    // the rows an upgrade seeds hold a torrent's folder as its row typed it,
    // until its first start writes them again as the filesystem spells
    // them. Reads the filesystem: run off the runtime.
    pub fn under(db: &Database, dirs: &[PathBuf]) -> Result<Self, StateError> {
        let mut keys: Vec<PathBuf> = dirs.iter().map(|dir| held_key(dir)).collect();
        keys.extend(dirs.iter().cloned());
        keys.sort();
        keys.dedup();
        let mut held = Self::default();
        for path in db.live_held_paths(&keys)? {
            held.above
                .extend(path.ancestors().skip(1).map(Path::to_path_buf));
            held.paths.insert(path);
        }
        Ok(held)
    }

    // Whether a live torrent holds `path`, is in it, or, for one that goes
    // `whole`, holds anything in it, by the path as the filesystem spells it
    // or as given (`under`).
    pub fn holds(&self, path: &Path, whole: bool) -> bool {
        if self.paths.is_empty() {
            return false;
        }
        let key = held_key(path);
        [key.as_path(), path].into_iter().any(|path| {
            path.ancestors().any(|at| self.paths.contains(at))
                || (whole && self.above.contains(path))
        })
    }
}

// What an archive from history, or a delete with files, did with what a
// torrent's job listed: how many paths were there and went, and the folders
// it left because nothing proves Weaver may remove from them.
#[derive(Debug, Default)]
pub struct ListedRemoval {
    pub removed: usize,
    pub not_managed: Vec<PathBuf>,
}

// An archive from history, or a delete with files, of a torrent's job whose
// torrent is gone: the files its row lists go, under `dir`, the folder its
// row records, and only while that folder's marker names the job, read now
// (`output_dir_owner`), the proof an NZB job's archive takes of its output
// folder. A folder with no marker, or another job's, has nothing removed
// and is not managed. Under a proven folder a listed path stays when a live
// torrent holds it (`held`), when a folder between `dir` and it is a link,
// or when it climbs out; the folders each removal empties go, deepest
// first, never `dir`'s own parents, and `dir` goes once its marker is all
// it holds. A listed path outside `dir` is the torrent's in a shared
// folder, and stays, not managed; one under `intermediate`, its unpacking
// folder, goes by its marker as an NZB job's working folder does.
pub fn remove_history_files(
    job_id: JobId,
    intermediate: &Path,
    dir: &Path,
    files: &[HistoryFile],
    held: &LiveHeld,
) -> ListedRemoval {
    let mut removal = ListedRemoval::default();
    let mut outside = BTreeSet::new();
    let mut inside = Vec::new();
    for file in files {
        let path = PathBuf::from(&file.path);
        let plain = path.is_absolute()
            && path.components().all(|part| {
                !matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            });
        if plain && path.starts_with(intermediate) {
            continue;
        }
        if !plain || path == dir || !path.starts_with(dir) {
            // Said by the folder it is in, read without climbing: a
            // relative path names no folder, and is left unsaid.
            if path.is_absolute() && std::fs::symlink_metadata(&path).is_ok() {
                let mut at = PathBuf::new();
                for part in path.components() {
                    match part {
                        std::path::Component::ParentDir => {
                            at.pop();
                        }
                        std::path::Component::CurDir => {}
                        part => at.push(part),
                    }
                }
                outside.insert(at.parent().map_or_else(|| at.clone(), Path::to_path_buf));
            }
            continue;
        }
        inside.push((path, file.whole));
    }
    removal.not_managed.extend(outside);
    match std::fs::symlink_metadata(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return removal,
        // A recorded folder that is a link is never followed: what it
        // points at was not proven the job's where the row recorded it.
        Ok(found) if found.file_type().is_symlink() => {
            tracing::warn!(job_id = job_id.0, dir = %dir.display(), "a torrent's recorded folder is a link; history leaves its files");
            removal.not_managed.push(dir.to_path_buf());
            return removal;
        }
        _ => {}
    }
    if crate::jobs::working_dir::output_dir_owner(dir)
        != Some(crate::jobs::working_dir::OutputDirOwner::Job(job_id))
    {
        tracing::warn!(job_id = job_id.0, dir = %dir.display(), "a torrent's folder does not carry its job's marker; history leaves its files");
        removal.not_managed.push(dir.to_path_buf());
        return removal;
    }
    // A link between the folder and a file would take the removal out of
    // it.
    let mut links = Links::under(dir);
    let mut folders = BTreeSet::new();
    // The folders its listed files are in, each swept once of the hidden
    // copies its job's moves left there (`JobMoves`), as a live removal
    // sweeps them (`delete_written`).
    let mut swept: HashSet<PathBuf> = HashSet::new();
    for (path, whole) in inside {
        if held.holds(&path, whole) {
            continue;
        }
        if links.above(&path) {
            tracing::warn!(file = %path.display(), "a torrent's listed file is under a link, and stays");
            continue;
        }
        if let Some(folder) = path.parent()
            && !swept.contains(folder)
        {
            swept.insert(folder.to_path_buf());
            weaver_server_disk::runtime::fs::sweep_move_copies(job_id.0, folder);
        }
        let found = match std::fs::symlink_metadata(&path) {
            Ok(found) => found,
            Err(_) => continue,
        };
        let removed = if found.is_dir() && whole {
            std::fs::remove_dir_all(&path)
        } else if found.is_dir() {
            tracing::warn!(file = %path.display(), "a folder is where a torrent's file was, and stays");
            continue;
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Ok(()) => removal.removed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), error = %error, "could not remove a torrent's listed file");
            }
        }
        for folder in path.ancestors().skip(1).take_while(|at| *at != dir) {
            // The folders above one already taken were taken with it.
            if !folders.insert((folder.components().count(), folder.to_path_buf())) {
                break;
            }
        }
    }
    // Deepest first, so a folder emptied by the one below it goes too; one
    // that is not empty stays, as `remove_dir` leaves it.
    for (_, folder) in folders.iter().rev() {
        let _ = std::fs::remove_dir(folder);
    }
    match crate::jobs::working_dir::remove_emptied_output_dir(dir, job_id) {
        Ok(true) => removal.removed += 1,
        Ok(false) => {}
        Err(error) => {
            tracing::debug!(dir = %dir.display(), error = %error, "a torrent's folder stays");
        }
    }
    removal
}

// libtorrent `storage_utils::delete_files`: the torrent's files go, here
// with what its job made of them, and then each folder above them, up to
// its directory, that this leaves empty and that was not there before the
// torrent. Whatever else is in its directory stays, and so does a folder
// found where one of the torrent's files was (`delete_one_file` removes a
// file, never a folder's contents). What its job made is an unpacked file:
// it goes only while its name holds a regular file, so a folder or a link
// someone put there stays, whole. Nothing goes through a link inside the
// directory (`Links`), on a live removal as at a start.
pub fn delete_written(deletion: &Deletion) {
    if let Some((root, work, job_id)) = &deletion.work {
        match crate::jobs::working_dir::remove_job_working_dir(root, work, *job_id) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(dir = %work.display(), error = %error, "a torrent's unpacking directory is not its own, or could not go; it stays");
            }
        }
    }
    let mut folders = BTreeSet::new();
    let mut links = Links::under(&deletion.dir);
    // The folders its unpacked files were moved into, each swept once of
    // the hidden copies its job's moves left there (`JobMoves`).
    let mut swept: HashSet<&Path> = HashSet::new();
    let listed = deletion.paths.iter().map(|path| (path, false));
    let made = deletion.made.iter().map(|path| (path, true));
    for (path, made) in listed.chain(made) {
        if links.above(path) {
            tracing::warn!(file = %path.display(), "a torrent's file is under a link, and stays");
            continue;
        }
        if let (true, Some(owner), Some(folder)) = (made, deletion.owner, path.parent())
            && swept.insert(folder)
        {
            weaver_server_disk::runtime::fs::sweep_move_copies(owner.0, folder);
        }
        let removed = match std::fs::symlink_metadata(path) {
            Ok(found) if found.is_dir() => {
                tracing::warn!(file = %path.display(), "a folder is where a torrent's file was, and stays");
                Ok(())
            }
            Ok(found) if made && !found.file_type().is_file() => {
                tracing::warn!(file = %path.display(), "an unpacked file's name does not hold a file, and is left");
                Ok(())
            }
            Ok(_) => std::fs::remove_file(path),
            Err(error) => Err(error),
        };
        match removed {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), error = %error, "could not delete a torrent's file");
            }
        }
        for folder in path.ancestors().skip(1) {
            if !folder.starts_with(&deletion.dir) {
                break;
            }
            if folder == deletion.dir && !deletion.own {
                break;
            }
            // The folders above one already taken were taken with it.
            if !folders.insert((folder.components().count(), folder.to_path_buf())) {
                break;
            }
        }
    }
    folders.extend(
        deletion
            .claimed
            .iter()
            .chain(deletion.own.then_some(&deletion.dir))
            .map(|folder| (folder.components().count(), folder.clone())),
    );
    // Deepest first, so a folder emptied by the one below it goes too; one
    // that is not empty stays, as `remove_dir` leaves it.
    // The torrent's own folder and the one claimed for it carry its job's
    // marker, which goes with them once it is all they hold.
    for (_, folder) in folders.iter().rev() {
        if deletion.kept_dirs.contains(folder) {
            continue;
        }
        let marked =
            (deletion.own && *folder == deletion.dir) || deletion.claimed.as_ref() == Some(folder);
        match deletion.owner.filter(|_| marked) {
            Some(owner) => {
                let _ = crate::jobs::working_dir::remove_emptied_output_dir(folder, owner);
            }
            None => {
                let _ = std::fs::remove_dir(folder);
            }
        }
    }
}

// The directory a torrent's unpacking works in, under `work`, marked the
// torrent's job's as an NZB job's working directory is: `torrent-<id>`, or
// the job's own suffixed name when another job's marker holds that one.
// Found by its marker first, so the name a job's folder took is the one it
// keeps (`own_named_dir`).
pub fn unpacking_dir(work: &Path, job_id: JobId) -> PathBuf {
    crate::jobs::working_dir::own_named_dir(work, &format!("torrent-{}", job_id.0), job_id)
}
