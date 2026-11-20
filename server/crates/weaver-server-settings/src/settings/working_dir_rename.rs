// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job working directory's rename, journalled in the settings table so a
// process interruption between the journal and the move recovers either name.

use std::path::{Path, PathBuf};

use weaver_server_base::jobs::ids::JobId;
use weaver_server_jobs::jobs::database::JobsDatabase;
use weaver_server_jobs::jobs::error::SchedulerError;
use weaver_server_jobs::jobs::persistence::JobsPersistenceQueries;
use weaver_server_jobs::jobs::working_dir::{
    MarkerMatch, marker_job_id, match_marker, open_working_directory, owned_working_directory,
    read_working_marker, upgrade_legacy_marker,
};

use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;

pub fn rename_journal_key(id: JobId) -> String {
    format!("working_dir_rename:{}", id.0)
}

pub fn rename_job_working_dir<D: JobsDatabase>(
    db: &D,
    root: &Path,
    old: &Path,
    new: &Path,
    id: JobId,
) -> Result<PathBuf, SchedulerError> {
    // The handle on the source closes before the move: a directory with an
    // open handle cannot be renamed on Windows.
    {
        let dir = owned_working_directory(root, old)?;
        if marker_job_id(&read_working_marker(&dir)?) != Some(id) || new.parent() != Some(root) {
            return Err(
                std::io::Error::other("rename directory belongs to another job or root").into(),
            );
        }
    }
    let journal = serde_json::to_string(&(old, new))
        .map_err(|error| SchedulerError::Internal(error.to_string()))?;
    db.borrow().set_setting(&rename_journal_key(id), &journal)?;
    recover_working_dir_rename(db, root, old, id)
}

// The journal precedes the atomic move. Either name can survive a process
// interruption; the marker binds the same inode and job under both names.
pub fn recover_working_dir_rename<D: JobsDatabase>(
    db: &D,
    root: &Path,
    current: &Path,
    id: JobId,
) -> Result<PathBuf, SchedulerError> {
    let journal = db.borrow().get_setting(&rename_journal_key(id))?;
    recover_working_dir_rename_from(db, root, current, id, journal)
}

// As `recover_working_dir_rename`, with the job's rename journal already
// read: None when it has none.
pub fn recover_working_dir_rename_from<D: JobsDatabase>(
    db: &D,
    root: &Path,
    current: &Path,
    id: JobId,
    journal: Option<String>,
) -> Result<PathBuf, SchedulerError> {
    let key = rename_journal_key(id);
    let Some(journal) = journal else {
        return Ok(current.to_path_buf());
    };
    let (old, new): (PathBuf, PathBuf) = serde_json::from_str(&journal)
        .map_err(|error| SchedulerError::Internal(error.to_string()))?;
    if old.parent() != Some(root)
        || new.parent() != Some(root)
        || (current != old && current != new)
    {
        return Err(std::io::Error::other(
            "rename journal does not belong to this working directory",
        )
        .into());
    }
    if old != new && old.try_exists()? {
        // The handle on the source closes before the move, as above.
        {
            let dir = owned_working_directory(root, &old)?;
            if marker_job_id(&read_working_marker(&dir)?) != Some(id) {
                return Err(std::io::Error::other("rename source belongs to another job").into());
            }
        }
        if let Err(error) = weaver_server_disk::runtime::fs::rename_path_exclusive(&old, &new) {
            db.borrow().delete_setting(&key)?;
            return Err(error.into());
        }
    }
    let dir = open_working_directory(root, &new)?;
    let stored = read_working_marker(&dir)?;
    if marker_job_id(&stored) != Some(id)
        || (matches!(
            match_marker(&dir, &old, &stored, id)?,
            MarkerMatch::Mismatch
        ) && matches!(
            match_marker(&dir, &new, &stored, id)?,
            MarkerMatch::Mismatch
        ))
    {
        return Err(
            std::io::Error::other("rename destination has a foreign ownership marker").into(),
        );
    }
    // Once the move succeeds the runtime must use its new path, even if a
    // later persistence write fails. Keep the journal until every step commits.
    let finish = || -> Result<(), SchedulerError> {
        upgrade_legacy_marker(&dir, &new, id)?;
        db.update_active_job_output_dir(id, &new)?;
        db.borrow().delete_setting(&key)?;
        Ok(())
    };
    if let Err(error) = finish() {
        tracing::error!(job_id = id.0, %error, "working directory moved; retaining rename journal for recovery");
    }
    Ok(new)
}
