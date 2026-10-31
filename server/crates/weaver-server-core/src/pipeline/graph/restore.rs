// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The restart seam of a graph job: its checkpoint read back, the job rebuilt
// from it, and the articles it has in hand given to the restore as the ones
// the queue does not fetch, with the parts that arrived and wait to be
// placed. A job with no checkpoint is admitted afresh, and so is a job whose
// checkpoint this build does not restore: to this build it has none.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc;
use weaver_graph::holds::HoldsAccountant;

use super::checkpoint::{self, CheckpointError};
use super::engine::Said;
use super::hardware::Hardware;
use super::job::restore::{RestoreFailure, Restored};
use super::job::{GraphJob, RootFile, Storage};
use super::limits::caps_for;
use crate::LoadError;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobSpec;
use crate::operations::disk::CapacityReader;
use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::extraction::ExtractionLimits;
use crate::pipeline::held::{HeldPart, held_parts_from_rows};
use crate::pipeline::{
    Pipeline, assemble_archive_password_candidates, read_job_password_candidates,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphRestoreError {
    #[error("graph job {0} has no checkpoint to restore from")]
    NoCheckpoint(u64),
    #[error("graph job checkpoint could not be read: {0}")]
    Load(#[from] crate::StateError),
    #[error("graph job checkpoint holds a value this build does not read: {0}")]
    Rows(crate::StateError),
    #[error("graph job checkpoint is not one this build restores: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("graph job could not be rebuilt from its checkpoint: {0}")]
    Rebuild(#[from] RestoreFailure),
    #[error("graph job restore stopped: {0}")]
    Join(String),
}

impl From<LoadError> for GraphRestoreError {
    fn from(error: LoadError) -> Self {
        match error {
            LoadError::Read(error) => Self::Load(error),
            LoadError::Rows(error) => Self::Rows(error),
        }
    }
}

impl GraphRestoreError {
    // The database returned the checkpoint, and this build cannot make a
    // job of it: a value of a type or shape it does not take, a name it
    // does not have, facts at a version it does not read, or records it
    // cannot make the tree or the root of. Each is found before the rebuild
    // touches a file. A read that failed, or a rebuild that failed writing,
    // is not this.
    fn not_this_builds(&self) -> bool {
        matches!(
            self,
            Self::Rows(_)
                | Self::Checkpoint(_)
                | Self::Rebuild(
                    RestoreFailure::Tree(_) | RestoreFailure::Root(_) | RestoreFailure::Post(_)
                )
        )
    }
}

pub(crate) struct GraphRestore {
    pub(crate) feed_verdict: Option<crate::jobs::feed_verdict::FeedVerdict>,
    pub(crate) job: GraphJob,
    // The articles the queue does not fetch again.
    pub(crate) skip: HashSet<SegmentId>,
    // The parts that arrived and wait to be placed, spooled where the
    // checkpoint names them; the queue does not fetch them again either.
    pub(crate) held: Vec<HeldPart>,
}

// What a graph job's restore takes from the pipeline, gathered on the
// pipeline thread so the restore itself, every read back included, runs off
// it.
pub(crate) struct RestoreSeat {
    db: crate::Database,
    working_dir: PathBuf,
    accountant: Arc<HoldsAccountant>,
    free_disk: CapacityReader,
    slots: usize,
    mail: mpsc::Sender<Said>,
    hardware: Hardware,
    metrics: Arc<PipelineMetrics>,
    extraction_limits: Arc<ExtractionLimits>,
    spec_password: Option<String>,
}

impl Pipeline {
    pub(crate) fn graph_restore_seat(
        &self,
        spec_password: Option<String>,
        working_dir: &Path,
    ) -> RestoreSeat {
        RestoreSeat {
            db: self.db.clone(),
            working_dir: working_dir.to_path_buf(),
            accountant: Arc::clone(&self.graph_holds),
            free_disk: self
                .storage_capacity
                .reader(crate::operations::StorageRoot::Intermediate),
            // The restore starts no run: it runs off the pipeline thread,
            // before the job is in place to hear what a run says. The runs
            // start as the restored job is installed.
            slots: 0,
            mail: self.graph_runs.sender(),
            hardware: self.graph_hardware.clone(),
            metrics: Arc::clone(&self.metrics),
            extraction_limits: Arc::clone(&self.extraction_limits),
            spec_password,
        }
    }

    // Rebuild a graph job from its checkpoint before its queues are built.
    // The reads back happen off the pipeline thread.
    pub(crate) async fn restore_graph_job(
        &self,
        job_id: JobId,
        spec: &JobSpec,
        working_dir: &Path,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let seat = self.graph_restore_seat(spec.password.clone(), working_dir);
        let files = root_files(spec);
        tokio::task::spawn_blocking(move || seat.restore(job_id, files))
            .await
            .map_err(|error| GraphRestoreError::Join(error.to_string()))?
    }
}

// The root's files as the job's spec lists them, article by article.
pub(crate) fn root_files(spec: &JobSpec) -> Vec<RootFile> {
    spec.files
        .iter()
        .map(|file| RootFile {
            name: file.filename.clone(),
            articles: file
                .segments
                .iter()
                .map(|segment| (segment.ordinal, segment.bytes))
                .collect(),
        })
        .collect()
}

impl RestoreSeat {
    pub(crate) fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    // Read the job's checkpoint back and rebuild the job from it. A
    // checkpoint this build does not restore is none: the job starts afresh
    // and fetches every article, and its first checkpoint replaces the old
    // rows whole. Blocks: called off the pipeline thread.
    pub(crate) fn restore(
        self,
        job_id: JobId,
        files: Vec<RootFile>,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let loaded = self.db.load_graph_checkpoint(job_id);
        loaded
            .map_err(GraphRestoreError::from)
            .and_then(|rows| rows.ok_or(GraphRestoreError::NoCheckpoint(job_id.0)))
            .and_then(|rows| self.rebuild(job_id, files, rows))
            .map_err(|error| {
                if !error.not_this_builds() {
                    return error;
                }
                tracing::warn!(
                    job_id = job_id.0,
                    %error,
                    "graph job checkpoint is not one this build restores; the job starts afresh"
                );
                GraphRestoreError::NoCheckpoint(job_id.0)
            })
    }

    fn rebuild(
        self,
        job_id: JobId,
        files: Vec<RootFile>,
        rows: crate::jobs::graph_checkpoint::GraphCheckpoint,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let snapshot = checkpoint::snapshot(&rows)?;
        sweep_unnamed(&self.working_dir, &snapshot, &rows.held_parts);
        let held = held_parts_from_rows(job_id, &self.working_dir, &rows.held_parts);
        let storage = Storage {
            passwords: restore_passwords(&self.db, job_id, self.spec_password.as_deref())?,
            caps: caps_for(&self.db, &self.extraction_limits, &files),
            working_dir: self.working_dir,
            accountant: self.accountant,
            free_disk: self.free_disk,
            slots: self.slots,
            mail: self.mail,
            nesting: super::limits::NESTING,
            hardware: self.hardware,
            metrics: Some(self.metrics),
        };
        let limit = storage.slots;
        let Restored {
            job,
            settled,
            refetched,
        } = GraphJob::restore(&snapshot, &files, job_id.0, storage, limit)?;
        // The next checkpoint writes only what it changes of these.
        let feed_verdict = rows.feed_verdict.clone();
        job.pending().written(rows);
        if !refetched.is_empty() {
            tracing::info!(
                job_id = job_id.0,
                articles = refetched.len(),
                "graph restore fetches again the articles not on disk"
            );
        }
        let skip = settled
            .into_iter()
            .map(|(volume, ordinal)| SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: volume.0,
                },
                segment_number: ordinal,
            })
            .collect();
        Ok(GraphRestore {
            job,
            skip,
            held,
            feed_verdict,
        })
    }
}

// The checkpoint a job comes back to is the truth of what it holds under
// its chunk directory: what the checkpoint names there is the job's, and
// nothing else is. A file a step wrote whose checkpoint never committed,
// or one a committed checkpoint stopped naming before it was removed, goes
// before the job is rebuilt, so no file the job opens again meets bytes
// it never recorded. A file that cannot be removed stays; the job does not
// read it.
pub(super) fn sweep_unnamed(
    working_dir: &Path,
    snapshot: &checkpoint::Snapshot,
    held_parts: &[crate::jobs::graph_checkpoint::HeldPartRow],
) {
    let internal = Path::new(weaver_graph::names::INTERNAL_DIR);
    let envelopes = snapshot.kept.iter().map(|kept| {
        kept.file
            .clone()
            .unwrap_or_else(|| super::envelope::envelope_name(kept.node, kept.envelope))
    });
    let waiting = snapshot
        .files
        .values()
        .filter_map(|file| file.on_disk.clone());
    let holds = snapshot
        .holds
        .iter()
        .map(|hold| super::namespace::scratch_name(hold.node));
    let spooled = held_parts.iter().map(|part| part.spool_path.clone());
    // Each name is a file, or a directory with all it holds, directly in
    // the chunk directory.
    let named: HashSet<std::ffi::OsString> = envelopes
        .chain(waiting)
        .chain(holds)
        .chain(spooled)
        .filter_map(|name| {
            let within = Path::new(&name).strip_prefix(internal).ok()?;
            within.iter().next().map(std::ffi::OsStr::to_os_string)
        })
        .collect();
    let Ok(entries) = std::fs::read_dir(working_dir.join(internal)) else {
        return;
    };
    for entry in entries.flatten() {
        if named.contains(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        let removed = match entry.file_type() {
            Ok(kind) if kind.is_dir() => std::fs::remove_dir_all(&path),
            _ => std::fs::remove_file(&path),
        };
        if let Err(error) = removed {
            tracing::warn!(
                path = %path.display(),
                %error,
                "a file the restored checkpoint does not name could not be removed"
            );
        }
    }
}

// The candidates in the order the job's admission tried them, so the index
// each node's checkpoint kept names the same candidate. Read from the spec
// and the database, as the job's state is not in place yet. The loaders say
// which failure is which, as the checkpoint's does: one that cannot read
// holds the job, as a list short of what admission tried would name the
// wrong candidate; one that reads what this build cannot take starts it
// afresh.
fn restore_passwords(
    db: &crate::Database,
    job_id: JobId,
    spec_password: Option<&str>,
) -> Result<Vec<String>, LoadError> {
    let (carried, _) = read_job_password_candidates(db, job_id)?;
    let global = db.global_archive_password_candidates()?;
    Ok(
        assemble_archive_password_candidates(spec_password, &carried, &global)
            .iter()
            .map(|candidate| candidate.value().to_owned())
            .collect(),
    )
}
