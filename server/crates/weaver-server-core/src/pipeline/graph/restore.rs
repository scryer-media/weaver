// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The restart seam of a graph job: its checkpoint read back, the job rebuilt
// from it, and the articles it has in hand given to the classic restore as
// the ones the queue does not fetch. A job stamped graph with no checkpoint
// is refused rather than restored wrong: nothing says what its files hold.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use super::checkpoint::{self, CheckpointError};
use super::job::restore::{RestoreFailure, Restored};
use super::job::{GraphJob, RootFile, Storage};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobSpec;
use crate::pipeline::{Pipeline, assemble_archive_password_candidates};

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphRestoreError {
    #[error("graph job {0} has no checkpoint to restore from")]
    NoCheckpoint(u64),
    #[error("graph job checkpoint could not be read: {0}")]
    Load(#[from] crate::StateError),
    #[error("graph job checkpoint is not one this build restores: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("graph job could not be rebuilt from its checkpoint: {0}")]
    Rebuild(#[from] RestoreFailure),
    #[error("graph job restore stopped: {0}")]
    Join(String),
}

pub(crate) struct GraphRestore {
    pub(crate) job: GraphJob,
    // The articles the queue does not fetch again.
    pub(crate) skip: HashSet<SegmentId>,
}

impl Pipeline {
    // Rebuild a graph job from its checkpoint before its queues are built.
    // The reads back happen off the pipeline thread.
    pub(crate) async fn restore_graph_job(
        &self,
        job_id: JobId,
        spec: &JobSpec,
        working_dir: &Path,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let rows = self
            .db_blocking(move |db| db.load_graph_checkpoint(job_id))
            .await?
            .ok_or(GraphRestoreError::NoCheckpoint(job_id.0))?;
        let snapshot = checkpoint::snapshot(&rows)?;
        let files: Vec<RootFile> = spec
            .files
            .iter()
            .map(|file| RootFile {
                name: file.filename.clone(),
                articles: file
                    .segments
                    .iter()
                    .map(|segment| (segment.ordinal, segment.bytes))
                    .collect(),
            })
            .collect();
        let storage = Storage {
            working_dir: working_dir.to_path_buf(),
            accountant: Arc::clone(&self.graph_holds),
            free_disk: self
                .storage_capacity
                .reader(crate::operations::StorageRoot::Intermediate),
            // An explicitly paused restore owns no NZB slot. Rebuild its
            // checkpoint, but leave engine runs queued until admission.
            slots: if self.nzb_admitted.contains(&job_id) {
                self.graph_slot_limit()
            } else {
                0
            },
            mail: self.graph_runs.sender(),
            passwords: self.restore_passwords(job_id, spec),
            caps: self.graph_caps(&files),
            nesting: super::limits::NESTING,
        };
        let limit = storage.slots;
        let Restored {
            job,
            settled,
            refetched,
        } = tokio::task::spawn_blocking(move || {
            GraphJob::restore(&snapshot, &files, job_id.0, storage, limit)
        })
        .await
        .map_err(|error| GraphRestoreError::Join(error.to_string()))??;
        // The next checkpoint writes only what it changes of these.
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
        Ok(GraphRestore { job, skip })
    }

    // The candidates in the order the job's admission tried them, so the
    // index each node's checkpoint kept names the same candidate. Read from
    // the spec and the database, as the job's state is not in place yet.
    fn restore_passwords(&self, job_id: JobId, spec: &JobSpec) -> Vec<String> {
        let (carried, _) = self.load_job_password_candidates(job_id);
        let global = self
            .db
            .global_archive_password_candidates()
            .unwrap_or_default();
        assemble_archive_password_candidates(spec.password.as_deref(), &carried, &global)
            .iter()
            .map(|candidate| candidate.value().to_owned())
            .collect()
    }
}
