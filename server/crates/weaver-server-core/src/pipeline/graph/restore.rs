// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The restart seam of a graph job: its checkpoint read back, the job rebuilt
// from it, and the articles it has in hand given to the restore as the ones
// the queue does not fetch, with the parts that arrived and wait to be
// placed. A job with no checkpoint is admitted afresh.

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
    #[error("graph job checkpoint is not one this build restores: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("graph job could not be rebuilt from its checkpoint: {0}")]
    Rebuild(#[from] RestoreFailure),
    #[error("graph job restore stopped: {0}")]
    Join(String),
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

    // Read the job's checkpoint back and rebuild the job from it. Blocks:
    // called off the pipeline thread.
    pub(crate) fn restore(
        self,
        job_id: JobId,
        files: Vec<RootFile>,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let rows = self
            .db
            .load_graph_checkpoint(job_id)?
            .ok_or(GraphRestoreError::NoCheckpoint(job_id.0))?;
        let snapshot = checkpoint::snapshot(&rows)?;
        let held = held_parts_from_rows(job_id, &self.working_dir, &rows.held_parts);
        let storage = Storage {
            passwords: restore_passwords(&self.db, job_id, self.spec_password.as_deref()),
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

// The candidates in the order the job's admission tried them, so the index
// each node's checkpoint kept names the same candidate. Read from the spec
// and the database, as the job's state is not in place yet.
fn restore_passwords(
    db: &crate::Database,
    job_id: JobId,
    spec_password: Option<&str>,
) -> Vec<String> {
    let (carried, _) = read_job_password_candidates(db, job_id);
    let global = db.global_archive_password_candidates().unwrap_or_default();
    assemble_archive_password_candidates(spec_password, &carried, &global)
        .iter()
        .map(|candidate| candidate.value().to_owned())
        .collect()
}
