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

use super::engine::DecodePool;

use super::checkpoint::{self, CheckpointError};
use super::engine::Said;
use super::hardware::Hardware;
use super::job::restore::{RestoreFailure, Restored};
use super::job::{GraphError, GraphJob, RootFile, Storage};
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
use weaver_graph::names::{INTERNAL_DIR, key};

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
    #[error("graph job storage could not be inspected: {0}")]
    Storage(#[from] super::leaf::LeafError),
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
    // touches a file. So is a record the rebuild found contradicting itself:
    // told again, it would fault again on every start, so the job starts
    // afresh instead. A read that failed, or a rebuild that failed writing,
    // is not this.
    fn not_this_builds(&self) -> bool {
        matches!(
            self,
            Self::Rows(_)
                | Self::Checkpoint(_)
                | Self::Rebuild(
                    RestoreFailure::Tree(_)
                        | RestoreFailure::Root(_)
                        | RestoreFailure::Post(_)
                        | RestoreFailure::Step(GraphError::Inconsistent(_))
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
// How many restores were entered, for the tests that show one was not.
#[cfg(test)]
pub(crate) static RESTORES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(crate) struct RestoreSeat {
    db: crate::Database,
    working_dir: PathBuf,
    // The job's staging directory on the complete device, when its
    // delivered files lie there, and the free space of that device.
    output: Option<(PathBuf, CapacityReader)>,
    output_free_disk: CapacityReader,
    accountant: Arc<HoldsAccountant>,
    decode: Arc<DecodePool>,
    free_disk: CapacityReader,
    slots: usize,
    mail: mpsc::Sender<Said>,
    hardware: Hardware,
    metrics: Arc<PipelineMetrics>,
    extraction_limits: Arc<ExtractionLimits>,
    spec_password: Option<String>,
    data_dir: Option<(PathBuf, CapacityReader, super::window::ScratchLedger)>,
    window_ledger: super::window::ScratchLedger,
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
            output: None,
            output_free_disk: self
                .storage_capacity
                .reader(crate::operations::StorageRoot::Complete),
            accountant: Arc::clone(&self.graph_holds),
            decode: Arc::clone(&self.graph_decode),
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
            data_dir: Some(self.window_data_dir()),
            window_ledger: self.window_ledgers.intermediate.clone(),
        }
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
    // The seat for one job: its password, its working directory as its
    // install found it, and its staging directory when one was found: then
    // the files it delivers lie there.
    pub(crate) fn for_job(
        mut self,
        spec_password: Option<String>,
        working_dir: &Path,
        staging_dir: Option<&Path>,
    ) -> Self {
        self.spec_password = spec_password;
        self.working_dir = working_dir.to_path_buf();
        self.output = staging_dir.map(|dir| (dir.to_path_buf(), self.output_free_disk.clone()));
        self
    }

    // The seat for a job whose metadata asks for no memory limit: its runs
    // are granted a window over the ceiling, as the reprocess that set it
    // granted them.
    pub(crate) fn lifted(mut self, metadata: &[(String, String)]) -> Self {
        self.hardware = self
            .hardware
            .lifted(crate::history::attributes::no_memory_limit(metadata));
        self
    }

    pub(crate) fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    // Read the job's checkpoint back and rebuild the job from it. A
    // checkpoint this build does not restore is none: the job starts afresh
    // and fetches every article, and its first checkpoint replaces the old
    // rows whole. Blocks: called off the pipeline thread. A job whose work
    // crashed the daemon never reaches here: its install is given no seat.
    // A job that starts afresh finds its directories as its last run left
    // them, which may have stopped before its first checkpoint committed:
    // no row names anything there as the job's, so all of it goes first.
    pub(crate) fn restore(
        self,
        job_id: JobId,
        files: Vec<RootFile>,
    ) -> Result<GraphRestore, GraphRestoreError> {
        #[cfg(test)]
        RESTORES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A restore replays the job's own facts: a panic in it is the job's.
        let _job = crate::runtime::panic_trap::JobScope::enter(job_id);
        let roots: Vec<PathBuf> = std::iter::once(self.working_dir.clone())
            .chain(self.output.as_ref().map(|(dir, _)| dir.clone()))
            .collect();
        let loaded = self.db.load_graph_checkpoint(job_id);
        let restored = loaded
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
            });
        if matches!(restored, Err(GraphRestoreError::NoCheckpoint(_))) {
            for root in &roots {
                sweep_all(root);
            }
        }
        restored
    }

    fn rebuild(
        self,
        job_id: JobId,
        files: Vec<RootFile>,
        rows: crate::jobs::graph_checkpoint::GraphCheckpoint,
    ) -> Result<GraphRestore, GraphRestoreError> {
        let snapshot = checkpoint::snapshot(&rows)?;
        sweep_unnamed(&self.working_dir, &snapshot, &rows.held_parts)?;
        if let Some((output_dir, _)) = &self.output {
            sweep_unnamed(output_dir, &snapshot, &rows.held_parts)?;
        }
        let held = held_parts_from_rows(job_id, &self.working_dir, &rows.held_parts);
        let storage = Storage {
            passwords: restore_passwords(&self.db, job_id, self.spec_password.as_deref())?,
            caps: caps_for(&self.db, &self.extraction_limits, &files),
            working_dir: self.working_dir,
            output: self.output,
            accountant: self.accountant,
            decode: self.decode,
            free_disk: self.free_disk,
            slots: self.slots,
            mail: self.mail,
            nesting: super::limits::NESTING,
            hardware: self.hardware,
            metrics: Some(self.metrics),
            data_dir: self.data_dir,
            window_ledger: self.window_ledger,
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

// The checkpoint a job comes back to is the truth of what its working
// directory holds: what the checkpoint names there is the job's, and
// nothing else is. A file a step wrote whose checkpoint never committed,
// or one a committed checkpoint stopped naming before it was removed, goes
// before the job is rebuilt, so no file the job opens again meets bytes it
// never recorded and no file it never recorded is delivered. The checkpoint
// names each member's file at the one place the restore opens it, every
// envelope, every holds scratch and spill file and every spooled part. The
// directory's own marker and the run's cache are the directory's, never the
// job's, and stay. A file that cannot be removed stays; the job does not
// read it.
pub(super) fn sweep_unnamed(
    working_dir: &Path,
    snapshot: &checkpoint::Snapshot,
    held_parts: &[crate::jobs::graph_checkpoint::HeldPartRow],
) -> Result<(), super::leaf::LeafError> {
    let members = snapshot.tree.nodes.iter().flat_map(|node| {
        node.members.iter().filter_map(|record| {
            let state = snapshot.files.get(&(node.node, record.member))?;
            state.has_file.then(|| {
                super::namespace::restored_at(
                    working_dir,
                    &record.output_name,
                    state.on_disk.as_deref(),
                )
                .map(str::to_owned)
            })
        })
    });
    let envelopes = snapshot.kept.iter().map(|kept| {
        kept.file
            .clone()
            .unwrap_or_else(|| super::envelope::envelope_name(kept.node, kept.envelope))
    });
    let holds = snapshot.holds.iter().map(|hold| {
        if hold.on_file {
            super::namespace::spill_name(hold.node, weaver_graph::MemberId(hold.held.volume.0))
        } else {
            super::namespace::scratch_name(hold.node)
        }
    });
    let spooled = held_parts.iter().map(|part| part.spool_path.clone());
    // Each name as the path it takes on disk, the key the name ledger and
    // the writer compare names by: a directory "Folder/" and a file
    // "folder/b.bin" lie in one directory where the disk folds case.
    // Resolve every recorded move before deleting anything. An inspection
    // error must never make a still-owned source look like an orphan.
    let members = members.collect::<Result<Vec<_>, _>>()?;
    let named: HashSet<String> = members
        .into_iter()
        .chain(envelopes)
        .chain(holds)
        .chain(spooled)
        .map(|name| key(&name.replace('\\', "/")))
        .collect();
    // The directories a named file lies in, the chunk directory always
    // among them.
    let mut within: HashSet<String> = HashSet::from([key(INTERNAL_DIR)]);
    for name in &named {
        within.extend(name.match_indices('/').map(|(at, _)| name[..at].to_owned()));
    }
    sweep_within(working_dir, Path::new(""), &named, &within);
    Ok(())
}

// A job with no checkpoint to come back to has nothing in its directory:
// every file a step wrote goes, as `sweep_unnamed` takes what no
// checkpoint names. The directory's own marker and the run's cache stay.
fn sweep_all(dir: &Path) {
    let within = HashSet::from([key(INTERNAL_DIR)]);
    sweep_within(dir, Path::new(""), &HashSet::new(), &within);
}

fn sweep_within(dir: &Path, at: &Path, named: &HashSet<String>, within: &HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir.join(at)) else {
        return;
    };
    for entry in entries.flatten() {
        let name = at.join(entry.file_name());
        let path_key = key(&name.to_string_lossy().replace('\\', "/"));
        let kind = entry.file_type();
        let directory = kind.as_ref().is_ok_and(std::fs::FileType::is_dir);
        if at.as_os_str().is_empty() && the_directorys(&name) {
            continue;
        }
        // A named directory stays, but only what is named in it does.
        if directory && (within.contains(&path_key) || named.contains(&path_key)) {
            sweep_within(dir, &name, named, within);
            continue;
        }
        if named.contains(&path_key) {
            continue;
        }
        let path = entry.path();
        let removed = if directory {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
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

// What the working directory holds for itself: its marker, the marker's
// replacement while one is written, and the run's cache.
fn the_directorys(name: &Path) -> bool {
    use crate::jobs::working_dir::{WORKING_DIR_MARKER, WORKING_DIR_MARKER_UPGRADE};
    [
        WORKING_DIR_MARKER,
        WORKING_DIR_MARKER_UPGRADE,
        weaver_graph::names::CACHE_DIR,
    ]
    .iter()
    .any(|own| name == Path::new(own))
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
) -> Result<Vec<crate::jobs::ArchivePasswordCandidate>, LoadError> {
    let (carried, _) = read_job_password_candidates(db, job_id)?;
    let global = db.global_archive_password_candidates()?;
    Ok(assemble_archive_password_candidates(
        spec_password,
        &carried,
        &global,
    ))
}

#[cfg(test)]
mod tests {
    use weaver_graph::VolumeIndex;

    use super::{GraphError, GraphRestoreError, RestoreFailure};
    use crate::pipeline::graph::root::source::Inconsistent;

    // A record the rebuild finds contradicting itself would fault again on
    // every start: the job starts afresh, as from a checkpoint this build
    // does not restore. A rebuild that failed writing is still held.
    #[test]
    fn a_rebuild_that_finds_its_record_inconsistent_starts_afresh() {
        let inconsistent = GraphError::Inconsistent(Inconsistent {
            volume: VolumeIndex(0),
            start: 27,
            end: 28,
        });
        assert!(GraphRestoreError::Rebuild(RestoreFailure::Step(inconsistent)).not_this_builds());
        let write = GraphError::Write(
            crate::pipeline::graph::namespace::WriteError::UnknownMember {
                node: weaver_graph::NodeId::ROOT,
                member: weaver_graph::MemberId(9),
            },
        );
        assert!(!GraphRestoreError::Rebuild(RestoreFailure::Step(write)).not_this_builds());
    }
}
