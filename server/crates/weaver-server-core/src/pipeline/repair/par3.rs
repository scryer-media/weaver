//! Authenticated PAR3 carrier discovery. All scanning runs on blocking workers;
//! the actor retains packet locations and incomplete metadata between arrivals.

use super::sources::PublishedSources;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::Pipeline;
use par3_rs::ingest::{IncrementalSet, PacketScanner, ScanEvent};
use par3_rs::runtime::{EngineError, EngineResult, ExecutionOptions, HandleBudget, MemoryBudget};
use par3_rs::source::{DiskSourceAccess, SourceAccess, SourceId, SourceSnapshot};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use weaver_model::files::FileRole;

const MAX_CARRIERS: usize = 4096;
const MAX_SETS: usize = 256;

fn execution_options() -> ExecutionOptions {
    static MEMORY: OnceLock<MemoryBudget> = OnceLock::new();
    static HANDLES: OnceLock<HandleBudget> = OnceLock::new();
    let mut options = ExecutionOptions::default();
    options.memory = MEMORY.get_or_init(|| MemoryBudget::new(256 << 20)).clone();
    options.handles = HANDLES.get_or_init(|| HandleBudget::new(128)).clone();
    options.open_handles = 128;
    options.workers = 1;
    options
}

struct Carrier {
    backing: SourceSnapshot,
    scanner: PacketScanner,
    revision: u64,
}

pub(in crate::pipeline) struct Par3Job {
    options: ExecutionOptions,
    sources: PublishedSources,
    carriers: BTreeMap<SourceId, Carrier>,
    sets: BTreeMap<par3_rs::InputSetId, IncrementalSet>,
}

impl Default for Par3Job {
    fn default() -> Self {
        Self {
            options: execution_options(),
            sources: PublishedSources::default(),
            carriers: BTreeMap::new(),
            sets: BTreeMap::new(),
        }
    }
}

impl Par3Job {
    /// Complete-carrier entrypoint. The same registry/scanner also accepts
    /// committed partial ranges through `publish_carrier`.
    fn scan_complete(&mut self, source: SourceId, path: PathBuf) -> EngineResult<()> {
        let mut disk = DiskSourceAccess::with_options(self.options.clone());
        disk.insert(source, path);
        let access: Arc<dyn SourceAccess> = Arc::new(disk);
        // Acquire immutable Windows sharing locks before snapshots so their
        // generation checks do not repeatedly hash the complete carrier.
        let access = access.pin(source, &self.options)?.unwrap_or(access);
        let snapshot = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        if self
            .carriers
            .get(&source)
            .is_some_and(|carrier| carrier.backing == snapshot)
        {
            return Ok(());
        }
        let ranges = if snapshot.len == 0 {
            Vec::new()
        } else {
            std::iter::once(0..snapshot.len).collect()
        };
        self.publish_carrier(source, access, snapshot.len, ranges, false)?;
        self.scan(source)
    }

    fn publish_carrier(
        &mut self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        len: u64,
        ranges: Vec<std::ops::Range<u64>>,
        arrival: bool,
    ) -> EngineResult<()> {
        if !self.carriers.contains_key(&source) && self.carriers.len() >= MAX_CARRIERS {
            return Err(EngineError::ResourceLimit("job carrier count"));
        }
        let backing = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        if arrival {
            self.sources.arrive(source, access, len, ranges)?;
        } else {
            self.sources.replace(source, access, len, ranges)?;
            self.carriers.remove(&source);
        }
        if let Some(carrier) = self.carriers.get_mut(&source) {
            carrier.backing = backing;
        } else {
            let scanner = PacketScanner::new(
                Arc::new(self.sources.clone()),
                source,
                self.options.clone(),
                par3_rs::ScanLimits::default(),
            )?;
            self.carriers.insert(
                source,
                Carrier {
                    backing,
                    scanner,
                    revision: 0,
                },
            );
        }
        Ok(())
    }

    fn scan(&mut self, source: SourceId) -> EngineResult<()> {
        let revision = self
            .sources
            .revision(source)?
            .ok_or(EngineError::InvalidState("unpublished carrier"))?;
        let carrier = self
            .carriers
            .get_mut(&source)
            .ok_or(EngineError::InvalidState("unknown carrier"))?;
        if revision == carrier.revision {
            return Ok(());
        }
        loop {
            match carrier.scanner.poll()? {
                ScanEvent::Packet(packet) => {
                    let id = packet.input_set_id();
                    if !self.sets.contains_key(&id) {
                        if self.sets.len() >= MAX_SETS {
                            return Err(EngineError::ResourceLimit("job PAR3 set count"));
                        }
                        self.sets
                            .insert(id, IncrementalSet::new(id, self.options.clone())?);
                    }
                    self.sets
                        .get_mut(&id)
                        .expect("inserted set")
                        .merge(packet)?;
                }
                ScanEvent::End | ScanEvent::NeedData { .. } => {
                    carrier.revision = revision;
                    return Ok(());
                }
            }
        }
    }
}

impl Pipeline {
    pub(in crate::pipeline) async fn try_load_par3_metadata(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return;
        };
        let signature = self
            .file_prefix_16k
            .get(&file_id)
            .is_some_and(|prefix| prefix.starts_with(par3_rs::MAGIC));
        if !(matches!(file.role(), FileRole::Par3 { .. }) || signature) || !file.is_complete() {
            return;
        }
        let path = state
            .working_dir
            .join(self.current_filename_for_file(job_id, file));
        let coordinator = self.par3_runtime.get_or_insert_with(Default::default);
        if let Err(error) = coordinator
            .enqueue(job_id, SourceId(u64::from(file_id.file_index)), path)
            .and_then(|()| coordinator.dispatch())
        {
            self.fail_job(job_id, format!("PAR3 discovery failed: {error}"));
        }
    }

    pub(in crate::pipeline) fn handle_par3_work_done(&mut self, done: work::WorkDone) {
        let Some(coordinator) = self.par3_runtime.as_mut() else {
            return;
        };
        let job_id = coordinator.settle(done);
        if let Err(error) = coordinator.dispatch() {
            tracing::error!(error = %error, "PAR3 worker dispatch failed");
        }
        if let Some(job_id) = job_id
            && self.jobs.contains_key(&job_id)
        {
            tracing::debug!(
                job_id = job_id.0,
                sets = coordinator.authenticated_set_count(job_id),
                "PAR3 carrier worker settled"
            );
            self.schedule_job_completion_check(job_id);
        }
    }
}

pub(in crate::pipeline) mod work;

#[cfg(test)]
mod tests;
