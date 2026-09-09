//! Verified installation images and bounded direct-store readback work.

use super::*;
use crate::pipeline::direct_store::repair::{RepairedSpan, read_repaired_range};

pub(super) const STRIPE_BYTES: u64 = 256 * 1024;
pub(super) const STRIPE_RESERVATION: usize = 2 * STRIPE_BYTES as usize + 4096;

pub(super) struct VerifiedOutput {
    pub path: PathBuf,
    pub len: u64,
    access: Arc<dyn SourceAccess>,
    snapshot: SourceSnapshot,
}

impl VerifiedOutput {
    pub fn capture(path: PathBuf, len: u64, options: &ExecutionOptions) -> EngineResult<Self> {
        let access = disk_source(SourceId(0), path.clone(), options)?;
        let snapshot = access
            .snapshot(SourceId(0))?
            .ok_or(EngineError::Unavailable {
                source_id: SourceId(0),
                offset: 0,
            })?;
        if snapshot.len != len {
            return Err(EngineError::SourceChanged(SourceId(0)));
        }
        Ok(Self {
            path,
            len,
            access,
            snapshot,
        })
    }

    fn check(&self) -> EngineResult<()> {
        if self.access.snapshot(SourceId(0))? != Some(self.snapshot) {
            return Err(EngineError::SourceChanged(SourceId(0)));
        }
        Ok(())
    }

    pub fn stripe(
        &self,
        volume: u32,
        offset: u64,
        options: &ExecutionOptions,
    ) -> EngineResult<RepairedSpan> {
        options.cancel.check()?;
        self.check()?;
        if offset >= self.len {
            return Err(EngineError::InvalidState(
                "PAR3 readback cursor exceeds output",
            ));
        }
        let end = offset.saturating_add(STRIPE_BYTES).min(self.len);
        // The helper opens at most one handle at a time. Release the lease
        // before the final snapshot, which may need its own handle on Windows.
        let span = {
            let _handle = options.handles.acquire()?;
            read_repaired_range(&self.path, volume, self.len, offset..end, false)?
                .ok_or(EngineError::InvalidState("empty PAR3 readback stripe"))?
        };
        options.cancel.check()?;
        self.check()?;
        Ok(span)
    }
}

pub(super) struct Target {
    pub file: NzbFileId,
    pub set: usize,
    pub volume: u32,
    pub output: usize,
}

pub(super) struct Installation {
    pub completion: work::RepairCompletion,
    pub targets: Vec<Target>,
    pub current: usize,
    pub offset: u64,
    pub crc32: u32,
}

impl Installation {
    pub fn read(&self, options: &ExecutionOptions) -> EngineResult<RepairedSpan> {
        let target = &self.targets[self.current];
        self.completion
            .outputs
            .as_ref()
            .map_err(|_| EngineError::InvalidState("PAR3 installed output was not captured"))?
            [target.output]
            .stripe(target.volume, self.offset, options)
    }
}

pub(super) struct ReadbackDone {
    pub installation: Box<Installation>,
    pub result: EngineResult<RepairedSpan>,
    pub _reservation: assessment::ViewReservation,
}

impl Pipeline {
    pub(super) async fn finish_par3_repair(
        &mut self,
        job_id: JobId,
        completion: work::RepairCompletion,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let outputs = match &completion.outputs {
            Ok(outputs) => outputs,
            Err(error) => {
                self.fail_job(job_id, format!("PAR3 output capture failed: {error}"));
                return;
            }
        };
        let mut targets = Vec::new();
        for (output, image) in outputs.iter().enumerate() {
            let Some(file) = state.assembly.files().find(|file| {
                state
                    .working_dir
                    .join(self.current_filename_for_file(job_id, file))
                    == image.path
            }) else {
                continue;
            };
            if let Some((index, set)) =
                self.direct_store
                    .sets_for(job_id)
                    .iter()
                    .enumerate()
                    .find(|(_, set)| {
                        !set.is_demoted()
                            && !set.is_finalized()
                            && set
                                .plan()
                                .volume_for_file(file.file_id().file_index)
                                .is_some()
                    })
            {
                targets.push(Target {
                    file: file.file_id(),
                    set: index,
                    volume: set
                        .plan()
                        .volume_for_file(file.file_id().file_index)
                        .expect("matched volume"),
                    output,
                });
            }
        }
        if targets.is_empty() {
            self.complete_par3_repair(job_id, completion).await;
            return;
        }
        targets.sort_by_key(|target| (target.set, target.volume));
        let installation = Box::new(Installation {
            completion,
            targets,
            current: 0,
            offset: 0,
            crc32: 0,
        });
        if let Err(error) = self
            .par3_runtime
            .as_mut()
            .expect("admitted job")
            .queue_readback(job_id, installation)
        {
            self.fail_job(job_id, format!("PAR3 readback dispatch failed: {error}"));
        }
    }

    pub(super) async fn apply_par3_readback(
        &mut self,
        job_id: JobId,
        done: EngineResult<ReadbackDone>,
    ) {
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        if let Err(error) = self.place_par3_readback(job_id, done).await {
            // Verified native images remain available. Reconstructing this
            // partially rewritten router could overwrite those good images.
            self.fail_direct_unpack_after_repair(job_id, &error);
            self.fail_job(job_id, error);
        }
    }

    async fn place_par3_readback(
        &mut self,
        job_id: JobId,
        done: EngineResult<ReadbackDone>,
    ) -> Result<(), String> {
        let ReadbackDone {
            mut installation,
            result,
            _reservation,
        } = done.map_err(|error| error.to_string())?;
        let span = result.map_err(|error| format!("PAR3 readback failed: {error}"))?;
        let target = &installation.targets[installation.current];
        let set_index = target.set;
        let volume = target.volume;
        let len = installation
            .completion
            .outputs
            .as_ref()
            .map_err(|error| error.to_string())?[target.output]
            .len;
        let set = self
            .direct_store
            .set_mut(job_id, set_index)
            .ok_or("missing PAR3 direct set")?;
        if set.is_demoted() || set.is_finalized() {
            return Err("PAR3 direct set changed during repair".into());
        }
        if installation.offset == 0 {
            let first = !installation.targets[..installation.current]
                .iter()
                .any(|previous| previous.set == set_index);
            if first && set.repair_attempted() {
                return Err("PAR3 direct set already repaired".into());
            }
            let mut persist = crate::pipeline::direct_store::barrier::DatabaseCoveragePersist::new(
                self.db.clone(),
            );
            set.delete_checkpoint_row(&mut persist)
                .map_err(|error| format!("PAR3 checkpoint retirement failed: {error}"))?;
            set.note_repair_attempted();
            self.block_crcs.forget_file(target.file);
        }
        if span.source_offset != installation.offset
            || span.len == 0
            || span.len > len - installation.offset
        {
            return Err("PAR3 readback returned inconsistent coverage".into());
        }
        let end = installation.offset + span.len;
        let finish = end == len;
        let routed = set
            .route_repaired_batch(volume, &span.chunks, &[], finish)
            .map_err(|reason| format!("PAR3 direct routing failed: {}", reason.metric()))?;
        self.try_place_direct_spans(job_id, set_index, &routed)
            .await
            .map_err(|error| format!("PAR3 direct placement failed: {error:?}"))?;
        installation.crc32 = weaver_yenc::crc32_combine(installation.crc32, span.crc32, span.len);
        installation.offset = end;
        drop(routed);
        drop(span);
        drop(_reservation);
        if finish {
            let set = self
                .direct_store
                .set_mut(job_id, set_index)
                .ok_or("missing PAR3 direct set")?;
            set.note_repaired_whole_volume_crc(volume, len, installation.crc32);
            let routed = set.note_volume_complete(volume, len).map_err(|reason| {
                format!("PAR3 volume confirmation failed: {}", reason.metric())
            })?;
            self.try_place_direct_spans(job_id, set_index, &routed)
                .await
                .map_err(|error| format!("PAR3 volume confirmation placement failed: {error:?}"))?;
            installation.current += 1;
            installation.offset = 0;
            installation.crc32 = 0;
        }
        if installation.current == installation.targets.len() {
            for set in installation
                .targets
                .iter()
                .map(|target| target.set)
                .collect::<std::collections::BTreeSet<_>>()
            {
                self.cache_direct_volume_facts(job_id, set).await;
                self.run_direct_barrier(
                    job_id,
                    set,
                    crate::pipeline::direct_store::barrier::BarrierTrigger::Demand(
                        crate::pipeline::direct_store::barrier::BarrierDemand::RepairRecreate,
                    ),
                )
                .await;
            }
            self.par3_runtime
                .as_mut()
                .expect("admitted job")
                .finish_installation(job_id);
            self.refresh_par3_sources(job_id)
                .map_err(|error| error.to_string())?;
            self.complete_par3_repair(job_id, installation.completion)
                .await;
        } else {
            self.par3_runtime
                .as_mut()
                .expect("admitted job")
                .queue_readback(job_id, installation)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_output_readback_is_bounded_and_exact() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repaired.rar");
        let bytes: Vec<_> = (0..STRIPE_BYTES as usize * 2 + 17)
            .map(|index| (index * 17) as u8)
            .collect();
        std::fs::write(&path, &bytes).unwrap();
        let mut options = execution_options();
        options.handles = HandleBudget::new(1);
        let image = VerifiedOutput::capture(path, bytes.len() as u64, &options).unwrap();
        let _memory = assessment::ViewReservation::acquire(STRIPE_RESERVATION).unwrap();
        let mut offset = 0;
        let mut crc = 0;
        let mut stripes = 0;
        while offset < image.len {
            let span = image.stripe(3, offset, &options).unwrap();
            assert_eq!(span.volume_index, 3);
            assert_eq!(span.source_offset, offset);
            assert!(span.len <= STRIPE_BYTES);
            assert_eq!(span.chunks.len(), 1);
            assert_eq!(
                &*span.chunks[0].1,
                &bytes[offset as usize..(offset + span.len) as usize]
            );
            assert!(span.lead_in.is_none() && span.lead_out.is_none());
            crc = weaver_yenc::crc32_combine(crc, span.crc32, span.len);
            offset += span.len;
            stripes += 1;
        }
        assert_eq!(stripes, 3);
        assert_eq!(crc, par2_rs::checksum::crc32(&bytes));
        assert!(matches!(
            image.stripe(3, offset, &options),
            Err(EngineError::InvalidState(_))
        ));
        // Every stripe released its handle, including the rejected cursor.
        drop(options.handles.acquire().unwrap());
    }

    #[test]
    fn readback_rejects_changed_output_and_cancelled_work() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repaired.rar");
        let options = execution_options();
        std::fs::write(&path, b"original").unwrap();
        let image = VerifiedOutput::capture(path.clone(), 8, &options).unwrap();
        let replacement = root.path().join("replacement");
        std::fs::write(&replacement, b"modified").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert!(matches!(
            image.stripe(0, 0, &options),
            Err(EngineError::SourceChanged(_))
        ));
        let image = VerifiedOutput::capture(path, 8, &options).unwrap();
        options.cancel.cancel();
        assert!(matches!(
            image.stripe(0, 0, &options),
            Err(EngineError::Cancelled)
        ));
    }

    #[test]
    fn readback_checks_authenticated_length_and_handle_admission() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repaired.rar");
        std::fs::write(&path, b"image").unwrap();
        let mut options = execution_options();
        options.handles = HandleBudget::new(1);
        assert!(matches!(
            VerifiedOutput::capture(path.clone(), 6, &options),
            Err(EngineError::SourceChanged(_))
        ));
        let image = VerifiedOutput::capture(path, 5, &options).unwrap();
        let handle = options.handles.acquire().unwrap();
        assert!(matches!(
            image.stripe(0, 0, &options),
            Err(EngineError::ResourceLimit(_))
        ));
        drop(handle);
        assert_eq!(image.stripe(0, 0, &options).unwrap().len, 5);
    }
}
