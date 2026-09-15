//! Verified installation images and bounded direct-store readback work.

use super::*;
use crate::pipeline::direct_store::repair::{RepairedSpan, read_repaired_range};
use crate::pipeline::direct_store::router::RestartReadRun;

pub(super) const STRIPE_BYTES: u64 = 256 * 1024;
pub(super) const STRIPE_RESERVATION: usize = 2 * STRIPE_BYTES as usize + 4096;
const MAX_GAP_PATH_BYTES: usize = 8192;
const MAX_EDGES: usize = 4096;
const EDGE_RESERVATION: usize = MAX_EDGES * 256;

type CipherEdge = (u32, u64, Arc<[u8]>);

pub(super) struct EdgeRead {
    target: usize,
    source: SourceId,
    volume: u32,
    offset: u64,
    len: u64,
    output: Option<usize>,
}

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
        cipher_edges: bool,
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
            read_repaired_range(&self.path, volume, self.len, offset..end, cipher_edges)?
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
    pub cipher: bool,
    pub edges: Vec<CipherEdge>,
}

pub(super) struct GapRead {
    run: RestartReadRun,
    path: PathBuf,
}

impl GapRead {
    fn read(&self, options: &ExecutionOptions) -> EngineResult<u32> {
        options.cancel.check()?;
        if self.run.len == 0 || self.run.len > STRIPE_BYTES {
            return Err(EngineError::InvalidState("invalid PAR3 gap stripe"));
        }
        let access = disk_source(SourceId(0), self.path.clone(), options)?;
        let snapshot = access
            .snapshot(SourceId(0))?
            .ok_or(EngineError::Unavailable {
                source_id: SourceId(0),
                offset: self.run.logical_offset,
            })?;
        let end = self
            .run
            .logical_offset
            .checked_add(self.run.len)
            .filter(|end| *end <= snapshot.len)
            .ok_or(EngineError::Unavailable {
                source_id: SourceId(0),
                offset: self.run.logical_offset,
            })?;
        let mut bytes = vec![0; 64 * 1024];
        let mut offset = self.run.logical_offset;
        let mut crc = 0;
        while offset < end {
            options.cancel.check()?;
            let count = bytes.len().min((end - offset) as usize);
            let read = access.read_at(SourceId(0), offset, &mut bytes[..count])?;
            if read == 0 {
                return Err(EngineError::Unavailable {
                    source_id: SourceId(0),
                    offset,
                });
            }
            crc = weaver_yenc::crc32_combine(
                crc,
                par2_rs::checksum::crc32(&bytes[..read]),
                read as u64,
            );
            offset += read as u64;
        }
        options.cancel.check()?;
        if access.snapshot(SourceId(0))? != Some(snapshot) {
            return Err(EngineError::SourceChanged(SourceId(0)));
        }
        Ok(crc)
    }
}

pub(super) enum ReadbackUnit {
    Stripe(RepairedSpan),
    Gap(u32),
}

pub(super) struct Installation {
    pub completion: work::RepairCompletion,
    pub targets: Vec<Target>,
    pub current: usize,
    pub offset: u64,
    pub crc32: u32,
    pub settling_set: Option<usize>,
    pub pending_gap: Option<GapRead>,
    pub edge_reads: Vec<EdgeRead>,
    pub preflight_failed: bool,
    pub _edge_reservation: Option<assessment::ViewReservation>,
}

impl Installation {
    pub fn read_unit(
        &mut self,
        sources: &PublishedSources,
        options: &ExecutionOptions,
    ) -> EngineResult<ReadbackUnit> {
        if let Some(gap) = &self.pending_gap {
            return gap.read(options).map(ReadbackUnit::Gap);
        }
        self.read(sources, options).map(ReadbackUnit::Stripe)
    }

    pub fn read(
        &mut self,
        sources: &PublishedSources,
        options: &ExecutionOptions,
    ) -> EngineResult<RepairedSpan> {
        if self.preflight_failed {
            return Err(EngineError::InvalidState(
                "PAR3 cipher preflight previously failed",
            ));
        }
        self.preflight_failed = true;
        // Capture every neighbour before any stripe changes a shared partial.
        // A repaired neighbour comes from its verified installed image; an
        // untouched neighbour comes from the retained, budgeted virtual source.
        let outputs = self
            .completion
            .outputs
            .as_ref()
            .map_err(|_| EngineError::InvalidState("PAR3 installed output was not captured"))?;
        for read in self.edge_reads.drain(..) {
            options.cancel.check()?;
            if read.len == 0 || read.len > 31 {
                return Err(EngineError::InvalidState("invalid PAR3 cipher edge"));
            }
            let mut bytes = vec![0; read.len as usize];
            if let Some(output) = read.output {
                let image = &outputs[output];
                image.check()?;
                let mut filled = 0;
                while filled < bytes.len() {
                    let n = image.access.read_at(
                        SourceId(0),
                        read.offset + filled as u64,
                        &mut bytes[filled..],
                    )?;
                    if n == 0 {
                        return Err(EngineError::Unavailable {
                            source_id: read.source,
                            offset: read.offset + filled as u64,
                        });
                    }
                    filled += n;
                }
                image.check()?;
            } else {
                let before = sources
                    .snapshot(read.source)?
                    .ok_or(EngineError::Unavailable {
                        source_id: read.source,
                        offset: read.offset,
                    })?;
                let mut filled = 0;
                while filled < bytes.len() {
                    let n = sources.read_at(
                        read.source,
                        read.offset + filled as u64,
                        &mut bytes[filled..],
                    )?;
                    if n == 0 {
                        return Err(EngineError::Unavailable {
                            source_id: read.source,
                            offset: read.offset + filled as u64,
                        });
                    }
                    filled += n;
                }
                if sources.snapshot(read.source)? != Some(before) {
                    return Err(EngineError::SourceChanged(read.source));
                }
            }
            self.targets[read.target]
                .edges
                .push((read.volume, read.offset, Arc::from(bytes)));
        }
        self.preflight_failed = false;
        let target = &self.targets[self.current];
        self.completion
            .outputs
            .as_ref()
            .map_err(|_| EngineError::InvalidState("PAR3 installed output was not captured"))?
            [target.output]
            .stripe(target.volume, self.offset, target.cipher, options)
    }
}

pub(super) struct ReadbackDone {
    pub installation: Box<Installation>,
    pub result: EngineResult<ReadbackUnit>,
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
                    cipher: set.router.routes_encrypted(),
                    edges: Vec::new(),
                });
            }
        }
        if targets.is_empty() {
            self.complete_par3_repair(job_id, completion).await;
            return;
        }
        targets.sort_by_key(|target| (target.set, target.volume));
        let edge_reservation = if targets.iter().any(|target| target.cipher) {
            match assessment::ViewReservation::acquire(EDGE_RESERVATION) {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    self.fail_job(job_id, error.to_string());
                    return;
                }
            }
        } else {
            None
        };
        let mut edge_reads = Vec::new();
        for (index, target) in targets
            .iter()
            .enumerate()
            .filter(|(_, target)| target.cipher)
        {
            let set = self
                .direct_store
                .set(job_id, target.set)
                .expect("matched set");
            let Some(reads) = set
                .router
                .cipher_replacement_edge_reads_bounded(target.volume, MAX_EDGES - edge_reads.len())
            else {
                self.fail_job(
                    job_id,
                    "PAR3 cipher edge plan is incomplete or exceeds the host budget".into(),
                );
                return;
            };
            for (volume, offset, len) in reads {
                let Some(file) = set.plan().volumes.get(&volume) else {
                    self.fail_job(job_id, "PAR3 cipher neighbour has no job binding".into());
                    return;
                };
                let output = targets
                    .iter()
                    .find(|target| target.file.file_index == *file)
                    .map(|target| target.output);
                edge_reads.push(EdgeRead {
                    target: index,
                    source: SourceId(u64::from(*file)),
                    volume,
                    offset,
                    len,
                    output,
                });
            }
        }
        let installation = Box::new(Installation {
            completion,
            targets,
            current: 0,
            offset: 0,
            crc32: 0,
            settling_set: None,
            pending_gap: None,
            edge_reads,
            preflight_failed: false,
            _edge_reservation: edge_reservation,
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
        let unit = result.map_err(|error| format!("PAR3 readback failed: {error}"))?;
        let span = match unit {
            ReadbackUnit::Stripe(span) => {
                if installation.pending_gap.is_some() || installation.settling_set.is_some() {
                    return Err("PAR3 stripe arrived during gap settlement".into());
                }
                span
            }
            ReadbackUnit::Gap(crc) => {
                let gap = installation
                    .pending_gap
                    .take()
                    .ok_or("missing PAR3 gap read")?;
                let set_index = installation.settling_set.ok_or("missing PAR3 gap set")?;
                let set = self
                    .direct_store
                    .set_mut(job_id, set_index)
                    .ok_or("missing PAR3 direct set")?;
                if set.is_demoted()
                    || set.is_finalized()
                    || set
                        .router
                        .next_stale_gap(STRIPE_BYTES, MAX_GAP_PATH_BYTES)
                        .map_err(|reason| reason.metric().to_string())?
                        .as_ref()
                        != Some(&gap.run)
                {
                    return Err("PAR3 gap layout changed before handback".into());
                }
                set.router
                    .note_restored_member_crc(
                        gap.run.member_id,
                        gap.run.logical_offset,
                        gap.run.len,
                        crc,
                    )
                    .map_err(|reason| {
                        format!("PAR3 gap verification failed: {}", reason.metric())
                    })?;
                crate::runtime::perf_probe::record_value(
                    "direct_store.repair.gap_reread_bytes",
                    gap.run.len,
                );
                drop(_reservation);
                return self.advance_par3_installation(job_id, installation).await;
            }
        };
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
            if first {
                let mut persist =
                    crate::pipeline::direct_store::barrier::DatabaseCoveragePersist::new(
                        self.db.clone(),
                    );
                set.delete_checkpoint_row(&mut persist)
                    .map_err(|error| format!("PAR3 checkpoint retirement failed: {error}"))?;
                let volumes = installation.targets[installation.current..]
                    .iter()
                    .take_while(|target| target.set == set_index)
                    .map(|target| target.volume)
                    .collect();
                set.begin_repair_transaction(volumes).map_err(|reason| {
                    format!("PAR3 replacement setup failed: {}", reason.metric())
                })?;
                set.note_repair_attempted();
            }
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
        let mut lead_in = target.edges.clone();
        lead_in.extend(
            [span.lead_in.clone(), span.lead_out.clone()]
                .into_iter()
                .flatten()
                .map(|(offset, bytes)| (volume, offset, bytes)),
        );
        let routed = set
            .route_repaired_batch(volume, &span.chunks, &lead_in, finish)
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
            if installation
                .targets
                .get(installation.current)
                .is_none_or(|next| next.set != set_index)
            {
                installation.settling_set = Some(set_index);
            }
            installation.offset = 0;
            installation.crc32 = 0;
        }
        self.advance_par3_installation(job_id, installation).await
    }

    async fn advance_par3_installation(
        &mut self,
        job_id: JobId,
        mut installation: Box<Installation>,
    ) -> Result<(), String> {
        if let Some(set_index) = installation.settling_set {
            let set = self
                .direct_store
                .set_mut(job_id, set_index)
                .ok_or("missing PAR3 direct set")?;
            if set.is_demoted() || set.is_finalized() {
                return Err("PAR3 direct set changed during gap settlement".into());
            }
            if let Some(run) = set
                .router
                .next_stale_gap(STRIPE_BYTES, MAX_GAP_PATH_BYTES)
                .map_err(|reason| format!("PAR3 gap planning failed: {}", reason.metric()))?
            {
                if set
                    .plan()
                    .destination_dir
                    .as_os_str()
                    .len()
                    .saturating_add(run.relative_partial.len())
                    > MAX_GAP_PATH_BYTES
                {
                    return Err("PAR3 gap path exceeds retained budget".into());
                }
                installation.pending_gap = Some(GapRead {
                    path: set.plan().destination_dir.join(&run.relative_partial),
                    run,
                });
                return self
                    .par3_runtime
                    .as_mut()
                    .expect("admitted job")
                    .queue_readback(job_id, installation)
                    .map_err(|error| error.to_string());
            }
            set.finish_repair_transaction().map_err(|reason| {
                format!("PAR3 replacement verification failed: {}", reason.metric())
            })?;
            installation.settling_set = None;
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
    fn gap_readback_streams_exact_crc_and_enforces_io_bounds() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("member.partial");
        let bytes: Vec<u8> = (0..STRIPE_BYTES + 29).map(|i| (i * 31) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let mut options = execution_options();
        options.handles = HandleBudget::new(1);
        let mut gap = GapRead {
            path: path.clone(),
            run: RestartReadRun {
                member_id: 1,
                relative_partial: "member.partial".into(),
                logical_offset: 17,
                len: STRIPE_BYTES,
            },
        };
        assert_eq!(
            gap.read(&options).unwrap(),
            par2_rs::checksum::crc32(&bytes[17..17 + STRIPE_BYTES as usize])
        );
        let handle = options.handles.acquire().unwrap();
        assert!(matches!(
            gap.read(&options),
            Err(EngineError::ResourceLimit(_))
        ));
        drop(handle);
        gap.run.len += 1;
        assert!(matches!(
            gap.read(&options),
            Err(EngineError::InvalidState(_))
        ));
        gap.run.len = STRIPE_BYTES;
        std::fs::write(&path, &bytes[..31]).unwrap();
        assert!(matches!(
            gap.read(&options),
            Err(EngineError::Unavailable { .. })
        ));
        options.cancel.cancel();
        assert!(matches!(gap.read(&options), Err(EngineError::Cancelled)));
    }

    #[test]
    fn gap_worker_continues_after_the_last_replacement_stripe() {
        let root = tempfile::tempdir().unwrap();
        let options = execution_options();
        let mut installation = encrypted_installation(root.path(), &options);
        let path = root.path().join("member.partial");
        std::fs::write(&path, [37; 71]).unwrap();
        installation.current = installation.targets.len();
        installation.settling_set = Some(0);
        installation.pending_gap = Some(GapRead {
            path,
            run: RestartReadRun {
                member_id: 1,
                relative_partial: "member.partial".into(),
                logical_offset: 3,
                len: 61,
            },
        });
        assert!(matches!(
            installation.read_unit(&PublishedSources::default(), &options).unwrap(),
            ReadbackUnit::Gap(crc) if crc == par2_rs::checksum::crc32(&[37; 61])
        ));
        assert_eq!(installation.current, installation.targets.len());
    }

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
            let span = image.stripe(3, offset, false, &options).unwrap();
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
            image.stripe(3, offset, false, &options),
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
            image.stripe(0, 0, false, &options),
            Err(EngineError::SourceChanged(_))
        ));
        let image = VerifiedOutput::capture(path, 8, &options).unwrap();
        options.cancel.cancel();
        assert!(matches!(
            image.stripe(0, 0, false, &options),
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
            image.stripe(0, 0, false, &options),
            Err(EngineError::ResourceLimit(_))
        ));
        drop(handle);
        assert_eq!(image.stripe(0, 0, false, &options).unwrap().len, 5);
    }
    fn encrypted_installation(root: &std::path::Path, options: &ExecutionOptions) -> Installation {
        let path = root.join("repaired.rar");
        std::fs::write(&path, vec![19; STRIPE_BYTES as usize + 17]).unwrap();
        Installation {
            completion: work::RepairCompletion {
                result: Ok(Default::default()),
                embedded_replacement: false,
                outputs: Ok(vec![
                    VerifiedOutput::capture(path, STRIPE_BYTES + 17, options).unwrap(),
                ]),
                _reservation: Some(assessment::ViewReservation::acquire(4096).unwrap()),
            },
            targets: vec![Target {
                file: NzbFileId {
                    job_id: JobId(1),
                    file_index: 0,
                },
                set: 0,
                volume: 0,
                output: 0,
                cipher: true,
                edges: Vec::new(),
            }],
            current: 0,
            offset: 0,
            crc32: 0,
            settling_set: None,
            pending_gap: None,
            edge_reads: vec![EdgeRead {
                target: 0,
                source: SourceId(1),
                volume: 1,
                offset: 3,
                len: 31,
                output: None,
            }],
            preflight_failed: false,
            _edge_reservation: Some(
                assessment::ViewReservation::acquire(EDGE_RESERVATION).unwrap(),
            ),
        }
    }

    #[test]
    fn cipher_edges_are_captured_before_shared_backings_change() {
        let root = tempfile::tempdir().unwrap();
        let options = execution_options();
        let mut installation = encrypted_installation(root.path(), &options);
        let neighbour = root.path().join("neighbour");
        let original: Vec<u8> = (0..64).collect();
        std::fs::write(&neighbour, &original).unwrap();
        let sources = PublishedSources::default();
        sources
            .replace(
                SourceId(1),
                disk_source(SourceId(1), neighbour.clone(), &options).unwrap(),
                64,
                std::iter::once(0..64).collect(),
            )
            .unwrap();
        let first = installation.read(&sources, &options).unwrap();
        assert_eq!(first.len, STRIPE_BYTES);
        assert_eq!(&*installation.targets[0].edges[0].2, &original[3..34]);
        assert_eq!(first.lead_out.as_ref().unwrap().1.len(), 16);
        sources.withdraw(SourceId(1)).unwrap();
        std::fs::write(neighbour, [0; 64]).unwrap();
        installation.offset = STRIPE_BYTES;
        let last = installation.read(&sources, &options).unwrap();
        assert_eq!(last.len, 17);
        assert_eq!(last.lead_in.as_ref().unwrap().1.len(), 32);
        assert_eq!(installation.targets[0].edges.len(), 1);
        assert_eq!(&*installation.targets[0].edges[0].2, &original[3..34]);
    }

    #[test]
    fn cipher_edge_uses_verified_repaired_neighbour_instead_of_old_source() {
        let root = tempfile::tempdir().unwrap();
        let options = execution_options();
        let mut installation = encrypted_installation(root.path(), &options);
        let neighbour = root.path().join("repaired-neighbour");
        std::fs::write(&neighbour, [23; 64]).unwrap();
        installation
            .completion
            .outputs
            .as_mut()
            .unwrap()
            .push(VerifiedOutput::capture(neighbour, 64, &options).unwrap());
        installation.edge_reads[0].output = Some(1);
        // No old source exists at all: only the verified output can answer.
        installation
            .read(&PublishedSources::default(), &options)
            .unwrap();
        assert_eq!(&*installation.targets[0].edges[0].2, &[23; 31]);
    }

    #[test]
    fn unavailable_cipher_edge_refuses_before_returning_any_replacement() {
        let root = tempfile::tempdir().unwrap();
        let options = execution_options();
        let mut installation = encrypted_installation(root.path(), &options);
        let neighbour = root.path().join("neighbour");
        std::fs::write(&neighbour, [1; 64]).unwrap();
        let sources = PublishedSources::default();
        sources
            .replace(
                SourceId(1),
                disk_source(SourceId(1), neighbour, &options).unwrap(),
                64,
                vec![0..10, 20..64],
            )
            .unwrap();
        assert!(matches!(
            installation.read(&sources, &options),
            Err(EngineError::Unavailable {
                source_id: SourceId(1),
                offset: 10
            })
        ));
        assert!(installation.targets[0].edges.is_empty());
        assert_eq!(installation.offset, 0);
        assert!(matches!(
            installation.read(&sources, &options),
            Err(EngineError::InvalidState(
                "PAR3 cipher preflight previously failed"
            ))
        ));
    }
}
