//! Continuation of the `impl Pipeline` block from `direct_store/wiring.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    /// The routing seam. Replaces the conventional write for one decoded
    /// segment of a direct source volume.
    pub(crate) async fn handle_direct_decode_success(
        &mut self,
        set_index: usize,
        volume_index: u32,
        segment: BufferedDecodedSegment,
        file_offset: u64,
    ) -> DirectRouteOutcome {
        let segment_id = segment.segment_id;
        let file_id = segment_id.file_id;
        let job_id = file_id.job_id;
        let decoded_size = segment.decoded_size;
        let part_crc = segment.part_crc;
        // The dual-CRC grid's half of the article, carried past routing to the
        // commit seam. `contiguous_bytes` gives the router a contiguous view
        // while the original decoded buffer stays owned here for fallback.
        let part_crc_verified = segment.part_crc_verified;
        let bytes = contiguous_bytes(&segment.data);

        let routed = {
            let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                return DirectRouteOutcome::Conventional(segment);
            };
            set.note_volume_part_crc(volume_index, file_offset, u64::from(decoded_size), part_crc);
            // The decoded geometry of this article, which only the decoder
            // knows: demotion-by-reconstruction uses it to decide which
            // articles it does *not* have to fetch again.
            set.note_segment_extent(
                volume_index,
                segment_id.segment_number,
                file_offset,
                u64::from(decoded_size),
            );
            set.route(volume_index, file_offset, &bytes)
        };
        let spans = match routed {
            Ok(spans) => spans,
            Err(reason) => {
                self.demote_direct_set_with_handoff(job_id, set_index, reason, Some(segment_id))
                    .await;
                return DirectRouteOutcome::Conventional(segment);
            }
        };

        // Before the writes, so a fact can never be newer on disk than in the
        // cache the restart reader rebuilds from. Cheap when nothing parsed: a
        // set parses a volume once provisionally and once confirmingly, so this
        // is two writes per volume for the life of the job.
        self.cache_direct_volume_facts(job_id, set_index).await;

        if !self
            .place_direct_spans(job_id, set_index, Some(segment_id), &spans)
            .await
        {
            return DirectRouteOutcome::Conventional(segment);
        }

        drop(bytes);

        self.commit_direct_segment(
            segment_id,
            decoded_size,
            set_index,
            volume_index,
            file_offset,
            part_crc,
            part_crc_verified,
            &segment.checkpoint_plan,
            &segment.segments,
        )
        .await;
        DirectRouteOutcome::Routed
    }

    /// Writes every destination a batch of routed spans touches, then records
    /// them as coverage. `false` means the set demoted and the caller must stop.
    ///
    /// The record only happens once **all** the writes returned: partial failure
    /// leaves orphan bytes, and the coverage map is the truth, not the bytes.
    /// Both span producers go through here — the routing seam and the confirming
    /// parse's drain at volume completion — so neither can grow its own,
    /// subtly different, ordering.
    pub(super) async fn place_direct_spans(
        &mut self,
        job_id: JobId,
        set_index: usize,
        handoff: Option<SegmentId>,
        spans: &[RoutedSpan],
    ) -> bool {
        let failure = match self.try_place_direct_spans(job_id, set_index, spans).await {
            Ok(()) => return true,
            Err(failure) => failure,
        };
        match failure {
            DirectPlacementError::Sparse { path, error } => {
                // A destination that could not be marked sparse is refused *before*
                // it holds a hole, so nothing has been allocated for it yet. Demote
                // and let the conventional path own the bytes.
                warn!(
                    job_id = job_id.0,
                    path = %path.display(),
                    error = %error,
                    "could not mark a direct-store destination sparse; demoting the set"
                );
                self.demote_direct_set_with_handoff(
                    job_id,
                    set_index,
                    DemotionReason::SparseMarkFailed,
                    handoff,
                )
                .await;
                false
            }
            DirectPlacementError::Write(error) => {
                // A destination write failure is a demotion, not a job failure: the
                // conventional path writes the same bytes to a different file, and
                // only if *that* also fails is the job genuinely unfinishable.
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "direct-store destination write failed; demoting the set"
                );
                self.demote_direct_set_with_handoff(
                    job_id,
                    set_index,
                    DemotionReason::DestinationWriteFailed,
                    handoff,
                )
                .await;
                if !self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(DirectSet::is_demoted)
                {
                    self.fail_job(
                        job_id,
                        format!(
                            "direct-store destination write failed for job {}: {error}",
                            job_id.0
                        ),
                    );
                }
                false
            }
        }
    }

    /// Places bytes and admits coverage only after every destination write
    /// succeeds. It does not choose a demotion policy: a repair caller may
    /// already own verified materialized outputs that reconstruction must not
    /// overwrite. Filesystem errors retain their original error values.
    pub(in crate::pipeline) async fn try_place_direct_spans(
        &mut self,
        job_id: JobId,
        set_index: usize,
        spans: &[RoutedSpan],
    ) -> Result<(), DirectPlacementError> {
        if spans.is_empty() {
            return Ok(());
        }
        self.invalidate_par3_direct_set(job_id, set_index);
        let batches = self.direct_write_batches(job_id, set_index, spans);
        self.prepare_direct_destinations(job_id, &batches).await?;
        crate::pipeline::orchestrator::write_direct_batches(batches)
            .await
            .map_err(DirectPlacementError::Write)?;
        // Where the bytes went, split by destination kind. Two counters answer
        // the question the disk acceptance target is stated in: how much of
        // a set landed at its final offset versus how much rode the envelope
        // as service data. Summed over the spans already in hand, and
        // `record_value` is a no-op unless the profiler is on.
        let (member_bytes, envelope_bytes) =
            spans.iter().fold((0u64, 0u64), |(member, envelope), span| {
                let len = span.bytes.len() as u64;
                match span.destination {
                    DirectDestination::Member { .. } => (member + len, envelope),
                    DirectDestination::Envelope { .. } => (member, envelope + len),
                }
            });
        if member_bytes > 0 {
            crate::runtime::perf_probe::record_value("direct_store.bytes.member", member_bytes);
        }
        if envelope_bytes > 0 {
            crate::runtime::perf_probe::record_value("direct_store.bytes.envelope", envelope_bytes);
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.record_writes(spans, Instant::now());
        }
        Ok(())
    }

    /// Caches whatever volume facts the set's parse just accepted, so a restart
    /// can rebuild its layout.
    ///
    /// The rows go into `active_rar_volume_facts` — the same table, keyed the
    /// same way, that the conventional path fills from a parsed volume file.
    /// There is no writer conflict: `try_update_archive_topology` needs a file
    /// to parse and it is suppressed for direct volumes, so for a live direct
    /// set this is the only writer, and after a demotion the conventional path
    /// upserts the same facts over the materialized volumes.
    pub(in crate::pipeline) async fn cache_direct_volume_facts(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) {
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        let dirty = set.router.take_dirty_facts();
        if dirty.is_empty() {
            return;
        }
        let set_name = set.set_name().to_string();
        for (volume_index, facts) in dirty {
            let encoded = match rmp_serde::to_vec_named(&facts) {
                Ok(encoded) => encoded,
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = volume_index,
                        error = %error,
                        "failed to encode direct-store volume facts"
                    );
                    continue;
                }
            };
            let name = set_name.clone();
            let saved = self
                .db_blocking(move |db| {
                    db.save_rar_volume_facts(job_id, &name, volume_index, &encoded)
                })
                .await;
            if let Err(error) = saved {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volume = volume_index,
                    error = %error,
                    "failed to cache direct-store volume facts; the set will redownload on restart"
                );
                if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                    set.router.remark_dirty_fact(volume_index);
                }
            }
        }
    }

    /// The gate re-arm: recomputes the member CRC for every restart-seeded
    /// range with **one sequential read** of the partials that hold them.
    ///
    /// `CrcRuns` does not survive a restart, so the bytes a previous run wrote
    /// are covered and unverified; the whole-member gate refuses to compose over
    /// them until they are re-read. This is the "PAR2 absent" arm — the direct
    /// analogue of `checksum_completed_file`'s fallback for physical files, at
    /// the same cost and the same assurance. It deliberately verifies what is on
    /// **disk now**, so a byte corrupted while the process was down fails the
    /// member gate and demotes the set instead of being committed.
    ///
    /// Runs **once** per set: every run it reads leaves the seeded set, so a
    /// second call finds nothing to do — and if anything is still seeded after
    /// a full pass, the set demotes rather than being re-read on every
    /// completion check for the life of the job.
    pub(super) async fn rearm_restart_seeded_gates(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || set.is_finalized() || !set.has_restart_seeded_coverage() {
            return;
        }
        let destination_dir = set.plan().destination_dir.clone();
        let set_name = set.set_name().to_string();
        let runs = set.router.restart_read_plan();
        if runs.is_empty() {
            return;
        }
        let total: u64 = runs.iter().map(|run| run.len).sum();
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            runs = runs.len(),
            bytes = total,
            "re-reading restart-seeded direct-store coverage to re-arm the member gates"
        );

        let read_runs = runs.clone();
        let read_dir = destination_dir;
        let checksums =
            tokio::task::spawn_blocking(move || read_restart_seeded_runs(&read_dir, &read_runs))
                .await;
        let checksums = match checksums {
            Ok(Ok(checksums)) => checksums,
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to re-read restart-seeded direct-store coverage; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RestartRereadFailed)
                    .await;
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "the restart-seeded re-read task did not complete; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RestartRereadFailed)
                    .await;
                return;
            }
        };

        crate::runtime::perf_probe::record_value("direct_store.restart.reread_bytes", total);
        let mut failure = None;
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            for (run, crc) in runs.iter().zip(checksums) {
                if let Err(reason) = set.router.note_restored_member_crc(
                    run.member_id,
                    run.logical_offset,
                    run.len,
                    crc,
                ) {
                    failure = Some(reason);
                    break;
                }
            }
        }
        if let Some(reason) = failure {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                reason = reason.metric(),
                "restart-seeded direct-store coverage failed its checksum on re-read"
            );
            self.demote_direct_set(job_id, set_index, reason).await;
            return;
        }

        // The terminating condition. The pass above read every run the plan
        // named, so nothing may still be seeded — a range that survives it is
        // one no plan reached, and re-running the pass would read the same runs
        // and reach the same place. Left alone, `try_verify_member` refuses
        // that member forever while the completion gate calls this back on
        // every check: a zombie that costs I/O. One pass, then a verdict.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| !set.is_demoted() && set.has_restart_seeded_coverage())
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                "restart-seeded direct-store coverage survived its re-read pass; demoting the set"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::RestartRearmUnplaceable)
                .await;
        }
    }

    /// Groups routed spans into one sub-batch per destination path.
    pub(super) fn direct_write_batches(
        &self,
        job_id: JobId,
        set_index: usize,
        spans: &[RoutedSpan],
    ) -> crate::pipeline::orchestrator::DirectWriteBatches {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Vec::new();
        };
        // Borrowed, never cloned (nit): this runs once per routed batch, and a
        // plan carries two maps sized by the set's volume count — 2 000 of them
        // on the sets this subsystem is sized for.
        let plan = set.plan();
        let partials: HashMap<u32, String> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(member_id, _, partial)| (member_id, partial.to_string()))
            .collect();

        let mut grouped: HashMap<PathBuf, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for span in spans {
            // The two roots part company here, and this is the seam the whole
            // split exists for: member payload is written straight into the
            // job's staging root on the **complete** volume, so the commit
            // rename and completion's publish are both same-filesystem, while
            // an envelope is working data and stays in the intermediate dir.
            let path = match span.destination {
                DirectDestination::Member { member_id } => match partials.get(&member_id) {
                    Some(relative) => plan.destination_path(relative),
                    None => continue,
                },
                // Envelope v2: one file per volume, written at true physical
                // offsets. The owner thread seeks to the offset and writes, so
                // the gaps member routing carried away are ordinary filesystem
                // holes on every platform that gives them for free. Windows
                // needs `FSCTL_SET_SPARSE` at creation to get the same, which a
                // later pass adds.
                DirectDestination::Envelope { volume_index } => plan.envelope_path(volume_index),
            };
            grouped
                .entry(path)
                .or_default()
                .push((span.destination_offset, span.bytes.clone()));
        }
        let mut batches: crate::pipeline::orchestrator::DirectWriteBatches =
            grouped.into_iter().collect();
        batches.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        // Sub-batches are ordered so the owner thread's sequential-write fast
        // path (no seek between adjacent runs) still applies inside a fragment.
        for (_, writes) in &mut batches {
            writes.sort_by_key(|(offset, _)| *offset);
        }
        batches
    }

    /// Creates the parent directory of every destination that needs one, and
    /// creates the destination file itself **marked sparse**, once per job (the
    /// Windows sparse rule).
    ///
    /// A member stored inside a directory — `Silver.Horizon/S01E06.mkv` — names
    /// a partial inside that directory, and the disk owner thread opens
    /// destinations with `create(true)` but never `create_dir_all`, so the
    /// first routed byte would fail with `ENOENT`. The conventional path never
    /// hits this because extraction creates the directory as it writes the
    /// member out; routing writes the member *before* extraction exists.
    ///
    /// The file is created here for the same reason, one step earlier than the
    /// disk owner would: `FSCTL_SET_SPARSE` has to be issued on a handle that
    /// has had nothing written through it, and the owner pool is shared with
    /// every conventional write in the process — it is not the place to teach
    /// about direct-store's sparseness. Creating (and marking) here leaves the
    /// pool's `open_or_reuse` opening a file that already exists and already
    /// carries the attribute, on Windows and everywhere else.
    ///
    /// Records a member name that **direct** finalization produced, in both the
    /// job-wide `extracted_members` (which completion reads) and the runtime's
    /// direct-only mirror (which the claim assertions subtract).
    ///
    /// Recorded under the *destination-relative* name, not the archive's own.
    /// RAR4 stores paths with `\` separators, and the destination is derived
    /// through `resolve_member_path`, which rewrites them to `/`. Recording the
    /// raw name left the two disagreeing for any RAR4 member with a directory
    /// component: completion resolved `work\sample.mkv` against the job's
    /// roots, found nothing on disk, declared the member a stale extracted
    /// record and re-ran conventional extraction — which then failed with "no
    /// on-disk RAR volumes", because direct finalization had deliberately never
    /// written any. A flat RAR4 member has no separator and so never showed it.
    ///
    /// The name is relative to the **staging root**, which is where the commit
    /// rename put the file and where the incremental extractor writes the
    /// members it produces — so completion resolves a direct member and an
    /// extracted one through exactly the same root
    /// (`Pipeline::resolve_job_input_path` tries the working dir and then the
    /// staging dir, and only the second can match a direct member).
    pub(super) fn record_direct_extracted(&mut self, job_id: JobId, name: String) {
        let name = DirectSetPlan::destination_relative_name(&name).unwrap_or(name);
        self.direct_store
            .direct_extracted_members
            .entry(job_id)
            .or_default()
            .insert(name.clone());
        self.extracted_members
            .entry(job_id)
            .or_default()
            .insert(name);
    }

    /// Member names the **incremental extractor** owns for this job: the
    /// blended `extracted_members` minus everything direct finalization put
    /// there. The claim assertions compare against this, not the blend — a
    /// sibling direct set finalizing the same member name is last-writer-wins
    /// by design, not a second checkpoint system claiming the member.
    pub(super) fn extraction_claimed_members(&self, job_id: JobId) -> HashSet<String> {
        let mut claimed = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        if let Some(direct) = self.direct_store.direct_extracted_members.get(&job_id) {
            claimed.retain(|name| !direct.contains(name));
        }
        claimed
    }

    /// A sparse-marking refusal includes its path and underlying I/O error.
    /// The caller chooses how to handle it before any hole is introduced.
    pub(super) async fn prepare_direct_destinations(
        &mut self,
        job_id: JobId,
        batches: &crate::pipeline::orchestrator::DirectWriteBatches,
    ) -> Result<(), DirectPlacementError> {
        // The choke point every direct write passes through, and the one place
        // that reliably runs for a **restored** set as well as a freshly
        // admitted one (`install_restored` marks the job examined, so
        // `ensure_direct_sets` returns early for it). Registering the staging
        // root on the job state here is what makes the rest of the pipeline
        // treat direct output like extraction output: `start_move_to_complete`
        // only sweeps a staging dir the state names, and the cancel and fail
        // paths only `remove_dir_all` one the state names. Idempotent and
        // cached after the first call — see `Pipeline::extraction_staging_dir`.
        let _ = self.extraction_staging_dir(job_id);
        let marking = self.direct_store.sparse_marking();
        for (path, _) in batches {
            if self
                .direct_store
                .prepared_destinations
                .get(&job_id)
                .is_some_and(|prepared| prepared.contains(path))
            {
                continue;
            }
            if let Some(parent) = path.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                warn!(
                    job_id = job_id.0,
                    path = %parent.display(),
                    error = %error,
                    "failed to create a direct-store destination directory"
                );
                // Left unprepared on purpose: the write below fails and demotes,
                // and a later attempt retries the directory rather than trusting
                // a failure it never saw succeed. Not a sparse refusal — the
                // write error path already distinguishes it.
                continue;
            }
            let created = {
                let path = path.clone();
                tokio::task::spawn_blocking(move || {
                    super::super::sparse::create_sparse(&path, &marking).map(drop)
                })
                .await
            };
            match created {
                Ok(Ok(())) => {}
                Ok(Err(super::super::sparse::SparseCreateError::Open(error))) => {
                    // An ordinary filesystem failure, and exactly the one the
                    // first routed write would have hit. Left unprepared so the
                    // write path reports it as `destination_write_failed`,
                    // which is what it is.
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "failed to create a direct-store destination"
                    );
                    continue;
                }
                Ok(Err(super::super::sparse::SparseCreateError::Mark(error))) => {
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "a direct-store destination could not be marked sparse"
                    );
                    return Err(DirectPlacementError::Sparse {
                        path: path.clone(),
                        error,
                    });
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "the sparse-marking task did not complete"
                    );
                    return Err(DirectPlacementError::Sparse {
                        path: path.clone(),
                        error: std::io::Error::other(error),
                    });
                }
            }
            // Keyed on the destination itself rather than its directory: the
            // marking is per file, and the directory is created on the way to
            // it. One entry per destination, which is `members + volumes` for
            // the life of the job.
            self.direct_store
                .prepared_destinations
                .entry(job_id)
                .or_default()
                .insert(path.clone());
        }
        Ok(())
    }

    /// The suppressed twin of `commit_persisted_segment`.
    // The extra four arguments are the conventional seam's own dual-CRC
    // contract, carried here rather than re-derived: placement, the article's
    // pCRC and whether it was independently verified, and the block-aligned
    // segments the decoder cut. Bundling them would only rename the tuple.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit_direct_segment(
        &mut self,
        segment_id: SegmentId,
        decoded_size: u32,
        set_index: usize,
        volume_index: u32,
        file_offset: u64,
        part_crc: u32,
        part_crc_verified: bool,
        checkpoint_plan: &weaver_yenc::CheckpointPlan,
        segments: &[weaver_yenc::Segment],
    ) {
        let file_id = segment_id.file_id;
        let job_id = file_id.job_id;

        let commit = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file_mut(file_id) else {
                return;
            };
            match file_asm.commit_segment(segment_id.segment_number, decoded_size) {
                Ok(commit) => (commit.file_complete, commit.was_duplicate),
                Err(error) => {
                    warn!(segment = %segment_id, error = %error, "direct-store assembly commit failed");
                    return;
                }
            }
        };
        let (file_complete, was_duplicate) = commit;
        if was_duplicate {
            // A duplicate must not advance CRC composition, coverage or
            // progress twice. Counted because a run where this is *never* zero
            // is a server or retry problem, and because the counter is what
            // makes "the duplicate did nothing" observable rather than assumed.
            crate::runtime::perf_probe::record(
                "direct_store.article.duplicate",
                std::time::Duration::from_nanos(1),
            );
        }
        if !was_duplicate {
            self.metrics
                .bytes_committed
                .fetch_add(decoded_size as u64, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .segments_committed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.send_segment_event(|| PipelineEvent::SegmentCommitted { segment_id });
        }

        // The dual-CRC grid, fed in **source-volume space** — the same
        // coordinates the conventional seam uses for the same file, because
        // `file_offset` is an offset into the volume either way. The only
        // difference is where those bytes are durable: a routed article is on
        // disk in member partials and envelopes rather than in a volume file,
        // and `place_direct_spans` awaited every one of those writes before this
        // seam was reached. That is the same ordering contract
        // `commit_persisted_segment` states — a block claimed here describes
        // content a later read (through the virtual-volume access adapter, which
        // reads exactly those partials) would find.
        //
        // Duplicates are fed on purpose, exactly as the conventional seam feeds
        // them: the grid is positional, and a replay that rewrote a range must
        // invalidate the verdicts derived over it whether or not its bytes
        // agreed. Withholding the feed would leave a claim describing content
        // that may no longer be there.
        self.note_block_crc_segments_for_plan(
            file_id,
            checkpoint_plan,
            file_offset,
            u64::from(decoded_size),
            part_crc,
            part_crc_verified,
            was_duplicate,
            segments,
        );

        if !file_complete {
            return;
        }

        let filename = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.filename().to_string())
            .unwrap_or_default();
        let total_bytes = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.received_bytes())
            .unwrap_or(0);
        info!(file_id = %file_id, filename = %filename, "direct source volume complete");
        let _ = self.event_tx.send(PipelineEvent::FileComplete {
            file_id,
            filename,
            total_bytes,
        });

        // The volume's length is what makes its short final block closable:
        // until now that block's extent was undecided, so no tiling of it could
        // be trusted to be the whole block.
        //
        // `total_bytes` is the assembly's **decoded** `received_bytes`, never
        // `total_bytes()` — the NZB's declared segment sum is yEnc-*encoded*,
        // around 3% larger on a real post, and a length that overstates the file
        // pushes the final block's boundary past the described extent, where
        // `verdicts_against` refuses to compare it at all. Same value, same
        // reason, as the conventional seam's `note_file_len`.
        self.block_crcs.note_file_len(file_id, total_bytes);

        // The file-complete state a physical volume drops here. Every one of
        // these is keyed by a file that will never exist, so leaving them
        // behind leaks for the life of the job and, worse, leaves
        // `unverified_segments` naming articles a whole-file CRC recovery would
        // try to replace by rewriting a file that is not there.
        let expected_file_crc = self.expected_file_crcs.remove(&file_id);
        self.pending_file_progress.remove(&file_id);
        self.persisted_file_progress.remove(&file_id);
        self.file_hash_reread_required.remove(&file_id);
        self.unverified_segments.remove(&file_id);
        self.file_crc_recoveries.remove(&file_id);
        self.unavailable_promoted_recovery_segments
            .retain(|segment_id| segment_id.file_id != file_id);

        // The yEnc whole-volume gate, composed rather than re-read.
        //
        // A physical volume is checked against its `=yend crc32` trailer when
        // the file completes; the per-article part CRC32s compose into exactly
        // that value, so the gate survives with no file to read. A mismatch
        // demotes: `schedule_file_crc_recovery` is deliberately *not* wired
        // here, because it replaces segments by rewriting a physical file, and
        // the provider is what gives a direct volume one.
        if let Some(expected) = expected_file_crc
            && let Some(composed) = self
                .direct_store
                .set(job_id, set_index)
                .and_then(|set| set.volume_crc(volume_index, total_bytes))
            && composed != expected
        {
            warn!(
                job_id = job_id.0,
                file_id = %file_id,
                expected = format!("{expected:08x}"),
                composed = format!("{composed:08x}"),
                "direct source volume failed its yEnc whole-file CRC32"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::VolumeCrcMismatch)
                .await;
            return;
        }

        // `total_bytes` — the assembly's decoded `received_bytes`, not the spec's
        // yEnc-encoded segment sizes — travels with the completion so the
        // checkpoint can tell "the download finished" from "every byte of it is
        // durable". Only the conjunction licenses restart to skip the volume's
        // segments; see `snapshot::VolumeFloor::complete`.
        let outcome = self
            .direct_store
            .set_mut(job_id, set_index)
            .map(|set| set.note_volume_complete(volume_index, total_bytes));
        match outcome {
            Some(Err(reason)) => {
                self.demote_direct_set(job_id, set_index, reason).await;
                return;
            }
            // The confirming parse can make the volume's trailing region
            // routable — it was held until the parse proved no further header
            // could appear there — so those spans are written here, before the
            // set is allowed to finalize and delete its envelopes.
            Some(Ok(spans)) => {
                self.cache_direct_volume_facts(job_id, set_index).await;
                if !self
                    .place_direct_spans(job_id, set_index, None, &spans)
                    .await
                {
                    return;
                }
            }
            None => return,
        }

        // The phase-change demand. The set's download phase ends exactly here,
        // at its last volume, and for a par2-bearing job the next thing that
        // happens is a verification wait that can run for the whole PAR2
        // download — which is precisely the window a restart is most likely to
        // land in. Checkpointing at the boundary means that restart resumes a
        // byte-complete set instead of refetching one.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.all_volumes_complete() && !set.is_demoted())
        {
            self.demand_direct_store_barriers(job_id, BarrierDemand::PhaseChange)
                .await;
        }

        // A completed volume's confirming parse is the one place an open
        // identity plan learns its size, and a set that closes over a gap —
        // its missing volume's file already settled — has no later event to
        // judge it. The sweep here is that judgment.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.plan().identity.is_some())
        {
            self.identity_viability_sweep(job_id).await;
        }

        self.finalize_ready_direct_sets(job_id).await;
        self.check_job_completion(job_id).await;
    }

    /// Commits every set of `job_id` whose members have all passed their gates
    /// and whose job is allowed to finalize (see
    /// [`Self::direct_finalization_waits_for_par2`]).
    ///
    /// Called from the routing seam and from the completion gate, because those
    /// are the two moments the answer can change: the last article of the last
    /// volume, and the verification that clears a par2-bearing job.
    pub(crate) async fn finalize_ready_direct_sets(&mut self, job_id: JobId) {
        if self.direct_store.sets_for(job_id).is_empty() {
            return;
        }
        // The gate re-arm, and deliberately **before** the PAR2 wait: the
        // re-read is about the member gates, not about verification, and
        // running it at the download/verify boundary means a par2-bearing set
        // is already gate-passed the moment its job's verification concludes. A
        // set still receiving articles is skipped — its unwritten ranges are
        // holes, not coverage to verify.
        let seeded: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| set.all_volumes_complete() && set.has_restart_seeded_coverage())
            .map(|(index, _)| index)
            .collect();
        for set_index in seeded {
            self.rearm_restart_seeded_gates(job_id, set_index).await;
        }
        if self.direct_finalization_waits_for_par2(job_id) || self.par3_verification_pending(job_id)
        {
            return;
        }
        // Damage on record and no PAR2 verdict left to answer it.
        //
        // Reaching here means the job is verified, bypassed, or carries no
        // recovery set that will ever be parsed — so a set still holding a
        // part-checksum mismatch is holding a question nothing can answer. It
        // never becomes ready (the member gate stalls by design), so without
        // this it would sit routed and uncommitted for the life of the job.
        // Demoting under the routing gate's own reason hands the archive to the
        // conventional path, which reads the same bytes and reports the same
        // CRC failure with the diagnostics that path has always had.
        let unanswerable: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| {
                !set.is_demoted()
                    && !set.is_finalized()
                    && set.all_volumes_complete()
                    // Native completion and router settlement are separate
                    // actor steps. The latter still owns this damage verdict.
                    && !set.router.awaits_par3_verdict()
                    && !set.router.damaged_volumes().is_empty()
            })
            .map(|(index, _)| index)
            .collect();
        for set_index in unanswerable {
            warn!(
                job_id = job_id.0,
                set_index,
                "a direct set's volume failed its archive part checksum and no PAR2 verdict \
                 is coming; demoting so the conventional path owns the archive"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::PartChecksumMismatch)
                .await;
        }
        let ready: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| set.ready_to_finalize())
            .map(|(index, _)| index)
            .collect();
        for set_index in ready {
            self.finalize_direct_set(job_id, set_index).await;
        }
        // The last set of a job finalizing is one of the two moments the answer
        // to "can anything still read a retained image" changes.
        self.release_retained_direct_volumes(job_id).await;
    }

    /// Whether a direct set must keep its envelopes and partials because the
    /// job's PAR2 verification has not concluded.
    ///
    /// Finalization renames the partials to their destinations and deletes the
    /// envelopes, which together *are* the virtual volume image: after it,
    /// nothing can answer a PAR2 read about a source volume, and nothing can
    /// reconstruct one for a demotion either. A par2-bearing set therefore
    /// waits — routed, gated, byte-complete, but uncommitted — until the job is
    /// verified, bypassed, or has no parsed PAR2 set to verify against.
    ///
    /// The release conditions are the completion gate's own, so a job that will
    /// never verify releases rather than waiting for something that is not
    /// coming — which matters because PAR2 is posted last and downloaded last:
    /// at the moment a set's final volume lands there is usually **no parsed
    /// PAR2 set yet**, and "no set" must mean "not yet" while an article can
    /// still arrive, and "never" once the download pipeline has drained.
    pub(super) fn direct_finalization_waits_for_par2(&self, job_id: JobId) -> bool {
        if !self.job_spec_has_par2_file(job_id) {
            return false;
        }
        if self.par2_bypassed.contains(&job_id) || self.par2_verified.contains(&job_id) {
            return false;
        }
        // The aggregate remains open until every servable recovery set has
        // settled, so any such set must retain direct source bytes for its own
        // pass rather than letting an earlier set commit them away.
        if !self.par2_servable_set_ids(job_id).is_empty() {
            return true;
        }
        self.job_has_pending_download_pipeline_work(job_id)
    }

    /// Polls the automatic barrier triggers for every live set. Called from the
    /// orchestrator's existing periodic seam.
    pub(crate) async fn poll_direct_store_barriers(&mut self) {
        let now = Instant::now();
        for job_id in self.direct_store.active_jobs() {
            // Polled once per orchestrator turn, so the common answer is
            // "nothing due": decide that without allocating.
            let sets = self.direct_store.sets_for(job_id);
            if !sets.iter().any(|set| set.due(now).is_some()) {
                continue;
            }
            let due: Vec<(usize, super::super::barrier::BarrierTrigger)> = sets
                .iter()
                .enumerate()
                .filter_map(|(index, set)| set.due(now).map(|trigger| (index, trigger)))
                .collect();
            for (set_index, trigger) in due {
                self.run_direct_barrier(job_id, set_index, trigger).await;
            }
        }
    }

    /// Demands a barrier for every live set of every job. Shutdown's entry
    /// point: a demanded barrier is always attempted, however many have just
    /// failed, so the last interval's work is not lost for free.
    pub(crate) async fn demand_direct_store_barriers_for_all_jobs(
        &mut self,
        demand: BarrierDemand,
    ) {
        for job_id in self.direct_store.active_jobs() {
            self.demand_direct_store_barriers(job_id, demand).await;
        }
    }

    /// Demands a barrier for every live set of a job — pause, shutdown, phase
    /// change, demotion and finalization all go through here.
    pub(crate) async fn demand_direct_store_barriers(
        &mut self,
        job_id: JobId,
        demand: BarrierDemand,
    ) {
        let indices: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted())
            .map(|(index, _)| index)
            .collect();
        for set_index in indices {
            self.run_direct_barrier(
                job_id,
                set_index,
                super::super::barrier::BarrierTrigger::Demand(demand),
            )
            .await;
        }
    }

    pub(in crate::pipeline) async fn run_direct_barrier(
        &mut self,
        job_id: JobId,
        set_index: usize,
        trigger: super::super::barrier::BarrierTrigger,
    ) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.router.repair_batch_in_progress() {
            return;
        }
        // Read before the barrier runs, which resets it. Two numbers, because
        // the interesting one is the second: the barrier's 256 MiB trigger is
        // checked per routed batch, so anything above it is the overshoot the
        // barrier bounds to "one decoded write batch" — and an overshoot that
        // starts tracking set size instead is the shape that regression looks
        // like.
        let dirty_bytes = set.dirty_bytes();
        // Relative name and absolute path together, straight from the set. An
        // earlier shape recovered the relative name by stripping the working
        // directory off the absolute path; with member payload under the
        // staging root and envelopes under the working directory there is no
        // single prefix to strip, and a silent `strip_prefix` failure would
        // have dropped exactly the payload destinations from the sync set.
        let touched = set.touched_paths();

        // Every sync is queued to its owner thread before any of them is
        // awaited. Envelope v2 made this set `members + volumes` rather than
        // two, and one `await` per destination serialized that many independent
        // fsyncs on the pipeline task; the barrier's contract only asks that
        // they have all completed before it persists, not that they happened
        // one after another.
        let paths: Vec<PathBuf> = touched.iter().map(|(_, path)| path.clone()).collect();
        let outcomes = crate::pipeline::orchestrator::sync_direct_destinations(paths).await;
        let results: HashMap<String, Result<(), String>> = touched
            .into_iter()
            .zip(outcomes)
            .map(|((relative, _), outcome)| (relative, outcome.map_err(|error| error.to_string())))
            .collect();

        let mut drain = InlineDrain;
        let mut sync = PreSyncedDestinations { results };
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        let now = Instant::now();
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        match set.run_barrier(trigger, now, &mut drain, &mut sync, &mut persist) {
            Some(Ok(report)) => {
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.snapshot_bytes",
                    report.snapshot_bytes as u64,
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.dirty_bytes",
                    dirty_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.overshoot_bytes",
                    dirty_bytes.saturating_sub(super::super::barrier::BARRIER_DIRTY_BYTES),
                );
                crate::runtime::perf_probe::record_value(
                    "direct_store.barrier.synced_destinations",
                    report.synced_destinations as u64,
                );
                debug!(
                    job_id = job_id.0,
                    generation = report.generation,
                    synced = report.synced_destinations,
                    "direct-store coverage barrier committed"
                );
            }
            Some(Err(error)) => {
                warn!(job_id = job_id.0, error = %error, "direct-store coverage barrier failed");
            }
            None => {}
        }
    }

    /// Commits a finished set: every member's partial becomes its destination
    /// through the extractor's own path resolution, and the set is marked
    /// extracted so the `Extracting` phase is pure bookkeeping.
    ///
    /// # The phase looks instant, and that is the documented behaviour
    ///
    /// There is nothing left to extract here — the payload has been at its
    /// destination since the articles arrived — so `Extracting` completes
    /// immediately and may not be visible at all. The settled answer to that:
    /// **document it, add no synthetic delay, and change no GraphQL surface.**
    /// The README carries the user-facing wording; the rule for this function
    /// is that it must not slow down, and must not emit a phase it did not
    /// really run, to make the UI look more familiar. A set that demotes
    /// reports a real extraction phase because it really runs one.
    pub(super) async fn finalize_direct_set(&mut self, job_id: JobId, set_index: usize) {
        self.run_direct_barrier(
            job_id,
            set_index,
            super::super::barrier::BarrierTrigger::Demand(BarrierDemand::Finalization),
        )
        .await;

        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let set_name = set.set_name().to_string();
        // Both are the set's own working files and both are meaningless once the
        // members are committed, but they part company under retention: the
        // envelopes *are* the virtual volume image and can be asked to outlive
        // finalization, while repair scratch is a materialized write target
        // nothing reads afterwards. A repair that ran earlier in this job has
        // already deleted its own.
        let envelopes = set.plan().envelope_paths();
        let repair_scratch = set.plan().repair_paths();
        // `member_partials` is in **archive order** — `(first volume, physical
        // offset)` — and the commit loop below walks it in that order, so two
        // members whose names sanitize to the same destination overwrite each
        // other exactly the way the incremental extractor makes them overwrite
        // each other: last one in the archive wins.
        let unpacked_sizes: HashMap<String, u64> =
            set.router.member_digest_entries().into_iter().collect();
        let members: Vec<(String, u64, PathBuf, PathBuf)> = set
            .router
            .member_partials()
            .into_iter()
            .filter_map(|(_, name, partial)| {
                let destination = set.plan().member_output_path(name).ok()?;
                // Both under the staging root, so the commit below is a
                // same-directory rename — never the cross-device rename the
                // working-dir-relative shape produced on a split-volume
                // install.
                Some((
                    name.to_string(),
                    unpacked_sizes.get(name).copied().unwrap_or(0),
                    set.plan().destination_path(partial),
                    destination,
                ))
            })
            .collect();
        // Extractor-claimed names only: a sibling direct set that finalized the
        // same member *name* is rename-order semantics, not a second checkpoint
        // system owning this member (see `extraction_claimed_members`).
        let extraction_claimed = self.extraction_claimed_members(job_id);
        set.assert_not_extraction_owned(&extraction_claimed);

        // The member tolerance, and strictly **before** the commit loop
        // below. The extraction reads the *virtual volumes*, which are the
        // envelopes overlaid with the members' `.direct.partial`s — so every
        // one of those files has to still be where the provider says it is.
        // Running it after the renames pointed the provider's partial map at
        // paths that had just been renamed away, turning every stored member's
        // extent into a hole: it happened to work only while a tolerated
        // member's header walk and decode never read through a stored extent,
        // and its failure mode was a demotion that could no longer reconstruct,
        // i.e. a full redownload.
        //
        // Nothing here needs the commit to have happened: the overwrite refusal
        // compares `plan().member_output_path` against the tolerated
        // destinations, which is derived from the layout and not from the
        // filesystem. It still has to run before the envelopes are deleted.
        let tolerated_directories = match self.extract_tolerated_members(job_id, set_index).await {
            Ok(Some(extracted)) => {
                for name in extracted.members {
                    self.record_direct_extracted(job_id, name);
                }
                extracted.directories
            }
            // Detached to a blocking worker. The set stays routed, gated and
            // uncommitted; the ticket's completion re-enters
            // `finalize_ready_direct_sets`, which reaches here again with the
            // result parked and takes it.
            Ok(None) => return,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to extract a tolerated member from the virtual volumes; demoting the set"
                );
                self.demote_direct_set(
                    job_id,
                    set_index,
                    DemotionReason::ToleratedExtractionFailed,
                )
                .await;
                return;
            }
        };

        // A failure here leaves the set neither committed nor abandoned: its
        // partials still hold every verified byte, but nothing downstream will
        // ever look at them again, so the job would sit in `Extracting`
        // forever. Demote instead — the volumes are refetched and the ordinary
        // extractor produces the same member (nit).
        //
        // A failure **part way through the loop** leaves the members before it
        // already renamed to their destinations, and the demotion then deletes
        // the partials of the ones after it and refetches every volume of the
        // set. The already-committed members are overwritten by the extractor
        // with byte-identical content, so the outcome is correct and the cost is
        // one wasted extraction of the members that had already landed.
        // Reviewed and accepted: unwinding the renames would mean moving
        // finished output back into scratch paths on a path that is already the
        // unhappy one, and the alternative — staging every rename and
        // committing them together — needs a directory-level atomic swap the
        // filesystem does not offer.
        for (name, unpacked_size, partial, destination) in &members {
            crate::pipeline::release_cached_write_handle(partial);
            if let Some(parent) = destination.parent()
                && let Err(error) = tokio::fs::create_dir_all(parent).await
            {
                warn!(job_id = job_id.0, error = %error, "failed to create direct-store destination directory; demoting the set");
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed)
                    .await;
                return;
            }
            // A zero-length stored member never had a byte routed for it, so it
            // has no partial to rename — but the archive declares the file and
            // the conventional extractor creates it, so finalization does too,
            // in the same archive order as every other member.
            let committed = match tokio::fs::rename(partial, destination).await {
                Err(error)
                    if *unpacked_size == 0 && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    tokio::fs::File::create(destination).await.map(drop)
                }
                other => other,
            };
            if let Err(error) = committed {
                warn!(
                    job_id = job_id.0,
                    member = %name,
                    error = %error,
                    "failed to commit a direct-store member to its destination; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::FinalizationFailed)
                    .await;
                return;
            }
            self.record_direct_extracted(job_id, name.clone());
        }

        // The archive's directory metadata, restored **last**. Every rename
        // above landed a member inside one of these directories and bumped its
        // mtime, so this is the first moment a restored time survives — and the
        // conventional extractor reaches the same state for the same reason,
        // because `rar` writes its directory headers after the files they hold.
        //
        // A refusal here is a warning, not a demotion: the directory itself
        // exists and every member is committed, so throwing the whole set away
        // to redownload it for a timestamp would cost far more than the
        // timestamp is worth. The conventional path treats the same failure as
        // fatal to *that member*, which for a directory is the same nothing.
        for (info, path) in &tolerated_directories {
            if let Err(error) =
                crate::pipeline::extraction::apply_rar_member_filesystem_metadata(info, path)
            {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    path = %path.display(),
                    error = %error,
                    "failed to restore a direct-store directory's archive metadata"
                );
            }
        }

        for scratch in &repair_scratch {
            crate::pipeline::release_cached_write_handle(scratch);
            let _ = tokio::fs::remove_file(scratch).await;
        }
        // The set's members are at their destinations now, which is the earliest
        // moment the retained image can point at them and the last moment its
        // coverage is still readable — `retire` below resets the controller.
        if self.retain_finalized_direct_volumes(job_id, set_index) {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                "keeping a finalized direct set's envelopes so a live neighbour's PAR2 repair \
                 can still read its source volumes"
            );
        } else {
            for envelope in &envelopes {
                crate::pipeline::release_cached_write_handle(envelope);
                let _ = tokio::fs::remove_file(envelope).await;
            }
        }
        // The scratch dies with the set, and its high-water is reported
        // separately from RAM so the disk claim stays legible against the 1.05×
        // acceptance target.
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            let scratch_bytes = set.router.scratch_bytes();
            if scratch_bytes > 0 {
                crate::runtime::perf_probe::record_value(
                    "direct_store.holds.scratch_bytes",
                    scratch_bytes,
                );
            }
            set.router.discard_scratch();
        }

        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a direct-store checkpoint");
        }
        // The other end of the direct phase, and the same rule the demotion
        // applies: the commit above renamed the member partials to their
        // destinations and (unless a live neighbour still needs to read them)
        // deleted the envelopes, so the virtual volume image the grid's claims
        // describe has been taken apart. A claim that outlived it would say a
        // *file* is intact when nothing can be read to check, and a later pass
        // would offer its slices to `plan_repair` as input it cannot open.
        // Retiring it costs at most the read a finalized volume always cost.
        let finalized_volume_files: Vec<NzbFileId> = self
            .direct_store
            .set(job_id, set_index)
            .map(|set| {
                set.plan()
                    .volumes
                    .values()
                    .map(|file_index| NzbFileId {
                        job_id,
                        file_index: *file_index,
                    })
                    .collect()
            })
            .unwrap_or_default();
        for file_id in finalized_volume_files {
            self.block_crcs.forget_file(file_id);
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.mark_finalized();
        }
        #[cfg(test)]
        {
            // Sticky, because the status is not observable after the fact: a set
            // can finalize, let its job complete and have its whole runtime
            // pruned inside a single completion check, so a test sampling the
            // set list between calls sees `Routing` and then nothing at all.
            self.direct_store.finalized_sets += 1;
        }
        self.extracted_archives
            .entry(job_id)
            .or_default()
            .insert(set_name.clone());
        crate::runtime::perf_probe::record(
            "direct_store.set.finalized",
            std::time::Duration::from_nanos(1),
        );
        self.metrics
            .direct_sets_finalized_direct
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            members = members.len(),
            "direct-store set finalized without materializing a volume"
        );
    }

    /// Keeps a finalizing set's virtual volume image alive past its own commit,
    /// when the job's PAR2 story can still need it.
    ///
    /// # The gap this closes
    ///
    /// Finalization renames a set's member partials to their destinations and
    /// deletes its per-volume envelopes, so nothing can serve its source volumes
    /// afterwards. If the job's recovery set also covers a **second** direct set
    /// that is later found damaged, Reed–Solomon needs the surviving input
    /// slices from *every* file it describes — the finalized set's volumes
    /// included — and `execute_repair` fails on the ones it cannot open. The two
    /// halves were mutually exclusive: with the finalized set's volumes absent
    /// the neighbour could not repair, and materializing them under their own
    /// names is the whole thing direct-store exists not to do.
    ///
    /// Retaining is the narrow answer. The bytes are already on disk twice over
    /// — the envelope holds every non-member byte at its true physical offset,
    /// and the member bytes are byte-identical at their destinations, because a
    /// commit is a rename — so the image needs no reconstruction, only a pointer
    /// swap and a stay of execution for the envelopes.
    ///
    /// # What is *not* retained
    ///
    /// - a job with no PAR2 file: there is no repair to serve;
    /// - a set with no **live** neighbour: nothing left in this job can ask, and
    ///   the release sweep below deletes what the last one held;
    /// - an **encrypted** set that cannot reproduce its posted bytes. One that
    ///   can is retained like any other: a commit is a rename, so
    ///   the overlay re-encrypts out of the committed member exactly as it did
    ///   out of the partial;
    /// - an image with a hole in it — see
    ///   [`DirectSet::retain_finalized_volumes`].
    ///
    /// A set that retains nothing keeps today's behaviour exactly, and
    /// `forgive_finalized_direct_volumes` keeps excusing its absent volumes.
    pub(super) fn retain_finalized_direct_volumes(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> bool {
        if !self.job_spec_has_par2_file(job_id) {
            return false;
        }
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return false;
        };
        if set.router.posted_bytes_unavailable() {
            return false;
        }
        // A neighbour that can still reach the repair path. Demoted is terminal
        // for this purpose too: a demoted set's volumes go back on disk and its
        // repair is the filesystem-bound `Par2Repairer`'s, which reads no
        // overlay at all.
        let has_live_neighbour =
            self.direct_store
                .sets_for(job_id)
                .iter()
                .enumerate()
                .any(|(index, other)| {
                    index != set_index && !other.is_finalized() && !other.is_demoted()
                });
        if !has_live_neighbour {
            return false;
        }

        // The same lengths `direct_par2_overlay` would have derived, captured
        // here because the assembly is the only place a virtual volume's length
        // lives and a retained image has to stop depending on it.
        let mut lengths = std::collections::BTreeMap::new();
        for (volume_index, file_index) in &set.plan().volumes {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let received = self
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .map(|file| file.received_bytes())
                .unwrap_or(0);
            lengths.insert(
                *volume_index,
                set.virtual_volume_len(*volume_index, received),
            );
        }
        let retained = self
            .direct_store
            .set_mut(job_id, set_index)
            .is_some_and(|set| set.retain_finalized_volumes(&lengths));
        crate::runtime::perf_probe::record_owned(
            format!(
                "direct_store.finalized_retained.{}",
                if retained { "kept" } else { "refused" }
            ),
            std::time::Duration::from_nanos(1),
        );
        retained
    }

    /// Deletes what [`Self::retain_finalized_direct_volumes`] kept, once the job
    /// has no live direct set left to ask for it.
    ///
    /// The window is deliberately the job's *direct* sets rather than the job:
    /// the only reader of a retained image is the repair behind
    /// [`Self::resolve_direct_sets_before_par2_repairer`], which runs for live
    /// sets only, so the moment the last one finalizes or demotes there is
    /// nothing left that can read one. Called from the two seams that can change
    /// that answer — a set finalizing and a set demoting — so the deferral costs
    /// one directory of envelopes for one job for the span between them and not
    /// a byte longer.
    ///
    /// Anything a crash leaves behind is swept at restart: a finalized set
    /// retired its checkpoint row, so restore rebuilds it fresh, claims none of
    /// its envelopes and `sweep_orphan_direct_files` deletes every one of them.
    /// Nothing about retention is persisted, and nothing needs to be.
    pub(super) async fn release_retained_direct_volumes(&mut self, job_id: JobId) {
        let sets = self.direct_store.sets_for(job_id);
        if sets.iter().all(|set| set.retained_volumes().is_none()) {
            return;
        }
        if sets
            .iter()
            .any(|set| !set.is_finalized() && !set.is_demoted())
        {
            return;
        }
        let retained: Vec<usize> = sets
            .iter()
            .enumerate()
            .filter(|(_, set)| set.retained_volumes().is_some())
            .map(|(index, _)| index)
            .collect();
        for set_index in retained {
            let Some(set) = self.direct_store.set(job_id, set_index) else {
                continue;
            };
            let set_name = set.set_name().to_string();
            let envelopes = set.plan().envelope_paths();
            for envelope in &envelopes {
                crate::pipeline::release_cached_write_handle(envelope);
                let _ = tokio::fs::remove_file(envelope).await;
            }
            if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
                set.release_retained_volumes();
            }
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                "released a finalized direct set's retained envelopes"
            );
        }
    }

    /// The member tolerance: extracts **only** the tolerated member indices,
    /// through the hybrid virtual-volume provider, straight to their
    /// destinations.
    ///
    /// # This is the tolerance's whole remaining cost
    ///
    /// One blocking task, once, after the set's last article. Nothing here is
    /// I/O amplification — the tolerated bytes are read once out of the
    /// envelope they were routed to, and the stored members are not touched at
    /// all — but it is a *serial tail*, and with the tolerance's size ceiling
    /// gone the list it walks can be large. The conventional incremental
    /// scheduler already runs the same decode volume by volume as chains close;
    /// feeding it this provider instead of files is the seam that would retire
    /// the tail, and it is not opened here.
    ///
    /// Returns the raw member names that were produced, for
    /// `extracted_members`. The distinction that separates this from the
    /// out-of-scope per-member physical fallback is that it extracts a strict
    /// *subset*: a direct-routed `Store` member is never re-extracted and never
    /// overwritten here, which is checked rather than assumed — a tolerated
    /// member resolving onto a stored member's destination is refused, and the
    /// set demotes.
    ///
    /// # Directory members are created, never extracted
    ///
    /// A directory entry is a dataless header. It is created through the same
    /// [`ExtractionRoot`] the conventional extractor creates one through — same
    /// path validator, same budget, same "already there is fine, a
    /// non-directory in the way is not" rule — and never through
    /// `File::create`, which would leave an empty *file* named like the
    /// directory the archive describes.
    ///
    /// Its **metadata** is not applied here: every later file that lands inside
    /// it bumps its mtime, and the commit loop that renames this set's stored
    /// members runs after this call. The [`unrar_rs::MemberInfo`] is carried
    /// back to [`Self::finalize_direct_set`] instead, which applies it once
    /// every member is at its destination.
    ///
    /// **Off the pipeline task.** The tolerance no longer caps a member's size,
    /// so this decode can run as long as a conventional extraction of the same
    /// bytes, and it reads the virtual volumes off disk. The first call for a
    /// set snapshots everything the decode needs and hands it to a blocking
    /// worker as a [`DirectToleratedWork`] ticket, returning `Ok(None)`; the
    /// ticket's completion re-enters [`Self::finalize_ready_direct_sets`], and
    /// the call that finds its result parked returns `Ok(Some(_))`. One ticket
    /// per job: a sibling set that is ready at the same time waits its turn,
    /// which is the same serialization the commit loop imposes anyway.
    pub(super) async fn extract_tolerated_members(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Result<Option<ToleratedExtraction>, String> {
        let (set_name, names) = {
            let Some(set) = self.direct_store.set(job_id, set_index) else {
                return Ok(Some(ToleratedExtraction::default()));
            };
            (set.set_name().to_string(), set.router.tolerated_members())
        };
        if names.is_empty() {
            return Ok(Some(ToleratedExtraction::default()));
        }
        // A finished ticket for this set: the pass that submitted it is this
        // one, re-entered by the ticket's completion.
        if let Some((result_set_index, result)) = self.direct_tolerated_results.remove(&job_id) {
            if result_set_index == set_index {
                self.direct_tolerated_in_flight.remove(&job_id);
                let extracted = result?;
                crate::runtime::perf_probe::record(
                    "direct_store.tolerated_members_extracted",
                    std::time::Duration::from_nanos(1),
                );
                if !extracted.directories.is_empty() {
                    crate::runtime::perf_probe::record_value(
                        "direct_store.tolerated_directories",
                        extracted.directories.len() as u64,
                    );
                }
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    members = extracted.members.len(),
                    directories = extracted.directories.len(),
                    "extracted tolerated members from the virtual volumes"
                );
                return Ok(Some(extracted));
            }
            self.direct_tolerated_results
                .insert(job_id, (result_set_index, result));
        }
        if let Some(in_flight) = self.direct_tolerated_in_flight.get(&job_id) {
            // This set's own ticket still running, or a sibling set's whose
            // result is parked for a pass that has not reached it yet. Either
            // way this pass waits; the completion that lands re-enters it.
            debug!(
                job_id = job_id.0,
                set_index,
                in_flight_set_index = in_flight.set_index,
                work_id = in_flight.work_id,
                "a direct tolerated extraction is already in flight for this job; waiting"
            );
            return Ok(None);
        }
        let staging = self.extraction_staging_dir(job_id);
        let extraction_budget = self.extraction_budget(job_id, &staging)?;
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Ok(Some(ToleratedExtraction::default()));
        };
        // An `-hp` set's virtual volumes are as header-encrypted as
        // the posted ones, so this extraction cannot even *open* the archive
        // without the key the router proved — and for a `-p` set a tolerated
        // member's data is encrypted too. `None` for a plaintext set, which is
        // every set that reached here before encryption existed.
        let password = set.router.archive_password().map(str::to_string);

        // The ordering invariant this extraction depends on, stated where it is
        // depended on. The provider serves every stored member's extent out of
        // its `.direct.partial`; the commit loop renames those away and records
        // the member in `extracted_members` as it goes. Running after it
        // therefore hands the header walk and the decode a volume whose stored
        // extents are all holes, and the failure path costs a full redownload.
        debug_assert!(
            !self
                .extraction_claimed_members(job_id)
                .iter()
                .any(|committed| {
                    set.router
                        .member_partials()
                        .iter()
                        .any(|(_, name, _)| committed == *name)
                }),
            "a stored member of {set_name} was committed before its set's tolerated \
             members were extracted; the virtual volumes no longer resolve"
        );

        // Every stored member's *eventual* destination, so the assertion below
        // compares resolved paths rather than raw header names — two names can
        // sanitize onto one path, which is exactly the collision that would let
        // a tolerated member overwrite verified direct output. Derived from the
        // layout, not from the filesystem, which is what lets this run before
        // the commit loop renames anything.
        let mut stored_outputs: HashSet<PathBuf> = HashSet::new();
        for (_, name, _) in set.router.member_partials() {
            if let Ok(destination) = set.plan().member_output_path(name) {
                stored_outputs.insert(destination);
            }
        }

        let mut targets: Vec<ToleratedTarget> = Vec::with_capacity(names.len());
        for member in &names {
            let name = &member.name;
            // The same validator the conventional extractor refuses an unsafe
            // member path with, reached through the same resolution a stored
            // member's destination is: a directory entry whose name escapes the
            // root is refused here and the set demotes, which is the refusal the
            // conventional path would reach over the materialized volumes.
            let destination = set
                .plan()
                .member_output_path(name)
                .map_err(|()| format!("tolerated member '{name}' has no safe destination"))?;
            if stored_outputs.contains(&destination) {
                return Err(format!(
                    "tolerated member '{name}' resolves onto a direct-store output at {}",
                    destination.display()
                ));
            }
            // Root-relative, because `ExtractionRoot` is a `cap-std` directory
            // handle and every path it takes is relative to it. Derived from the
            // destination rather than re-sanitized, so the directory this
            // creates and the file a stored member commits to can never resolve
            // through two different rules.
            let relative = destination
                .strip_prefix(&staging)
                .map_err(|_| {
                    format!("tolerated member '{name}' resolved outside the staging root")
                })?
                .to_path_buf();
            targets.push(ToleratedTarget {
                name: name.clone(),
                destination,
                relative,
                is_directory: member.is_directory,
            });
        }
        debug_assert!(
            targets
                .iter()
                .all(|target| !stored_outputs.contains(&target.destination)),
            "a tolerated member of {set_name} would overwrite a direct-store output"
        );

        // The volumes' decoded lengths, which only the download layer knows: a
        // virtual volume has no file whose length could be read instead.
        let mut lengths = std::collections::BTreeMap::new();
        for (volume_index, file_index) in &set.plan().volumes {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let received = self
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .map(|file| file.received_bytes())
                .unwrap_or(0);
            lengths.insert(
                *volume_index,
                set.virtual_volume_len(*volume_index, received),
            );
        }
        let first_volume = *lengths
            .keys()
            .next()
            .ok_or_else(|| format!("direct set '{set_name}' has no volumes to extract from"))?;
        let provider = set.virtual_provider(&lengths);
        let other_volumes: Vec<u32> = lengths
            .keys()
            .copied()
            .filter(|volume_index| *volume_index != first_volume)
            .collect();
        let extraction_memory_limit = self.extraction_limits.max_memory_bytes;
        let extraction_root = staging.clone();

        self.next_direct_tolerated_work_id = self.next_direct_tolerated_work_id.wrapping_add(1);
        let work_id = self.next_direct_tolerated_work_id;
        self.direct_tolerated_in_flight.insert(
            job_id,
            DirectToleratedWork {
                work_id,
                set_index,
                submitted_at: Instant::now(),
            },
        );
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            work_id,
            members = targets.len(),
            "submitting a direct tolerated-extraction ticket"
        );
        let done_tx = self.direct_tolerated_done_tx.clone();
        tokio::spawn(async move {
            let joined = tokio::task::spawn_blocking(move || {
                let reader = provider
                    .open(first_volume)
                    .ok_or_else(|| format!("virtual volume {first_volume} is not registered"))?;
                let mut archive = match password.as_deref() {
                    Some(password) => unrar_rs::RarArchive::open_with_password(reader, password),
                    None => unrar_rs::RarArchive::open(reader),
                }
                .map_err(|error| format!("failed to open the virtual archive: {error}"))?;
                // The same decode ceilings the incremental extractor applies.
                // Nothing about a tolerated member bounds the *declared* dictionary
                // in a hostile header, so the same admission runs here.
                let max_dict_bytes =
                    crate::pipeline::extraction::apply_server_rar_limits_with_memory_limit(
                        &mut archive,
                        extraction_memory_limit,
                    );
                for volume_index in other_volumes {
                    let Some(reader) = provider.open(volume_index) else {
                        continue;
                    };
                    archive
                        .add_volume(volume_index as usize, Box::new(reader))
                        .map_err(|error| {
                            format!("failed to add virtual volume {volume_index}: {error}")
                        })?;
                }
                crate::pipeline::extraction::ensure_rar_dictionary_within_limit(
                    &archive,
                    max_dict_bytes,
                )
                .map_err(|error| format!("RAR dictionary admission failed: {error}"))?;
                let _memory_permit = extraction_budget
                    .reserve_memory_wait(crate::pipeline::extraction::rar_decoder_memory_bytes(
                        &archive,
                    ))
                    .map_err(|error| format!("RAR decoder memory admission failed: {error}"))?;
                let options = unrar_rs::ExtractOptions {
                    verify: true,
                    password: password.clone(),
                    restore_owners: false,
                };
                // Every target goes through the sandboxed root now, files included,
                // so it is opened unconditionally. It used to be opened only for a
                // set with a directory entry, and the file arm wrote through a bare
                // `File::create` with `create_dir_all` parents: that skipped the
                // path validator, the entry accounting, and — the reason it can no
                // longer stand — the *byte* budget. While the tolerance carried at
                // most 256 MiB of declared unpacked size, an unbudgeted write was
                // bounded by that ceiling; with the ceiling gone, the only thing
                // that may bound a tolerated decode is the same
                // `JobExtractionBudget` the conventional extractor writes through.
                let root = crate::pipeline::extraction::ExtractionRoot::open(&extraction_root)?;
                let mut produced = Vec::with_capacity(targets.len());
                let mut directories = Vec::new();
                for target in &targets {
                    let name = &target.name;
                    let index = archive.find_member(name).ok_or_else(|| {
                        format!("tolerated member '{name}' is not in the archive")
                    })?;
                    if target.is_directory {
                        // The conventional extractor's own directory arm, byte for
                        // byte: create through the sandboxed root under the
                        // extraction budget, tolerating a directory that is already
                        // there — the routed members' parents were created when
                        // their `.direct.partial`s were prepared, so on an ordinary
                        // folder-tree set every one of these already exists.
                        root.create_dir(&target.relative, &extraction_budget)?;
                        let info = archive.member_info(index).ok_or_else(|| {
                            format!("tolerated directory '{name}' has no member metadata")
                        })?;
                        directories.push((info.clone(), target.destination.clone()));
                        produced.push(name.clone());
                        continue;
                    }
                    // Budgeted, and through the same root the directory arm uses:
                    // `create_file` validates the relative path, creates the
                    // parents as archive entries, and hands back a writer that
                    // charges every byte against the job's member, job-total and
                    // free-space limits. A rejection fails the tolerated extraction,
                    // which demotes the set to a conventional extraction that will
                    // meet the very same budget.
                    let mut file = root.create_file(&target.relative, &extraction_budget)?;
                    // The provider is the set's, keyed by the set's own volume
                    // indices, which is what the entry asks for — a member
                    // starting in volume 3 requests volume 3.
                    let written = crate::pipeline::extraction::rar_entry_via(
                        &mut archive,
                        index,
                        &provider,
                        &options,
                    )
                    .and_then(|entry| entry.copy_to(&mut file))
                    .map_err(|error| format!("failed to extract '{name}': {error}"))?;
                    // The tolerated half of the byte account: everything else a
                    // direct set produces is counted at the router as
                    // `direct_store.bytes.member`. Read against it to see how much
                    // of a mixed set the tolerance is carrying.
                    crate::runtime::perf_probe::record_value(
                        "direct_store.bytes.tolerated",
                        written,
                    );
                    produced.push(name.clone());
                }
                Ok::<ToleratedExtraction, String>(ToleratedExtraction {
                    members: produced,
                    directories,
                })
            })
            .await;
            let result = joined
                .map_err(|error| format!("tolerated extraction task panicked: {error}"))
                .and_then(|result| result);
            let _ = done_tx
                .send(DirectToleratedWorkDone {
                    job_id,
                    work_id,
                    set_index,
                    result,
                })
                .await;
        });
        Ok(None)
    }

    /// The tolerated-extraction ticket's completion, on the pipeline task.
    ///
    /// Parks the result for the finalization pass and re-enters that pass at
    /// once: the set the ticket belongs to is ready and waiting on nothing but
    /// this, and the completion check will not judge the job while the ticket
    /// is outstanding. A ticket the demotion or job-teardown seams already
    /// forgot is discarded by the fence.
    pub(in crate::pipeline) async fn handle_direct_tolerated_done(
        &mut self,
        done: DirectToleratedWorkDone,
    ) {
        let Some(in_flight) = self.direct_tolerated_in_flight.get(&done.job_id) else {
            return;
        };
        if in_flight.work_id != done.work_id || in_flight.set_index != done.set_index {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                "discarding a stale direct tolerated-extraction ticket"
            );
            return;
        }
        let elapsed = in_flight.submitted_at.elapsed();
        let outcome = match &done.result {
            Ok(_) => "extracted",
            Err(_) => "error",
        };
        info!(
            job_id = done.job_id.0,
            work_id = done.work_id,
            set_index = done.set_index,
            elapsed_ms = elapsed.as_millis() as u64,
            outcome,
            "direct tolerated-extraction ticket completed"
        );
        crate::runtime::perf_probe::record("direct_store.tolerated_extract", elapsed);
        if !self.jobs.contains_key(&done.job_id) {
            self.direct_tolerated_in_flight.remove(&done.job_id);
            return;
        }
        self.direct_tolerated_results
            .insert(done.job_id, (done.set_index, done.result));
        self.finalize_ready_direct_sets(done.job_id).await;
        self.schedule_job_completion_check(done.job_id);
    }
}
