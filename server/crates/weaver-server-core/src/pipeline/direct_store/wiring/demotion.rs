//! Continuation of the `impl Pipeline` block from `direct_store/wiring.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    /// Forgets a job's tolerated-extraction ticket and any parked result. The
    /// detached worker keeps running to its end; its done message then finds
    /// no taker and is discarded by the fence.
    pub(crate) fn forget_direct_tolerated_work(&mut self, job_id: JobId) {
        self.direct_tolerated_in_flight.remove(&job_id);
        self.direct_tolerated_results.remove(&job_id);
    }

    /// Abandons direct output for a set and hands its volumes back (the
    /// **archive-group demotion**, the transition that ends direct mode).
    ///
    /// Two shapes, and the first one is tried first:
    ///
    /// 1. **Reconstruction.** Every volume is rebuilt byte-exactly from the
    ///    envelope plus the member extents, its covered runs are verified against
    ///    the yEnc part-CRC composition, and only then are legacy floors and
    ///    completed-file rows persisted, the coverage row retired, and the
    ///    partials and envelopes deleted. Verified bytes are never refetched —
    ///    a volume the demotion caught mid-download comes back to the
    ///    conventional path with the prefix it had already received already
    ///    covered, so only the articles that never arrived are scheduled.
    ///    A run the sweep cannot vouch for costs that run's articles and
    ///    nothing else: not the rest of its volume, and not its siblings.
    /// 2. **Refetch** — the conservative form, and by now only for the two
    ///    refusals that are properties of the whole set and are raised before
    ///    a byte is swept: a set with no layout at all, and an encrypted set
    ///    whose posted bytes the overlay cannot reproduce. Neither leaves a
    ///    single range anything could vouch for, so the routed bytes are thrown
    ///    away and every article comes back off the wire. Everything that used
    ///    to reach here — a deleted envelope, a truncated partial, a covered
    ///    run whose CRC32 disagrees — is now handled inside the sweep, range by
    ///    range.
    ///
    /// Ordering is normative. Unlike *repair* over checkpoint-covered output —
    /// which deletes the checkpoint row **first**, because it is about to
    /// overwrite the very bytes the row claims — demotion retires the row as
    /// part of reconciliation, after the legacy state that replaces it is
    /// durable. Retiring first would leave a window where neither the direct
    /// coverage nor the legacy floors describe what is on disk.
    ///
    /// **This call does not wait for shape 1.** It marks the set demoted,
    /// clears the state a demotion invalidates, and hands the sweep to a
    /// detached worker as a ticket; the reconciliation — floors, rows,
    /// retirement, deletion, the completion replay — runs in
    /// [`Self::handle_direct_demotion_done`], on the pipeline task, in exactly
    /// the order above. Everything that must not observe the job in between is
    /// held off by the ticket: the completion check refuses to judge a job with
    /// one outstanding, and the PAR2 gate waits on the materializations this
    /// call has already begun. Shape 2 has no sweep to detach and finishes
    /// inline.
    pub(in crate::pipeline) async fn demote_direct_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        reason: DemotionReason,
    ) {
        self.demote_direct_set_with_handoff(job_id, set_index, reason, None)
            .await;
    }

    pub(super) async fn demote_direct_set_with_handoff(
        &mut self,
        job_id: JobId,
        set_index: usize,
        reason: DemotionReason,
        handoff: Option<SegmentId>,
    ) {
        let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
            return;
        };
        // One cleanup per set, and never for a finalized one. The original
        // guard read `is_demoted() && is_finalized()`, which two mutually
        // exclusive states can never both satisfy, so a finalized set could be
        // flipped to `Demoted` and have its committed members deleted out from
        // under a job that had already counted them.
        if !set.claim_demotion(reason) {
            if let Some(segment_id) = handoff {
                self.direct_store
                    .note_materialization_handoff(set_index, segment_id);
            }
            return;
        }
        let set_name = set.set_name().to_string();
        // A demoted set's volumes become real files and hand off to the
        // conventional repairer, which brings its own post-repair pass — so
        // any post-repair state this job is carrying for the direct gate is
        // no longer this set's business, and must not outlive the demotion
        // to describe bytes a different repair path now owns. Cleared for the
        // whole job rather than filtered to this set: the carry and the
        // ticket bookkeeping are job-scoped (a job serves one recovery set
        // through this gate at a time), so a demotion of any of its sets
        // invalidates whatever the gate was mid-resolving.
        self.direct_post_repair_carry.remove(&job_id);
        self.direct_post_repair_in_flight.remove(&job_id);
        self.direct_post_repair_results.remove(&job_id);
        // Likewise a tolerated extraction: it was reading the virtual volumes
        // this demotion is about to reconstruct and delete, and the members it
        // produced are the conventional extractor's to overwrite now.
        self.direct_tolerated_in_flight.remove(&job_id);
        self.direct_tolerated_results.remove(&job_id);
        if matches!(
            reason,
            DemotionReason::HoldsScratchCeiling | DemotionReason::HoldsScratchDiskReserve
        ) {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                scratch_bytes = set.router.scratch_bytes(),
                "direct-store demoting after a holds scratch cap event"
            );
        }
        // Every volume of this set is about to become a real file: either
        // reconstruction writes it from the routed bytes, or the refetch pulls
        // it back off the wire article by article — and in both cases the
        // conventional seam owns the feeds from here.
        //
        // The direct phase's grid state has to go *before* that can happen. It
        // describes a virtual volume assembled out of member partials and
        // envelopes, and the file the conventional path is about to fill is a
        // different image of the same coordinates: reconstruction may write a
        // shorter prefix than the direct phase claimed, and a refetch rewrites
        // ranges wholesale. Leaving it would let a block closed in one image
        // adjudicate bytes of the other — and worse, survive into
        // `in_stream_verified_par2_match`, whose whole job is to say a file need
        // not be read. Per file rather than per job: a job's other sets, and its
        // conventional files, are untouched by this demotion.
        let demoted_volume_files: Vec<NzbFileId> = set
            .plan()
            .volumes
            .values()
            .map(|file_index| NzbFileId {
                job_id,
                file_index: *file_index,
            })
            .collect();
        // Damage established before any recovery set has been asked. Recorded
        // as the *fact* rather than the reason, because the completion gate
        // reads it to refuse a stored set's "a clean decode proves integrity"
        // claim, and that refusal has to survive reasons being added or
        // retired. Recorded before the materialization below, so the volume
        // replay at the end of this function — which is what puts these files
        // in front of the extraction scheduler — cannot get there first.
        if reason.is_source_damage() {
            self.note_known_archive_set_damage(job_id, &set_name);
        }
        // A parked damaged-path verdict goes the way of the post-repair carry
        // above, and for the same reason: it describes volumes that were
        // virtual when it was reached and are about to become files the
        // conventional path writes. The next pass reads the set as it now is.
        self.clear_pending_par2_repairs_for_job(job_id);
        self.direct_store.begin_materialization(
            job_id,
            set_index,
            demoted_volume_files.iter().copied(),
        );
        if let Some(segment_id) = handoff {
            self.direct_store
                .note_materialization_handoff(set_index, segment_id);
        }
        for file_id in &demoted_volume_files {
            self.block_crcs.forget_file(*file_id);
        }

        crate::runtime::perf_probe::record_owned(
            format!("direct_store.demoted.{}", reason.metric()),
            std::time::Duration::from_nanos(1),
        );
        // Reported, not yet acted on: how many demotions could have been served
        // by the set's own virtual volumes, split from the ones that genuinely
        // need files on disk. Every set still materializes below; this is the
        // measurement that says what keeping the overlay would be worth.
        let volume_demand = reason.volume_demand();
        crate::runtime::perf_probe::record_owned(
            format!("direct_store.demoted.{volume_demand}"),
            std::time::Duration::from_nanos(1),
        );
        // Guarded by `claim_demotion` above, so this counts each set exactly
        // once; the per-reason breakdown lives in the perf-probe key and the
        // warn line.
        self.metrics
            .direct_sets_demoted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            job_id = job_id.0,
            set_name = %set_name,
            reason = reason.metric(),
            volumes = %volume_demand,
            "direct-store set demoted"
        );

        match self.prepare_demoted_set_sweep(job_id, set_index, &set_name, reason) {
            Ok(prepared) => {
                // Handed off, not run here. Everything below this point —
                // floors, rows, retirement, deletion, the completion replay —
                // happens on the pipeline task when the ticket lands, in
                // `apply_demoted_set_reconstruction`.
                self.submit_demoted_set_sweep(job_id, set_index, prepared);
            }
            Err(failure) => {
                crate::runtime::perf_probe::record_owned(
                    format!("direct_store.demote_refetch.{}", failure.metric()),
                    std::time::Duration::from_nanos(1),
                );
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    failure = %failure,
                    "direct-store reconstruction is not possible; refetching the set's volumes"
                );
                // Cheap by construction — it retires the row, deletes the
                // routed output and requeues; there is no sweep to detach — so
                // it stays on the actor and the handback tail runs at once.
                self.refetch_demoted_set(job_id, set_index).await;
                self.finish_demoted_set_handback(job_id, demoted_volume_files)
                    .await;
            }
        }
    }

    /// The tail of a demotion, once the set's volumes are files on disk.
    ///
    /// Reached from the refetch fallback immediately, and from the detached
    /// sweep's ticket when it lands.
    pub(super) async fn finish_demoted_set_handback(
        &mut self,
        job_id: JobId,
        volume_files: Vec<NzbFileId>,
    ) {
        // Re-enter every complete volume into the conventional completion
        // seam. While the set was direct, `refresh_archive_state_for_completed_file`
        // suppressed itself for these files at its own entry — a direct set
        // never enters the archive topology, and its extraction never needs
        // one. Demotion is the moment that stops being true: the volumes are
        // ordinary files now, and everything downstream of the conventional
        // decode-completion hook — classification, RAR volume facts, the
        // topology entry the extraction planner chains from — has never run
        // for any of them that completed while direct. Without this replay a
        // materialized volume is invisible to the topology forever: the plan
        // waits on a volume whose bytes sit complete on disk, and nothing
        // ever arrives to change its mind. The replay walks the same door a
        // conventional completion walks (`allow_probe` included), and the
        // hook's own guards skip files that are still incomplete — those
        // complete later through the decode path and get the hook naturally.
        for file_id in volume_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, true)
                .await;
        }
        // The other moment a retained image can lose its last possible reader: a
        // demoted set is repaired by the filesystem-bound `Par2Repairer`, which
        // reads no overlay, so a job whose last live set demotes has nothing
        // left that could open one.
        self.release_retained_direct_volumes(job_id).await;
    }

    /// The read-only half of the reconstruction path: everything the sweep and
    /// its reconciliation need, snapshotted off the set and the job without
    /// touching either.
    ///
    /// `Err` is reserved for the two refusals that are properties of the whole
    /// set and can be raised before a byte is swept — no layout, and an
    /// encrypted set whose posted bytes the overlay cannot reproduce. A run
    /// that fails *during* the sweep is reported inside
    /// [`ReconstructionSummary`] and costs only the articles its own bytes
    /// back: the volume keeps everything the sweep verified, and its siblings
    /// are untouched.
    pub(super) fn prepare_demoted_set_sweep(
        &mut self,
        job_id: JobId,
        set_index: usize,
        set_name: &str,
        reason: DemotionReason,
    ) -> Result<PreparedDemotedSweep, ReconstructionFailure> {
        // Read before the set is borrowed, and used twice below: to stop the
        // sweep truncating away an article its owner is writing right now, and
        // to stop the reconciliation requeueing one it already wrote.
        let handoffs: HashSet<SegmentId> = self
            .direct_store
            .pending_materializations(job_id)
            .into_iter()
            .find(|(index, _)| *index == set_index)
            .map(|(_, pending)| pending.handoffs)
            .unwrap_or_default();
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Err(ReconstructionFailure::NoLayout);
        };
        if set.router.member_partials().is_empty() {
            // Nothing was ever routed to a member, so there is nothing to
            // reconstruct *from* beyond headers. Refetching is both correct and
            // cheaper than materializing header-only volumes.
            return Err(ReconstructionFailure::NoLayout);
        }
        if set.router.posted_bytes_unavailable() {
            // The member partials hold **plaintext**; the volume
            // being rebuilt holds cipher, and the overlay is what turns one into
            // the other on the way out. This is the residual it cannot do — a
            // routed encrypted member with no declared cipher size, or one whose
            // tail padding never arrived whole — and the sweep must refuse
            // rather than write a volume that is right except for one block,
            // because the yEnc part CRCs it checks against would then refuse
            // *after* the write rather than before it.
            return Err(ReconstructionFailure::EncryptedPostedBytes);
        }
        let working_dir = set.plan().working_dir.clone();
        let volume_files: Vec<(u32, u32)> = set
            .plan()
            .volumes
            .iter()
            .map(|(volume_index, file_index)| (*volume_index, *file_index))
            .collect();

        // Decoded volume lengths and per-volume article geometry, both of which
        // only the download layer knows.
        let mut lengths = std::collections::BTreeMap::new();
        let mut targets = Vec::with_capacity(volume_files.len());
        let mut extents_by_volume = HashMap::new();
        for (volume_index, file_index) in &volume_files {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(ReconstructionFailure::NoLayout);
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                continue;
            };
            // The same name the conventional write path resolves, not the
            // assembly's raw one: a file whose identity was rewritten (a PAR2
            // canonical name, say) is about to be downloaded into *that* path,
            // and reconstructing into a different one would leave the refetch
            // filling a file with a hole where the rebuilt prefix should be.
            let filename = self.current_filename_for_file(job_id, file_asm);
            let received = file_asm.received_bytes();
            // Placed bytes only, deliberately. The provider can serve holds
            // too, but this sweep hands the set to the conventional path, whose
            // decode handoff owns the article that was routing when demotion
            // struck and whose targeted requeue owns every segment the atoms do
            // not wholly back; a hold materialized here would be written twice
            // and counted against a completion gate nothing then clears.
            let physical_coverage = set.volume_coverage(*volume_index);
            let crcs = set.volume_crc_runs(*volume_index);
            // Routing can demote after durably placing only part of the current
            // article. Keep that range provisional: the decode handoff owns the
            // current segment, and the targeted requeue below owns any other
            // segment that is not wholly backed by an article CRC atom.
            let coverage = crcs.materializable_coverage(&physical_coverage);
            let extents = set.segment_extents(*volume_index);
            // The sweep sets the file's length before it writes, to cut off a
            // stale tail an interrupted earlier attempt could have left above
            // the volume. It runs detached, though, and the decode seam writes
            // the handed-off article into the same file as soon as this
            // demotion returns — so the length has to reach that article too,
            // or whichever of the two goes second decides whether its bytes
            // survive. The sweep never *writes* the range: the coverage above
            // excludes it precisely because its owner is elsewhere.
            let handoff_end = handoffs
                .iter()
                .filter(|segment_id| segment_id.file_id == file_id)
                .filter_map(|segment_id| extents.get(&segment_id.segment_number))
                .map(|(offset, len)| offset.saturating_add(*len))
                .max()
                .unwrap_or(0);
            // A volume that never completed has no authoritative length; its
            // received bytes are the most that can be on disk. A routed range
            // above a hole can end later than that aggregate, though, so its
            // durable coverage is also a lower bound for this sweep.
            let len = set
                .virtual_volume_len(*volume_index, received)
                .max(physical_coverage.end())
                .max(handoff_end);
            lengths.insert(*volume_index, len);
            extents_by_volume.insert(*volume_index, extents);
            let path = working_dir.join(&filename);
            targets.push((
                *volume_index,
                *file_index,
                filename,
                VolumeReconstruction {
                    volume_index: *volume_index,
                    path,
                    len,
                    assembly_complete: file_asm.is_complete(),
                    covered: coverage,
                    crcs,
                    // `coverage` is already clipped to whole articles, so this
                    // never fires; it states that a floor is published over
                    // what this sweep writes and nothing unverified may sit
                    // under one.
                    partial_article: super::super::reconstruct::PartialArticle::Refuse,
                },
            ));
        }

        let provider = set.virtual_provider(&lengths);
        let plans: Vec<VolumeReconstruction> =
            targets.iter().map(|(_, _, _, plan)| plan.clone()).collect();
        let sparse = self.direct_store.sparse_marking();
        let volume_files: Vec<NzbFileId> = volume_files
            .iter()
            .map(|(_, file_index)| NzbFileId {
                job_id,
                file_index: *file_index,
            })
            .collect();
        Ok(PreparedDemotedSweep {
            provider,
            plans,
            sparse,
            plan: DemotedSweepPlan {
                set_name: set_name.to_string(),
                reason,
                targets,
                extents_by_volume,
                volume_files,
                handoffs,
            },
        })
    }

    /// Hands a prepared sweep to a detached worker and takes a ticket for it.
    ///
    /// The sweep itself is bounded only by the size of the archive, so it runs
    /// nowhere near the pipeline task: the demotion returns as soon as the
    /// ticket is recorded, and the job's other articles keep flowing.
    ///
    /// **The sweep owns the volume files while it runs.** An article of this
    /// set that decodes during that window takes the conventional write path —
    /// its bytes land in the volume file, its segment commits — and the
    /// reconciliation then resets and requeues it along with everything else
    /// the sweep did not vouch for. That is one wasted fetch per article that
    /// happened to be in flight at the demotion instant, and it is deliberate:
    /// the sweep is the only writer that knows what the rebuilt image contains,
    /// down to removing a volume file outright when it could vouch for nothing
    /// in it, so nothing else may claim a range of it.
    pub(super) fn submit_demoted_set_sweep(
        &mut self,
        job_id: JobId,
        set_index: usize,
        prepared: PreparedDemotedSweep,
    ) {
        let PreparedDemotedSweep {
            provider,
            plans,
            sparse,
            plan,
        } = prepared;
        self.next_direct_demotion_work_id = self.next_direct_demotion_work_id.wrapping_add(1);
        let work_id = self.next_direct_demotion_work_id;
        let volumes = plans.len();
        let set_name = plan.set_name.clone();
        let reason = plan.reason.metric();
        self.direct_demotion_in_flight
            .entry(job_id)
            .or_default()
            .insert(
                set_index,
                DirectDemotionWork {
                    work_id,
                    submitted_at: Instant::now(),
                    plan,
                },
            );
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            work_id,
            volumes,
            reason,
            "submitting a direct demotion reconstruction ticket"
        );
        let done_tx = self.direct_demotion_done_tx.clone();
        tokio::spawn(async move {
            let rebuilt = tokio::task::spawn_blocking(move || {
                crate::pipeline::direct_store::reconstruct::reconstruct_volumes(
                    &provider, &plans, sparse,
                )
            })
            .await
            // A panicked sweep leaves no outcomes at all, which the
            // reconciliation reads as "every volume kept nothing" — the same
            // handback a set whose destinations all failed gets, refetch
            // included. Dropping the ticket instead would wedge the job behind
            // a completion gate nothing ever clears.
            .unwrap_or_else(|error| {
                warn!(
                    job_id = job_id.0,
                    work_id,
                    set_index,
                    error = %error,
                    "a direct demotion reconstruction sweep panicked; refetching the set's volumes"
                );
                Vec::new()
            });
            let _ = done_tx
                .send(DirectDemotionWorkDone {
                    job_id,
                    work_id,
                    set_index,
                    rebuilt,
                })
                .await;
        });
    }

    /// The demotion sweep's ticket, on the pipeline task.
    ///
    /// Applies the durable half of the handback and re-enters the completion
    /// seam the demotion could not reach while the sweep was outstanding. A
    /// ticket whose job was torn down, or whose set demoted again behind it, is
    /// discarded by the fence.
    pub(in crate::pipeline) async fn handle_direct_demotion_done(
        &mut self,
        done: DirectDemotionWorkDone,
    ) {
        let Some(sets) = self.direct_demotion_in_flight.get_mut(&done.job_id) else {
            return;
        };
        let matches = sets
            .get(&done.set_index)
            .is_some_and(|work| work.work_id == done.work_id);
        if !matches {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                set_index = done.set_index,
                "discarding a stale direct demotion reconstruction ticket"
            );
            return;
        }
        let work = sets.remove(&done.set_index).expect("just matched");
        if sets.is_empty() {
            self.direct_demotion_in_flight.remove(&done.job_id);
        }
        let elapsed = work.submitted_at.elapsed();
        crate::runtime::perf_probe::record("direct_store.demote.sweep", elapsed);
        if !self.jobs.contains_key(&done.job_id) {
            return;
        }
        let plan = work.plan;
        let set_name = plan.set_name.clone();
        let volume_files = plan.volume_files.clone();
        let summary = self
            .apply_demoted_set_reconstruction(done.job_id, done.set_index, plan, &done.rebuilt)
            .await;
        crate::runtime::perf_probe::record(
            "direct_store.demoted.reconstructed",
            std::time::Duration::from_nanos(1),
        );
        // The other half of the materialization account: a whole-set demotion
        // materializes *every* volume of the group, and this is how many that
        // was. Read against `direct_store.repair.materialized_volumes`, whose
        // whole point is being much smaller.
        let volumes = summary.materialized;
        crate::runtime::perf_probe::record_value(
            "direct_store.demote.materialized_volumes",
            volumes as u64,
        );
        // One bucket per volume that refused a run, under the same metric names
        // the whole-set fallback used — so the reason breakdown reads the same
        // as before while the *count* is now volumes rather than sets.
        for (volume_index, failure) in &summary.refetched {
            crate::runtime::perf_probe::record_owned(
                format!("direct_store.demote_refetch.{}", failure.metric()),
                std::time::Duration::from_nanos(1),
            );
            warn!(
                job_id = done.job_id.0,
                set_name = %set_name,
                volume_index,
                failure = %failure,
                "a demoted volume could not be reconstructed in full; refetching the \
                 articles it could not vouch for"
            );
        }
        if !summary.refetched.is_empty() {
            crate::runtime::perf_probe::record_value(
                "direct_store.demote.refetched_volumes",
                summary.refetched.len() as u64,
            );
        }
        // The measurement the demotion account was missing: how many of the
        // bytes this set had already paid for survived the handback, and how
        // many it is about to pay for a second time. A volume count cannot say
        // — one refused article and a whole refetched volume both count as one.
        crate::runtime::perf_probe::record_value(
            "direct_store.demote.retained_bytes",
            summary.retained_bytes,
        );
        crate::runtime::perf_probe::record_value(
            "direct_store.demote.refetched_bytes",
            summary.refetched_bytes,
        );
        info!(
            job_id = done.job_id.0,
            set_name = %set_name,
            work_id = done.work_id,
            elapsed_ms = elapsed.as_millis() as u64,
            volumes,
            refetched = summary.refetched.len(),
            retained_bytes = summary.retained_bytes,
            refetched_bytes = summary.refetched_bytes,
            "direct-store set materialized from its own routed bytes"
        );
        self.finish_demoted_set_handback(done.job_id, volume_files.clone())
            .await;
        self.relieve_handed_back_write_backlog(&volume_files).await;
        self.schedule_job_completion_check(done.job_id);
    }

    /// Forgets a job's outstanding demotion sweeps. The detached workers keep
    /// running to their end; their done messages then find no ticket and are
    /// discarded by the fence.
    pub(crate) fn forget_direct_demotion_work(&mut self, job_id: JobId) {
        self.direct_demotion_in_flight.remove(&job_id);
    }

    /// The durable half of the reconstruction path, on the pipeline task.
    ///
    /// Mutates durable state in this order: legacy floors and completed-file
    /// rows, then the coverage row, then the direct outputs.
    pub(super) async fn apply_demoted_set_reconstruction(
        &mut self,
        job_id: JobId,
        set_index: usize,
        plan: DemotedSweepPlan,
        rebuilt: &[super::super::reconstruct::ReconstructedVolume],
    ) -> ReconstructionSummary {
        let DemotedSweepPlan {
            targets,
            mut extents_by_volume,
            handoffs,
            ..
        } = plan;
        let mut materialized = 0usize;
        // Volumes the sweep could not rebuild in full, each with the first
        // reason it refused. A refusal is no longer a write-off: the volume
        // keeps every run the sweep verified, and only the articles that
        // `verified` does not back are refetched. A volume whose *destination*
        // failed comes back with an empty `verified`, so the same code path
        // gives it the full-refetch treatment — `mark_file_incomplete`
        // included — that a refused volume used to get unconditionally.
        let mut refetched: Vec<(u32, ReconstructionFailure)> = Vec::new();
        let mut retained_bytes = 0u64;
        let mut refetched_bytes = 0u64;
        let mut keep: HashMap<u32, Vec<u32>> = HashMap::new();
        for (outcome, (volume_index, file_index, filename, plan)) in
            rebuilt.iter().zip(targets.iter())
        {
            debug_assert_eq!(outcome.volume_index, *volume_index);
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            let extents = extents_by_volume.remove(volume_index).unwrap_or_default();
            if let Some(failure) = &outcome.failure {
                refetched.push((*volume_index, failure.clone()));
            }
            // `outcome.verified`, never `plan.covered`: the plan states what the
            // coverage map claimed, and only the sweep knows which of that it
            // actually wrote and checked. They agree for a volume that swept end
            // to end and diverge for one that refused a run, which is precisely
            // when keeping an article the sweep skipped would leave a hole
            // nothing ever fetches.
            let (on_disk, floor) = crate::pipeline::direct_store::reconstruct::segments_on_disk(
                &extents,
                &outcome.verified,
                outcome.contiguous,
            );
            // The byte account of this volume's handback, article by article,
            // over the articles the coverage map claimed. Kept ones are the
            // bytes the demotion no longer pays for twice; the rest were routed
            // once and come off the wire again.
            for (segment_number, (offset, len)) in &extents {
                if !plan.covered.missing(*offset, *len).is_empty() {
                    continue;
                }
                match on_disk.contains(segment_number) {
                    true => retained_bytes = retained_bytes.saturating_add(*len),
                    false => refetched_bytes = refetched_bytes.saturating_add(*len),
                }
            }
            keep.insert(*file_index, on_disk);
            if outcome.contiguous == 0 {
                continue;
            }
            materialized += 1;

            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if outcome.complete && outcome.contiguous >= plan.len {
                let md5 = outcome.md5;
                let name = filename.clone();
                let index = *file_index;
                if let Err(error) = self
                    .db_blocking(move |db| {
                        db.complete_file_with_optional_hash(job_id, index, &name, md5.as_ref())
                    })
                    .await
                {
                    warn!(
                        job_id = job_id.0,
                        file_index, error = %error,
                        "failed to record a reconstructed volume as complete"
                    );
                }
            } else if floor > 0 {
                // A partial volume persists only a contiguous, segment-aligned
                // floor. `note_file_progress_floor` suppresses direct source
                // files, and this one still is one until the set's status is
                // read again — so the upsert goes straight to the batch the
                // flush drains, which is the same row `coverage_skip_plan` and
                // `segments_covered_by_floor` read back at restart.
                self.pending_file_progress.insert(file_id, floor);
            }
        }
        // Awaited, not fire-and-forget: the coverage row is retired immediately
        // below, so until these floors are committed the job has no durable
        // account of the volumes at all.
        if let Err(error) = self
            .flush_file_progress_batch_awaited("direct_store.demote.reconstructed_floors")
            .await
        {
            warn!(job_id = job_id.0, error = %error, "failed to persist reconstructed volume floors");
        }

        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a reconstructed direct-store checkpoint");
        }
        self.delete_direct_outputs(job_id, set_index).await;
        self.requeue_after_reconstruction(job_id, set_index, &keep, &handoffs)
            .await;
        for (outcome, (_, file_index, _, plan)) in rebuilt.iter().zip(targets.iter()) {
            if outcome.complete && outcome.contiguous >= plan.len {
                self.direct_store.settle_materialized_file(NzbFileId {
                    job_id,
                    file_index: *file_index,
                });
            }
        }
        ReconstructionSummary {
            materialized,
            refetched,
            retained_bytes,
            refetched_bytes,
        }
    }

    /// The last-resort demotion: retire routed storage and requeue every
    /// article whose bytes went into the routed storage this is about to
    /// delete.
    ///
    /// Whole-set on purpose, and only reachable for the two refusals that are
    /// whole-set facts — no layout was ever learned, or the re-encrypting
    /// overlay cannot reproduce the posted bytes. Neither leaves a range any
    /// composition could vouch for, so there is nothing per-volume or per-range
    /// to salvage. Every other failure is a property of one run and is handled
    /// inside the sweep, which keeps the runs around it.
    pub(super) async fn refetch_demoted_set(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let volumes: Vec<u32> = set.plan().volumes.values().copied().collect();

        // On this path the checkpoint row goes first, because everything it
        // claims is about to be deleted and nothing replaces it. A crash
        // between here and the refetch costs a redownload, which is what the
        // fallback is doing anyway.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.retire(&mut persist)
        {
            warn!(job_id = job_id.0, error = %error, "failed to retire a demoted direct-store checkpoint");
        }

        self.delete_direct_outputs(job_id, set_index).await;
        self.refetch_direct_volumes(job_id, &volumes).await;
    }

    /// Deletes a set's partial members, envelope files and holds scratch.
    ///
    /// A sparse half-written output would masquerade as finished work, and the
    /// envelopes and the scratch are scratch by construction.
    pub(super) async fn delete_direct_outputs(&mut self, job_id: JobId, set_index: usize) {
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.router.discard_scratch();
        }
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let mut doomed: Vec<PathBuf> = set
            .router
            .member_partials()
            .into_iter()
            .map(|(_, _, partial)| set.plan().destination_path(partial))
            .collect();
        doomed.extend(set.plan().envelope_paths());
        // Repair scratch. Normally deleted the moment its spans are routed, so
        // this only ever finds one a demotion interrupted — but a leftover
        // would sit in the working directory for the life of the job, and the
        // reconstruction sweep is about to write the real volume files beside
        // it.
        doomed.extend(set.plan().repair_paths());
        for path in doomed {
            crate::pipeline::release_cached_write_handle(&path);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    /// Hands a reconstructed set back to the conventional path, keeping the
    /// articles that are now genuinely on disk.
    ///
    /// Unlike the full-refetch fallback, `keep` names, per NZB file, the
    /// articles whose decoded extents the sweep rebuilt. Those stay committed
    /// in the assembly and are never fetched again. Everything else that the
    /// direct path had committed comes back exactly as the refetch path would
    /// have brought it back. The decode seam still owns its current article and
    /// carries it directly into conventional assembly.
    ///
    /// A file with nothing kept takes the full refetch treatment, including
    /// `mark_file_incomplete`: there is no reconstructed state to protect.
    pub(super) async fn requeue_after_reconstruction(
        &mut self,
        job_id: JobId,
        set_index: usize,
        keep: &HashMap<u32, Vec<u32>>,
        handoffs: &HashSet<SegmentId>,
    ) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        let volume_files: Vec<(u32, u32)> = set
            .plan()
            .volumes
            .iter()
            .map(|(volume_index, file_index)| (*volume_index, *file_index))
            .collect();
        let extents: HashMap<u32, std::collections::BTreeMap<u32, (u64, u64)>> = volume_files
            .iter()
            .map(|(volume_index, file_index)| (*file_index, set.segment_extents(*volume_index)))
            .collect();

        let scheduled_retries: HashSet<SegmentId> = self
            .pending_retries_by_segment
            .keys()
            .copied()
            .filter(|segment_id| segment_id.file_id.job_id == job_id)
            .collect();

        let mut work = Vec::new();
        let mut fully_reset: Vec<u32> = Vec::new();
        // Chunks the write buffer could not place until this pass seeded the
        // sweep's extents into it, drained here and written below.
        let mut unblocked: Vec<UnblockedHandbackWrites> = Vec::new();
        let write_buf_max_pending = self.write_buf_max_pending;
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let mut queued: HashSet<SegmentId> = HashSet::new();
            state.download_queue.extend_segment_ids(&mut queued);
            state.recovery_queue.extend_segment_ids(&mut queued);

            let mut lost_bytes = 0u64;
            for (_, file_index) in &volume_files {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let verified: HashSet<u32> = keep
                    .get(file_index)
                    .map(|segments| segments.iter().copied().collect())
                    .unwrap_or_default();
                // The articles the decode seam owns are kept alongside the ones
                // the sweep verified, but they are *its* bytes: it holds them in
                // the write buffer, parked there until this handback seeds the
                // sweep's extents and drains it. So they belong in the
                // assembly and out of the requeue, and nowhere near the sparse
                // seeding below.
                let handed_off: HashSet<u32> = handoffs
                    .iter()
                    .filter(|segment_id| segment_id.file_id == file_id)
                    .map(|segment_id| segment_id.segment_number)
                    .collect();
                let kept: HashSet<u32> = verified.union(&handed_off).copied().collect();
                if kept.is_empty() {
                    fully_reset.push(*file_index);
                }
                let Some(file) = state.spec.files.get(*file_index as usize) else {
                    continue;
                };
                let file = file.clone();
                let Some(file_asm) = state.assembly.file(file_id) else {
                    continue;
                };
                let previously_received = file_asm.received_bytes();
                let committed: HashSet<u32> = file
                    .segments
                    .iter()
                    .filter(|segment| file_asm.has_segment(segment.ordinal))
                    .map(|segment| segment.ordinal)
                    .collect();

                // Rebuild the assembly to exactly the kept set. `commit_segment`
                // is the only way in and `reset` the only way out, so the
                // sequence is reset-then-re-commit rather than a surgical
                // removal; the decoded sizes come from the recorded extents, so
                // the byte counters land where they were.
                if let Some(file_asm) = state.assembly.file_mut(file_id) {
                    file_asm.reset();
                }
                let mut kept_bytes = 0u64;
                let mut materialized_extents = Vec::with_capacity(kept.len());
                let file_extents = extents.get(file_index).cloned().unwrap_or_default();
                for segment_number in &kept {
                    let Some((offset, len)) = file_extents.get(segment_number).copied() else {
                        continue;
                    };
                    if let Some(file_asm) = state.assembly.file_mut(file_id)
                        && file_asm.commit_segment(*segment_number, len as u32).is_ok()
                    {
                        kept_bytes = kept_bytes.saturating_add(len);
                        if verified.contains(segment_number) {
                            materialized_extents.push((offset, len));
                        } else {
                            // A handed-off article arrived through the ordinary
                            // writer, which recorded where it landed; the blanket
                            // reset above erased that record. Put it back from the
                            // set's own geometry — the same offset the writer used,
                            // since both derive it from the volume's article
                            // extents — so a later duplicate re-places at the copy
                            // already on disk instead of at a cursor-derived offset.
                            file_asm.record_placement(*segment_number, offset, len as u32);
                        }
                    }
                }
                lost_bytes =
                    lost_bytes.saturating_add(previously_received.saturating_sub(kept_bytes));
                let needs_more_bytes = state
                    .assembly
                    .file(file_id)
                    .is_some_and(|file| !file.is_complete());
                // A file that needs nothing more can still have a *buffer* that
                // needs this seeding: the handed-off article is inserted at its
                // own offset while the sweep is outstanding, so it waits behind
                // a cursor still at zero — and it is often the very article
                // that completed the file. Skipping the seeding on completeness
                // would leave its bytes in memory and a hole on disk.
                let has_buffered_writes = self.write_buffers.contains_key(&file_id);
                if has_buffered_writes || (!materialized_extents.is_empty() && needs_more_bytes) {
                    // Reconstruction made these article extents durable without
                    // passing through the conventional writer. Seed its sparse
                    // markers so a later missing article bridges the cursor;
                    // only the contiguous floor is persisted across restart.
                    let write_buf = self
                        .write_buffers
                        .entry(file_id)
                        .or_insert_with(|| WriteReorderBuffer::new(write_buf_max_pending));
                    for (offset, len) in materialized_extents {
                        write_buf.mark_persisted(offset, len as usize);
                    }
                    // Whatever became writable only now is carried out to be
                    // written, not dropped. The sweep runs detached, and the
                    // decode seam parks every conventional article for this
                    // file while it is: one that decoded in the window sits in
                    // the write buffer whether its offset was at the cursor or
                    // not, and it is the handed-off article's own bytes as
                    // often as not. Seeding the sweep's extents and draining
                    // here is what writes it — even when the sweep verified
                    // nothing, since an article at the cursor is writable on
                    // its own and would otherwise wait for a neighbour that
                    // may never come.
                    let (ready, contiguous_end) = write_buf.drain_ready_with_contiguous_end();
                    if !ready.is_empty() {
                        unblocked.push((file_id, ready, contiguous_end));
                    }
                }

                for segment in &file.segments {
                    if kept.contains(&segment.ordinal) {
                        continue;
                    }
                    let segment_id = SegmentId {
                        file_id,
                        segment_number: segment.ordinal,
                    };
                    if queued.contains(&segment_id) || scheduled_retries.contains(&segment_id) {
                        continue;
                    }
                    if !committed.contains(&segment.ordinal) {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: std::sync::Arc::from(file.groups.as_slice()),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
            }
            state.downloaded_bytes = state.downloaded_bytes.saturating_sub(lost_bytes);
        }

        for (file_id, ready, contiguous_end) in unblocked {
            if let Err(error) = self
                .persist_ready_segments(file_id, ready, contiguous_end)
                .await
            {
                warn!(
                    job_id = job_id.0,
                    file_index = file_id.file_index,
                    error = %error,
                    "failed to write the articles a demoted volume's handback unblocked"
                );
            }
        }

        // Only files the sweep rebuilt nothing for: everything else has legacy
        // rows this path just wrote, and `mark_file_incomplete` deletes exactly
        // those.
        for file_index in fully_reset {
            let file_id = NzbFileId { job_id, file_index };
            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if let Err(error) = self.db.mark_file_incomplete(job_id, file_index) {
                warn!(
                    job_id = job_id.0,
                    file_index, error = %error,
                    "failed to invalidate a demoted direct-store volume"
                );
            }
        }
        for item in work {
            self.requeue_retry_work(item);
        }
    }

    /// Hands a demoted set's source volumes back to the conventional path.
    ///
    /// Requeues **only what nothing else owns**: the articles whose bytes were
    /// routed into direct destinations that have just been deleted. A segment
    /// still sitting in a queue, still in flight, waiting on a scheduled retry,
    /// or held by the decode seam is left alone; each reaches the conventional
    /// path through its existing owner.
    ///
    /// The job's byte counter is *adjusted*, never zeroed: it is job-wide, and
    /// the other files' contribution to it has nothing to do with this set.
    pub(super) async fn refetch_direct_volumes(&mut self, job_id: JobId, file_indices: &[u32]) {
        // Snapshotted before the job borrow: a segment whose retry is already
        // scheduled re-enters the queue on its own.
        let scheduled_retries: HashSet<SegmentId> = self
            .pending_retries_by_segment
            .keys()
            .copied()
            .filter(|segment_id| segment_id.file_id.job_id == job_id)
            .collect();

        let mut work = Vec::new();
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let mut queued: HashSet<SegmentId> = HashSet::new();
            state.download_queue.extend_segment_ids(&mut queued);
            state.recovery_queue.extend_segment_ids(&mut queued);

            let mut routed_bytes = 0u64;
            for file_index in file_indices {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let Some(file) = state.spec.files.get(*file_index as usize) else {
                    continue;
                };
                let Some(file_asm) = state.assembly.file(file_id) else {
                    continue;
                };
                routed_bytes = routed_bytes.saturating_add(file_asm.received_bytes());
                for segment in &file.segments {
                    let segment_id = SegmentId {
                        file_id,
                        segment_number: segment.ordinal,
                    };
                    if queued.contains(&segment_id) || scheduled_retries.contains(&segment_id) {
                        continue;
                    }
                    // Committed articles lost their bytes with the partials.
                    // Every other segment is somebody else's outstanding work.
                    let committed = file_asm.has_segment(segment.ordinal);
                    if !committed {
                        continue;
                    }
                    work.push(DownloadWork {
                        segment_id,
                        message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                        groups: std::sync::Arc::from(file.groups.as_slice()),
                        priority: file.role.download_priority(),
                        byte_estimate: segment.bytes,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: vec![],
                        avoid_server: None,
                    });
                }
                if let Some(file_asm) = state.assembly.file_mut(file_id) {
                    file_asm.reset();
                }
            }
            state.downloaded_bytes = state.downloaded_bytes.saturating_sub(routed_bytes);
        }
        for file_index in file_indices {
            let file_id = NzbFileId {
                job_id,
                file_index: *file_index,
            };
            self.pending_file_progress.remove(&file_id);
            self.persisted_file_progress.remove(&file_id);
            if let Err(error) = self.db.mark_file_incomplete(job_id, *file_index) {
                warn!(
                    job_id = job_id.0,
                    file_index, error = %error,
                    "failed to invalidate a demoted direct-store volume"
                );
            }
        }
        for item in work {
            self.requeue_retry_work(item);
        }
    }
}
