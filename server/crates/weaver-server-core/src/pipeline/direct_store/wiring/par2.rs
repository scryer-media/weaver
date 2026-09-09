//! Continuation of the `impl Pipeline` block from `direct_store/wiring.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    /// Take any set claiming this file off the direct path, because its
    /// articles arrived uuencoded.
    ///
    /// Sets are admitted from the NZB's filenames, before a single article has
    /// been decoded, so an archive posted in uuencode is admitted exactly like a
    /// yEnc one. It can never be routed: routing writes an article's bytes into
    /// a volume at the offset the article declares, and a uuencode article
    /// declares no offset — its position is the decoded length of its whole
    /// prefix, which only sequential assembly can supply.
    ///
    /// Excluding those articles from the routing seam is not enough on its own.
    /// A set that is merely starved never finalizes and never demotes, so every
    /// suppression keyed on [`Self::is_direct_source_file`] keeps holding for
    /// its volumes — including the archive probe that dispatches extraction,
    /// which would leave the job completing with its archive unextracted on
    /// disk. Demoting puts the volumes back on the conventional path, where the
    /// sequential cursor is already writing them.
    ///
    /// `ensure_direct_sets` runs first for the same reason
    /// [`Self::direct_route_target`] runs it: admission is lazy, so a job whose
    /// very first article is uuencoded would otherwise find no set to demote
    /// and admit one moments later.
    pub(crate) async fn demote_direct_sets_for_uu_article(&mut self, file_id: NzbFileId) {
        let job_id = file_id.job_id;
        self.ensure_direct_sets(job_id);
        // Identity rosters go with the sets, and for the same reason: nothing
        // uuencoded can ever be routed, so a binding that would only starve is
        // never made.
        self.direct_store.identity.remove(&job_id);
        let set_indices: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter_map(|(index, set)| {
                (!set.is_demoted() && set.plan().volume_for_file(file_id.file_index).is_some())
                    .then_some(index)
            })
            .collect();
        for set_index in set_indices {
            self.demote_direct_set(job_id, set_index, DemotionReason::UuencodedSourceVolume)
                .await;
        }
    }

    /// Whether this file's bytes are a direct set's source volume, so no legacy
    /// floor, completed-file row or archive re-probe may be written for it.
    /// `&self`, because the suppression checks sit inside paths that already
    /// hold the pipeline immutably.
    ///
    /// Deliberately **not** narrowed to still-routing sets the way
    /// [`Self::direct_route_target`] is: a finalized set's source volumes were
    /// never written and never will be, so every suppression the routing seam
    /// relied on has to keep holding afterwards. Only a demotion puts the
    /// volume back on the conventional path, and only then does it get a file.
    pub(crate) fn is_direct_source_file(&self, file_id: NzbFileId) -> bool {
        self.direct_store
            .sets_for(file_id.job_id)
            .iter()
            .any(|set| {
                !set.is_demoted() && set.plan().volume_for_file(file_id.file_index).is_some()
            })
    }

    /// Whether this file is a volume of a demoted set whose reconstruction
    /// sweep is still outstanding.
    ///
    /// A demoted set fails [`Self::is_direct_source_file`] on purpose — its
    /// volumes are ordinary files from the demotion on — but for as long as the
    /// detached sweep runs, those files are half written: the sweep owns them,
    /// sets their length, and may remove one outright. An article that
    /// completes such a volume through the conventional path in that window
    /// must not have the file classified, probed or entered into the archive
    /// topology over an image the sweep has not finished; the handback replays
    /// the completion hook for every volume once the ticket lands.
    pub(crate) fn demotion_sweep_owns_file(&self, file_id: NzbFileId) -> bool {
        self.direct_demotion_in_flight
            .get(&file_id.job_id)
            .is_some_and(|sets| {
                sets.keys().any(|set_index| {
                    self.direct_store
                        .set(file_id.job_id, *set_index)
                        .is_some_and(|set| set.plan().volume_for_file(file_id.file_index).is_some())
                })
            })
    }

    /// The virtual volume behind one direct source file, as a **one-volume**
    /// provider plus its logical length.
    ///
    /// A test-only accessor: production reads a direct set through
    /// [`super::super::par2_access::DirectVolumeFileAccess`], which builds the whole
    /// set's provider once for the pass. This answers the one-volume question a
    /// test asks when it wants to inspect what a single volume reads back as,
    /// without rebuilding the set's plan lookup in test code.
    ///
    /// The length is the decoded total the download layer tracks, never a
    /// file's `metadata().len()`: for a direct volume there is no file to ask.
    /// `None` for anything that is not a live direct set's source volume,
    /// including a demoted set's — whose volumes are materializing or being
    /// refetched, and are read from disk like any other file.
    ///
    /// An **encrypted** set answers here like any other: the provider
    /// re-encrypts the member ranges it reads out of the partials, so what comes
    /// back is the posted bytes the caller asked for rather than the plaintext
    /// sitting on disk.
    #[cfg(test)]
    pub(crate) fn direct_virtual_volume(
        &self,
        file_id: NzbFileId,
    ) -> Option<(u32, u64, super::super::provider::HybridVolumeProvider)> {
        let job_id = file_id.job_id;
        let (set, volume_index) =
            self.direct_store
                .sets_for(job_id)
                .iter()
                .find_map(|set| match set.is_demoted() {
                    true => None,
                    false => set
                        .plan()
                        .volume_for_file(file_id.file_index)
                        .map(|volume_index| (set, volume_index)),
                })?;
        let received = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .map(|file| file.received_bytes())
            .unwrap_or(0);
        let len = set.virtual_volume_len(volume_index, received);
        let lengths = std::collections::BTreeMap::from([(volume_index, len)]);
        Some((volume_index, len, set.virtual_provider(&lengths)))
    }

    /// The direct sets of `job_id` that the authoritative PAR2 pass must read
    /// virtually, or `None` when it has none and today's `PlacementFileAccess`
    /// is the whole answer.
    ///
    /// A volume is included only when its PAR2 identity resolves unambiguously
    /// through the same name candidates the grid's binding resolver uses. An
    /// unresolved one is skipped **here**, but that skip is not the safety net:
    /// a half-bound set would have the pass read its remaining volumes off a
    /// disk they are not on and report them missing, and
    /// [`Self::demote_direct_sets_with_par2_damage`] could not even attribute
    /// that damage back to the set, because attribution is keyed by the very
    /// binding that failed. The net is [`Self::demote_unbindable_direct_sets`],
    /// which runs *before* the pass and demotes any live set with an unbindable
    /// volume outright, so what reaches here is either a fully bound set or no
    /// set at all.
    pub(crate) fn direct_par2_overlay(&self, job_id: JobId) -> Option<DirectPar2Overlay> {
        self.direct_par2_overlay_for_set(job_id, self.par2_served_set_id(job_id)?)
    }

    /// The virtual direct volumes that bind wholly to one recovery set.
    ///
    /// The compatibility wrapper above still answers the served set. Callers
    /// that already know which recovery set they are verifying must use this
    /// form, so a direct set owned by another parsed set is neither read nor
    /// damaged by the wrong pass.
    pub(crate) fn direct_par2_overlay_for_set(
        &self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> Option<DirectPar2Overlay> {
        let mut volumes = Vec::new();
        let mut virtual_volumes = Vec::new();
        let mut sets = HashMap::new();
        let mut file_indices = HashMap::new();
        let mut set_lengths = Vec::new();
        for (set_index, set) in self.direct_store.sets_for(job_id).iter().enumerate() {
            // A demoted set's volumes are materializing or being refetched, so
            // they are read from disk like any other file.
            if set.is_demoted() {
                continue;
            }
            // A **finalized** set has renamed its partials to their
            // destinations and — unless it was asked to keep them for a live
            // neighbour — deleted its envelopes, so nothing answers for its
            // source volumes and serving it would report damage that is not
            // there. One that *did* keep them serves the very same image out of
            // the committed members instead, which is what lets a neighbour's
            // repair read the surviving input slices Reed–Solomon needs from
            // every file the recovery set describes. Either way it is never a
            // repair *target*: `repair_direct_sets_with_par2_damage` skips a
            // finalized set, and `forgive_finalized_direct_volumes` still
            // excuses one whose image was not kept.
            let retained = set.retained_volumes();
            if set.is_finalized() && retained.is_none() {
                continue;
            }
            // One `virtual_volumes` call for the whole set, so the shared
            // partial-path map is built once rather than once per volume (nit).
            let mut lengths = std::collections::BTreeMap::new();
            let mut bindings = HashMap::new();
            let mut belongs_to_recovery_set = true;
            for (volume_index, file_index) in &set.plan().volumes {
                let file_id = NzbFileId {
                    job_id,
                    file_index: *file_index,
                };
                let Some(binding) = self.resolve_par2_file_binding(file_id) else {
                    belongs_to_recovery_set = false;
                    break;
                };
                if binding.recovery_set_id != recovery_set_id {
                    belongs_to_recovery_set = false;
                    break;
                }
                // A retained image carries the lengths it was captured with, so
                // it stops depending on an assembly the job may have moved on
                // from.
                let len = match retained {
                    Some(volumes) => volumes
                        .iter()
                        .find(|volume| volume.volume_index == *volume_index)
                        .map(|volume| volume.len)
                        .unwrap_or_default(),
                    None => {
                        let received = self
                            .jobs
                            .get(&job_id)
                            .and_then(|state| state.assembly.file(file_id))
                            .map(|file| file.received_bytes())
                            .unwrap_or(0);
                        set.virtual_volume_len(*volume_index, received)
                    }
                };
                lengths.insert(*volume_index, len);
                bindings.insert(
                    *volume_index,
                    (*file_index, binding.par2_file_id, binding.recovery_set_id),
                );
            }
            if !belongs_to_recovery_set {
                continue;
            }
            let set_volumes = match retained {
                Some(volumes) => volumes.to_vec(),
                None => set.virtual_volumes(&lengths),
            };
            for mut volume in set_volumes {
                let Some((file_index, par2_file_id, binding_set_id)) =
                    bindings.get(&volume.volume_index).copied()
                else {
                    continue;
                };
                debug_assert_eq!(binding_set_id, recovery_set_id);
                // Re-keyed from the set's own volume index to the job's file
                // index: a job can hold several sets, each numbering its volumes
                // from zero, and one provider answers for all of them.
                volume.volume_index = file_index;
                virtual_volumes.push(volume);
                volumes.push(super::super::par2_access::VirtualPar2Volume {
                    par2_file_id,
                    volume_index: file_index,
                });
                sets.insert(par2_file_id, set_index);
                file_indices.insert(par2_file_id, file_index);
            }
            if !lengths.is_empty() {
                set_lengths.push((set_index, lengths));
            }
        }
        if volumes.is_empty() {
            return None;
        }
        Some(DirectPar2Overlay {
            recovery_set_id,
            provider: super::super::provider::HybridVolumeProvider::new(virtual_volumes),
            volumes,
            sets,
            file_indices,
            lengths: set_lengths,
        })
    }

    /// Whether the authoritative PAR2 pass may run over `job_id`'s direct sets
    /// yet, or has to wait for their payload.
    ///
    /// Deliberately the same shape as the completion gate's own
    /// `par2_primary_payload_ready`: **every live set's volumes have finished
    /// downloading, or nothing more is coming**. A set that is still receiving
    /// articles reads its outstanding ranges as holes, and PAR2 cannot tell a
    /// hole from corruption — so a pass run early would report damage that is
    /// only a download in progress, demote a healthy set and hand the repairer
    /// volumes it would have to rebuild from scratch. The second half of the
    /// disjunction is what keeps this from waiting forever: once the download
    /// pipeline has drained, the holes are permanent and the verdict is real.
    ///
    /// `true` for every job with no live direct set, which is every conventional
    /// job — the gate is unchanged for them by construction.
    pub(super) fn direct_set_binds_to_par2_set(
        &self,
        job_id: JobId,
        direct_set: &DirectSet,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        direct_set
            .plan()
            .volumes
            .values()
            .copied()
            .any(|file_index| {
                self.resolve_par2_file_binding(NzbFileId { job_id, file_index })
                    .is_some_and(|binding| binding.recovery_set_id == recovery_set_id)
            })
    }

    pub(crate) fn direct_sets_ready_for_authoritative_par2_for_set(
        &self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let waiting = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .any(|set| !set.is_demoted() && !set.is_finalized() && !set.all_volumes_complete());
        !waiting || !self.job_has_pending_download_pipeline_work(job_id)
    }

    pub(crate) fn direct_sets_ready_for_authoritative_par2(&self, job_id: JobId) -> bool {
        self.par2_served_set_id(job_id).is_none_or(|set_id| {
            self.direct_sets_ready_for_authoritative_par2_for_set(job_id, set_id)
        })
    }

    /// The one ownership gate between direct demotion and every PAR2 verdict.
    /// A pending file leaves through the durable conventional completion seam,
    /// or once every article it still lacks is terminally unavailable.
    pub(crate) fn demoted_materializations_ready_for_par2(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let pending = self.direct_store.pending_materializations(job_id);
        if pending.is_empty() {
            return true;
        }
        if !self.jobs.contains_key(&job_id) {
            self.direct_store.clear_pending_materializations(job_id);
            return true;
        }

        let mut ready = true;
        for (set_index, pending) in pending {
            let applicability = self.direct_store.set(job_id, set_index).map(|set| {
                let mut unresolved = false;
                let binds_served =
                    set.plan().volumes.values().copied().any(|file_index| {
                        match self.resolve_par2_file_binding(NzbFileId { job_id, file_index }) {
                            Some(binding) => binding.recovery_set_id == recovery_set_id,
                            None => {
                                unresolved = true;
                                false
                            }
                        }
                    });
                binds_served || unresolved
            });
            match applicability {
                Some(true) => {}
                Some(false) => continue,
                None => {
                    for file_id in pending.files {
                        self.direct_store.settle_materialized_file(file_id);
                    }
                    continue;
                }
            }

            for file_id in pending.files {
                let Some((missing, file_has_owner)) = self.jobs.get(&job_id).and_then(|state| {
                    let file = state.spec.files.get(file_id.file_index as usize)?;
                    let assembly = state.assembly.file(file_id)?;
                    let mut owned = HashSet::new();
                    state.download_queue.extend_segment_ids(&mut owned);
                    state.recovery_queue.extend_segment_ids(&mut owned);
                    owned.extend(state.held_segments.iter().map(|work| work.segment_id));

                    let missing = file
                        .segments
                        .iter()
                        .filter(|segment| !assembly.has_segment(segment.ordinal))
                        .map(|segment| {
                            let segment_id = SegmentId {
                                file_id,
                                segment_number: segment.ordinal,
                            };
                            (
                                segment_id,
                                DownloadWork {
                                    segment_id,
                                    message_id: crate::jobs::ids::MessageId::new(
                                        &segment.message_id,
                                    ),
                                    groups: std::sync::Arc::from(file.groups.as_slice()),
                                    priority: file.role.download_priority(),
                                    byte_estimate: segment.bytes,
                                    retry_count: 0,
                                    is_recovery: false,
                                    completion_critical: false,
                                    exclude_servers: vec![],
                                    avoid_server: None,
                                },
                                owned.contains(&segment_id),
                            )
                        })
                        .collect::<Vec<_>>();
                    let file_has_owner = self
                        .active_downloads_by_file
                        .get(&file_id)
                        .is_some_and(|count| *count > 0)
                        || self
                            .active_decodes_by_file
                            .get(&file_id)
                            .is_some_and(|count| *count > 0)
                        || self
                            .write_buffers
                            .get(&file_id)
                            .is_some_and(|buffer| !buffer.is_empty())
                        || self
                            .pending_released_download_results_by_job
                            .get(&job_id)
                            .is_some_and(|count| *count > 0);
                    Some((missing, file_has_owner))
                }) else {
                    self.direct_store.settle_materialized_file(file_id);
                    continue;
                };

                // No missing article does not mean durable yet: the completing
                // commit still owes its buffer flush, handle release and row.
                if missing.is_empty() {
                    ready = false;
                    continue;
                }
                if missing
                    .iter()
                    .all(|(segment_id, _, _)| self.segment_terminal_states.contains_key(segment_id))
                {
                    self.direct_store.settle_materialized_file(file_id);
                    continue;
                }

                let has_owner = file_has_owner
                    || pending
                        .handoffs
                        .iter()
                        .any(|segment_id| segment_id.file_id == file_id)
                    || missing.iter().any(|(segment_id, _, queued)| {
                        *queued
                            || self.pending_retries_by_segment.contains_key(segment_id)
                            || self.server_quota_parked.contains(segment_id)
                    });
                if has_owner {
                    ready = false;
                    continue;
                }

                let missing_ids: Vec<SegmentId> = missing
                    .iter()
                    .map(|(segment_id, _, _)| *segment_id)
                    .collect();
                let mut rescued = Vec::new();
                for (segment_id, work, _) in missing {
                    if self.segment_terminal_states.contains_key(&segment_id) {
                        continue;
                    }
                    if pending.rescued.contains(&segment_id) {
                        self.book_failed_segment(segment_id);
                    } else if self
                        .direct_store
                        .note_materialization_rescue(job_id, set_index, segment_id)
                    {
                        rescued.push(work);
                    }
                }
                for work in rescued {
                    self.requeue_retry_work(work);
                    ready = false;
                }
                if missing_ids
                    .iter()
                    .all(|segment_id| self.segment_terminal_states.contains_key(segment_id))
                {
                    self.direct_store.settle_materialized_file(file_id);
                } else {
                    ready = false;
                }
            }
        }
        ready
    }

    /// Demotes every live direct set of `job_id` holding a source volume that
    /// cannot be bound, unambiguously, to a PAR2 description.
    ///
    /// The overlay is keyed by PAR2 file id, so an unbound volume is one the
    /// pass cannot be told about *and* one whose verdict cannot be attributed
    /// back to its set. Leaving it out — which is all
    /// [`Self::direct_par2_overlay`] can do on its own — produces the worst of
    /// both: the pass reads that volume off a disk it is not on and calls it
    /// missing, `demote_direct_sets_with_par2_damage` finds no set to blame, and
    /// the repairer is handed a virtual volume to write into. A set with *every*
    /// volume unbound does not even produce an overlay, so the damage path is
    /// skipped entirely.
    ///
    /// Demoting up front is what makes the pass's world binary: either a fully
    /// bound virtual set, or real files on disk.
    pub(crate) async fn demote_unbindable_direct_sets_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        // Without a parsed recovery set nothing can bind, and demoting every
        // direct set of a job whose PAR2 has simply not arrived yet would undo
        // the whole feature.
        if self.par2_set(job_id).is_none() {
            return false;
        }
        // An encrypted set is served to the pass through the
        // re-encrypting overlay like any other — but only while the overlay can
        // really reproduce what was posted. The residual it cannot is a routed
        // encrypted member with no declared cipher size, or one whose tail
        // padding is not whole, and such a set is taken out here rather than
        // half-answered: the pass's world has to stay binary, exactly as the
        // unbindable rule below keeps it.
        let unavailable: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter(|(_, set)| set.router.posted_bytes_unavailable())
            .map(|(set_index, _)| set_index)
            .collect();
        let mut demoted_any = !unavailable.is_empty();
        for set_index in unavailable {
            warn!(
                job_id = job_id.0,
                "an encrypted direct set cannot reproduce its posted bytes; demoting so the \
                 authoritative pass reads real files instead of a volume the overlay can only \
                 half answer"
            );
            self.demote_direct_set(
                job_id,
                set_index,
                DemotionReason::EncryptedPostedBytesUnavailable,
            )
            .await;
        }
        let unbindable: Vec<(usize, u32)> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .filter_map(|(set_index, set)| {
                set.plan()
                    .volumes
                    .iter()
                    .find(|(_, file_index)| {
                        self.resolve_par2_file_binding(NzbFileId {
                            job_id,
                            file_index: **file_index,
                        })
                        .is_none()
                    })
                    .map(|(volume_index, _)| (set_index, *volume_index))
            })
            .collect();
        if unbindable.is_empty() {
            return demoted_any;
        }
        demoted_any = true;
        for (set_index, volume_index) in unbindable {
            warn!(
                job_id = job_id.0,
                volume_index,
                "a direct set's source volume has no unambiguous PAR2 identity; demoting \
                 so the authoritative pass reads a real file instead of a volume it \
                 cannot name"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Unbindable)
                .await;
        }
        demoted_any
    }

    pub(crate) async fn demote_unbindable_direct_sets(&mut self, job_id: JobId) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_unbindable_direct_sets_for_set(job_id, set_id)
            .await
    }

    /// Rewrites `Missing` to `Complete` for every source volume of a
    /// **finalized** direct set, before the caller counts damage.
    ///
    /// Exactly the eager-delete precedent, and for exactly the same reason: the
    /// bytes were verified and the file is legitimately absent. A finalized set
    /// passed the whole-member CRC32 gate on every member *and* the job's own
    /// PAR2 verdict — finalization is gated on that verdict — and then renamed
    /// its partials to their destinations and deleted its envelopes. Nothing on
    /// disk answers for its source volumes afterwards, and nothing should: they
    /// were never written and never will be.
    ///
    /// Without this, any *later* pass over the same job — a conventional set's
    /// extraction failing after the direct set finalized is enough — reports
    /// every finalized volume missing and either fails the job as unrepairable
    /// or has the repairer reconstruct source volumes onto disk that the job
    /// already finished without.
    ///
    /// Live and demoted sets are deliberately untouched: a live set's volumes
    /// are served virtually and its verdict is real, and a demoted set's are
    /// materializing or being refetched, so missing means missing.
    ///
    /// # Retention does not replace it, and does not fight it either
    ///
    /// A set that finalized beside a live neighbour keeps its envelopes and
    /// serves its volumes out of the committed members
    /// ([`Self::retain_finalized_direct_volumes`]), so in that window they read
    /// `Complete` on their own and there is nothing here to forgive — the same
    /// verdict, reached by reading rather than by excusing. This still runs, and
    /// still has to: retention covers one window of one shape, and the pass that
    /// motivated this rule is the *later* one, over a job whose sets are all
    /// committed and whose envelopes are therefore gone. It is also the only
    /// answer on the paths that read no overlay at all — `analyze_par2_damage`'s
    /// filesystem-bound repairer among them.
    ///
    /// Deliberately confined to `Missing`. A retained volume that read `Damaged`
    /// would be one whose destination moved or whose image is short, and
    /// excusing that would hand the repair bytes it should not trust; it is left
    /// as damage, the repair refuses on an unmaterialized write target, and the
    /// job falls back to the demotion path.
    ///
    /// Returns the number of missing slices forgiven.
    pub(crate) fn forgive_finalized_direct_volumes(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> u32 {
        if self.par2_set(job_id).is_none() {
            return 0;
        }
        let finalized: HashSet<par2_rs::FileId> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| set.is_finalized() && !set.is_demoted())
            .flat_map(|set| set.plan().volumes.values().copied())
            .filter_map(|file_index| {
                let file_id = NzbFileId { job_id, file_index };
                // Belt and braces: the set says the volume is its own, and
                // `is_direct_source_file` is the rule every other suppression
                // point reads, so the two cannot drift apart here.
                if !self.is_direct_source_file(file_id) {
                    return None;
                }
                self.resolve_par2_file_binding(file_id)
                    .map(|binding| binding.par2_file_id)
            })
            .collect();
        if finalized.is_empty() {
            return 0;
        }

        let mut forgiven = 0u32;
        for file in &mut verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Missing)
                || !finalized.contains(&file.file_id)
            {
                continue;
            }
            forgiven = forgiven.saturating_add(file.missing_slice_count);
            file.status = par2_rs::verify::FileStatus::Complete;
            file.valid_slices.fill(true);
            file.missing_slice_count = 0;
        }
        if forgiven == 0 {
            return 0;
        }
        verification.total_missing_blocks =
            verification.total_missing_blocks.saturating_sub(forgiven);
        verification.refresh_repairability();
        forgiven
    }

    /// Answers PAR2 damage on a job's direct sets.
    ///
    /// The entry point, and the whole of *repair while still direct* transition
    /// seen from the pipeline. It tries the repair first and falls back to the
    /// whole-set demotion on any refusal, so `Resolved` means the job's next
    /// move is a fresh completion check, over either repaired virtual volumes
    /// or materialized physical ones.
    ///
    /// `Deferred` is the third answer and it is not a refusal: the damage is
    /// coverable by the recovery set, just not by the slices merged so far, so
    /// the missing recovery has been asked for and the sets are staying direct
    /// until it arrives. Falling through to the demotion there would materialize
    /// every volume moments before the blocks that would have repaired them in
    /// place land.
    ///
    /// The ordering is normative:
    ///
    /// 1. the set's **checkpoint row is deleted first**, because everything
    ///    below rewrites bytes the row claims. The next barrier recreates
    ///    coverage from scratch. Deliberately lossy: a crash between here and
    ///    that barrier costs a full redownload of the set, which is bounded and
    ///    is what the whole model already accepts for uncheckpointed work;
    /// 2. only the damaged volumes materialize, into scratch files;
    /// 3. the repair runs with every clean volume read **virtually**;
    /// 4. the repaired spans re-enter the router with replacement semantics and
    ///    their destination writes are awaited before anything is recorded;
    /// 5. the stale composition gaps the rewrite left are re-read from the
    ///    partials that hold them, which re-arms the whole-member gates;
    /// 6. the scratch is deleted, and the set is back to fully virtual.
    pub(crate) async fn resolve_direct_sets_with_par2_damage_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectDamageResolution {
        match self
            .repair_direct_sets_with_par2_damage(job_id, verification)
            .await
        {
            DirectRepairAnswer::Acted => return DirectDamageResolution::Resolved,
            DirectRepairAnswer::Deferred => return DirectDamageResolution::Deferred,
            DirectRepairAnswer::Declined => {}
        }
        // Same reason-preserving demotion as the pre-repairer seam: a set that
        // reached here carrying its own part-checksum mismatch demotes under
        // that, not under the verdict that merely agreed with it.
        let part_damage = self
            .demote_direct_sets_with_unanswered_part_damage(
                job_id,
                recovery_set_id,
                "repair_declined",
            )
            .await;
        if self
            .demote_direct_sets_with_par2_damage_for_set(job_id, recovery_set_id, verification)
            .await
            || part_damage
        {
            DirectDamageResolution::Resolved
        } else {
            DirectDamageResolution::Unresolved
        }
    }

    pub(crate) async fn resolve_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectDamageResolution {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return DirectDamageResolution::Unresolved;
        };
        self.resolve_direct_sets_with_par2_damage_for_set(job_id, set_id, verification)
            .await
    }

    /// The repair chance for a live direct set, taken **before** the completion
    /// gate hands the job to `Par2Repairer`.
    ///
    /// That branch exists for jobs the fast paths could not clear, and a live
    /// direct set reaches it routinely: it contributes nothing to the
    /// clean-PAR2 integrity gate — a direct set never enters the archive
    /// topology — so a damaged one always arrives here rather than at the
    /// verify branch. The repairer is filesystem-bound, so today's answer is
    /// [`Self::demote_live_direct_sets_for_par2_repair`]: materialize
    /// everything and let it work over real files. This is what repair puts in
    /// front of that, and `Unresolved` means the demotion is still the answer.
    ///
    /// The verdict is computed here rather than borrowed, because the branch has
    /// none yet. It is deliberately a **quiet** pass — no status transition, no
    /// verification events — for two reasons: the analyze pass immediately below
    /// emits its own, so a job that falls through would report verifying twice;
    /// and this one exists to answer a question about direct sets, not to record
    /// the job's verdict.
    pub(crate) async fn resolve_direct_sets_before_par2_repairer_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> DirectPar2Resolution {
        if !self.direct_store.sets_for(job_id).iter().any(|set| {
            self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id)
                && !set.is_demoted()
                && !set.is_finalized()
        }) {
            return DirectPar2Resolution::Unresolved;
        }
        // A job already waiting on a promoted recovery wave answers without
        // verifying anything. The pass below is a full PAR2 scan, the gate ticks
        // on every article that completes, and until the wave has merged the
        // scan can only reach the verdict that started the wait — so re-running
        // it is the repeated-scan storm this branch has already paid for once,
        // at ~64 slow scans on a single job while 75 others starved. When the
        // wave has drained the fast path lapses and the pass runs, which is
        // exactly the one moment it can learn something new.
        if self.direct_store.repair_defer_pending(job_id)
            && self.job_has_promoted_recovery_pipeline_work(job_id, "direct repair defer")
        {
            return DirectPar2Resolution::Deferred;
        }
        let Some(verification) = self
            .verify_direct_sets_quietly(job_id, par2_set, working_dir)
            .await
        else {
            return if self.direct_post_repair_in_flight.contains_key(&job_id) {
                DirectPar2Resolution::Pending
            } else {
                DirectPar2Resolution::Unresolved
            };
        };
        if !verification.needs_repair() {
            // The safety valve for a damaged-volume fact PAR2 cannot answer.
            //
            // A part-checksum mismatch parks its set: the member holds its
            // whole-member gate, so the set never finalizes on its own. That is
            // right while a repair might still come, and wrong the moment PAR2
            // reads the very volume and calls it whole — the two checksums
            // disagree about bytes PAR2 has no reason to touch, which is the
            // archive itself being wrong rather than the transfer. Nothing here
            // can improve on that, and the set must not sit on it: it demotes
            // under the reason the routing gate would have used, and the
            // conventional path reports the failure it was always going to.
            if self
                .demote_direct_sets_with_unanswered_part_damage(
                    job_id,
                    recovery_set_id,
                    "par2_clean",
                )
                .await
            {
                return DirectPar2Resolution::Demoted;
            }
            return DirectPar2Resolution::Clean(Box::new(verification));
        }
        match self
            .repair_direct_sets_with_par2_damage(job_id, &verification)
            .await
        {
            DirectRepairAnswer::Acted => {
                // The write set this repair actually touched, taken from the
                // verdict that decided the repair was needed — the same
                // reading [`par2_repair_write_set`] gives the conventional
                // selective pass. It is what lets the *next* completion
                // check's post-repair read stay selective too, instead of
                // reading every volume this set describes to answer a
                // question only these few volumes can have a new answer to.
                let write_set = crate::pipeline::completion::finalize::check::par2_repair_write_set(
                    &verification,
                );
                self.direct_post_repair_carry.insert(
                    job_id,
                    DirectPostRepairCarry {
                        recovery_set_id,
                        pre_repair: verification,
                        write_set,
                    },
                );
                DirectPar2Resolution::Repaired
            }
            DirectRepairAnswer::Deferred => DirectPar2Resolution::Deferred,
            DirectRepairAnswer::Declined => {
                if self.par3_direct_checks_available(job_id)
                    && !self.job_has_pending_download_pipeline_work(job_id)
                    && let par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } =
                        verification.repairable
                {
                    return DirectPar2Resolution::RecoveryExhausted {
                        needed: blocks_needed,
                        available: verification.recovery_blocks_available,
                    };
                }
                // The other end of the safety valve above. The repair refused,
                // so the parked set has run out of help; it demotes under the
                // routing gate's own reason rather than under the generic
                // `par2_damaged` the fall-through demotion would use, so the
                // metric still says what went wrong.
                if self
                    .demote_direct_sets_with_unanswered_part_damage(
                        job_id,
                        recovery_set_id,
                        "repair_declined",
                    )
                    .await
                {
                    return DirectPar2Resolution::Demoted;
                }
                DirectPar2Resolution::Unresolved
            }
        }
    }

    pub(crate) async fn resolve_direct_sets_before_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> DirectPar2Resolution {
        self.resolve_direct_sets_before_par2_repairer_for_set(
            job_id,
            par2_set.recovery_set_id,
            par2_set,
            working_dir,
        )
        .await
    }

    pub(super) fn take_or_start_direct_post_repair_verification(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        access: std::sync::Arc<super::super::par2_access::DirectVolumeFileAccess>,
        to_read: Vec<par2_rs::FileId>,
        selective: bool,
    ) -> Option<Result<par2_rs::VerificationResult, String>> {
        let recovery_set_id = par2_set.recovery_set_id;
        if let Some((result_set_id, result)) = self.direct_post_repair_results.remove(&job_id) {
            if result_set_id == recovery_set_id {
                self.direct_post_repair_in_flight.remove(&job_id);
                return Some(result);
            }
            self.direct_post_repair_results
                .insert(job_id, (result_set_id, result));
        }
        if let Some(in_flight) = self.direct_post_repair_in_flight.get(&job_id) {
            // A ticket for a *different* recovery set is not this call's to
            // wait on — the set it was reading has been rebound out from
            // under it (a re-parsed index, a different served set) — and
            // nothing ever clears it on its own: `handle_direct_post_repair_done`
            // only ever discards a mismatched *work id* against a `recovery_set_id`
            // it already agrees with, so a mismatched `recovery_set_id` here
            // means that done message, whenever it lands, will find no taker
            // either. Left alone, this was a permanent park: no new ticket
            // ever starts, so no result ever arrives, so nothing ever re-arms
            // the job. Dropping the stale entry (and any result parked
            // beside it under the old set id) frees the slot for a fresh
            // ticket against the set this call actually cares about; the
            // work id we are about to hand out fences the old task's done
            // message if it lands late.
            if in_flight.recovery_set_id != recovery_set_id {
                warn!(
                    job_id = job_id.0,
                    stale_recovery_set_id = ?in_flight.recovery_set_id,
                    "dropping a direct post-repair ticket parked against a recovery set this \
                     job no longer serves"
                );
                self.direct_post_repair_in_flight.remove(&job_id);
                self.direct_post_repair_results.remove(&job_id);
            } else {
                return None;
            }
        }

        self.next_direct_post_repair_work_id = self.next_direct_post_repair_work_id.wrapping_add(1);
        let work_id = self.next_direct_post_repair_work_id;
        let submitted_at = Instant::now();
        self.direct_post_repair_in_flight.insert(
            job_id,
            DirectPostRepairWork {
                work_id,
                recovery_set_id,
                submitted_at,
            },
        );
        info!(
            job_id = job_id.0,
            work_id,
            files = to_read.len(),
            selective,
            "submitting a direct post-repair verification ticket"
        );

        let pp_pool = self.pp_pool.clone();
        let done_tx = self.direct_post_repair_done_tx.clone();
        tokio::spawn(async move {
            let joined = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    crate::e2e_failpoint::maybe_delay("direct_store.post_repair_verify");
                    if to_read.is_empty() {
                        return par2_rs::VerificationResult {
                            files: Vec::new(),
                            recovery_blocks_available: par2_set.recovery_block_count(),
                            total_missing_blocks: 0,
                            repairable: par2_rs::verify::Repairability::NotNeeded,
                        };
                    }
                    par2_rs::verify_selected_file_ids_with_options(
                        &par2_set,
                        access.as_ref(),
                        &to_read,
                        &crate::pipeline::completion::finalize::check::selective_pass_verify_options(),
                    )
                })
            })
            .await;
            let result = joined
                .map_err(|error| format!("direct post-repair verification panicked: {error}"));
            let _ = done_tx
                .send(DirectPostRepairWorkDone {
                    job_id,
                    work_id,
                    recovery_set_id,
                    result,
                })
                .await;
        });
        None
    }

    pub(in crate::pipeline) fn handle_direct_post_repair_done(
        &mut self,
        done: DirectPostRepairWorkDone,
    ) {
        let Some(in_flight) = self.direct_post_repair_in_flight.get(&done.job_id) else {
            return;
        };
        if in_flight.work_id != done.work_id || in_flight.recovery_set_id != done.recovery_set_id {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                "discarding stale direct post-repair verification"
            );
            return;
        }
        let elapsed = in_flight.submitted_at.elapsed();
        let outcome = match &done.result {
            Ok(verification) if verification.needs_repair() => "damaged",
            Ok(_) => "clean",
            Err(_) => "error",
        };
        info!(
            job_id = done.job_id.0,
            work_id = done.work_id,
            elapsed_ms = elapsed.as_millis() as u64,
            outcome,
            "direct post-repair verification ticket completed"
        );
        crate::runtime::perf_probe::record("direct_store.post_repair_verify", elapsed);
        if !self.jobs.contains_key(&done.job_id) {
            self.direct_post_repair_in_flight.remove(&done.job_id);
            return;
        }
        self.direct_post_repair_results
            .insert(done.job_id, (done.recovery_set_id, done.result));
        self.schedule_job_completion_check(done.job_id);
    }

    /// One verification pass over the job's recovery set, reading every live
    /// direct volume virtually and emitting nothing.
    ///
    /// The verdict is **adjusted before it is returned**, by exactly the two
    /// rules the authoritative pass applies to its own
    /// ([`Pipeline::apply_direct_damage_adjustments`]). Skipping them was not a
    /// small omission: a job with a *finalized* direct set beside a live
    /// damaged one reads every finalized volume as `Missing` here,
    /// `damaged_files_by_set` finds no live owner for them and refuses the
    /// whole attempt with `DamageOutsideDirectSets` — so the live set demotes
    /// for damage that belongs to files the job legitimately finished without,
    /// which is precisely the case repair exists for.
    ///
    /// # Before a repair, and after one
    ///
    /// The same pass runs on both sides of a repair-while-direct, and the two
    /// are not asking the same question.
    ///
    /// *Before*, it is asking whether the set is damaged, and a file the
    /// dual-CRC grid adjudicated in stream is answered from that evidence
    /// rather than read. That is the clean path and it is unchanged.
    ///
    /// *After*, it is asking whether the repair landed — and that question has
    /// to be answered by reading the bytes. Every claim source this pass has is
    /// a statement about what the **wire** delivered: the grid folds per-article
    /// CRCs recorded at the durability seam, and the session is seeded from the
    /// same verdicts. None of them can see a `pwrite` that silently short-wrote,
    /// a bad sector under the envelope, or a repaired span that never reached
    /// the platter. A direct set's source volumes are exactly the files nothing
    /// else ever re-reads, so if this pass stands on wire evidence, a disk fault
    /// under a repaired set ships in a `Completed` job.
    ///
    /// So a post-repair pass takes no *wire* claims — the grid and the
    /// session are both skipped, unconditionally and with no knob to turn
    /// that off. See [`Self::direct_sets_repaired_in_place`] for how the two
    /// are told apart.
    ///
    /// It does not follow that every described file is read, though. When
    /// [`Pipeline::resolve_direct_sets_before_par2_repairer_for_set`] left a
    /// [`DirectPostRepairCarry`] for this recovery set, the files the repair
    /// did not rewrite carry their entry forward from that *disk* read — the
    /// pre-repair pass's own, taken minutes ago in this same flow — rather
    /// than being re-read. That is not wire evidence standing in for a read;
    /// it is the same trust class [`Pipeline::verify_repaired_par2_files_with_placement`]
    /// already extends to a conventional set's untouched files, applied here
    /// for the same reason: the repair could only ever have rewritten the
    /// files its own pre-repair verdict called not-`Complete`, so re-reading
    /// the rest answers a question the disk already answered once this pass.
    /// A carry that is missing or stale for this recovery set gets no such
    /// shortcut; every described file is read, which is this pass's answer
    /// whenever it cannot prove a narrower one is enough.
    ///
    /// The reads themselves go to real files — [`super::super::provider::VirtualVolumeReader`]
    /// holds an open handle on the envelope and on each member `.direct.partial`
    /// — so "read the bytes" here means the same thing it means for a
    /// conventional file, even though the volume it reconstructs is virtual.
    ///
    /// # Why the post-repair pass may still verify from slice proof
    ///
    /// `fast_verify` is not a sampled read: par2-rs proves an intact candidate
    /// from its per-slice IFSC checksums scanned at read speed and skips only
    /// the inherently serial whole-file MD5, and a file it cannot prove that way
    /// falls through to the strict pipeline with its per-slice accounting fully
    /// intact (par2-rs `verify.rs`, the `fast_verify && let Some(..)` arms). So
    /// every byte is still read and a damaged volume — the only kind whose
    /// accounting a follow-up repair would be sized from — is still measured
    /// slice by slice.
    ///
    /// The pre-repair pass keeps the strict default. Its verdict is what sizes
    /// the repair, and it is not the pass this optimisation was measured for.
    pub(crate) async fn verify_direct_sets_quietly(
        &mut self,
        job_id: JobId,
        par2_set: std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: PathBuf,
    ) -> Option<par2_rs::VerificationResult> {
        let overlay = self.direct_par2_overlay_for_set(job_id, par2_set.recovery_set_id)?;
        let overlay_set_id = overlay.recovery_set_id;
        let volumes = overlay.volumes.clone();
        let provider = overlay.provider;
        // No placement scan: the direct volumes are absent from the directory
        // by construction and every other file is at its declared name, which
        // is the same assumption the repair's own fallback access makes.
        let plan = par2_rs::PlacementPlan {
            exact: volumes.iter().map(|volume| volume.par2_file_id).collect(),
            swaps: Vec::new(),
            renames: Vec::new(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
        };
        let inner = par2_rs::PlacementFileAccess::from_plan(working_dir.clone(), &par2_set, &plan);
        let access = std::sync::Arc::new(super::super::par2_access::DirectVolumeFileAccess::new(
            inner, provider, &volumes,
        ));

        // A repair already ran for one of this job's live sets, so this pass is
        // the read-back that decides whether it landed. Every claim below is
        // wire evidence; see this function's docs for why none of it may stand
        // in on this pass.
        let post_repair = self.direct_sets_repaired_in_place(job_id);

        // The narrower read this pass may take instead: the write set a live
        // carry names, but only when the carry is actually for the recovery
        // set this call is resolving. A mismatch means the carry belongs to a
        // repair against a set this job no longer serves — the set was
        // rebound by a later PAR2 index, say — and using its write set here
        // would silently stand in for files a *different* set's pre-repair
        // pass vouched for. Cloned out from under the borrow up front so the
        // mutable calls below are free to take the carry for real once the
        // read they start actually finishes.
        let selective_write_set: Option<Vec<par2_rs::FileId>> = post_repair
            .then(|| self.direct_post_repair_carry.get(&job_id))
            .flatten()
            .filter(|carry| carry.recovery_set_id == par2_set.recovery_set_id)
            .map(|carry| carry.write_set.clone());

        let session_verification = if post_repair {
            None
        } else {
            self.verify_direct_sets_through_session(
                job_id,
                overlay_set_id,
                &par2_set,
                &working_dir,
                &access,
            )
            .await
        };

        let mut verification = match session_verification {
            Some(verification) => verification,
            None if post_repair && selective_write_set.is_some() => {
                // The selective post-repair read-back: only the volumes the
                // repair rewrote, standing in for everything else with the
                // pre-repair pass's own entries. The direct-store mirror of
                // [`Pipeline::verify_repaired_par2_files_with_placement`] —
                // see this function's docs for why the carry, not the grid or
                // the session, is what a post-repair pass may stand on.
                let to_read = selective_write_set.expect("checked by the match guard");
                #[cfg(test)]
                {
                    self.direct_post_repair_read_splits.push((0, to_read.len()));
                }
                info!(
                    job_id = job_id.0,
                    rewritten = to_read.len(),
                    "post-repair direct-store verification reads only what the repair rewrote"
                );
                let fresh = match self.take_or_start_direct_post_repair_verification(
                    job_id,
                    std::sync::Arc::clone(&par2_set),
                    std::sync::Arc::clone(&access),
                    to_read,
                    true,
                ) {
                    Some(Ok(fresh)) => fresh,
                    Some(Err(error)) => {
                        warn!(
                            job_id = job_id.0,
                            error = %error,
                            "direct post-repair verification failed"
                        );
                        // The carry answered no question this attempt — the
                        // read that was meant to settle it never landed — so
                        // it must not survive to describe a future attempt
                        // against bytes that may have moved again by then.
                        self.direct_post_repair_carry.remove(&job_id);
                        return None;
                    }
                    None => return None,
                };
                // Taken only now that a fresh read actually landed: while the
                // ticket is still in flight, later laps of this same pass
                // need the carry's write set again to resubmit or to notice
                // the ticket is already running, so it stays in the map
                // until there is a result to fold it into.
                let carry = self
                    .direct_post_repair_carry
                    .remove(&job_id)
                    .expect("selective_write_set was read from a live carry moments ago");
                par2_rs::verify::merge_verification_results(&par2_set, &carry.pre_repair, fresh)
            }
            None => {
                // Read and verify through the access adapter. A direct set's
                // source volumes have no files, so the adapter answers every
                // read out of the envelope plus the routed member partials —
                // and for an encrypted set the overlay re-derives the posted
                // cipher on the way out — which is what lets the ordinary pass
                // reach a verdict without materializing a single volume.
                //
                // The grid's claims are honoured **here** too, per file. The
                // session above is all-or-nothing by necessity, and a set with
                // one damaged volume therefore always lands in this arm — where
                // re-reading the volumes the decode pass already proved clean is
                // pure cost. So the files the grid adjudicated are stood in for,
                // and only the rest are read. The bar is the session's own,
                // unchanged: every described slice `Intact` with independent
                // (pCRC-verified) article coverage at exactly the described
                // length, over bytes that were durable before the claim was
                // made. Anything less is not adjudicated and is read.
                //
                // Post-repair, no file is stood in for here either — this arm
                // is reached post-repair only when there is no live carry for
                // this recovery set (a restart, an evicted job, a set that was
                // rebound since the repair ran), and the sibling arm above is
                // what a fresh carry routes to instead. Without one there is
                // nothing to merge a selective read against, so the fallback
                // is the same full, unconditional read this pass has always
                // taken post-repair: every described file, standing in for
                // none of them.
                // Volumes whose posted bytes an archive checksum already
                // contradicted. Their grid claims come from the same wire that
                // vouched for the damage, so they are dropped here and the
                // volumes are read — which is also what gives the repair a
                // slice-accurate account of what to rebuild.
                let suspect = self.direct_suspect_par2_file_ids(job_id, overlay_set_id);
                let claimed = if post_repair {
                    Vec::new()
                } else {
                    let mut claimed = self.grid_claimed_file_verifications(job_id, &par2_set);
                    claimed.retain(|file| !suspect.contains(&file.file_id));
                    claimed
                };
                let claimed_ids: HashSet<par2_rs::FileId> =
                    claimed.iter().map(|file| file.file_id).collect();
                let to_read: Vec<par2_rs::FileId> = par2_set
                    .recovery_file_ids
                    .iter()
                    .copied()
                    .filter(|file_id| !claimed_ids.contains(file_id))
                    .collect();
                if !claimed.is_empty() {
                    debug!(
                        job_id = job_id.0,
                        claimed_in_stream = claimed.len(),
                        read = to_read.len(),
                        "the direct read-and-verify pass is standing in for volumes the \
                         dual-CRC grid already adjudicated"
                    );
                    crate::runtime::perf_probe::record_value(
                        "direct_store.verify.files_claimed_in_stream",
                        claimed.len() as u64,
                    );
                }
                if !post_repair && !to_read.is_empty() {
                    // Why each read is happening at all: the first failing
                    // rung of the claim ladder, per unclaimed file, folded
                    // into a histogram. A healthy 100%-grid-fed job that
                    // still pays a multi-minute read should say WHY in its
                    // own log line, not leave a silent gap to reconstruct
                    // from timestamps.
                    let to_read_set: HashSet<par2_rs::FileId> = to_read.iter().copied().collect();
                    let mut shortfalls: BTreeMap<&'static str, u32> = BTreeMap::new();
                    let mut bound: HashSet<par2_rs::FileId> = HashSet::new();
                    if let Some(state) = self.jobs.get(&job_id) {
                        let file_ids: Vec<NzbFileId> =
                            state.assembly.files().map(|file| file.file_id()).collect();
                        for file_id in file_ids {
                            let Some(binding) = self.resolve_par2_file_binding_in_set(
                                file_id,
                                par2_set.recovery_set_id,
                            ) else {
                                continue;
                            };
                            if !to_read_set.contains(&binding.par2_file_id) {
                                continue;
                            }
                            bound.insert(binding.par2_file_id);
                            if let Some(reason) =
                                self.in_stream_par2_claim_shortfall(file_id, &par2_set)
                            {
                                *shortfalls.entry(reason).or_default() += 1;
                            }
                        }
                    }
                    let unbound = to_read_set.len().saturating_sub(bound.len());
                    if unbound > 0 {
                        *shortfalls.entry("no_bound_pipeline_file").or_default() += unbound as u32;
                    }
                    info!(
                        job_id = job_id.0,
                        read = to_read.len(),
                        shortfalls = ?shortfalls,
                        "direct verify is reading files the grid could not claim"
                    );
                    for (reason, count) in &shortfalls {
                        crate::runtime::perf_probe::record_value_owned(
                            format!("direct_store.verify.claim_shortfall.{reason}"),
                            u64::from(*count),
                        );
                    }
                }
                #[cfg(test)]
                {
                    self.direct_verify_read_splits
                        .push((claimed.len(), to_read.len()));
                    if post_repair {
                        self.direct_post_repair_read_splits
                            .push((claimed.len(), to_read.len()));
                    }
                }
                if to_read.is_empty() {
                    // Nothing left to read: every described file carries an
                    // in-stream proof. Synthesised in exactly the shape the
                    // completion gate's quick pass synthesises for the same
                    // evidence on the conventional side.
                    par2_rs::VerificationResult {
                        files: claimed,
                        recovery_blocks_available: par2_set.recovery_block_count(),
                        total_missing_blocks: 0,
                        repairable: par2_rs::verify::Repairability::NotNeeded,
                    }
                } else {
                    let mut verification = if post_repair {
                        match self.take_or_start_direct_post_repair_verification(
                            job_id,
                            std::sync::Arc::clone(&par2_set),
                            std::sync::Arc::clone(&access),
                            to_read,
                            false,
                        ) {
                            Some(Ok(verification)) => verification,
                            Some(Err(error)) => {
                                warn!(
                                    job_id = job_id.0,
                                    error = %error,
                                    "direct post-repair verification failed"
                                );
                                return None;
                            }
                            None => return None,
                        }
                    } else {
                        // The grid's per-slice proofs for the very files being
                        // read: a file lands in `to_read` when one slice is
                        // damaged or unverdicted, but every slice the grid DID
                        // prove is attested here, so the pass seeks over the
                        // proven ranges and reads only the slices in
                        // question. Same bar as the whole-file claim above,
                        // applied per slice.
                        let to_read_ids: HashSet<par2_rs::FileId> =
                            to_read.iter().copied().collect();
                        let mut proven_slices: std::collections::HashMap<
                            par2_rs::FileId,
                            Vec<bool>,
                        > = std::collections::HashMap::new();
                        if let Some(state) = self.jobs.get(&job_id) {
                            let file_ids: Vec<NzbFileId> =
                                state.assembly.files().map(|file| file.file_id()).collect();
                            for file_id in file_ids {
                                let Some((par2_file_id, slices)) =
                                    self.in_stream_proven_slices(file_id, &par2_set)
                                else {
                                    continue;
                                };
                                // Same refusal as the whole-file claim above,
                                // per slice: a suspect volume is read slice by
                                // slice, with nothing proven in advance.
                                if to_read_ids.contains(&par2_file_id)
                                    && !suspect.contains(&par2_file_id)
                                {
                                    proven_slices.insert(par2_file_id, slices);
                                }
                            }
                        }
                        if !proven_slices.is_empty() {
                            let slices_proven: usize = proven_slices
                                .values()
                                .map(|slices| slices.iter().filter(|proven| **proven).count())
                                .sum();
                            info!(
                                job_id = job_id.0,
                                partially_proven_files = proven_slices.len(),
                                slices_proven,
                                "direct verify reads only the slices the grid could not prove"
                            );
                            crate::runtime::perf_probe::record_value(
                                "direct_store.verify.slices_proven_in_stream",
                                slices_proven as u64,
                            );
                        }
                        let pp_pool = self.pp_pool.clone();
                        let read_set = std::sync::Arc::clone(&par2_set);
                        let access = std::sync::Arc::clone(&access);
                        tokio::task::spawn_blocking(move || {
                            pp_pool.install(move || {
                                let mut options = par2_rs::VerifyOptions::default();
                                options.proven_slices = proven_slices;
                                par2_rs::verify_selected_file_ids_with_options(
                                    &read_set,
                                    access.as_ref(),
                                    &to_read,
                                    &options,
                                )
                            })
                        })
                        .await
                        .ok()?
                    };
                    // Appended, then re-ordered to the recovery set's own file
                    // order so the result is shaped exactly as `verify_all`'s
                    // would have been. `total_missing_blocks` is untouched — a
                    // claimed file contributes no missing block — and
                    // `refresh_repairability` re-reads the assessment over the
                    // combined files, preserving a resource-limited verdict the
                    // read half may have reached.
                    verification.files.extend(claimed);
                    let order: HashMap<par2_rs::FileId, usize> = par2_set
                        .recovery_file_ids
                        .iter()
                        .enumerate()
                        .map(|(position, file_id)| (*file_id, position))
                        .collect();
                    verification.files.sort_by_key(|file| {
                        order.get(&file.file_id).copied().unwrap_or(usize::MAX)
                    });
                    verification.refresh_repairability();
                    verification
                }
            }
        };
        let adjustments = self.apply_direct_damage_adjustments(job_id, &mut verification);
        if adjustments.any() {
            debug!(
                job_id = job_id.0,
                skipped_blocks = adjustments.skipped_blocks,
                retained_suspect_blocks = adjustments.retained_suspect_blocks,
                forgiven_direct_blocks = adjustments.forgiven_direct_blocks,
                "adjusted the quiet direct-set pass before attributing damage"
            );
        }
        #[cfg(test)]
        {
            self.last_direct_verdict = Some(verification.clone());
        }
        Some(verification)
    }

    /// Has a repair-while-direct already run for one of this job's live sets?
    ///
    /// The discriminator between the two passes
    /// [`Self::verify_direct_sets_quietly`] serves. The latch it reads is burned
    /// at a repair's first irreversible step, so it is true from the moment any
    /// byte of a set could have moved — which is exactly when a claim about what
    /// the wire delivered stops being a claim about what is on disk.
    ///
    /// Per job rather than per set. The pass verifies the job's whole recovery
    /// set in one go and its claim sources are job-scoped, so there is no
    /// coherent way to read half of it from evidence and half from disk; one
    /// repaired set makes the whole pass a read-back.
    ///
    /// Demoted sets are skipped: their volumes are real files that the
    /// conventional repairer and its own post-repair pass now own.
    ///
    /// # This is defence in depth, and it is worth having anyway
    ///
    /// The repaired set's grid claims are already retired on a post-repair pass:
    /// the repair drops its affected files before it rewrites a byte, and the
    /// session arm is gated on that evidence. A counterfactual run with both
    /// guards removed still reads every volume back.
    ///
    /// It stays because the emptiness is a *consequence* of a decision made
    /// several hundred lines away, for a different reason — retiring claims over
    /// bytes that moved — and the requirement here is a different statement: a
    /// post-repair pass must read the disk. Deriving a safety property from
    /// another decision's side effect is how it lapses silently when that
    /// decision is refactored. One bool is a cheap price for saying it where it
    /// is meant.
    /// The PAR2 descriptions of live direct volumes carrying a recorded
    /// part-checksum mismatch.
    ///
    /// The bridge between an archive-level fact and a PAR2-level one. A volume
    /// lands here because RAR's own packed checksum disagreed with bytes the
    /// wire delivered *and vouched for*, which makes every claim derived from
    /// that same wire — the retained session's slice evidence, the dual-CRC
    /// grid's whole-file and per-slice proofs — evidence from the witness whose
    /// account is in question. So the pass reads these volumes, and nothing
    /// stands in for them.
    ///
    /// Empty for every set with no damage on record, which is every set in
    /// every healthy job: the claim ladder is untouched for them.
    pub(crate) fn direct_suspect_par2_file_ids(
        &self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> HashSet<par2_rs::FileId> {
        let mut suspect: HashSet<par2_rs::FileId> = HashSet::new();
        for set in self.direct_store.sets_for(job_id) {
            if set.is_demoted() || set.is_finalized() {
                continue;
            }
            for volume_index in set.router.damaged_volumes() {
                let Some(file_index) = set.plan().volumes.get(volume_index).copied() else {
                    continue;
                };
                if let Some(binding) = self.resolve_par2_file_binding_in_set(
                    NzbFileId { job_id, file_index },
                    recovery_set_id,
                ) {
                    suspect.insert(binding.par2_file_id);
                }
            }
        }
        suspect
    }

    pub(in crate::pipeline) fn direct_sets_repaired_in_place(&self, job_id: JobId) -> bool {
        self.direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.repair_attempted())
    }

    /// The `FileVerification` entries the dual-CRC grid can stand in for, in
    /// the shape `par2_rs::verify_all` would have produced by reading them.
    ///
    /// The claim is per description and it is the same claim
    /// [`Pipeline::grid_adjudicated_par2_bindings`] makes for the whole set:
    /// this file bound uniquely to this description, its assembled decoded
    /// length equals the described length, and every described slice closed
    /// `Intact` with independent article coverage. Nothing here is derived from
    /// the *pass*; it is derived from evidence the download seam recorded once
    /// the bytes were durable.
    ///
    /// Empty on ambiguity — two pipeline files claiming one description — so an
    /// unresolvable binding costs the reads it always did rather than producing
    /// a claim from a resolution that cannot be trusted.
    pub(crate) fn grid_claimed_file_verifications(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Vec<par2_rs::verify::FileVerification> {
        let Some(adjudicated) = self.grid_adjudicated_par2_file_ids(job_id, par2_set) else {
            return Vec::new();
        };
        if adjudicated.is_empty() {
            return Vec::new();
        }
        par2_set
            .recovery_file_ids
            .iter()
            .filter(|file_id| adjudicated.contains(file_id))
            .filter_map(|file_id| {
                let description = par2_set.file_description(file_id)?;
                let slice_count = par2_set.slice_count_for_file(description.length) as usize;
                Some(par2_rs::verify::FileVerification {
                    file_id: *file_id,
                    // The description's own name, not a sanitized one: that is
                    // what the read pass puts here, and a consumer that
                    // compares the two must not be able to tell which produced
                    // the entry.
                    filename: description.filename.clone(),
                    status: par2_rs::verify::FileStatus::Complete,
                    valid_slices: vec![true; slice_count],
                    missing_slice_count: 0,
                })
            })
            .collect()
    }

    /// The retained session's verdict for a job's direct sets, or `None` to
    /// fall back to the read-and-verify pass.
    ///
    /// # Why this can refuse
    ///
    /// An access-backed session reads **no** source bytes: `analyze()` skips
    /// the scan entirely, because `base_dir` holds no sources to find. It
    /// reports what its evidence established and nothing more. So it can stand
    /// in for the pass only when the dual-CRC grid already adjudicated every
    /// described slice in stream, which is what
    /// [`Pipeline::grid_adjudicated_par2_bindings`] checks. A slice with no
    /// verdict does not qualify, and one of those is enough to send the whole
    /// job back to `verify_all`, which can actually read a virtual volume.
    ///
    /// Refusing is therefore ordinary, not exceptional — any set the grid could
    /// not fully claim in stream takes the pass, as does every damaged one.
    ///
    /// # What feeds the gate
    ///
    /// The grid is fed for a direct volume by `commit_direct_segment`, in
    /// source-volume coordinates, on the same durability contract the
    /// conventional seam states: the article's destination writes returned
    /// before the claim was recorded. So a clean direct set can satisfy the gate
    /// and take this arm, and a set the grid could only partly claim falls to
    /// the pass below — which stands in for the files it *did* claim and reads
    /// only the rest.
    pub(super) async fn verify_direct_sets_through_session(
        &mut self,
        job_id: JobId,
        overlay_set_id: par2_rs::RecoverySetId,
        par2_set: &std::sync::Arc<par2_rs::Par2FileSet>,
        working_dir: &std::path::Path,
        access: &std::sync::Arc<super::super::par2_access::DirectVolumeFileAccess>,
    ) -> Option<par2_rs::VerificationResult> {
        if overlay_set_id != par2_set.recovery_set_id {
            return None;
        }
        // A volume with an archive-level checksum mismatch on record cannot be
        // reported from wire evidence, and this session is all-or-nothing: one
        // suspect volume and the whole pass reads instead. See
        // [`Self::direct_suspect_par2_file_ids`].
        if !self
            .direct_suspect_par2_file_ids(job_id, overlay_set_id)
            .is_empty()
        {
            return None;
        }
        if !self.grid_adjudicated_par2_bindings(job_id, par2_set) {
            return None;
        }
        // Blocks the decode pass already adjudicated are what this session
        // reports from: they cost no I/O, and the gate above proved they cover
        // every described slice.
        let set_id = overlay_set_id;
        let in_stream = self.in_stream_slice_evidence_for_set(job_id, set_id);
        if in_stream.is_empty() {
            return None;
        }

        let memory_limit =
            crate::pipeline::completion::finalize::check::configured_par2_repair_memory_limit_bytes(
            );
        let handle: std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync> =
            std::sync::Arc::clone(access) as std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync>;
        let (mut session, _) = match self
            .take_or_open_par2_repair_session(
                job_id,
                set_id,
                working_dir.to_path_buf(),
                memory_limit,
                None,
                Some(handle),
            )
            .await
        {
            Ok(Some(session)) => session,
            Ok(None) => return None,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable for the direct pass");
                return None;
            }
        };

        let pp_pool = self.pp_pool.clone();
        let par2_set = std::sync::Arc::clone(par2_set);
        let joined = tokio::task::spawn_blocking(move || {
            let outcome = pp_pool.install(|| {
                // Keyed by FileId, not by path: a direct volume has no path to
                // key on.
                for slice in in_stream {
                    if let Err(error) = session.add_slice_evidence_for_file(slice) {
                        return Err(format!("failed to seed in-stream slice evidence: {error}"));
                    }
                }
                session
                    .analyze()
                    .map_err(|error| format!("direct session analysis failed: {error}"))
            });
            (session, outcome, par2_set)
        })
        .await;

        let (session, outcome, _) = match joined {
            Ok(joined) => joined,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "direct PAR2 session task panicked");
                return None;
            }
        };
        self.restore_par2_repair_session(job_id, set_id, session);
        match outcome {
            Ok(outcome) => {
                #[cfg(test)]
                {
                    self.direct_session_pass_calls += 1;
                }
                Some(outcome.verification)
            }
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "falling back to the direct read-and-verify pass");
                None
            }
        }
    }

    /// Repair-while-direct. [`DirectRepairAnswer::Declined`] means nothing was
    /// repaired and the caller should fall back to demotion;
    /// [`DirectRepairAnswer::Deferred`] means the sets are waiting for recovery
    /// that has been asked for, and the caller must leave them alone.
    pub(super) async fn repair_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> DirectRepairAnswer {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return DirectRepairAnswer::Declined;
        };
        let Some(overlay) = self.direct_par2_overlay(job_id) else {
            return DirectRepairAnswer::Declined;
        };
        // The same settle guard the demotion path carries, in the same shape:
        // while articles are in flight a set's outstanding ranges read as
        // holes, and PAR2 cannot tell a hole from corruption. Repairing on that
        // verdict would spend recovery blocks rebuilding bytes that are still
        // on their way. A set whose volumes have all finished downloading is
        // settled whatever the rest of the job is doing, which is what keeps a
        // job with one slow conventional file from blocking its RAR set's
        // repair.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        if matches!(
            verification.repairable,
            par2_rs::verify::Repairability::NotNeeded
        ) {
            return DirectRepairAnswer::Declined;
        }

        let by_set = match super::super::repair::damaged_files_by_set(verification, |file_id| {
            overlay.owner_of(file_id)
        }) {
            Ok(by_set) => by_set,
            Err(failure) => {
                Self::record_direct_repair_failure(job_id, &failure);
                return DirectRepairAnswer::Declined;
            }
        };
        if by_set.is_empty() {
            return DirectRepairAnswer::Declined;
        }

        // The wait, decided **before** the first attempt.
        //
        // `blocks_available` counts recovery slices that have been *merged*, and
        // recovery volumes are only fetched once damage is known — so the first
        // damage verdict of a job's life always reads zero, and any damage at
        // all exceeds zero. Attempting the repair anyway is not free: the
        // attempt burns the set's one-shot latch, deletes its checkpoint row and
        // retires its live-PAR2 state before the planner gets far enough to say
        // it has nothing to repair with. So the set would arrive at its own
        // retry already latched, and demote for a verdict the arriving recovery
        // was about to answer.
        //
        // Deciding here instead costs the set nothing — the same reasoning the
        // over-budget pre-check is built on — and leaves the deferred pass as
        // the set's *first* real attempt, which is what the latch is for.
        if let par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } =
            verification.repairable
        {
            // Waiting is only ever right for a set that could act on the
            // recovery when it arrives. A demoted or finalized set has left,
            // an unsettled one's "damage" may be bytes in flight — promoting
            // recovery to rebuild those would spend the bandwidth the deferred
            // fetch exists to save — and a latched one will refuse the retry
            // with `AlreadyRepaired` however much recovery lands.
            let any_set_could_use_it = by_set.keys().any(|set_index| {
                self.direct_store
                    .set(job_id, *set_index)
                    .is_some_and(|set| {
                        !set.is_demoted()
                            && !set.is_finalized()
                            && (payload_settled || set.all_volumes_complete())
                            && !set.repair_attempted()
                    })
            });
            if blocks_needed > 0
                && any_set_could_use_it
                && self.defer_direct_repair_for_recovery(
                    job_id,
                    blocks_needed,
                    verification.recovery_blocks_available,
                )
            {
                return DirectRepairAnswer::Deferred;
            }
            // Not waiting, and not attempting either: the planner refuses an
            // insufficient verdict outright, so the attempt below could only
            // burn the latch, the checkpoint row and the live-PAR2 state on
            // its way to the same refusal — and the set would then face its
            // retry already latched. Declining from here costs the sets
            // nothing. The caller can demote or preserve the virtual sources
            // for an eligible PAR3 handoff. The wave budget belongs to the wait that just
            // ended, and the next damage verdict starts its own.
            self.direct_store.repair_defer_waves.remove(&job_id);
            let any_live_settled = by_set.keys().any(|set_index| {
                self.direct_store
                    .set(job_id, *set_index)
                    .is_some_and(|set| {
                        !set.is_demoted()
                            && !set.is_finalized()
                            && (payload_settled || set.all_volumes_complete())
                    })
            });
            if any_live_settled {
                Self::record_direct_repair_failure(
                    job_id,
                    &super::super::repair::DirectRepairFailure::Unrepairable,
                );
                warn!(
                    job_id = job_id.0,
                    failure = %super::super::repair::DirectRepairFailure::Unrepairable,
                    "direct PAR2 repair exhausted reachable recovery"
                );
            }
            return DirectRepairAnswer::Declined;
        }
        // Any wave the job was waiting through has delivered: the verdict no
        // longer reads Insufficient, and the attempt below is the wait's
        // conclusion whichever way it goes.
        self.direct_store.repair_defer_waves.remove(&job_id);

        let mut repaired_any = false;
        for (set_index, files) in by_set {
            if !self
                .direct_store
                .set(job_id, set_index)
                .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
            {
                continue;
            }
            if !payload_settled
                && !self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(DirectSet::all_volumes_complete)
            {
                continue;
            }
            // The bound. A set that has already been repaired and is damaged
            // again is a set the repair did not fix, and running it a second
            // time reaches the same verdict — so it demotes instead, which is
            // what every other refusal here does.
            if self
                .direct_store
                .set(job_id, set_index)
                .is_some_and(DirectSet::repair_attempted)
            {
                Self::record_direct_repair_failure(
                    job_id,
                    &super::super::repair::DirectRepairFailure::AlreadyRepaired,
                );
                continue;
            }
            match self
                .repair_one_direct_set(job_id, set_index, &par2_set, verification, &overlay, &files)
                .await
            {
                Ok(()) => repaired_any = true,
                Err(failure) => {
                    Self::record_direct_repair_failure(job_id, &failure);
                    warn!(
                        job_id = job_id.0,
                        failure = %failure,
                        "repairing a direct set in place was not possible; demoting it"
                    );
                    // A refusal that got as far as routing has already demoted
                    // the set itself — a destination write failed, a repaired
                    // span found no destination — and a demoted set is a state
                    // change the caller has to act on exactly as a repair is:
                    // its volumes are materializing, so the job's next move is a
                    // fresh pass over them, not another lap of the verdict that
                    // sent it here.
                    let already_demoted = self
                        .direct_store
                        .set(job_id, set_index)
                        .is_some_and(DirectSet::is_demoted);
                    return if repaired_any || already_demoted {
                        DirectRepairAnswer::Acted
                    } else {
                        DirectRepairAnswer::Declined
                    };
                }
            }
        }
        if repaired_any {
            DirectRepairAnswer::Acted
        } else {
            DirectRepairAnswer::Declined
        }
    }

    /// Asks for the recovery the verdict needs and says whether the sets should
    /// wait for it instead of demoting.
    ///
    /// Three questions, in the order that makes each one cheap:
    ///
    /// 1. **Can the recovery set cover this at all?** `blocks_available` is what
    ///    is merged; the NZB's advertised recovery is the ceiling. If the damage
    ///    exceeds even that, no amount of downloading helps, and the demotion
    ///    has to be immediate — the conventional path reaches the same dead end
    ///    with better diagnostics, and delaying it helps nobody.
    /// 2. **Has this job spent its waves?** The budget below.
    /// 3. **Is recovery actually coming?** Either this call promoted some, or a
    ///    previous wave is still on the wire. Neither, and there is nothing to
    ///    wait for: waiting on recovery that cannot arrive is how this branch
    ///    livelocked before, so the exhausted case demotes rather than parks.
    pub(crate) fn defer_direct_repair_for_recovery(
        &mut self,
        job_id: JobId,
        blocks_needed: u32,
        recovery_merged_now: u32,
    ) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        let total_capacity = self.total_recovery_block_capacity(job_id, set_id);
        if total_capacity < blocks_needed {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                total_capacity,
                "not waiting for recovery on a direct set: the damage exceeds every \
                 recovery block the NZB advertises"
            );
            return false;
        }

        let waves = self
            .direct_store
            .repair_defer_waves
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        // A new wave is only started while the budget lasts. Spent, the sets
        // still see out whatever is already on the wire — that wave was paid
        // for, and throwing it away one article short is the same waste the
        // whole defer exists to avoid — but nothing new is asked for, so the
        // next verdict with a quiet pipeline demotes.
        let promoted = if waves < MAX_DIRECT_REPAIR_DEFER_WAVES {
            self.promote_recovery_targeted(job_id, set_id, blocks_needed)
        } else {
            0
        };
        if promoted == 0 && !self.job_has_promoted_recovery_pipeline_work(job_id, "direct repair") {
            debug!(
                job_id = job_id.0,
                blocks_needed,
                waves,
                "not waiting for recovery on a direct set: none was promoted and none \
                 is still arriving"
            );
            return false;
        }
        if promoted > 0 {
            // Counted only for a genuinely new wave. The gate ticks many times
            // while one wave downloads and each tick re-reaches this point with
            // nothing left to promote; charging those against the budget would
            // spend it on the waiting itself.
            self.direct_store
                .repair_defer_waves
                .insert(job_id, waves + 1);
        }

        crate::runtime::perf_probe::record(
            "direct_store.repair_deferred",
            std::time::Duration::from_nanos(1),
        );
        #[cfg(test)]
        {
            self.direct_store.repair_defers += 1;
        }
        info!(
            job_id = job_id.0,
            blocks_needed,
            recovery_merged_now,
            promoted_blocks = promoted,
            total_capacity,
            wave = if promoted > 0 { waves + 1 } else { waves },
            "a direct set's damage needs recovery that has not been downloaded yet; \
             staying direct while the targeted recovery arrives"
        );
        true
    }

    pub(super) fn record_direct_repair_failure(
        job_id: JobId,
        failure: &super::super::repair::DirectRepairFailure,
    ) {
        crate::runtime::perf_probe::record_owned(
            format!("direct_store.repair_refused.{}", failure.metric()),
            std::time::Duration::from_nanos(1),
        );
        debug!(job_id = job_id.0, failure = %failure, "direct-store repair refused");
    }

    /// One set's repair, from the checkpoint delete to the scratch cleanup.
    pub(super) async fn repair_one_direct_set(
        &mut self,
        job_id: JobId,
        set_index: usize,
        par2_set: &std::sync::Arc<par2_rs::Par2FileSet>,
        verification: &par2_rs::VerificationResult,
        overlay: &DirectPar2Overlay,
        files: &[par2_rs::FileId],
    ) -> Result<(), super::super::repair::DirectRepairFailure> {
        let slice_size = par2_set.slice_size;
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        };
        let set_name = set.set_name().to_string();
        let holds_budget = set.holds_budget();
        // The set's own volume lengths, in *set* volume space, which is what a
        // provider over this set alone needs — the overlay's own copy is the
        // same numbers under the same key, and it is the only place they live.
        let set_lengths: std::collections::BTreeMap<u32, u64> = overlay
            .lengths
            .iter()
            .find(|(index, _)| *index == set_index)
            .map(|(_, lengths)| lengths.clone())
            .unwrap_or_default();

        // The damaged volumes, in the set's own volume space. `overlay` is keyed
        // by the job's file index, which is what makes one provider answer for
        // every set of a job; the set's plan translates back.
        let mut damaged = Vec::new();
        let mut affected_files = Vec::new();
        for file_id in files {
            let Some(file_index) = overlay.file_index_of(file_id) else {
                return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            affected_files.push(NzbFileId { job_id, file_index });
            let Some(volume_index) = set.plan().volume_for_file(file_index) else {
                return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            let Some(file) = verification
                .files
                .iter()
                .find(|file| &file.file_id == file_id)
            else {
                return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            // The **PAR2-described** length, not the assembly's received bytes.
            // A volume whose damage is a lost article is short by exactly that
            // article, and materializing it at the short length would truncate
            // the very slices the repair is about to write. The description is
            // the authoritative length in the coordinate space every slice
            // offset is defined in, which is what the repair needs and what the
            // conventional path would have restored the file to.
            let Some(len) = par2_set
                .file_description(file_id)
                .map(|description| description.length)
            else {
                return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
            };
            let ranges = super::super::repair::damaged_ranges(&file.valid_slices, slice_size, len);
            let rewrite = super::super::repair::widen_to_articles(
                &ranges,
                &set.segment_extents(volume_index),
                len,
            );
            damaged.push(super::super::repair::DamagedDirectVolume {
                volume_index,
                par2_file_id: *file_id,
                len,
                path: set.plan().repair_path(volume_index),
                rewrite,
                reconstruction: VolumeReconstruction {
                    // **The job's file index, not the set's volume index.** The
                    // sweep reads through the hybrid provider, and the provider
                    // is keyed by file index so that one instance can answer for
                    // every set of a job — see `virtual_volumes_for`. The two
                    // coincide only when a set's volumes happen to be NZB files
                    // `0..n-1`, which is true of every fixture (PAR2 is appended
                    // last) and false the moment a `.par2` or `.nfo` leads the
                    // NZB or the job carries a second set: the sweep would then
                    // read *another* volume's bytes, fail its composed CRC32 and
                    // demote the whole set with only a metric to say why. The
                    // scratch `path` stays in set space, because that is what
                    // names the file.
                    //
                    // Nothing on this path reads the index back:
                    // `repair_damaged_volumes` discards `reconstruct_volumes`'
                    // `Ok`, so it survives only inside a
                    // [`ReconstructionFailure`]'s message.
                    volume_index: file_index,
                    path: set.plan().repair_path(volume_index),
                    len,
                    // The materialized copy is a repair target, never a
                    // completed-file claim, so nothing reads the `complete`
                    // flag the sweep derives from this — and the pass only runs
                    // once the payload has settled anyway.
                    assembly_complete: true,
                    // Placed bytes and holds alike: the provider serves both,
                    // and an encrypted member's held edge block is the byte
                    // the composition needs to reach the article boundary.
                    covered: set.volume_coverage_with_holds(volume_index),
                    crcs: set.volume_crc_runs(volume_index),
                    // The raw physical coverage, not the article-whole clip the
                    // demotion sweep takes: PAR2 needs every slice it judged
                    // valid to be in the scratch, and those reach to the placed
                    // frontier, not to the last whole article. An encrypted
                    // member's frontier before a hole is *always* inside the
                    // last article — its final cipher block waits for the block
                    // after it — so refusing that run would demote every
                    // encrypted set the moment it needed a repair.
                    partial_article: super::super::reconstruct::PartialArticle::CarryThrough,
                },
            });
        }
        if damaged.is_empty() {
            return Err(super::super::repair::DirectRepairFailure::DamageOutsideDirectSets);
        }
        // Sized **before** anything is materialized, read or deleted, so an
        // over-budget repair costs the set nothing and demotes with a name.
        // Every repaired byte re-enters the router as a hold, so the holds
        // budget is the ceiling it is charged against; reading first and
        // finding out afterwards is what let a three-volume rewrite of a large
        // set peak at gigabytes with nothing bounding it.
        let rewrite_bytes: u64 = damaged
            .iter()
            .flat_map(|volume| volume.rewrite.iter())
            .map(|(start, end)| end.saturating_sub(*start))
            .sum();
        if rewrite_bytes > holds_budget {
            return Err(
                super::super::repair::DirectRepairFailure::RewriteOverBudget {
                    bytes: rewrite_bytes,
                    budget: holds_budget,
                },
            );
        }

        // Step 1: the row goes **before** any byte the row claims changes. The
        // materialization writes only scratch, but the re-route below rewrites
        // member partials and envelopes at offsets the checkpoint's floors
        // cover, and a row that outlived that would let a restart trust floors
        // over bytes that moved underneath them.
        //
        // The repair once-latch is burned in the same statement, because this is
        // the first step that cannot be undone: everything above refuses for
        // free, and everything below leaves the set changed whether or not it
        // ends up repaired.
        let mut persist = DatabaseCoveragePersist::new(self.db.clone());
        if let Some(set) = self.direct_store.set_mut(job_id, set_index) {
            set.note_repair_attempted();
        }
        #[cfg(test)]
        {
            self.direct_store.repair_attempts += 1;
        }
        if let Some(set) = self.direct_store.set_mut(job_id, set_index)
            && let Err(error) = set.delete_checkpoint_row(&mut persist)
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %error,
                "failed to delete a direct-store checkpoint before repairing; the set \
                 demotes rather than repairing over a row that still claims its bytes"
            );
            return Err(super::super::repair::DirectRepairFailure::PlanRefused(
                format!("checkpoint delete failed: {error}"),
            ));
        }
        // A repair rewrites only these direct volumes. Retire their byte-owned
        // grid evidence before the first rewrite without discarding another
        // set's untouched claims.
        for file_id in affected_files {
            self.block_crcs.forget_file(file_id);
        }
        // Announced from here rather than from a status transition: the set
        // never enters `JobStatus::Repairing` — that status carries the repair
        // concurrency queue, and this repair holds no slot in it — so the event
        // stream is the only public record that a repair ran. Consumers derive
        // the repair stage from the `RepairStarted`/`RepairComplete` pair, and
        // a job whose history shows a repair it never announced would read as
        // one that was never damaged. Sent at the first irreversible step for
        // the same reason the latch burns here: everything above refuses for
        // free and unannounced, everything below is a repair in progress. A
        // refusal past this point sends no terminal — the demotion hands the
        // job to the conventional repairer, whose own pair records how the
        // repair actually ended.
        let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });

        let working_dir = self
            .jobs
            .get(&job_id)
            .map(|state| state.working_dir.clone())
            .unwrap_or_default();
        let provider = super::super::provider::HybridVolumeProvider::new(
            overlay
                .virtual_volumes_for(&self.direct_store, job_id)
                .unwrap_or_default(),
        );
        // No overrides: the fallback answers only files this set does not own,
        // and each one is at its declared PAR2 name — the placement scan that
        // produced the verification already ran and reported no conflicts, and
        // a rename since then would have invalidated the verdict this repair is
        // planned from.
        let inner_plan = par2_rs::PlacementPlan {
            exact: Vec::new(),
            swaps: Vec::new(),
            renames: Vec::new(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
        };
        let inner = par2_rs::PlacementFileAccess::from_plan(
            working_dir.clone(),
            par2_set.as_ref(),
            &inner_plan,
        );
        let memory_limit = Some(self.par2_repair_memory_limit_bytes());
        let volumes = overlay.volumes.clone();
        let set_bytes = par2_set.clone();
        let verification = verification.clone();
        let damaged_for_task = damaged.clone();
        let pp_pool = self.pp_pool.clone();
        let sparse = self.direct_store.sparse_marking();
        let outcome = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                super::super::repair::repair_damaged_volumes(
                    set_bytes.as_ref(),
                    &verification,
                    &provider,
                    inner,
                    &volumes,
                    &damaged_for_task,
                    memory_limit,
                    sparse,
                )
            })
        })
        .await;
        let outcome = match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(failure)) => return Err(failure),
            Err(error) => {
                for volume in &damaged {
                    let _ = tokio::fs::remove_file(&volume.path).await;
                }
                return Err(super::super::repair::DirectRepairFailure::ExecuteFailed(
                    format!("the repair task did not complete: {error}"),
                ));
            }
        };

        info!(
            job_id = job_id.0,
            set_name = %set_name,
            volumes = outcome.materialized_volumes,
            recovery_blocks = outcome.recovery_blocks_used,
            rewrite_bytes,
            "repaired a direct set's damaged volumes in place; its clean volumes stayed virtual"
        );
        // "Only the damaged volumes materialize" is the claim repair-while-direct
        // rests on, and the scratch is deleted as soon as its spans are routed —
        // so this counter is the only thing that can contradict it in
        // production. The test build asserts the same number through
        // `repair_materialized_volumes`.
        crate::runtime::perf_probe::record_value(
            "direct_store.repair.materialized_volumes",
            outcome.materialized_volumes as u64,
        );
        crate::runtime::perf_probe::record_value(
            "direct_store.repair.recovery_blocks",
            outcome.recovery_blocks_used as u64,
        );
        #[cfg(test)]
        {
            // Counted from the outcome, so it is volumes the sweep actually
            // rebuilt rather than volumes this seam intended to rebuild: a plan
            // that refuses before reconstruction materializes nothing, and the
            // scratch is deleted either way, so nothing on disk could tell the
            // two apart afterwards.
            self.direct_store.repair_materialized_volumes += outcome.materialized_volumes;
            self.direct_store.repair_recovery_blocks_used += outcome.recovery_blocks_used;
        }

        let routed = self
            .route_repaired_volumes(job_id, set_index, &damaged, &set_lengths)
            .await;
        for path in &outcome.scratch {
            let _ = tokio::fs::remove_file(path).await;
        }
        if !routed {
            return Err(super::super::repair::DirectRepairFailure::ExecuteFailed(
                "the repaired spans could not be routed back into the set".to_string(),
            ));
        }
        // Every byte of a repaired volume is now accounted for, whatever the
        // assembly thinks: the damage may well have *been* a lost article, and
        // that article is never coming. Saying so is what runs the confirming
        // parse over the repaired image — a volume whose end record was in the
        // lost bytes can only be confirmed here — and what lets the set finalize
        // instead of waiting forever for a download that already finished by
        // another route.
        for volume in &damaged {
            let spans = {
                let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                    return Err(super::super::repair::DirectRepairFailure::ExecuteFailed(
                        "the set went away mid-repair".to_string(),
                    ));
                };
                set.note_volume_complete(volume.volume_index, volume.len)
            };
            let spans = match spans {
                Ok(spans) => spans,
                Err(reason) => {
                    return Err(super::super::repair::DirectRepairFailure::ExecuteFailed(
                        format!(
                            "the repaired volume could not be confirmed: {}",
                            reason.metric()
                        ),
                    ));
                }
            };
            if !self
                .place_direct_spans(job_id, set_index, None, &spans)
                .await
            {
                return Err(super::super::repair::DirectRepairFailure::ExecuteFailed(
                    "a confirming parse's spans could not be written".to_string(),
                ));
            }
        }
        self.reread_direct_stale_gaps(job_id, set_index).await;
        // The other half: the row was deleted before anything moved, so the set
        // has no durable coverage at all until a barrier writes one. Demanding
        // it here rather than waiting for the 5 s timer is what keeps the
        // deliberately-lossy window to the length of this call.
        self.run_direct_barrier(
            job_id,
            set_index,
            super::super::barrier::BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
        )
        .await;
        crate::runtime::perf_probe::record(
            "direct_store.repaired_while_direct",
            std::time::Duration::from_nanos(1),
        );
        self.metrics
            .direct_sets_repaired_while_direct
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Complete,
            outcome.recovery_blocks_used as u64,
        );
        let _ = self.event_tx.send(PipelineEvent::RepairComplete {
            job_id,
            slices_repaired: u32::try_from(outcome.recovery_blocks_used).unwrap_or(u32::MAX),
        });
        Ok(())
    }

    /// Reads each repaired volume's spans back and feeds them through the
    /// router and out to every destination they touch (replacement semantics).
    ///
    /// **One volume at a time, read then routed then dropped.** Both halves of
    /// that are load-bearing and they pull in opposite directions:
    ///
    /// - a whole volume at once, because the classification frontier is a
    ///   per-volume fact — a span routed before its volume's end record was
    ///   staged would be held rather than routed, and the repair would refuse;
    /// - never more than one volume, because the spans are bytes, and holding
    ///   every damaged volume's rewrite so the last one could be staged put the
    ///   set's whole repair in RAM twice over with nothing bounding it.
    pub(super) async fn route_repaired_volumes(
        &mut self,
        job_id: JobId,
        set_index: usize,
        damaged: &[super::super::repair::DamagedDirectVolume],
        lengths: &std::collections::BTreeMap<u32, u64>,
    ) -> bool {
        // An encrypted member's repaired span decrypts on the way
        // in, and every byte its CBC chain needs was dropped from staging when
        // the original article was routed. Two sources put them back, and
        // neither of them changes a byte: the ones just below the span come off
        // the materialized volume, and the ≤46 in a *neighbouring* volume that
        // complete an edge block of a member extent — and, at the low edge, that
        // block's own CBC predecessor — come off that neighbour's own virtual
        // volume, re-encrypted by the overlay.
        let cipher_lead_in = self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| set.router.routes_encrypted());
        for volume in damaged {
            let volume_index = volume.volume_index;
            let for_task = volume.clone();
            let spans = match tokio::task::spawn_blocking(move || {
                super::super::repair::read_repaired_spans(&for_task, cipher_lead_in)
            })
            .await
            {
                Ok(Ok(spans)) => spans,
                Ok(Err(failure)) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        failure = %failure,
                        "a repaired volume could not be read back"
                    );
                    return false;
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        error = %error,
                        "the repaired read-back task did not complete"
                    );
                    return false;
                }
            };
            if spans.is_empty() {
                continue;
            }
            // The neighbouring-volume halves of this volume's member edge
            // blocks, read through the overlay so what is staged is what was
            // posted rather than the plaintext on disk. A read that refuses —
            // the neighbour has a hole there — simply contributes nothing, and
            // the span that needed it holds, which `route_repaired` turns into
            // the whole-set demotion the fallback exists for.
            let edges = match cipher_lead_in {
                true => self.read_cipher_edges(job_id, set_index, volume_index, lengths),
                false => Vec::new(),
            };
            let routed = {
                let Some(set) = self.direct_store.set_mut(job_id, set_index) else {
                    return false;
                };
                set.note_repaired_volume_crcs(volume_index, &spans);
                // The chunks are reference-counted, so this hands staging the
                // very buffers the read produced rather than a second copy of
                // them; `spans` drops its side at the end of the iteration.
                let staged: Vec<super::super::router::RepairedChunk> = spans
                    .iter()
                    .flat_map(|span| span.chunks.iter().cloned())
                    .collect();
                let mut lead_in: Vec<(u32, u64, std::sync::Arc<[u8]>)> = spans
                    .iter()
                    .flat_map(|span| [span.lead_in.clone(), span.lead_out.clone()])
                    .flatten()
                    .map(|(offset, data)| (volume_index, offset, data))
                    .collect();
                lead_in.extend(edges);
                set.route_repaired(
                    volume_index,
                    &staged,
                    &lead_in,
                    volume.rewrote_whole_volume(),
                )
            };
            drop(spans);
            let routed = match routed {
                Ok(routed) => routed,
                Err(reason) => {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_index,
                        reason = reason.metric(),
                        "a repaired span could not be routed back into its direct set"
                    );
                    self.demote_direct_set(job_id, set_index, reason).await;
                    return false;
                }
            };
            if !self
                .place_direct_spans(job_id, set_index, None, &routed)
                .await
            {
                return false;
            }
        }
        true
    }

    /// The few posted bytes per member-extent edge that live in a
    /// **neighbouring** volume of the same set.
    ///
    /// Read through the set's own virtual provider, which re-encrypts them out
    /// of the neighbour's destination — those bytes did not change, so what
    /// comes back is exactly what was posted there. Blocking work, but bounded
    /// at 46 bytes per member extent of one volume — ≤31 below it, which is the
    /// straddling block plus its CBC predecessor, and ≤15 above — so it is done
    /// inline rather than on the pool.
    pub(super) fn read_cipher_edges(
        &self,
        job_id: JobId,
        set_index: usize,
        volume_index: u32,
        lengths: &std::collections::BTreeMap<u32, u64>,
    ) -> Vec<(u32, u64, std::sync::Arc<[u8]>)> {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return Vec::new();
        };
        let reads = set.router.cipher_edge_reads(volume_index);
        if reads.is_empty() {
            return Vec::new();
        }
        let provider = set.virtual_provider(lengths);
        let mut edges = Vec::with_capacity(reads.len());
        for (volume, offset, len) in reads {
            let Some(mut reader) = provider.open(volume) else {
                continue;
            };
            if std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(offset)).is_err() {
                continue;
            }
            let mut bytes = vec![0u8; len as usize];
            if std::io::Read::read_exact(&mut reader, &mut bytes).is_err() {
                continue;
            }
            edges.push((volume, offset, std::sync::Arc::from(bytes.as_slice())));
        }
        edges
    }

    /// Closes the composition gaps a repair's rewrite left, with one bounded
    /// read of the partials that hold them.
    ///
    /// The shape is deliberately the restart re-arm: the same plan, the same
    /// reader, the same "a run that will not read demotes rather than passes"
    /// rule. What differs is only why the value is missing — a rewrite
    /// discarded it rather than a restart losing it — and that difference has
    /// no bearing on what it costs to recover.
    pub(super) async fn reread_direct_stale_gaps(&mut self, job_id: JobId, set_index: usize) {
        let Some(set) = self.direct_store.set(job_id, set_index) else {
            return;
        };
        if set.is_demoted() || !set.router.has_stale_gaps() {
            return;
        }
        let destination_dir = set.plan().destination_dir.clone();
        let set_name = set.set_name().to_string();
        let runs = set.router.stale_gap_read_plan();
        if runs.is_empty() {
            return;
        }
        let total: u64 = runs.iter().map(|run| run.len).sum();
        debug!(
            job_id = job_id.0,
            set_name = %set_name,
            runs = runs.len(),
            bytes = total,
            "re-reading the composition gaps a direct-store repair left behind"
        );

        let read_runs = runs.clone();
        let read_dir = destination_dir;
        let checksums =
            tokio::task::spawn_blocking(move || read_restart_seeded_runs(&read_dir, &read_runs))
                .await;
        let checksums = match checksums {
            Ok(Ok(checksums)) => checksums,
            _ => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "failed to re-read a repaired member's composition gaps; demoting the set"
                );
                self.demote_direct_set(job_id, set_index, DemotionReason::RepairGapUnreadable)
                    .await;
                return;
            }
        };
        crate::runtime::perf_probe::record_value("direct_store.repair.gap_reread_bytes", total);

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
                "a repaired member failed its gate once its composition gaps were re-read"
            );
            self.demote_direct_set(job_id, set_index, reason).await;
            return;
        }
        // Same terminating condition as the restart re-arm: the pass read every
        // run the plan named, so a gap that survives it is one no plan reached,
        // and re-running would reach the same place. One pass, then a verdict.
        if self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(|set| !set.is_demoted() && set.router.has_stale_gaps())
        {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                "a repaired member's composition gaps survived their re-read; demoting the set"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::RepairGapUnreadable)
                .await;
        }
    }

    /// Demotes every direct set the PAR2 pass found damage on, and reports
    /// whether any did.
    ///
    /// The fallback, and the earlier whole answer. A demoted set materializes
    /// its volumes from its own routed bytes, refetches whatever reconstruction
    /// could not verify, and hands the job to the conventional repair path —
    /// which is exactly the shape the same job would have had with the gate
    /// off.
    pub(crate) async fn demote_direct_sets_with_par2_damage_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        verification: &par2_rs::VerificationResult,
    ) -> bool {
        // Scoped to the set this pass is verifying, not to whichever set the
        // gate currently has selected: the damage below is filtered by
        // `recovery_set_id`, so the table its file ids are looked up in has to
        // describe the same set or every lookup misses and nothing demotes.
        let Some(overlay) = self.direct_par2_overlay_for_set(job_id, recovery_set_id) else {
            return false;
        };
        // The second settle guard, paired with
        // [`Self::direct_sets_ready_for_authoritative_par2`]: while articles
        // are still arriving, a set's outstanding ranges read as holes and PAR2
        // calls them damage. The caller is supposed to have deferred already,
        // so this is the belt to that braces — and it is scoped the same way,
        // so a set whose bytes are genuinely never coming still demotes and
        // still gets materialized for the conventional repair path.
        let payload_settled = !self.job_has_pending_download_pipeline_work(job_id);
        let mut damaged: Vec<(usize, String)> = Vec::new();
        for file in &verification.files {
            if matches!(
                file.status,
                par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
            ) {
                continue;
            }
            let Some(set_index) = overlay.sets.get(&file.file_id).copied() else {
                continue;
            };
            if damaged.iter().any(|(index, _)| *index == set_index) {
                continue;
            }
            damaged.push((set_index, file.filename.clone()));
        }
        let mut demoted = false;
        for (set_index, filename) in damaged {
            // Claimed before the log line, so "demoted" means a set really left
            // direct mode. A caller that returned early on a set it did not
            // actually demote would leave the job waiting for a materialization
            // that never happens.
            if !self.direct_store.set(job_id, set_index).is_some_and(|set| {
                self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id)
                    && !set.is_demoted()
                    && !set.is_finalized()
                    // Insufficient PAR2 recovery can hand off to PAR3 without
                    // materializing every virtual source in this archive.
                    && !(set.router.awaits_par3_verdict()
                        && matches!(
                            verification.repairable,
                            par2_rs::verify::Repairability::Insufficient { .. }
                        ))
            }) {
                continue;
            }
            if !payload_settled
                && !self
                    .direct_store
                    .set(job_id, set_index)
                    .is_some_and(DirectSet::all_volumes_complete)
            {
                debug!(
                    job_id = job_id.0,
                    volume = %filename,
                    "PAR2 reported damage on a direct set that is still downloading; \
                     leaving it direct until its volumes complete"
                );
                continue;
            }
            warn!(
                job_id = job_id.0,
                volume = %filename,
                "PAR2 verification found damage on a direct set's virtual volume; \
                 demoting so the conventional path can repair a materialized volume"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged)
                .await;
            demoted = true;
        }
        demoted
    }

    #[allow(dead_code)]
    pub(crate) async fn demote_direct_sets_with_par2_damage(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_direct_sets_with_par2_damage_for_set(job_id, set_id, verification)
            .await
    }

    /// Demotes any live set still holding a part-checksum mismatch that the
    /// PAR2 pass could not answer — either because it read the volume and
    /// called it whole, or because it found the damage and had nothing to
    /// repair it with.
    ///
    /// A part-checksum mismatch parks its set: the member holds its
    /// whole-member gate, so the set never finalizes on its own. That is right
    /// while a repair might still come and wrong the moment one cannot, and a
    /// parked set must never be the end state. `verdict` names which of the two
    /// endings this was, for the log only.
    ///
    /// The reason is the routing gate's own, so the metric bucket
    /// (`part_checksum_mismatch`) counts it exactly where it was always
    /// counted, and a job whose archive is genuinely wrong reports the same
    /// failure it reported before any of this existed.
    pub(super) async fn demote_direct_sets_with_unanswered_part_damage(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
        verdict: &'static str,
    ) -> bool {
        let stuck: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| {
                !set.is_demoted() && !set.is_finalized() && !set.router.damaged_volumes().is_empty()
                    // The PAR3 completion gate still owes this archive its
                    // native verdict, including any eligible fallback repair.
                    && !set.router.awaits_par3_verdict()
            })
            .map(|(index, _)| index)
            .collect();
        if stuck.is_empty() {
            return false;
        }
        for set_index in stuck {
            warn!(
                job_id = job_id.0,
                set_index,
                verdict,
                "a direct set's volume failed its archive part checksum and PAR2 did not put it \
                 right; demoting so the conventional path owns the archive"
            );
            self.demote_direct_set(job_id, set_index, DemotionReason::PartChecksumMismatch)
                .await;
        }
        true
    }

    /// Demotes every set of `job_id` that is still routing, because a PAR2
    /// **repair** is about to run and repair needs a file to write into.
    ///
    /// Returns whether anything demoted, so the caller can let the job go round
    /// again over materialized volumes rather than repairing against nothing.
    pub(crate) async fn demote_live_direct_sets_for_par2_repair_for_set(
        &mut self,
        job_id: JobId,
        recovery_set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let live: Vec<usize> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| self.direct_set_binds_to_par2_set(job_id, set, recovery_set_id))
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .map(|(index, _)| index)
            .collect();
        if live.is_empty() {
            return false;
        }
        for set_index in live {
            self.demote_direct_set(job_id, set_index, DemotionReason::Par2Damaged)
                .await;
        }
        true
    }

    pub(crate) async fn demote_live_direct_sets_for_par2_repair(&mut self, job_id: JobId) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        self.demote_live_direct_sets_for_par2_repair_for_set(job_id, set_id)
            .await
    }
}
