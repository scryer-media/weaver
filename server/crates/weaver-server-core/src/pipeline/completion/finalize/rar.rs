use super::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[cfg(test)]
use std::collections::BTreeMap;

/// What [`Pipeline::clear_archive_set_if_unreferenced_and_idle`] did.
///
/// Retirement has three outcomes and only one of them touches any state, so the
/// caller has to be told which one it got. The absence of a `rar_sets` key is
/// not evidence of retirement: a set can be a bare name with no runtime entry at
/// all, and reading "no key" as "retired" invents a teardown that never
/// happened — and then re-arms the completion check that produced the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveSetRetirement {
    /// A worker or an in-flight member is still reading the set.
    Busy,
    /// Live file identities still classify into the set, so its names remain
    /// this job's own answer for those bytes.
    StillReferenced,
    /// The set's topology is not a RAR one, so nothing this function can measure
    /// says anything about whether it is in use. Refused rather than retired.
    NotRar,
    /// The set was actually torn down through
    /// [`Pipeline::clear_archive_set_for_source_retry`].
    Retired,
}

/// What a RAR set with no on-disk volumes and live claimants turned out to be.
///
/// Reached only from extraction entry, where the set's names resolve to nothing
/// readable yet live file identities still classify into it — so retirement is
/// refused and there is nothing to open. The job is either already whole under
/// another name or genuinely short of volumes, and the two must be told apart
/// before anything reschedules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnmaterializedArchiveSet {
    /// The claimants' bytes were consumed by the named set of the same job,
    /// which has already extracted or finalized. This name is a second view of
    /// content the job already holds.
    ConsumedBy(String),
    /// No name link survives an identity rebind either way, but the job
    /// demonstrably consumed archive content under another name — extracted in
    /// this run, or volume facts persisted under another set — and never
    /// parsed a RAR header under this set's name: it was named, never
    /// materialized.
    NeverMaterialized,
    /// Volumes are genuinely absent — nothing consumed them and nothing left
    /// can deliver them.
    MissingVolumes,
}

impl Pipeline {
    pub(crate) fn purge_empty_rar_set_if_idle(&mut self, job_id: JobId, set_name: &str) {
        let set_key = (job_id, set_name.to_string());
        if self
            .rar_refresh_state
            .get(&set_key)
            .is_some_and(|refresh| refresh.in_flight.is_some())
        {
            return;
        }
        let should_remove = self.rar_sets.get(&set_key).is_some_and(|state| {
            state.volume_files.is_empty()
                && state.active_workers == 0
                && state.in_flight_members.is_empty()
        });
        if !should_remove {
            return;
        }

        self.rar_sets.remove(&set_key);
        self.rar_refresh_state.remove(&set_key);
        self.mark_rar_unlock_priorities_dirty(job_id);
    }

    pub(crate) fn invalidate_archive_set_for_identity_rebind(
        &mut self,
        job_id: JobId,
        set_name: &str,
        touched_filenames: &HashSet<String>,
    ) {
        let set_key = (job_id, set_name.to_string());
        let affected_volumes: HashSet<u32> = self
            .rar_sets
            .get(&set_key)
            .map(|state| {
                state
                    .volume_files
                    .iter()
                    .filter_map(|(volume, filename)| {
                        touched_filenames.contains(filename).then_some(*volume)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let refresh_in_flight = self
            .rar_refresh_state
            .get(&set_key)
            .is_some_and(|refresh| refresh.in_flight.is_some());

        let remove_empty_set = if let Some(state) = self.rar_sets.get_mut(&set_key) {
            state
                .volume_files
                .retain(|_, filename| !touched_filenames.contains(filename));
            for volume in &affected_volumes {
                state.facts.remove(volume);
                // Keep touched swap volumes suspect until the next cached-header
                // rebuild refreshes them from the corrected on-disk bytes.
                state.verified_suspect_volumes.insert(*volume);
            }
            state.plan = None;
            state.extraction_generation = state.extraction_generation.saturating_add(1);
            state.volume_files.is_empty()
                && state.active_workers == 0
                && state.in_flight_members.is_empty()
                && !refresh_in_flight
        } else {
            false
        };

        if remove_empty_set {
            self.rar_sets.remove(&set_key);
            self.rar_refresh_state.remove(&set_key);
        } else if let Some(state) = self.rar_sets.get_mut(&set_key) {
            state.phase = if state.active_workers > 0 || !state.in_flight_members.is_empty() {
                crate::pipeline::archive::rar_state::RarSetPhase::Extracting
            } else {
                crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
            };
        }

        // The affected volume facts force the next rebuild to refresh these
        // headers from disk. Retain the snapshot while the set survives so an
        // eagerly deleted volume zero remains available as the topology base.
        if remove_empty_set {
            self.clear_rar_snapshot(job_id, set_name);
        }
        self.mark_rar_unlock_priorities_dirty(job_id);

        if let Err(error) = self.db.clear_extraction_chunks_for_set(job_id, set_name) {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %error,
                "failed to clear archive-set identity rebind extraction state"
            );
        }
    }

    pub(crate) fn clear_archive_set_if_unreferenced_and_idle(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> ArchiveSetRetirement {
        let set_key = (job_id, set_name.to_string());

        // This function's whole vocabulary is RAR: `busy` reads `rar_sets`, and
        // `still_referenced` below counts only files classified `RarVolume`. A
        // non-RAR topology can satisfy neither, so it would fall through both
        // tests and be torn down — not because anything decided it was
        // finished, but because nothing here can express the idea that it is
        // still in use. That is a category error, and it is how every 7z
        // topology in a job was being deleted on each PAR2 registration.
        if let Some(archive_type) = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for(set_name))
            .map(|topology| topology.archive_type)
            && !matches!(archive_type, crate::jobs::assembly::ArchiveType::Rar)
        {
            return ArchiveSetRetirement::NotRar;
        }

        let busy = self
            .rar_sets
            .get(&set_key)
            .is_some_and(|state| state.active_workers > 0 || !state.in_flight_members.is_empty());
        if busy {
            return ArchiveSetRetirement::Busy;
        }

        let still_referenced = self.jobs.get(&job_id).is_some_and(|state| {
            state.assembly.files().any(|file| {
                matches!(
                    self.classified_role_for_file(job_id, file),
                    weaver_model::files::FileRole::RarVolume { .. }
                ) && self
                    .classified_archive_set_name_for_file(job_id, file)
                    .as_deref()
                    == Some(set_name)
            })
        });
        if still_referenced {
            return ArchiveSetRetirement::StillReferenced;
        }

        self.clear_archive_set_for_source_retry(job_id, set_name);
        ArchiveSetRetirement::Retired
    }

    /// Rules on a set whose names resolve to nothing on disk while live file
    /// identities still claim them.
    ///
    /// Three facts are available and none of them is a map key. Whether every
    /// claimant is complete says whether anything is still owed to this job.
    /// Whether another already-extracted set of the job knows these files under
    /// any of their names says where the bytes went. Whether this job ever
    /// persisted volume facts under this name says whether the set was ever
    /// more than a name — facts are written the moment a volume's headers parse
    /// off real bytes, so a set with none never had a volume to read.
    pub(crate) fn classify_unmaterialized_archive_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> UnmaterializedArchiveSet {
        let Some(state) = self.jobs.get(&job_id) else {
            return UnmaterializedArchiveSet::MissingVolumes;
        };

        // Every name this job has known these files by. A PAR2 rebind moves a
        // file between its posted name and its canonical one, and the set that
        // actually consumed the bytes may be keyed off either side.
        let mut claimant_names: HashSet<String> = HashSet::new();
        let mut claimants = 0usize;
        for file in state.assembly.files() {
            if !matches!(
                self.classified_role_for_file(job_id, file),
                weaver_model::files::FileRole::RarVolume { .. }
            ) || self
                .classified_archive_set_name_for_file(job_id, file)
                .as_deref()
                != Some(set_name)
            {
                continue;
            }
            // A claimant still short of its bytes is the ordinary missing-volume
            // story: something is still owed to this job, and no other set can
            // be holding what never arrived.
            if !file.is_complete() {
                return UnmaterializedArchiveSet::MissingVolumes;
            }
            claimants += 1;
            claimant_names.insert(file.filename().to_string());
            if let Some(identity) = self.effective_file_identity(job_id, file.file_id()) {
                claimant_names.insert(identity.source_filename);
                claimant_names.insert(identity.current_filename);
                if let Some(canonical) = identity.canonical_filename {
                    claimant_names.insert(canonical);
                }
            }
        }
        if claimants == 0 {
            return UnmaterializedArchiveSet::MissingVolumes;
        }

        let mut extracted: Vec<String> = self
            .extracted_archives
            .get(&job_id)
            .map(|sets| {
                sets.iter()
                    .filter(|name| name.as_str() != set_name)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        extracted.sort();

        for owner in &extracted {
            let owned_by_topology =
                state
                    .assembly
                    .archive_topology_for(owner)
                    .is_some_and(|topology| {
                        topology
                            .volume_map
                            .keys()
                            .any(|filename| claimant_names.contains(filename))
                    });
            let owned_by_runtime =
                self.rar_sets
                    .get(&(job_id, owner.clone()))
                    .is_some_and(|owner_state| {
                        owner_state
                            .volume_files
                            .values()
                            .any(|filename| claimant_names.contains(filename))
                    });
            if owned_by_topology || owned_by_runtime {
                return UnmaterializedArchiveSet::ConsumedBy(owner.clone());
            }
        }

        // `extracted_archives` is runtime memory and does not survive a
        // restart, so it cannot be the only admissible proof that this job's
        // archive content went somewhere real. The persisted volume facts are
        // durable: facts parsed under another set name record that real archive
        // bytes were read under that name, which is the same consumption story
        // after a restart has wiped the in-memory record.
        if self
            .rar_sets
            .get(&(job_id, set_name.to_string()))
            .is_some_and(|state| !state.facts.is_empty())
        {
            return UnmaterializedArchiveSet::MissingVolumes;
        }
        match self.db.load_all_rar_volume_facts(job_id) {
            Ok(facts) => {
                let named_here = facts
                    .get(set_name)
                    .is_some_and(|volumes| !volumes.is_empty());
                let parsed_elsewhere = facts
                    .iter()
                    .any(|(name, volumes)| name != set_name && !volumes.is_empty());
                if !named_here && (!extracted.is_empty() || parsed_elsewhere) {
                    return UnmaterializedArchiveSet::NeverMaterialized;
                }
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to read persisted RAR volume facts; treating the set as materialized"
                );
                // Unreadable evidence is not absence of evidence: fall through
                // to the conservative answer so an unread row cannot absorb a
                // set whose volumes are genuinely missing.
            }
        }

        UnmaterializedArchiveSet::MissingVolumes
    }

    /// Records a set as extracted so the completion check stops offering it.
    ///
    /// The candidate-name sources cannot be edited away here — a live file's
    /// classification is the job's own record of what that file is, and
    /// rewriting it to make a completion check quieter would lose the only
    /// trace of the rebind. Marking the set extracted leaves those sources
    /// intact and still moves the check forward: the name is offered again and
    /// answered immediately, instead of being dispatched to an extraction that
    /// has nothing to open.
    pub(crate) fn absorb_archive_set_into_extracted(&mut self, job_id: JobId, set_name: &str) {
        self.extracted_archives
            .entry(job_id)
            .or_default()
            .insert(set_name.to_string());
        self.schedule_job_completion_check(job_id);
    }

    pub(crate) fn clear_archive_set_for_source_retry(&mut self, job_id: JobId, set_name: &str) {
        let set_key = (job_id, set_name.to_string());
        let refresh_in_flight = self
            .rar_refresh_state
            .get(&set_key)
            .is_some_and(|refresh| refresh.in_flight.is_some());
        let retired_generation = self
            .rar_sets
            .get(&set_key)
            .map_or(1, |state| state.extraction_generation.saturating_add(1));
        let retry_filenames: HashSet<String> = {
            let mut filenames = HashSet::new();
            if let Some(state) = self.jobs.get(&job_id)
                && let Some(topology) = state.assembly.archive_topology_for(set_name)
            {
                filenames.extend(topology.volume_map.keys().cloned());
            }
            if let Some(rar_state) = self.rar_sets.get(&set_key) {
                filenames.extend(rar_state.volume_files.values().cloned());
            }
            filenames
        };

        if !retry_filenames.is_empty() {
            let mut remove_deleted_entry = false;
            if let Some(deleted) = self.eagerly_deleted.get_mut(&job_id) {
                for filename in &retry_filenames {
                    deleted.remove(filename);
                }
                remove_deleted_entry = deleted.is_empty();
            }
            if remove_deleted_entry {
                self.eagerly_deleted.remove(&job_id);
            }
        }

        // Volumes no NZB file accounts for — the ones a standalone `.rev`
        // restore wrote next to the downloaded volumes — have nothing to
        // re-queue. Their registration is the only thing that tells the
        // volume-0 rebuild they exist: `volume_paths_for_rar_set` walks the
        // set's `volume_files` and the assembly, and the assembly never held
        // them. Dropping them here left the restored file on disk and the set
        // waiting on a volume that was already present.
        let preserved_volumes = self.restored_rar_volumes_to_preserve(job_id, &set_key);

        self.clear_rar_snapshot(job_id, set_name);
        if refresh_in_flight {
            let tombstone = RarSetState {
                extraction_generation: retired_generation,
                ..Default::default()
            };
            self.rar_sets.insert(set_key.clone(), tombstone);
            if let Some(refresh) = self.rar_refresh_state.get_mut(&set_key) {
                let in_flight = refresh.in_flight;
                *refresh = RarRefreshState {
                    in_flight,
                    ..Default::default()
                };
            }
        } else {
            self.rar_sets.remove(&set_key);
            self.rar_refresh_state.remove(&set_key);
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.assembly.archive_topologies_mut().remove(set_name);
        }

        for result in [
            self.db.delete_rar_volume_facts_for_set(job_id, set_name),
            self.db.clear_volume_status_for_set(job_id, set_name),
            self.db.clear_extraction_chunks_for_set(job_id, set_name),
        ] {
            if let Err(error) = result {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to clear archive-set retry state"
                );
            }
        }

        if preserved_volumes.is_empty() {
            return;
        }
        let preserved: Vec<u32> = preserved_volumes
            .iter()
            .map(|(volume, _, _)| *volume)
            .collect();
        let state = self.rar_sets.entry(set_key).or_default();
        for (volume, filename, facts) in preserved_volumes {
            match rmp_serde::to_vec_named(&facts) {
                Ok(encoded) => {
                    if let Err(error) = self
                        .db
                        .save_rar_volume_facts(job_id, set_name, volume, &encoded)
                    {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            error = %error,
                            "failed to re-persist restored RAR volume facts across source retry"
                        );
                    }
                }
                Err(error) => warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volume,
                    error = %error,
                    "failed to encode restored RAR volume facts across source retry"
                ),
            }
            state.volume_files.insert(volume, filename);
            state.facts.insert(volume, facts);
        }
        state.facts_generation = state.facts_generation.wrapping_add(1);
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            volumes = ?preserved,
            "kept restored RAR volumes registered across source retry"
        );
    }

    /// Registered volumes of a set that no NZB file accounts for — written by
    /// a standalone `.rev` restore — that are still on disk, with the facts
    /// parsed from them. A restore that rewrote a downloaded volume in place
    /// shares that volume's NZB filename and is deliberately not in this list:
    /// it is a source the retry re-downloads like any other.
    fn restored_rar_volumes_to_preserve(
        &self,
        job_id: JobId,
        set_key: &(JobId, String),
    ) -> Vec<(u32, String, unrar_rs::RarVolumeFacts)> {
        let (Some(rar_state), Some(state)) = (self.rar_sets.get(set_key), self.jobs.get(&job_id))
        else {
            return Vec::new();
        };
        let nzb_filenames: HashSet<String> = state
            .spec
            .files
            .iter()
            .map(|file| file.filename.clone())
            .chain(
                state
                    .assembly
                    .files()
                    .map(|file| self.current_filename_for_file(job_id, file)),
            )
            .collect();
        rar_state
            .volume_files
            .iter()
            .filter(|(_, filename)| !nzb_filenames.contains(filename.as_str()))
            .filter(|(_, filename)| {
                self.resolve_job_input_path(job_id, filename)
                    .is_some_and(|path| path.exists())
            })
            .filter_map(|(volume, filename)| {
                rar_state
                    .facts
                    .get(volume)
                    .map(|facts| (*volume, filename.clone(), facts.clone()))
            })
            .collect()
    }

    fn rar_volume_numbers_by_filename(&self, job_id: JobId) -> HashMap<String, u32> {
        let mut volume_numbers = HashMap::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return volume_numbers;
        };

        for topology in state.assembly.archive_topologies().values() {
            for (filename, &volume_number) in &topology.volume_map {
                volume_numbers.insert(filename.clone(), volume_number);
            }
        }

        volume_numbers
    }

    pub(crate) fn claim_clean_rar_volume(
        decision: &crate::pipeline::archive::rar_state::RarVolumeDeleteDecision,
    ) -> bool {
        decision.pending_owners.is_empty()
            && decision.failed_owners.is_empty()
            && !decision.unresolved_boundary
    }

    pub(crate) fn suspect_rar_volumes_for_job(&self, job_id: JobId) -> HashSet<u32> {
        let suspect: HashSet<u32> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .flat_map(|(_, state)| {
                let mut volumes = state
                    .verified_suspect_volumes
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                if let Some(plan) = state.plan.as_ref() {
                    volumes.extend(plan.delete_decisions.iter().filter_map(
                        |(volume, decision)| {
                            (!decision.failed_owners.is_empty()
                                || !decision.pending_owners.is_empty()
                                || plan.waiting_on_volumes.contains(volume))
                            .then_some(*volume)
                        },
                    ));
                }
                volumes
            })
            .collect();
        suspect
    }

    pub(crate) fn apply_eager_delete_exclusions(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> (u32, u32) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let mut skipped_blocks = 0u32;
        let mut retained_suspect_blocks = 0u32;
        for file_verification in &mut verification.files {
            if matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Missing
            ) && eagerly_deleted_names.contains(file_verification.filename.as_str())
            {
                let Some(&volume_number) = volume_numbers.get(file_verification.filename.as_str())
                else {
                    continue;
                };
                if suspect_volumes.contains(&volume_number) {
                    retained_suspect_blocks = retained_suspect_blocks
                        .saturating_add(file_verification.missing_slice_count);
                    continue;
                }
                skipped_blocks += file_verification.missing_slice_count;
                file_verification.status = par2_rs::verify::FileStatus::Complete;
                file_verification.valid_slices.fill(true);
                file_verification.missing_slice_count = 0;
            }
        }
        verification.total_missing_blocks = verification
            .total_missing_blocks
            .saturating_sub(skipped_blocks);
        verification.refresh_repairability();
        (skipped_blocks, retained_suspect_blocks)
    }

    pub(crate) fn recompute_volume_safety_from_verification(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let status_by_name: HashMap<&str, &par2_rs::FileVerification> = verification
            .files
            .iter()
            .map(|file| (file.filename.as_str(), file))
            .collect();

        let plans: Vec<(String, HashSet<u32>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .map(|(set_name, topo)| {
                    let mut suspect = HashSet::new();
                    for (filename, &volume_number) in &topo.volume_map {
                        if let Some(file) = status_by_name.get(filename.as_str()) {
                            match file.status {
                                par2_rs::verify::FileStatus::Complete
                                | par2_rs::verify::FileStatus::Renamed(_) => {}
                                par2_rs::verify::FileStatus::Missing
                                    if eagerly_deleted_names.contains(filename.as_str())
                                        && !volume_numbers.get(filename.as_str()).is_some_and(
                                            |number| suspect_volumes.contains(number),
                                        ) => {}
                                par2_rs::verify::FileStatus::Missing
                                | par2_rs::verify::FileStatus::Damaged(_) => {
                                    suspect.insert(volume_number);
                                }
                            }
                        }
                    }
                    (set_name.clone(), suspect)
                })
                .collect()
        };

        let plan_names: HashSet<String> =
            plans.iter().map(|(set_name, _)| set_name.clone()).collect();
        for set_name in self.rar_set_names_for_job(job_id) {
            if !plan_names.contains(&set_name)
                && let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone()))
            {
                state.verified_suspect_volumes.clear();
            }
        }

        for (set_name, suspect) in plans {
            if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                state.verified_suspect_volumes = suspect;
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn refresh_rar_topology_after_normalization(
        &mut self,
        job_id: JobId,
        normalized_files: &HashSet<String>,
    ) -> Result<(), String> {
        if normalized_files.is_empty() {
            return Ok(());
        }

        let touched_sets: BTreeMap<String, HashSet<String>> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };

            state
                .assembly
                .files()
                .filter_map(|file| {
                    let current_filename = self.current_filename_for_file(job_id, file);
                    if !normalized_files.contains(&current_filename) {
                        return None;
                    }
                    match self.classified_role_for_file(job_id, file) {
                        weaver_model::files::FileRole::RarVolume { .. } => self
                            .classified_archive_set_name_for_file(job_id, file)
                            .map(|set_name| (set_name, current_filename)),
                        _ => None,
                    }
                })
                .fold(BTreeMap::new(), |mut acc, (set_name, filename)| {
                    acc.entry(set_name).or_default().insert(filename);
                    acc
                })
        };
        let mut errors = Vec::new();
        for (set_name, touched_filenames) in touched_sets {
            match self
                .refresh_rar_volume_facts_for_set(job_id, &set_name, &touched_filenames)
                .await
            {
                Ok(()) => info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "refreshed RAR topology after normalization"
                ),
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR topology after normalization; retaining previous snapshot and topology"
                    );
                    errors.push(format!("{set_name}: {error}"));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub(crate) fn has_active_rar_workers(&self, job_id: JobId) -> bool {
        self.rar_set_names_for_job(job_id).iter().any(|set_name| {
            self.rar_sets
                .get(&(job_id, set_name.clone()))
                .is_some_and(|state| {
                    state.active_workers > 0 || !state.in_flight_members.is_empty()
                })
        })
    }

    fn placement_normalization_map(plan: &par2_rs::PlacementPlan) -> HashMap<String, String> {
        let mut normalized_files = HashMap::new();
        for (left, right) in &plan.swaps {
            normalized_files.insert(left.current_name.clone(), left.correct_name.clone());
            normalized_files.insert(right.current_name.clone(), right.correct_name.clone());
        }
        for entry in &plan.renames {
            normalized_files.insert(entry.current_name.clone(), entry.correct_name.clone());
        }
        normalized_files
    }

    fn placement_touched_files(plan: &par2_rs::PlacementPlan) -> HashSet<String> {
        let mut touched = HashSet::new();
        for (left, right) in &plan.swaps {
            touched.insert(left.current_name.clone());
            touched.insert(left.correct_name.clone());
            touched.insert(right.current_name.clone());
            touched.insert(right.correct_name.clone());
        }
        for entry in &plan.renames {
            touched.insert(entry.current_name.clone());
            touched.insert(entry.correct_name.clone());
        }
        touched
    }

    pub(super) fn log_placement_plan(job_id: JobId, plan: &par2_rs::PlacementPlan) {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return;
        }

        let swap_pairs: Vec<String> = plan
            .swaps
            .iter()
            .map(|(left, right)| {
                format!(
                    "{} -> {} | {} -> {}",
                    left.current_name, left.correct_name, right.current_name, right.correct_name
                )
            })
            .collect();
        let renames: Vec<String> = plan
            .renames
            .iter()
            .map(|entry| format!("{} -> {}", entry.current_name, entry.correct_name))
            .collect();

        info!(
            job_id = job_id.0,
            swaps = ?swap_pairs,
            renames = ?renames,
            "placement scan identified remapped files"
        );
    }

    pub(super) async fn apply_placement_plan_for_retry_or_repair(
        &mut self,
        job_id: JobId,
        working_dir: PathBuf,
        plan: &par2_rs::PlacementPlan,
    ) -> Result<(), String> {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return Ok(());
        }

        let plan = plan.clone();
        let normalization_map = Self::placement_normalization_map(&plan);
        let normalized_files = Self::placement_touched_files(&plan);
        // Renames and swaps move the bytes a chase is reading out from under
        // it, under names it never saw.
        for name in &normalized_files {
            self.taint_direct_unpack_for_file(job_id, name);
        }
        let plan_for_apply = plan.clone();
        let moved = tokio::task::spawn_blocking(move || {
            par2_rs::apply_placement_plan(&working_dir, &plan_for_apply)
                .map_err(|e| format!("placement normalization failed: {e}"))
        })
        .await
        .map_err(|e| format!("placement normalization task panicked: {e}"))??;

        // Placement changes paths, not the bytes still owned by each
        // NzbFileId. Binding and archive identity are refreshed below; raw
        // grid evidence remains valid for every set.
        //
        // A parked damaged-path verdict is the exception: it names files by the
        // path each carried when it was reached, and the post-repair read-back
        // stands on those names. Moved files invalidate it, so the next pass
        // analyses the directory it is actually looking at.
        self.clear_pending_par2_repairs_for_job(job_id);

        info!(
            job_id = job_id.0,
            swaps = plan.swaps.len(),
            renames = plan.renames.len(),
            moved,
            "applied placement normalization after verify"
        );

        let touched_rar_files: HashMap<String, HashSet<String>> = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter_map(|file| {
                        let current_filename = self.current_filename_for_file(job_id, file);
                        let future_filename = normalization_map
                            .get(&current_filename)
                            .cloned()
                            .unwrap_or_else(|| current_filename.clone());
                        if !normalized_files.contains(&current_filename)
                            && !normalized_files.contains(&future_filename)
                        {
                            return None;
                        }
                        match self.classified_role_for_file(job_id, file) {
                            weaver_model::files::FileRole::RarVolume { .. } => self
                                .classified_archive_set_name_for_file(job_id, file)
                                .map(|set_name| (set_name, current_filename)),
                            _ => None,
                        }
                    })
                    .fold(
                        HashMap::<String, HashSet<String>>::new(),
                        |mut acc, (set_name, current_filename)| {
                            acc.entry(set_name).or_default().insert(current_filename);
                            acc
                        },
                    )
            })
            .unwrap_or_default();
        let file_rows: Vec<(NzbFileId, crate::jobs::record::ActiveFileIdentity, bool)> = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter_map(|file| {
                        self.effective_file_identity(job_id, file.file_id())
                            .map(|identity| (file.file_id(), identity, file.is_complete()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let by_current: HashMap<String, (NzbFileId, bool)> = file_rows
            .iter()
            .map(|(file_id, identity, is_complete)| {
                (identity.current_filename.clone(), (*file_id, *is_complete))
            })
            .collect();

        for (current_name, correct_name) in &normalization_map {
            let Some((file_id, _)) = by_current.get(current_name).copied() else {
                continue;
            };
            let Some((_, identity, _)) = file_rows
                .iter()
                .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
                .cloned()
            else {
                continue;
            };
            let classification = Self::canonical_archive_identity_from_filename(correct_name)
                .or(identity.classification.clone());
            let mut rebound_identity = identity;
            rebound_identity.current_filename = correct_name.clone();
            rebound_identity.canonical_filename = Some(correct_name.clone());
            rebound_identity.classification = classification;
            rebound_identity.classification_source = crate::jobs::record::FileIdentitySource::Par2;
            self.set_file_identity(job_id, rebound_identity)?;
        }

        let touched_complete_files: Vec<NzbFileId> = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .filter_map(|file| {
                        let identity = self.effective_file_identity(job_id, file.file_id())?;
                        let current_filename = identity.current_filename.clone();
                        let future_filename = normalization_map
                            .get(&current_filename)
                            .cloned()
                            .unwrap_or_else(|| current_filename.clone());
                        if !normalized_files.contains(&current_filename)
                            && !normalized_files.contains(&future_filename)
                        {
                            return None;
                        }
                        file.is_complete().then_some(file.file_id())
                    })
                    .collect()
            })
            .unwrap_or_default();

        for (set_name, touched_filenames) in &touched_rar_files {
            self.invalidate_archive_set_for_identity_rebind(job_id, set_name, touched_filenames);
        }

        for file_id in touched_complete_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        Ok(())
    }

    pub(super) async fn recompute_rar_retry_frontier(&mut self, job_id: JobId) {
        for set_name in self.rar_set_names_for_job(job_id) {
            if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to recompute RAR set while rebuilding retry frontier"
                );
            }
        }
    }

    pub(crate) fn invalid_rar_retry_frontier_reason(&self, job_id: JobId) -> Option<String> {
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut has_incomplete_sets = false;

        for set_name in self.rar_set_names_for_job(job_id) {
            let Some(set_state) = self.rar_sets.get(&(job_id, set_name.clone())) else {
                continue;
            };
            let Some(plan) = set_state.plan.as_ref() else {
                continue;
            };
            let set_complete = !plan.member_names.is_empty()
                && plan
                    .member_names
                    .iter()
                    .all(|member| extracted.contains(member));
            if set_complete {
                continue;
            }

            has_incomplete_sets = true;

            let waiting_marked_deletable: Vec<u32> = plan
                .waiting_on_volumes
                .intersection(&plan.deletion_eligible)
                .copied()
                .collect();
            if !waiting_marked_deletable.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes marked deletable: {:?}",
                    waiting_marked_deletable
                ));
            }

            let waiting_already_deleted: Vec<u32> = plan
                .waiting_on_volumes
                .iter()
                .copied()
                .filter(|volume| {
                    self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, *volume)
                })
                .collect();
            if !waiting_already_deleted.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes already deleted: {:?}",
                    waiting_already_deleted
                ));
            }

            if !plan.ready_members.is_empty()
                || matches!(
                    plan.phase,
                    crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
                )
            {
                return None;
            }
        }

        if has_incomplete_sets {
            Some("no retryable work remains for incomplete RAR sets".to_string())
        } else {
            None
        }
    }

    pub(super) fn job_has_only_rar_archives(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };

        if !state.assembly.archive_topologies().is_empty() {
            let has_rar = state
                .assembly
                .archive_topologies()
                .values()
                .any(|topology| topology.archive_type == crate::jobs::assembly::ArchiveType::Rar);
            let has_non_rar = state
                .assembly
                .archive_topologies()
                .values()
                .any(|topology| topology.archive_type != crate::jobs::assembly::ArchiveType::Rar);
            if has_non_rar {
                return false;
            }
            if has_rar {
                return true;
            }
        }

        let mut has_rar = false;
        for file in state.assembly.files() {
            match self.classified_role_for_file(job_id, file) {
                weaver_model::files::FileRole::RarVolume { .. } => has_rar = true,
                weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. } => return false,
                _ => {}
            }
        }

        has_rar
    }

    pub(super) fn rar_set_names_for_job(&self, job_id: JobId) -> Vec<String> {
        let mut set_names: HashSet<String> = HashSet::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };

        for (set_name, topology) in state.assembly.archive_topologies() {
            if topology.archive_type == crate::jobs::assembly::ArchiveType::Rar {
                set_names.insert(set_name.clone());
            }
        }

        for (jid, set_name) in self.rar_sets.keys() {
            if *jid == job_id {
                set_names.insert(set_name.clone());
            }
        }

        for file in state.assembly.files() {
            if matches!(
                self.classified_role_for_file(job_id, file),
                weaver_model::files::FileRole::RarVolume { .. }
            ) && let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file)
            {
                set_names.insert(set_name);
            }
        }
        let mut set_names: Vec<String> = set_names.into_iter().collect();
        set_names.sort();
        set_names
    }

    fn all_rar_sets_complete(&self, job_id: JobId) -> bool {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return false;
        }

        set_names.into_iter().all(|set_name| {
            let Some(set_state) = self.rar_sets.get(&(job_id, set_name)) else {
                return false;
            };
            let phase = set_state
                .plan
                .as_ref()
                .map(|plan| plan.phase)
                .unwrap_or(set_state.phase);
            phase == crate::pipeline::archive::rar_state::RarSetPhase::Complete
        })
    }

    pub(super) async fn finalize_completed_archive_job(&mut self, job_id: JobId) {
        if !self.job_has_only_rar_archives(job_id) {
            let already_extracted = self
                .extracted_archives
                .get(&job_id)
                .cloned()
                .unwrap_or_default();
            let already_spawned = self
                .inflight_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default();

            let pending_source_files: Vec<NzbFileId> = {
                let Some(state) = self.jobs.get(&job_id) else {
                    return;
                };
                state
                    .assembly
                    .files()
                    .filter(|file| file.is_complete())
                    .filter_map(|file| {
                        if !matches!(
                            self.classified_role_for_file(job_id, file),
                            weaver_model::files::FileRole::SevenZipArchive
                                | weaver_model::files::FileRole::SevenZipSplit { .. }
                                | weaver_model::files::FileRole::ZipArchive
                                | weaver_model::files::FileRole::TarArchive
                                | weaver_model::files::FileRole::TarGzArchive
                                | weaver_model::files::FileRole::TarBz2Archive
                                | weaver_model::files::FileRole::TarXzArchive
                                | weaver_model::files::FileRole::GzArchive
                                | weaver_model::files::FileRole::DeflateArchive
                                | weaver_model::files::FileRole::BrotliArchive
                                | weaver_model::files::FileRole::ZstdArchive
                                | weaver_model::files::FileRole::Bzip2Archive
                                | weaver_model::files::FileRole::XzArchive
                                | weaver_model::files::FileRole::SplitFile { .. }
                        ) {
                            return None;
                        }
                        let set_name = self.classified_archive_set_name_for_file(job_id, file)?;
                        (!already_extracted.contains(&set_name)
                            && !already_spawned.contains(&set_name))
                        .then_some(file.file_id())
                    })
                    .collect()
            };

            for file_id in &pending_source_files {
                self.refresh_archive_state_for_completed_file(job_id, *file_id, false)
                    .await;
            }

            let pending_archives: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                let Some(state) = self.jobs.get(&job_id) else {
                    return;
                };
                state
                    .assembly
                    .archive_topologies()
                    .iter()
                    .filter(|(_, topology)| {
                        !matches!(
                            topology.archive_type,
                            crate::jobs::assembly::ArchiveType::Rar
                        )
                    })
                    .filter(|(name, _)| {
                        !already_extracted.contains(*name) && !already_spawned.contains(*name)
                    })
                    .map(|(name, topology)| (name.clone(), topology.archive_type))
                    .collect()
            };

            if !already_spawned.is_empty() && pending_archives.is_empty() {
                return;
            }
            if !pending_archives.is_empty() {
                if !self.maybe_start_extraction(job_id).await {
                    return;
                }
                self.spawn_extractions(job_id, &pending_archives).await;
                return;
            }
            if !pending_source_files.is_empty() {
                self.reconcile_job_progress(job_id).await;
                self.schedule_job_completion_check(job_id);
                return;
            }
        }

        if self
            .reconcile_extracted_outputs_for_completion(job_id)
            .await
        {
            self.reconcile_job_progress(job_id).await;
            self.schedule_job_completion_check(job_id);
            return;
        }

        let cleanup_files: HashSet<String> = {
            let state = self.jobs.get(&job_id).unwrap();
            let mut cleanup_files: HashSet<String> = state
                .assembly
                .files()
                .filter(|f| {
                    matches!(
                        self.classified_role_for_file(job_id, f),
                        weaver_model::files::FileRole::Par2 { .. }
                            | weaver_model::files::FileRole::RarVolume { .. }
                            | weaver_model::files::FileRole::SevenZipArchive
                            | weaver_model::files::FileRole::SevenZipSplit { .. }
                    )
                })
                .map(|f| self.current_filename_for_file(job_id, f))
                .collect();
            for topology in state.assembly.archive_topologies().values() {
                cleanup_files.extend(topology.volume_map.keys().cloned());
            }
            cleanup_files.extend(self.par2_joined_split_part_names(job_id));
            cleanup_files
        };

        let nested_decision = match self.maybe_start_nested_extraction(job_id).await {
            Ok(decision) => decision,
            Err(error) => {
                self.fail_job(job_id, error);
                return;
            }
        };

        match nested_decision {
            NestedExtractionDecision::Started | NestedExtractionDecision::NoNestedArchives => {
                let mut removed = 0u32;
                for filename in &cleanup_files {
                    let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                        continue;
                    };
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => removed += 1,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            warn!(
                                file = %path.display(),
                                error = %e,
                                "failed to clean up source file"
                            );
                        }
                    }
                }
                info!(
                    job_id = job_id.0,
                    removed,
                    total = cleanup_files.len(),
                    "post-extraction cleanup complete"
                );
                if matches!(nested_decision, NestedExtractionDecision::Started) {
                    return;
                }
            }
            NestedExtractionDecision::PreserveOutputsAtDepthLimit => {}
        }

        // Low-frequency: one observation per job-level extraction, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics
            .job_lifecycle
            .note_extraction(crate::operations::instrumentation::StageOutcomeKind::Complete);
        let _ = self
            .event_tx
            .send(PipelineEvent::ExtractionComplete { job_id });

        if let Err(error) = self.start_move_to_complete(job_id).await {
            self.fail_job(job_id, error);
        }
    }

    async fn recompute_rar_sets_after_verify_or_repair(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        for set_name in self.rar_set_names_for_job(job_id) {
            if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                if crate::pipeline::archive::topology::is_incoherent_rar_waiting_state_error(&error)
                {
                    return Err(error);
                }

                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "RAR state recompute after verify/repair fell back; preserving retryable state"
                );
            }

            if let Some(error) = self.ownerless_live_rar_plan_error_for_set(job_id, &set_name) {
                return Err(error);
            }
        }

        Ok(())
    }

    pub(crate) fn ownerless_live_rar_plan_error_for_job(&self, job_id: JobId) -> Option<String> {
        self.rar_set_names_for_job(job_id)
            .into_iter()
            .find_map(|set_name| self.ownerless_live_rar_plan_error_for_set(job_id, &set_name))
    }

    fn ownerless_live_rar_plan_error_for_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Option<String> {
        let set_state = self.rar_sets.get(&(job_id, set_name.to_string()))?;
        let plan = set_state.plan.as_ref()?;
        let ownerless_volumes =
            crate::pipeline::archive::topology::ownerless_present_member_volumes(
                plan,
                &set_state.facts,
            );
        (!ownerless_volumes.is_empty()).then(|| {
            crate::pipeline::archive::topology::ownerless_rar_plan_error(
                set_name,
                &ownerless_volumes,
            )
        })
    }

    /// Whether the job carries any standalone RAR recovery volume (`.rev`),
    /// the only input that makes a recovery-volume restore worth attempting.
    /// Cheap on purpose: it runs from the completion checkpoint, which fires
    /// far more often than a restore is possible.
    pub(in crate::pipeline) fn job_has_rar_recovery_volume_files(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state.assembly.files().any(|file| {
                std::path::Path::new(file.filename())
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("rev"))
            })
        })
    }

    /// Restore missing RAR data volumes from sibling standalone `.rev` files.
    ///
    /// The recovery crate discovers and validates matching recovery volumes from
    /// the already-known RAR set paths.  Weaver does not infer missing names or
    /// trust a filename convention here: only a successful, verified recovery
    /// report is registered back into the set topology.
    pub(in crate::pipeline) async fn try_restore_rar_recovery_volumes(
        &mut self,
        job_id: JobId,
    ) -> Result<bool, String> {
        let set_names = self.rar_set_names_for_job(job_id);
        let mut restored_sets = HashSet::new();

        for set_name in set_names {
            let volume_paths: Vec<PathBuf> = self
                .volume_paths_for_rar_set(job_id, &set_name)
                .into_values()
                .collect();
            if volume_paths.is_empty() {
                continue;
            }

            let report = match tokio::task::spawn_blocking(move || {
                unrar_rs::restore_volumes_from_paths(
                    &volume_paths,
                    &unrar_rs::RecoveryOptions::default(),
                )
            })
            .await
            .map_err(|error| format!("RAR recovery worker failed: {error}"))?
            {
                Ok(report) => report,
                Err(error) => {
                    // Warn, not debug: a set that carries `.rev` files and
                    // still cannot rebuild its hole is the one signal an
                    // operator has that the job is about to fall through to
                    // PAR2 or fail, and it named the unrar-side refusal that
                    // hid an encrypted-headers restore bug for a release.
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "RAR recovery volumes did not restore a missing volume"
                    );
                    continue;
                }
            };

            let roots = self
                .jobs
                .get(&job_id)
                .map(|state| (state.working_dir.clone(), state.staging_dir.clone()))
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            let mut restored = Vec::with_capacity(report.restored_paths.len());
            for path in report.restored_paths {
                let relative_path = if let Ok(relative_path) = path.strip_prefix(&roots.0) {
                    relative_path
                } else if let Some(staging_dir) = roots.1.as_deref() {
                    path.strip_prefix(staging_dir).map_err(|_| {
                        format!(
                            "RAR recovery restored {} outside job input directories",
                            path.display()
                        )
                    })?
                } else {
                    return Err(format!(
                        "RAR recovery restored {} outside job input directories",
                        path.display()
                    ));
                };
                let relative_path = relative_path
                    .to_str()
                    .ok_or_else(|| {
                        format!(
                            "RAR recovery restored non-UTF-8 input path {}",
                            path.display()
                        )
                    })?
                    .to_string();
                let facts =
                    Self::parse_rar_volume_facts_from_path(path, password_candidates.clone())
                        .await?;
                // The restored file's name is the layout's statement of which
                // volume this is. Registration keys by the header's stated
                // number when the format states one and by this layout claim
                // otherwise — without the layout claim, an old-numbering RAR4
                // volume (whose headers state nothing) would register as 0.
                let layout_volume = std::path::Path::new(&relative_path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(weaver_model::files::FileRole::from_filename)
                    .and_then(|role| match role {
                        weaver_model::files::FileRole::RarVolume { volume_number } => {
                            Some(volume_number)
                        }
                        _ => None,
                    });
                let registered_volume = Self::rar_registration_volume(layout_volume, &facts);
                restored.push((registered_volume, relative_path, facts));
            }

            if restored.is_empty() {
                continue;
            }

            let restored_volumes: Vec<u32> = restored
                .iter()
                .map(|(volume_number, _, _)| *volume_number)
                .collect();
            for (volume_number, relative_path, facts) in restored {
                self.persist_rar_volume_facts(
                    job_id,
                    &set_name,
                    &relative_path,
                    Some(volume_number),
                    facts,
                )?;
            }
            // The header snapshot predates the restored volume: it was built
            // while the set still had the hole, and its member spans say so.
            // Rebuilding from it after the restore fed the extractor a member
            // that "lived" in volume 0 alone, and the decode ran out of bits at
            // the end of that volume. Invalidating the persisted copy too is
            // what makes the recompute below read every volume from disk.
            self.invalidate_rar_snapshot(job_id, &set_name);
            if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                for volume_number in restored_volumes {
                    set_state.verified_suspect_volumes.remove(&volume_number);
                }
            }
            self.recompute_rar_set_state(job_id, &set_name).await?;
            restored_sets.insert(set_name);
        }

        if restored_sets.is_empty() {
            return Ok(false);
        }

        let restored_members: HashSet<String> = restored_sets
            .iter()
            .filter_map(|set_name| {
                self.rar_sets
                    .get(&(job_id, set_name.clone()))
                    .and_then(|state| state.plan.as_ref())
            })
            .flat_map(|plan| plan.member_names.iter().cloned())
            .collect();
        if !restored_members.is_empty() {
            let remaining_failed = self
                .failed_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|member| !restored_members.contains(member))
                .collect();
            self.replace_failed_extraction_members(job_id, remaining_failed);
        }

        info!(
            job_id = job_id.0,
            sets = ?restored_sets,
            "restored RAR volumes from standalone recovery files"
        );
        self.retry_archive_extraction_after_verify_or_repair(job_id)
            .await;
        Ok(true)
    }

    pub(in crate::pipeline) async fn retry_archive_extraction_after_verify_or_repair(
        &mut self,
        job_id: JobId,
    ) {
        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));

        if let Err(error) = self.recompute_rar_sets_after_verify_or_repair(job_id).await {
            self.fail_job(job_id, error);
            return;
        }

        if self.job_has_only_rar_archives(job_id) {
            self.reconcile_job_progress(job_id).await;
            if self.all_rar_sets_complete(job_id) {
                self.finalize_completed_archive_job(job_id).await;
                return;
            }
            self.try_rar_extraction(job_id).await;
            return;
        }

        let already_extracted = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let already_spawned = self
            .inflight_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let sets_to_extract: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .filter(|(name, _)| {
                    !already_extracted.contains(*name) && !already_spawned.contains(*name)
                })
                .map(|(name, topo)| (name.clone(), topo.archive_type))
                .collect()
        };

        if !already_spawned.is_empty() && sets_to_extract.is_empty() {
            return;
        }

        if !sets_to_extract.is_empty() {
            if !self.maybe_start_extraction(job_id).await {
                return;
            }
            self.spawn_extractions(job_id, &sets_to_extract).await;
            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    pub(super) async fn retry_failed_archive_sources_without_par2(
        &mut self,
        job_id: JobId,
    ) -> Result<bool, String> {
        if self.normalization_retried.contains(&job_id) {
            return Ok(false);
        }

        let failed_entries = self
            .failed_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        if failed_entries.is_empty() {
            return Ok(false);
        }

        struct SourceRetryFile {
            file_id: NzbFileId,
            filename: String,
            work: Vec<DownloadWork>,
        }

        let (retry_files, retry_sets, retry_members, working_dir) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(false);
            };

            let mut file_indices = HashSet::new();
            let mut retry_sets: HashSet<String> = HashSet::new();
            let mut retry_members: HashSet<String> = HashSet::new();

            for failed in &failed_entries {
                if let Some(topo) = state.assembly.archive_topology_for(failed) {
                    retry_sets.insert(failed.clone());
                    retry_members.extend(topo.members.iter().map(|member| member.name.clone()));
                    for filename in topo.volume_map.keys() {
                        if let Some((index, _)) = state
                            .spec
                            .files
                            .iter()
                            .enumerate()
                            .find(|(_, file)| file.filename == *filename)
                        {
                            file_indices.insert(index as u32);
                        }
                    }
                    continue;
                }

                let mut matched_member = false;
                for (set_name, topo) in state.assembly.archive_topologies() {
                    if !topo.members.iter().any(|member| member.name == *failed) {
                        continue;
                    }
                    matched_member = true;
                    retry_sets.insert(set_name.clone());
                    retry_members.extend(topo.members.iter().map(|member| member.name.clone()));
                    for filename in topo.volume_map.keys() {
                        if let Some((index, _)) = state
                            .spec
                            .files
                            .iter()
                            .enumerate()
                            .find(|(_, file)| file.filename == *filename)
                        {
                            file_indices.insert(index as u32);
                        }
                    }
                    break;
                }

                if matched_member {
                    continue;
                }

                let mut matched_runtime_rar_set = false;
                for ((rar_job_id, set_name), rar_state) in &self.rar_sets {
                    if *rar_job_id != job_id {
                        continue;
                    }

                    let plan = rar_state.plan.as_ref();
                    let failed_is_set = set_name == failed;
                    let failed_is_member = plan.is_some_and(|plan| {
                        plan.member_names.iter().any(|member| member == failed)
                            || plan
                                .topology
                                .members
                                .iter()
                                .any(|member| member.name == *failed)
                    });
                    if !failed_is_set && !failed_is_member {
                        continue;
                    }

                    matched_runtime_rar_set = true;
                    retry_sets.insert(set_name.clone());
                    if let Some(plan) = plan {
                        retry_members.extend(plan.member_names.iter().cloned());
                    }

                    for filename in rar_state.volume_files.values() {
                        if let Some((index, _)) = state
                            .spec
                            .files
                            .iter()
                            .enumerate()
                            .find(|(_, file)| file.filename == *filename)
                        {
                            file_indices.insert(index as u32);
                        }
                    }
                }

                if matched_runtime_rar_set {
                    continue;
                }

                if let Some((index, _)) = state
                    .spec
                    .files
                    .iter()
                    .enumerate()
                    .find(|(_, file)| file.filename == *failed)
                {
                    file_indices.insert(index as u32);
                }
            }

            let retry_files = file_indices
                .into_iter()
                .filter_map(|file_index| {
                    let file = state.spec.files.get(file_index as usize)?;
                    let file_id = NzbFileId { job_id, file_index };
                    let work = file
                        .segments
                        .iter()
                        .map(|segment| DownloadWork {
                            segment_id: SegmentId {
                                file_id,
                                segment_number: segment.ordinal,
                            },
                            message_id: crate::jobs::ids::MessageId::new(&segment.message_id),
                            groups: std::sync::Arc::from(file.groups.as_slice()),
                            priority: file.role.download_priority(),
                            byte_estimate: segment.bytes,
                            retry_count: 0,
                            is_recovery: false,
                            completion_critical: false,
                            exclude_servers: vec![],
                            avoid_server: None,
                        })
                        .collect();
                    Some(SourceRetryFile {
                        file_id,
                        filename: file.filename.clone(),
                        work,
                    })
                })
                .collect::<Vec<_>>();

            (
                retry_files,
                retry_sets,
                retry_members,
                state.working_dir.clone(),
            )
        };

        if retry_files.is_empty() {
            return Ok(false);
        }

        self.set_normalization_retried_state(job_id, true);
        self.replace_failed_extraction_members(job_id, HashSet::new());

        let mut clear_extracted_archives = false;
        if let Some(extracted_archives) = self.extracted_archives.get_mut(&job_id) {
            for set_name in &retry_sets {
                extracted_archives.remove(set_name);
            }
            clear_extracted_archives = extracted_archives.is_empty();
        }
        if clear_extracted_archives {
            self.extracted_archives.remove(&job_id);
        }

        let mut clear_inflight_extractions = false;
        if let Some(inflight_extractions) = self.inflight_extractions.get_mut(&job_id) {
            for set_name in &retry_sets {
                inflight_extractions.remove(set_name);
            }
            clear_inflight_extractions = inflight_extractions.is_empty();
        }
        if clear_inflight_extractions {
            self.inflight_extractions.remove(&job_id);
        }

        let mut clear_extracted_members = false;
        if let Some(extracted_members) = self.extracted_members.get_mut(&job_id) {
            for member_name in &retry_members {
                extracted_members.remove(member_name);
            }
            clear_extracted_members = extracted_members.is_empty();
        }
        if clear_extracted_members {
            self.extracted_members.remove(&job_id);
        }
        if !retry_members.is_empty() {
            self.clear_persisted_extracted_members(job_id).await;
        }

        for set_name in &retry_sets {
            self.clear_archive_set_for_source_retry(job_id, set_name);
        }

        for retry_file in &retry_files {
            // The file is about to be deleted and downloaded again, so any
            // chase over it is reading bytes that are being withdrawn.
            self.direct_unpack_abort_sets_containing(
                job_id,
                &retry_file.filename,
                "archive source deleted for re-download",
            );
            let path = working_dir.join(&retry_file.filename);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to remove corrupt archive {} before source retry: {error}",
                        path.display()
                    ));
                }
            }
            if let Err(error) = self
                .db
                .mark_file_incomplete(job_id, retry_file.file_id.file_index)
            {
                warn!(
                    job_id = job_id.0,
                    file_index = retry_file.file_id.file_index,
                    error = %error,
                    "failed to persist file invalidation before source retry"
                );
            }
        }

        let mut cleared_detected_file_ids = Vec::new();
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(false);
            };

            for mut retry_file in retry_files {
                if let Some(file_asm) = state.assembly.file_mut(retry_file.file_id) {
                    file_asm.reset();
                    if state
                        .detected_archives
                        .remove(&retry_file.file_id.file_index)
                        .is_some()
                    {
                        cleared_detected_file_ids.push(retry_file.file_id);
                    }
                }

                for topo in state.assembly.archive_topologies_mut().values_mut() {
                    if let Some(&volume_number) = topo.volume_map.get(&retry_file.filename) {
                        topo.complete_volumes.remove(&volume_number);
                    }
                }

                for work in retry_file.work.drain(..) {
                    state.download_queue.push(work);
                }
            }
        }

        for file_id in cleared_detected_file_ids {
            self.clear_detected_archive_identity(job_id, file_id);
        }

        info!(
            job_id = job_id.0,
            files = retry_sets.len().max(1),
            failed = ?failed_entries,
            exclude_servers = ?Vec::<usize>::new(),
            "re-queueing archive source files after extraction failure without PAR2"
        );

        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));
        Ok(true)
    }
}
