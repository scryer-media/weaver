use crate::jobs::assembly::{
    DetectedArchiveIdentity, DetectedArchiveKind as PersistedDetectedArchiveKind,
    ExtractionReadiness,
};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::pipeline::Pipeline;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use weaver_model::files::FileRole;

const SEVEN_Z_SIGNATURE: [u8; 6] = [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArchiveProbeCandidate {
    pub(crate) file_id: NzbFileId,
    pub(crate) filename: String,
    pub(crate) set_key: String,
    pub(crate) numeric_suffix: Option<u32>,
}

impl Pipeline {
    fn default_file_identity_for_file(
        &self,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> ActiveFileIdentity {
        ActiveFileIdentity {
            file_index: file.file_id().file_index,
            source_filename: file.filename().to_string(),
            current_filename: file.filename().to_string(),
            canonical_filename: None,
            classification: match file.declared_role() {
                FileRole::RarVolume { volume_number } => {
                    weaver_model::files::archive_base_name(file.filename(), file.declared_role())
                        .map(|set_name| DetectedArchiveIdentity {
                            kind: PersistedDetectedArchiveKind::Rar,
                            set_name,
                            volume_index: Some(*volume_number),
                        })
                }
                FileRole::SevenZipArchive => {
                    weaver_model::files::archive_base_name(file.filename(), file.declared_role())
                        .map(|set_name| DetectedArchiveIdentity {
                            kind: PersistedDetectedArchiveKind::SevenZipSingle,
                            set_name,
                            volume_index: None,
                        })
                }
                FileRole::SevenZipSplit { number } => {
                    weaver_model::files::archive_base_name(file.filename(), file.declared_role())
                        .map(|set_name| DetectedArchiveIdentity {
                            kind: PersistedDetectedArchiveKind::SevenZipSplit,
                            set_name,
                            volume_index: Some(*number),
                        })
                }
                _ => None,
            },
            classification_source: FileIdentitySource::Declared,
        }
    }

    pub(crate) fn file_identity(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<&ActiveFileIdentity> {
        self.jobs
            .get(&job_id)
            .and_then(|state| state.file_identities.get(&file_id.file_index))
    }

    pub(crate) fn effective_file_identity(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<ActiveFileIdentity> {
        if let Some(identity) = self.file_identity(job_id, file_id) {
            return Some(identity.clone());
        }
        let state = self.jobs.get(&job_id)?;
        let file = state.assembly.file(file_id)?;
        let mut identity = self.default_file_identity_for_file(file);
        if let Some(detected) = state.detected_archives.get(&file_id.file_index) {
            identity.classification = Some(detected.clone());
            identity.classification_source = FileIdentitySource::Probe;
        }
        Some(identity)
    }

    pub(crate) fn current_filename_for_file(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> String {
        self.current_filename_for_file_id(job_id, file.file_id())
            .unwrap_or_else(|| file.filename().to_string())
    }

    pub(crate) fn current_filename_for_file_id(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<String> {
        self.effective_file_identity(job_id, file_id)
            .map(|identity| identity.current_filename)
    }

    pub(crate) fn set_file_identity(
        &mut self,
        job_id: JobId,
        identity: ActiveFileIdentity,
    ) -> Result<(), String> {
        let file_id = NzbFileId {
            job_id,
            file_index: identity.file_index,
        };
        let previous = self.effective_file_identity(job_id, file_id);
        let filename_changed = previous
            .as_ref()
            .is_none_or(|previous| previous.current_filename != identity.current_filename);
        self.db
            .save_file_identity(job_id, &identity)
            .map_err(|error| format!("failed to save file identity: {error}"))?;
        // Only a current-name change moves the bytes a retained PAR2 session
        // refers to. Binding reads source and canonical aliases live.
        if filename_changed {
            self.invalidate_par2_session_for_identity_rebind(job_id);
            // A direct-unpack chase holds its parts by path and reopens them
            // lazily, so a rename would surface later as an unexplained
            // not-found deep inside the decoder. End it here, where the change
            // is known.
            if let Some(previous) = previous.as_ref() {
                self.direct_unpack_abort_sets_containing(
                    job_id,
                    &previous.current_filename,
                    "archive part renamed",
                );
            }
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.file_identities.insert(identity.file_index, identity);
        }
        self.refresh_par2_md5_substitution_binding(file_id);
        Ok(())
    }

    pub(crate) fn update_file_identity_classification(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        classification: Option<DetectedArchiveIdentity>,
        source: FileIdentitySource,
    ) -> Result<(), String> {
        let mut identity = self
            .effective_file_identity(job_id, file_id)
            .ok_or_else(|| format!("job {} file {} not found", job_id.0, file_id.file_index))?;
        identity.classification =
            if classification.is_none() && matches!(source, FileIdentitySource::Declared) {
                self.jobs
                    .get(&job_id)
                    .and_then(|state| state.assembly.file(file_id))
                    .and_then(|file| self.default_file_identity_for_file(file).classification)
            } else {
                classification
            };
        identity.classification_source = source;
        self.set_file_identity(job_id, identity)
    }

    pub(crate) async fn refresh_archive_state_for_completed_file(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        allow_probe: bool,
    ) {
        // A direct set's source volume has no file: classifying
        // it probes a path that does not exist, and the topology update it
        // feeds would then dispatch incremental extraction over volumes nobody
        // ever wrote. The routing seam suppresses its own call, but this
        // function has nine other callers — completion checks, PAR2 merge, RAR
        // finalization, the job service — and every one of them fires for a
        // complete direct volume. The rule belongs at the entry, once.
        if self.is_direct_source_file(file_id) {
            return;
        }
        // A demoted set's volume is a file, but not yet a finished one while
        // the demotion's detached sweep is writing it. An article that
        // completes it through the conventional path in that window would
        // otherwise probe a half-materialized image and hand it to the
        // extraction planner; the sweep's handback replays this hook for every
        // volume once the ticket lands, after the image is durable.
        if self.demotion_sweep_owns_file(file_id) {
            return;
        }
        self.classify_completed_file(job_id, file_id, allow_probe)
            .await;

        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return;
        };
        if !file.is_complete() {
            return;
        }

        match self.classified_role_for_file(job_id, file) {
            FileRole::RarVolume { .. } => {
                // Before the topology: a volume completing can make the set
                // ready, and the completion check that follows must already
                // see any gate the volume's recovery verdicts raise.
                self.publish_completed_part_to_chase(job_id, file_id);
                self.try_update_archive_topology(job_id, file_id).await;
                // After it: grouping reads the facts the topology update just
                // registered for this volume.
                self.group_nameless_rar_volumes(job_id, file_id).await;
            }
            FileRole::SevenZipArchive
            | FileRole::SevenZipSplit { .. }
            | FileRole::SplitFile { .. }
            | FileRole::ZipArchive
            | FileRole::TarArchive
            | FileRole::TarGzArchive
            | FileRole::TarBz2Archive
            | FileRole::GzArchive
            | FileRole::DeflateArchive
            | FileRole::BrotliArchive
            | FileRole::ZstdArchive
            | FileRole::Bzip2Archive
            | FileRole::XzArchive
            | FileRole::TarXzArchive => {
                self.try_update_7z_topology(job_id, file_id);
                // Retried on every part completion rather than attempted once:
                // the topology may only just have appeared, and the first
                // part's opening bytes may only just have landed.
                self.try_arm_direct_unpack_for_file(job_id, file_id);
            }
            _ => {}
        }
    }

    pub(crate) fn classified_role_for_file(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> FileRole {
        let identity = self.file_identity(job_id, file.file_id());
        if let Some(identity) = identity
            && identity.classification_source == FileIdentitySource::Par3
        {
            // The whole-file fingerprint established this current name. It
            // also supplies roles without a content-probe classification,
            // including ZIP and ordinary split files.
            return FileRole::from_filename(&identity.current_filename);
        }
        identity
            .and_then(|identity| identity.classification.as_ref())
            .or_else(|| self.detected_archive_identity(job_id, file.file_id()))
            .map(DetectedArchiveIdentity::effective_role)
            .unwrap_or_else(|| file.role().clone())
    }

    pub(crate) fn classified_archive_set_name_for_file(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> Option<String> {
        let current_filename = self.current_filename_for_file(job_id, file);
        self.file_identity(job_id, file.file_id())
            .and_then(|identity| identity.classification.as_ref())
            .map(|detected| detected.set_name.clone())
            .or_else(|| {
                self.detected_archive_identity(job_id, file.file_id())
                    .map(|detected| detected.set_name.clone())
            })
            .or_else(|| {
                weaver_model::files::archive_base_name(
                    &current_filename,
                    &self.classified_role_for_file(job_id, file),
                )
            })
    }

    pub(crate) fn detected_archive_identity(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<&DetectedArchiveIdentity> {
        self.jobs
            .get(&job_id)
            .and_then(|state| state.detected_archives.get(&file_id.file_index))
    }

    fn set_detected_archive_identity(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        identity: DetectedArchiveIdentity,
    ) -> Result<(), String> {
        self.db
            .save_detected_archive_identity(job_id, file_id.file_index, &identity)
            .map_err(|error| format!("failed to save detected archive identity: {error}"))?;
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.detected_archives.insert(file_id.file_index, identity);
        }
        self.update_file_identity_classification(
            job_id,
            file_id,
            self.detected_archive_identity(job_id, file_id).cloned(),
            FileIdentitySource::Probe,
        )?;
        Ok(())
    }

    pub(crate) fn clear_detected_archive_identity(&mut self, job_id: JobId, file_id: NzbFileId) {
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.detected_archives.remove(&file_id.file_index);
        }
        let _ = self.update_file_identity_classification(
            job_id,
            file_id,
            None,
            FileIdentitySource::Declared,
        );
        if let Err(error) = self
            .db
            .delete_detected_archive_identity(job_id, file_id.file_index)
        {
            tracing::warn!(
                job_id = job_id.0,
                file_index = file_id.file_index,
                error = %error,
                "failed to clear detected archive identity"
            );
        }
    }

    /// Whether this archive file is a source volume of a finalized **7z**
    /// direct set — one that has already put its members where the extractor
    /// would have put them.
    ///
    /// A direct set never enters the archive topology: its volumes are never
    /// written, so nothing ever probes one, and the completion hook that is
    /// the topology's only writer returns early for them. Once the set has
    /// finalized there is also nothing left to extract — the members are at
    /// their destinations and the set is already in `extracted_archives`. So
    /// counting its volumes as archives still waiting for a topology would
    /// leave the job blocked on a description that will never be built, of
    /// work that is already done.
    ///
    /// **RAR sets are excluded deliberately, in both states.** A job whose
    /// archives are all RAR never reaches this readiness check at all — the
    /// completion gate sends it to the RAR check instead — so a RAR direct set
    /// has never needed the clause; and a mixed job's RAR sets reach it on a
    /// path that has been answering for them since before there was a 7z
    /// layout. Narrowing to the format that needs it is what keeps this from
    /// being a change to how a RAR set completes.
    pub(in crate::pipeline) fn direct_set_already_installed(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> bool {
        let file_index = file.file_id().file_index;
        self.direct_store.sets_for(job_id).iter().any(|set| {
            set.plan().format == crate::pipeline::direct_store::plan::SetFormat::SevenZip
                && set.is_finalized()
                && set.plan().volume_for_file(file_index).is_some()
        })
    }

    pub(crate) fn extraction_readiness_for_job(&self, job_id: JobId) -> ExtractionReadiness {
        let Some(state) = self.jobs.get(&job_id) else {
            return ExtractionReadiness::NotApplicable;
        };

        if state.assembly.archive_topologies().is_empty() {
            let has_archive = state.assembly.files().any(|file| {
                matches!(
                    self.classified_role_for_file(job_id, file),
                    FileRole::RarVolume { .. }
                        | FileRole::SevenZipArchive
                        | FileRole::SevenZipSplit { .. }
                ) && !self.direct_set_already_installed(job_id, file)
                    && !self.recovery_superseded_source(job_id, file.file_id())
            });
            if has_archive {
                return ExtractionReadiness::Blocked {
                    reason: "archive topology not yet available".into(),
                };
            }
            return ExtractionReadiness::NotApplicable;
        }

        let all_archive_files_covered =
            state
                .assembly
                .files()
                .all(|file| match self.classified_role_for_file(job_id, file) {
                    FileRole::RarVolume { .. }
                    | FileRole::SevenZipArchive
                    | FileRole::SevenZipSplit { .. } => {
                        // A posted copy a repaired output replaced belongs to
                        // no set: the output's roster is the one that extracts.
                        self.direct_set_already_installed(job_id, file)
                            || self.recovery_superseded_source(job_id, file.file_id())
                            || state
                                .assembly
                                .archive_topologies()
                                .values()
                                .any(|topology| {
                                    topology
                                        .volume_map
                                        .contains_key(&self.current_filename_for_file(job_id, file))
                                })
                    }
                    _ => true,
                });
        if !all_archive_files_covered {
            return ExtractionReadiness::Blocked {
                reason: "archive topology not yet available for all sets".into(),
            };
        }

        if state.assembly.archive_topologies().len() == 1 {
            let set_name = state.assembly.archive_topologies().keys().next().unwrap();
            return state.assembly.set_extraction_readiness(set_name);
        }

        let mut all_ready = true;
        let mut any_ready = false;
        let mut extractable = Vec::new();
        let mut waiting_on = Vec::new();

        for set_name in state.assembly.archive_topologies().keys() {
            match state.assembly.set_extraction_readiness(set_name) {
                ExtractionReadiness::Ready => {
                    any_ready = true;
                    extractable.push(set_name.clone());
                }
                ExtractionReadiness::NotApplicable => {}
                _ => {
                    all_ready = false;
                    waiting_on.push(set_name.clone());
                }
            }
        }

        if all_ready && any_ready {
            ExtractionReadiness::Ready
        } else if any_ready {
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            }
        } else {
            ExtractionReadiness::Blocked {
                reason: "no archive sets are complete yet".into(),
            }
        }
    }

    async fn classify_completed_file(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        allow_probe: bool,
    ) {
        let Some(candidate) = self.archive_probe_candidate(job_id, file_id) else {
            return;
        };
        if !allow_probe {
            return;
        }

        let password_candidates = self.archive_password_candidates_for_job(job_id);
        let identity = match self
            .probe_archive_candidate(job_id, &candidate, password_candidates)
            .await
        {
            Ok(identity) => identity,
            Err(error) => {
                tracing::warn!(
                    job_id = job_id.0,
                    file_id = %file_id,
                    filename = %candidate.filename,
                    error = %error,
                    "archive content probe failed"
                );
                None
            }
        };

        let Some(identity) = identity else {
            return;
        };

        if identity.kind == PersistedDetectedArchiveKind::SevenZipSplit {
            if let Err(error) = self.set_detected_seven_zip_split_group(job_id, &candidate.set_key)
            {
                tracing::warn!(
                    job_id = job_id.0,
                    file_id = %file_id,
                    set_name = %candidate.set_key,
                    error = %error,
                    "failed to persist detected 7z split classification group"
                );
            }
            return;
        }

        if let Err(error) = self.set_detected_archive_identity(job_id, file_id, identity.clone()) {
            tracing::warn!(
                job_id = job_id.0,
                file_id = %file_id,
                set_name = %identity.set_name,
                error = %error,
                "failed to persist detected archive identity"
            );
        }
    }

    fn archive_probe_candidate(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<ArchiveProbeCandidate> {
        let state = self.jobs.get(&job_id)?;
        let file = state.assembly.file(file_id)?;
        if self
            .file_identity(job_id, file_id)
            .and_then(|identity| identity.classification.as_ref())
            .is_some()
            || !file.is_complete()
        {
            return None;
        }
        let role = file.declared_role();

        match role {
            FileRole::Unknown | FileRole::SplitFile { .. } => {}
            _ => return None,
        }

        let filename = self.current_filename_for_file(job_id, file);
        let (set_key, numeric_suffix) = probe_set_key_and_suffix(&filename, role)?;
        Some(ArchiveProbeCandidate {
            file_id,
            filename,
            set_key,
            numeric_suffix,
        })
    }

    async fn probe_archive_candidate(
        &self,
        job_id: JobId,
        candidate: &ArchiveProbeCandidate,
        password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    ) -> Result<Option<DetectedArchiveIdentity>, String> {
        let Some(path) = self.resolve_job_input_path(job_id, &candidate.filename) else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }

        if let Ok(facts) =
            Self::parse_rar_volume_facts_from_path(path.clone(), password_candidates).await
        {
            return Ok(Some(DetectedArchiveIdentity {
                kind: PersistedDetectedArchiveKind::Rar,
                set_name: candidate.set_key.clone(),
                volume_index: facts.volume_number,
            }));
        }

        if candidate.numeric_suffix.is_some()
            && self.known_detected_archive_kind(job_id, &candidate.set_key)
                == Some(PersistedDetectedArchiveKind::SevenZipSplit)
        {
            let Some((volume_map, _, _)) =
                self.detected_seven_zip_split_group(job_id, &candidate.set_key)
            else {
                return Ok(None);
            };
            let Some(volume_index) = volume_map.get(&candidate.filename).copied() else {
                return Ok(None);
            };
            return Ok(Some(DetectedArchiveIdentity {
                kind: PersistedDetectedArchiveKind::SevenZipSplit,
                set_name: candidate.set_key.clone(),
                volume_index: Some(volume_index),
            }));
        }

        if !Self::path_has_7z_signature(path).await? {
            return Ok(None);
        }

        if candidate.numeric_suffix.is_some() {
            let Some((volume_map, _, _)) =
                self.detected_seven_zip_split_group(job_id, &candidate.set_key)
            else {
                return Ok(None);
            };
            let Some(volume_index) = volume_map.get(&candidate.filename).copied() else {
                return Ok(None);
            };
            return Ok(Some(DetectedArchiveIdentity {
                kind: PersistedDetectedArchiveKind::SevenZipSplit,
                set_name: candidate.set_key.clone(),
                volume_index: Some(volume_index),
            }));
        }

        Ok(Some(DetectedArchiveIdentity {
            kind: PersistedDetectedArchiveKind::SevenZipSingle,
            set_name: candidate.set_key.clone(),
            volume_index: None,
        }))
    }

    fn known_detected_archive_kind(
        &self,
        job_id: JobId,
        set_key: &str,
    ) -> Option<PersistedDetectedArchiveKind> {
        self.jobs.get(&job_id).and_then(|state| {
            state
                .detected_archives
                .values()
                .find_map(|detected| {
                    (detected.set_name == set_key).then_some(detected.kind.clone())
                })
                .or_else(|| {
                    state.file_identities.values().find_map(|identity| {
                        identity.classification.as_ref().and_then(|classification| {
                            (classification.set_name == set_key)
                                .then_some(classification.kind.clone())
                        })
                    })
                })
        })
    }

    pub(crate) async fn path_has_7z_signature(path: PathBuf) -> Result<bool, String> {
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(&path)
                .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
            let mut signature = [0u8; 6];
            match file.read_exact(&mut signature) {
                Ok(()) => Ok(signature == SEVEN_Z_SIGNATURE),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(error) => Err(format!("failed to read {}: {error}", path.display())),
            }
        })
        .await
        .map_err(|error| format!("7z probe task panicked: {error}"))?
    }

    fn detected_seven_zip_split_group(
        &self,
        job_id: JobId,
        set_key: &str,
    ) -> Option<(HashMap<String, u32>, HashSet<u32>, u32)> {
        let state = self.jobs.get(&job_id)?;
        let mut numbered_files: Vec<(u32, String, bool)> = state
            .assembly
            .files()
            .filter_map(|file| {
                let role = file.declared_role();
                if !matches!(role, FileRole::Unknown | FileRole::SplitFile { .. }) {
                    return None;
                }
                let (candidate_set_key, numeric_suffix) =
                    probe_set_key_and_suffix(file.filename(), role)?;
                (candidate_set_key == set_key).then_some((
                    numeric_suffix?,
                    self.current_filename_for_file(job_id, file),
                    file.is_complete(),
                ))
            })
            .collect();

        if numbered_files.is_empty() {
            return None;
        }

        numbered_files.sort_by_key(|(suffix, filename, _)| (*suffix, filename.clone()));
        let expected_volume_count = numbered_files.len() as u32;
        let mut volume_map = HashMap::new();
        let mut complete_volumes = HashSet::new();

        for (index, (_, filename, is_complete)) in numbered_files.into_iter().enumerate() {
            let normalized_number = index as u32;
            volume_map.insert(filename, normalized_number);
            if is_complete {
                complete_volumes.insert(normalized_number);
            }
        }

        Some((volume_map, complete_volumes, expected_volume_count))
    }

    fn set_detected_seven_zip_split_group(
        &mut self,
        job_id: JobId,
        set_key: &str,
    ) -> Result<(), String> {
        let Some((volume_map, _, _)) = self.detected_seven_zip_split_group(job_id, set_key) else {
            return Ok(());
        };

        let matches: Vec<(NzbFileId, u32)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };
            state
                .assembly
                .files()
                .filter_map(|file| {
                    let (candidate_set_key, _) = probe_set_key_and_suffix(
                        &self.current_filename_for_file(job_id, file),
                        file.declared_role(),
                    )?;
                    if candidate_set_key != set_key {
                        return None;
                    }
                    let current_filename = self.current_filename_for_file(job_id, file);
                    let volume_index = volume_map.get(&current_filename).copied()?;
                    Some((file.file_id(), volume_index))
                })
                .collect()
        };

        for (file_id, volume_index) in matches {
            self.set_detected_archive_identity(
                job_id,
                file_id,
                DetectedArchiveIdentity {
                    kind: PersistedDetectedArchiveKind::SevenZipSplit,
                    set_name: set_key.to_string(),
                    volume_index: Some(volume_index),
                },
            )?;
        }

        Ok(())
    }
}

/// One complete RAR volume whose place in its set only its headers can give:
/// its filename says nothing about the set (no `.rar`/`.partNN.rar`/`.rNN`
/// shape and no numeric suffix, typical of a posting whose every volume
/// carries its own obfuscated hex name), or its set's names contradict what
/// the headers say.
struct NamelessRarVolume {
    file_id: NzbFileId,
    filename: String,
    classification: DetectedArchiveIdentity,
    facts: unrar_rs::RarVolumeFacts,
    /// Whether the registry holds these facts under this volume's name. A
    /// volume another took the key of is placed again even where its
    /// identity already says the right thing.
    registered: bool,
}

impl NamelessRarVolume {
    fn ordered_members(&self) -> Vec<&unrar_rs::RarVolumeMemberFacts> {
        let mut members: Vec<_> = self.facts.members.iter().collect();
        members.sort_by_key(|member| member.order);
        members
    }

    /// The member this volume opens on, when it continues one begun earlier.
    fn continued_member(&self) -> Option<&unrar_rs::RarVolumeMemberFacts> {
        self.ordered_members()
            .first()
            .copied()
            .filter(|member| member.split_before)
    }

    /// The member this volume ends on, when it carries on into the next one.
    fn continuing_member(&self) -> Option<&unrar_rs::RarVolumeMemberFacts> {
        self.ordered_members()
            .last()
            .copied()
            .filter(|member| member.split_after)
    }

    /// Whether the headers say this is a set's first volume: part of a
    /// multi-volume set, stating no number other than 0, and opening on a
    /// member of its own rather than the tail of an earlier one.
    fn opens_set(&self) -> bool {
        self.facts.is_volume
            && matches!(self.facts.volume_number, None | Some(0))
            && self
                .ordered_members()
                .first()
                .is_some_and(|member| !member.split_before)
    }

    /// Whether this volume can follow `previous` at index `index`, judged only
    /// by what both volumes' headers state.
    fn follows(&self, previous: &NamelessRarVolume, index: u32) -> bool {
        if self.facts.format != previous.facts.format
            || self.facts.is_encrypted != previous.facts.is_encrypted
            || self
                .facts
                .volume_number
                .is_some_and(|stated| stated != index)
        {
            return false;
        }
        match (previous.continuing_member(), self.continued_member()) {
            (Some(tail), Some(head)) => {
                head.name == tail.name
                    && match (head.unpacked_size, tail.unpacked_size) {
                        (Some(head_size), Some(tail_size)) => head_size == tail_size,
                        _ => true,
                    }
            }
            // A boundary that falls between two members links only on a
            // stated number; nothing else in the headers ties the volumes.
            (None, None) => self.facts.volume_number == Some(index),
            _ => false,
        }
    }
}

/// Why a volume with these headers is not a set's first volume, in words an
/// operator can check against the files.
fn not_first_volume_reason(facts: &unrar_rs::RarVolumeFacts) -> String {
    if let Some(stated) = facts.volume_number.filter(|stated| *stated != 0) {
        return format!("header states volume {stated}");
    }
    let first = facts.members.iter().min_by_key(|member| member.order);
    match first {
        Some(member) if member.split_before => format!(
            "opens mid-member, continuing '{}' from an earlier volume",
            member.name
        ),
        Some(_) => "states no volume number and opens on a member of its own, \
                    but was not placed in this set as its first volume"
            .to_string(),
        None => "headers name no member to place it by".to_string(),
    }
}

impl Pipeline {
    /// The complete RAR volumes of a job whose place only their headers can
    /// give, each with the header facts registered for it: volumes whose
    /// names carry no set identity at all, and every volume of a set whose
    /// names placed a volume at index 0 that its own headers say is not a
    /// first volume (a misnumbered `.001`, a swapped `.rar`/`.r00`).
    ///
    /// Facts come from the registry, and from the file itself for a volume
    /// the registry lost: two volumes whose names give them one key leave
    /// only the later one registered.
    async fn header_placed_rar_volumes(&self, job_id: JobId) -> Vec<NamelessRarVolume> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let mut found = Vec::new();
        for file in state.assembly.files().filter(|file| file.is_complete()) {
            let Some(identity) = self.effective_file_identity(job_id, file.file_id()) else {
                continue;
            };
            if identity.classification_source == FileIdentitySource::Par3 {
                continue;
            }
            let Some(classification) = identity.classification else {
                continue;
            };
            if classification.kind != PersistedDetectedArchiveKind::Rar {
                continue;
            }
            found.push((file.file_id(), identity.current_filename, classification));
        }
        let mut nameless = Vec::new();
        let mut named = Vec::new();
        for (file_id, filename, classification) in found {
            let (facts, registered) =
                match self.registered_rar_facts_for_filename(job_id, &filename) {
                    Some(facts) => (facts, true),
                    None => {
                        let Some(path) = self.resolve_job_input_path(job_id, &filename) else {
                            continue;
                        };
                        let passwords = self
                            .archive_password_candidates_for_set(job_id, &classification.set_name);
                        match Self::parse_rar_volume_facts_from_path(path, passwords).await {
                            Ok(facts) => (facts, false),
                            Err(_) => continue,
                        }
                    }
                };
            let has_name = Self::canonical_archive_identity_from_filename(&filename).is_some()
                || numeric_suffix_set_key(&filename).is_some();
            let volume = NamelessRarVolume {
                file_id,
                filename,
                classification,
                facts,
                registered,
            };
            if has_name {
                named.push(volume);
            } else {
                nameless.push(volume);
            }
        }
        // A set whose name-given volume 0 the headers rule out as a first
        // volume: it continues a member, or states another number.
        let contradicted: HashSet<String> = named
            .iter()
            .filter(|volume| {
                volume.classification.volume_index.unwrap_or(0) == 0
                    && volume.facts.is_volume
                    && !volume.opens_set()
            })
            .map(|volume| volume.classification.set_name.clone())
            .collect();
        nameless.extend(
            named
                .into_iter()
                .filter(|volume| contradicted.contains(&volume.classification.set_name)),
        );
        nameless
    }

    fn registered_rar_facts_for_filename(
        &self,
        job_id: JobId,
        filename: &str,
    ) -> Option<unrar_rs::RarVolumeFacts> {
        self.rar_sets
            .iter()
            .filter(|((set_job_id, _), _)| *set_job_id == job_id)
            .find_map(|(_, state)| {
                state
                    .volume_files
                    .iter()
                    .find(|(_, registered)| registered.as_str() == filename)
                    .and_then(|(volume, _)| state.facts.get(volume).cloned())
            })
    }

    /// Chains of nameless volumes, each starting at a first volume and
    /// following the volume whose headers continue it. A link is taken only
    /// when exactly one volume can be next; an ambiguous or missing link ends
    /// the chain there, and the volumes past it stay where they were.
    fn chain_nameless_rar_volumes(volumes: &[NamelessRarVolume]) -> Vec<Vec<(usize, u32)>> {
        let mut assigned = vec![false; volumes.len()];
        let mut openers: Vec<usize> = (0..volumes.len())
            .filter(|index| volumes[*index].opens_set())
            .collect();
        openers.sort_by(|left, right| volumes[*left].filename.cmp(&volumes[*right].filename));
        for opener in &openers {
            assigned[*opener] = true;
        }

        let mut chains = Vec::new();
        for opener in openers {
            let mut chain = vec![(opener, 0u32)];
            let mut current = opener;
            let mut current_index = 0u32;
            while volumes[current].facts.more_volumes {
                let next_index = current_index + 1;
                let candidates: Vec<usize> = (0..volumes.len())
                    .filter(|candidate| {
                        !assigned[*candidate]
                            && volumes[*candidate].follows(&volumes[current], next_index)
                    })
                    .collect();
                let stated: Vec<usize> = candidates
                    .iter()
                    .copied()
                    .filter(|candidate| volumes[*candidate].facts.volume_number == Some(next_index))
                    .collect();
                let next = match (stated.as_slice(), candidates.as_slice()) {
                    ([only], _) => *only,
                    ([], [only]) => *only,
                    _ => break,
                };
                assigned[next] = true;
                chain.push((next, next_index));
                current = next;
                current_index = next_index;
            }
            chains.push(chain);
        }
        chains
    }

    /// Group a job's nameless RAR volumes into sets by what their headers say.
    ///
    /// A filename that is only an obfuscated hex string puts every volume in
    /// a set of its own, named after itself, and every set but the first then
    /// has no volume 0 to open from. The headers do carry the set's shape: a
    /// first volume opens on a member of its own, and each later one opens on
    /// the tail of the member its predecessor ended on (and, for RAR5 and
    /// numbered RAR4, states its number). Each chain becomes one set, named
    /// after its first volume's set so the name never moves once that volume
    /// has landed.
    ///
    /// A PAR2 binding that gave a volume a real archive name has already
    /// taken it out of this group; only names that still say nothing are
    /// placed here, along with every volume of a set whose names put at index
    /// 0 a volume its own headers say is not first. A name's suffix is a fast
    /// path, never a verdict the headers cannot overrule. Volumes no chain
    /// reaches are left as they are, and a set that still has no first volume
    /// waits for one through the ordinary missing-volume path.
    pub(crate) async fn group_nameless_rar_volumes(&mut self, job_id: JobId, file_id: NzbFileId) {
        let volumes = self.header_placed_rar_volumes(job_id).await;
        if !volumes.iter().any(|volume| volume.file_id == file_id) {
            return;
        }

        let mut rebinds: Vec<(usize, DetectedArchiveIdentity)> = Vec::new();
        for chain in Self::chain_nameless_rar_volumes(&volumes) {
            let set_name = volumes[chain[0].0].classification.set_name.clone();
            for (volume, index) in chain {
                let wanted = DetectedArchiveIdentity {
                    kind: PersistedDetectedArchiveKind::Rar,
                    set_name: set_name.clone(),
                    volume_index: Some(index),
                };
                let current = &volumes[volume].classification;
                if current.set_name != wanted.set_name
                    || current.volume_index.unwrap_or(0) != index
                    || !volumes[volume].registered
                {
                    rebinds.push((volume, wanted));
                }
            }
        }

        // Every identity moves before any registration is redone: a volume
        // taking index 0 from another in the same set must not land on a key
        // that volume still holds.
        let mut touched_by_set: BTreeMap<String, HashSet<String>> = BTreeMap::new();
        let mut moved = Vec::new();
        for (volume, wanted) in rebinds {
            let volume = &volumes[volume];
            let old_set = volume.classification.set_name.clone();
            // A set already extracting keeps the layout it started with.
            if self.rar_set_is_busy(job_id, &old_set)
                || self.rar_set_is_busy(job_id, &wanted.set_name)
            {
                continue;
            }
            tracing::info!(
                job_id = job_id.0,
                filename = %volume.filename,
                from_set = %old_set,
                from_volume = ?volume.classification.volume_index,
                to_set = %wanted.set_name,
                to_volume = ?wanted.volume_index,
                "placing RAR volume by its headers"
            );
            if let Err(error) = self.set_detected_archive_identity(job_id, volume.file_id, wanted) {
                tracing::warn!(
                    job_id = job_id.0,
                    filename = %volume.filename,
                    error = %error,
                    "failed to persist header-derived RAR set placement"
                );
                continue;
            }
            touched_by_set
                .entry(old_set)
                .or_default()
                .insert(volume.filename.clone());
            moved.push(volume.file_id);
        }
        for (old_set, touched) in &touched_by_set {
            self.invalidate_archive_set_for_identity_rebind(job_id, old_set, touched);
        }
        for file_id in moved {
            self.try_update_archive_topology(job_id, file_id).await;
        }
        for old_set in touched_by_set.keys() {
            let _ = self.clear_archive_set_if_unreferenced_and_idle(job_id, old_set);
        }
    }

    fn rar_set_is_busy(&self, job_id: JobId, set_name: &str) -> bool {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .is_some_and(|state| state.active_workers > 0 || !state.in_flight_members.is_empty())
            || self
                .inflight_extractions
                .get(&job_id)
                .is_some_and(|sets| sets.contains(set_name))
    }

    /// For every RAR set of the job that has never had a first volume, which
    /// volumes were seen and why none of them is volume 0.
    pub(crate) fn missing_first_rar_volume_report(&self, job_id: JobId) -> Option<String> {
        let mut sets: Vec<&String> = self
            .rar_sets
            .keys()
            .filter(|(set_job_id, set_name)| {
                *set_job_id == job_id && self.rar_set_lacks_first_volume(job_id, set_name)
            })
            .map(|(_, set_name)| set_name)
            .collect();
        if sets.is_empty() {
            return None;
        }
        sets.sort();
        let mut seen = Vec::new();
        for set_name in &sets {
            let Some(state) = self.rar_sets.get(&(job_id, (*set_name).clone())) else {
                continue;
            };
            for (volume, facts) in &state.facts {
                let filename = state
                    .volume_files
                    .get(volume)
                    .map(String::as_str)
                    .unwrap_or("<unnamed>");
                seen.push(format!(
                    "'{filename}' (set '{set_name}') {}",
                    not_first_volume_reason(facts)
                ));
            }
        }
        Some(format!(
            "no first RAR volume (volume 0) was found for {} set(s); volumes seen: {}",
            sets.len(),
            seen.join(", ")
        ))
    }
}

pub(crate) fn probe_set_key_and_suffix(
    filename: &str,
    role: &FileRole,
) -> Option<(String, Option<u32>)> {
    match role {
        FileRole::SplitFile { .. } => {
            let (set_key, numeric_suffix) = numeric_suffix_set_key(filename)?;
            Some((set_key, Some(numeric_suffix)))
        }
        FileRole::Unknown => {
            if let Some((set_key, numeric_suffix)) = numeric_suffix_set_key(filename) {
                return Some((set_key, Some(numeric_suffix)));
            }
            Some((filename.to_string(), None))
        }
        _ => None,
    }
}

fn numeric_suffix_set_key(filename: &str) -> Option<(String, u32)> {
    let dot = filename.rfind('.')?;
    let suffix = filename.get(dot + 1..)?;
    if suffix.is_empty() || suffix.len() > 5 || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    let numeric_suffix = suffix.parse::<u32>().ok()?;
    Some((filename[..dot].to_string(), numeric_suffix))
}
