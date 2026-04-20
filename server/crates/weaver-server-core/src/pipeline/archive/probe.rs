use crate::jobs::assembly::{
    DetectedArchiveIdentity, DetectedArchiveKind as PersistedDetectedArchiveKind,
    ExtractionReadiness,
};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::Pipeline;
use std::collections::{HashMap, HashSet};
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
    pub(crate) async fn refresh_archive_state_for_completed_file(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        allow_probe: bool,
    ) {
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
                self.try_update_archive_topology(job_id, file_id).await;
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
            | FileRole::Bzip2Archive => {
                self.try_update_7z_topology(job_id, file_id);
            }
            _ => {}
        }
    }

    pub(crate) fn classified_role_for_file(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> FileRole {
        self.detected_archive_identity(job_id, file.file_id())
            .map(DetectedArchiveIdentity::effective_role)
            .unwrap_or_else(|| file.role().clone())
    }

    pub(crate) fn classified_archive_set_name_for_file(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
    ) -> Option<String> {
        self.detected_archive_identity(job_id, file.file_id())
            .map(|detected| detected.set_name.clone())
            .or_else(|| weaver_model::files::archive_base_name(file.filename(), file.role()))
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
        Ok(())
    }

    pub(crate) fn clear_detected_archive_identity(&mut self, job_id: JobId, file_id: NzbFileId) {
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.detected_archives.remove(&file_id.file_index);
        }
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

    pub(crate) fn replace_detected_archive_identities_for_job(
        &mut self,
        job_id: JobId,
        detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    ) -> Result<(), String> {
        let previous_keys: Vec<u32> = self
            .jobs
            .get(&job_id)
            .map(|state| state.detected_archives.keys().copied().collect())
            .unwrap_or_default();

        for file_index in previous_keys {
            if detected_archives.contains_key(&file_index) {
                continue;
            }
            self.db
                .delete_detected_archive_identity(job_id, file_index)
                .map_err(|error| {
                    format!(
                        "failed to delete detected archive identity for file {file_index}: {error}"
                    )
                })?;
        }

        for (file_index, identity) in &detected_archives {
            self.db
                .save_detected_archive_identity(job_id, *file_index, identity)
                .map_err(|error| {
                    format!(
                        "failed to save detected archive identity for file {file_index}: {error}"
                    )
                })?;
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.detected_archives = detected_archives;
        }

        Ok(())
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
                )
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
                    | FileRole::SevenZipSplit { .. } => state
                        .assembly
                        .archive_topologies()
                        .values()
                        .any(|topology| topology.volume_map.contains_key(file.filename())),
                    _ => true,
                });
        if !all_archive_files_covered {
            return ExtractionReadiness::Blocked {
                reason: "archive topology not yet available for all sets".into(),
            };
        }

        state.assembly.extraction_readiness()
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

        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        let identity = match self
            .probe_archive_candidate(job_id, &candidate, password)
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
        if self.detected_archive_identity(job_id, file_id).is_some() || !file.is_complete() {
            return None;
        }
        let role = file.declared_role();

        match role {
            FileRole::Unknown | FileRole::SplitFile { .. } => {}
            _ => return None,
        }

        let filename = file.filename().to_string();
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
        password: Option<String>,
    ) -> Result<Option<DetectedArchiveIdentity>, String> {
        let Some(path) = self.resolve_job_input_path(job_id, &candidate.filename) else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }

        if let Ok(facts) = Self::parse_rar_volume_facts_from_path(path.clone(), password).await {
            return Ok(Some(DetectedArchiveIdentity {
                kind: PersistedDetectedArchiveKind::Rar,
                set_name: candidate.set_key.clone(),
                volume_index: Some(facts.volume_number),
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
            state.detected_archives.values().find_map(|detected| {
                (detected.set_name == set_key).then_some(detected.kind.clone())
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
                    file.filename().to_string(),
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
                    let (candidate_set_key, _) =
                        probe_set_key_and_suffix(file.filename(), file.declared_role())?;
                    if candidate_set_key != set_key {
                        return None;
                    }
                    let volume_index = volume_map.get(file.filename()).copied()?;
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
