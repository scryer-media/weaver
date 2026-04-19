use crate::jobs::assembly::{
    ArchiveMember, ArchiveTopology, ArchiveType, DetectedArchiveIdentity,
    DetectedArchiveKind as PersistedDetectedArchiveKind,
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

#[derive(Debug, Clone)]
pub(crate) enum ProbedArchiveKind {
    Rar {
        facts: weaver_rar::RarVolumeFacts,
    },
    SevenZipSingle,
    SevenZipSplit {
        volume_map: HashMap<String, u32>,
        complete_volumes: HashSet<u32>,
        expected_volume_count: u32,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct DetectedArchiveRegistration {
    pub(crate) identity: DetectedArchiveIdentity,
    pub(crate) kind: ProbedArchiveKind,
}

impl Pipeline {
    pub(crate) async fn try_register_archive_topology_for_completed_file(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
    ) {
        let role = match self
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
        {
            Some(file) if file.is_complete() => file.effective_role(),
            None => return,
            Some(_) => return,
        };

        match role {
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

        let Some(candidate) = self.archive_probe_candidate(job_id, file_id) else {
            return;
        };

        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        let registration = match self
            .probe_archive_candidate(job_id, &candidate, password)
            .await
        {
            Ok(registration) => registration,
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

        let Some(registration) = registration else {
            return;
        };

        if let Err(error) = self.persist_detected_archive_identity(
            job_id,
            candidate.file_id,
            &registration.identity,
        ) {
            tracing::warn!(
                job_id = job_id.0,
                file_id = %file_id,
                set_name = %registration.identity.set_name,
                error = %error,
                "failed to persist detected archive identity"
            );
            return;
        }

        match registration.kind {
            ProbedArchiveKind::Rar { facts } => {
                match self.persist_rar_volume_facts(
                    job_id,
                    &registration.identity.set_name,
                    &candidate.filename,
                    None,
                    facts,
                ) {
                    Ok(_) => {
                        if let Err(error) = self
                            .recompute_rar_set_state(job_id, &registration.identity.set_name)
                            .await
                        {
                            tracing::warn!(
                                job_id = job_id.0,
                                file_id = %file_id,
                                set_name = %registration.identity.set_name,
                                error,
                                "failed to recompute detected RAR set state"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            job_id = job_id.0,
                            file_id = %file_id,
                            set_name = %registration.identity.set_name,
                            error = %error,
                            "failed to register detected RAR volume"
                        );
                    }
                }
            }
            ProbedArchiveKind::SevenZipSingle => {
                self.register_detected_seven_zip_single(
                    job_id,
                    &registration.identity.set_name,
                    &candidate,
                );
            }
            ProbedArchiveKind::SevenZipSplit {
                volume_map,
                complete_volumes,
                expected_volume_count,
            } => {
                self.register_detected_seven_zip_split(
                    job_id,
                    &registration.identity.set_name,
                    volume_map,
                    complete_volumes,
                    expected_volume_count,
                );
            }
        }
    }

    fn persist_detected_archive_identity(
        &mut self,
        job_id: JobId,
        file_id: NzbFileId,
        identity: &DetectedArchiveIdentity,
    ) -> Result<(), String> {
        self.db
            .save_detected_archive_identity(job_id, file_id.file_index, identity)
            .map_err(|error| format!("failed to save detected archive identity: {error}"))?;
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return Ok(());
        };
        let Some(file) = state.assembly.file_mut(file_id) else {
            return Ok(());
        };
        file.set_detected_archive(identity.clone());
        Ok(())
    }

    fn archive_probe_candidate(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> Option<ArchiveProbeCandidate> {
        let state = self.jobs.get(&job_id)?;
        let file = state.assembly.file(file_id)?;
        if file.detected_archive().is_some() || !file.is_complete() {
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
    ) -> Result<Option<DetectedArchiveRegistration>, String> {
        let Some(path) = self.resolve_job_input_path(job_id, &candidate.filename) else {
            return Ok(None);
        };
        if !path.exists() {
            return Ok(None);
        }

        if let Ok(facts) = Self::parse_rar_volume_facts_from_path(path.clone(), password).await {
            return Ok(Some(DetectedArchiveRegistration {
                identity: DetectedArchiveIdentity {
                    kind: PersistedDetectedArchiveKind::Rar,
                    set_name: candidate.set_key.clone(),
                    volume_index: Some(facts.volume_number),
                },
                kind: ProbedArchiveKind::Rar { facts },
            }));
        }

        if candidate.numeric_suffix.is_some()
            && self.known_detected_archive_kind(job_id, &candidate.set_key)
                == Some(PersistedDetectedArchiveKind::SevenZipSplit)
        {
            let Some((volume_map, complete_volumes, expected_volume_count)) =
                self.detected_seven_zip_split_group(job_id, &candidate.set_key)
            else {
                return Ok(None);
            };
            let Some(volume_index) = volume_map.get(&candidate.filename).copied() else {
                return Ok(None);
            };
            return Ok(Some(DetectedArchiveRegistration {
                identity: DetectedArchiveIdentity {
                    kind: PersistedDetectedArchiveKind::SevenZipSplit,
                    set_name: candidate.set_key.clone(),
                    volume_index: Some(volume_index),
                },
                kind: ProbedArchiveKind::SevenZipSplit {
                    volume_map,
                    complete_volumes,
                    expected_volume_count,
                },
            }));
        }

        if !Self::path_has_7z_signature(path).await? {
            return Ok(None);
        }

        if candidate.numeric_suffix.is_some() {
            let Some((volume_map, complete_volumes, expected_volume_count)) =
                self.detected_seven_zip_split_group(job_id, &candidate.set_key)
            else {
                return Ok(None);
            };
            let Some(volume_index) = volume_map.get(&candidate.filename).copied() else {
                return Ok(None);
            };
            return Ok(Some(DetectedArchiveRegistration {
                identity: DetectedArchiveIdentity {
                    kind: PersistedDetectedArchiveKind::SevenZipSplit,
                    set_name: candidate.set_key.clone(),
                    volume_index: Some(volume_index),
                },
                kind: ProbedArchiveKind::SevenZipSplit {
                    volume_map,
                    complete_volumes,
                    expected_volume_count,
                },
            }));
        }

        Ok(Some(DetectedArchiveRegistration {
            identity: DetectedArchiveIdentity {
                kind: PersistedDetectedArchiveKind::SevenZipSingle,
                set_name: candidate.set_key.clone(),
                volume_index: None,
            },
            kind: ProbedArchiveKind::SevenZipSingle,
        }))
    }

    fn known_detected_archive_kind(
        &self,
        job_id: JobId,
        set_key: &str,
    ) -> Option<PersistedDetectedArchiveKind> {
        self.jobs.get(&job_id).and_then(|state| {
            state.assembly.files().find_map(|file| {
                let detected = file.detected_archive()?;
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

    fn register_detected_seven_zip_single(
        &mut self,
        job_id: JobId,
        set_name: &str,
        candidate: &ArchiveProbeCandidate,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let mut complete_volumes = HashSet::new();
        complete_volumes.insert(0);
        state.assembly.set_archive_topology(
            set_name.to_string(),
            ArchiveTopology {
                archive_type: ArchiveType::SevenZip,
                volume_map: HashMap::from([(candidate.filename.clone(), 0)]),
                complete_volumes,
                expected_volume_count: Some(1),
                members: vec![ArchiveMember {
                    name: set_name.to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    fn register_detected_seven_zip_split(
        &mut self,
        job_id: JobId,
        set_name: &str,
        volume_map: HashMap<String, u32>,
        complete_volumes: HashSet<u32>,
        expected_volume_count: u32,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let last_volume = expected_volume_count.saturating_sub(1);
        state.assembly.set_archive_topology(
            set_name.to_string(),
            ArchiveTopology {
                archive_type: ArchiveType::SevenZip,
                volume_map,
                complete_volumes,
                expected_volume_count: Some(expected_volume_count),
                members: vec![ArchiveMember {
                    name: set_name.to_string(),
                    first_volume: 0,
                    last_volume,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
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
    let (set_key, suffix) = filename.rsplit_once('.')?;
    if set_key.is_empty() || suffix.is_empty() || suffix.len() > 3 {
        return None;
    }
    if !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    suffix
        .parse::<u32>()
        .ok()
        .map(|numeric_suffix| (set_key.to_string(), numeric_suffix))
}
