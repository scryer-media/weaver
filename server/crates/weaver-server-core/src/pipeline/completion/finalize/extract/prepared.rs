use super::*;
use std::collections::BTreeMap;

// Prepared work owns explicit inputs. The worker never resolves a job assembly
// or changes scheduler state; its caller owns admission and scoped completion.
pub(in crate::pipeline) struct PreparedRar {
    pub job_id: JobId,
    pub set_name: String,
    pub volume_paths: BTreeMap<u32, PathBuf>,
    pub cached_headers: Option<Vec<u8>>,
    pub password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub shared_kdf_cache: Arc<unrar_rs::crypto::KdfCache>,
    pub open_mode: crate::pipeline::extraction::RarArchiveOpenMode,
    pub already_extracted: HashSet<String>,
    pub root: Arc<ExtractionRoot>,
    pub budget: Arc<JobExtractionBudget>,
    pub policy: Arc<crate::post_processing::model::PostProcessingSettings>,
    pub event_tx: broadcast::Sender<PipelineEvent>,
    pub output_dir: PathBuf,
    pub phase_counters: Arc<PhaseCounters>,
}

impl PreparedRar {
    pub(in crate::pipeline) fn run_to<O: ArchiveOutput>(
        self,
        output: &O,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        self.run_with(Some(output), false)
    }

    pub(in crate::pipeline) fn run_archive_members_to<O: ArchiveOutput>(
        self,
        output: &O,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        self.run_with(Some(output), true)
    }

    fn run_with<O: ArchiveOutput>(
        self,
        output: Option<&O>,
        only_archives: bool,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let Self {
            job_id,
            set_name,
            volume_paths,
            cached_headers,
            password_candidates,
            shared_kdf_cache,
            open_mode,
            already_extracted,
            root,
            budget,
            policy,
            event_tx,
            output_dir,
            phase_counters,
        } = self;
        if volume_paths.is_empty() {
            return Err(ExtractionError::terminal(format!(
                "no on-disk RAR volumes for set '{set_name}'"
            )));
        }

        let selection = Pipeline::open_rar_archive_for_extraction_with_password_candidates(
            RarExtractionOpenRequest {
                set_name: &set_name,
                volume_paths: volume_paths.clone(),
                password_candidates: password_candidates.clone(),
                cached_headers,
                shared_kdf_cache: shared_kdf_cache.clone(),
                open_mode,
                requested_members: &[],
                already_extracted: Some(&already_extracted),
                budget: Some(Arc::clone(&budget)),
            },
        )?;
        if let Some(member) = Pipeline::blocked_rar_member(&selection.archive, &policy) {
            return Err(budget
                .reject_content_policy(crate::post_processing::model::unwanted_extension_reason(
                    &member,
                ))
                .into());
        }
        let _memory_permit = budget.reserve_memory_wait(selection.decoder_memory_bytes)?;
        let mut archive = selection.archive;
        let selected_password = selection.password;

        let meta = archive.metadata();
        let archive_password_required = meta.is_encrypted;
        let options = unrar_rs::ExtractOptions {
            verify: true,
            password: selected_password.clone(),
            restore_owners: false,
        };
        let is_solid = archive.is_solid();

        let mut extracted_members = Vec::new();
        let mut failed_members: Vec<(String, ExtractionError)> = Vec::new();
        let mut validated_password = selection.validated_password;
        for (idx, member) in meta.members.iter().enumerate() {
            if already_extracted.contains(&member.name) {
                continue;
            }

            let member_password_required = archive_password_required || member.is_encrypted;
            let route_archive = matches!(
                weaver_model::files::FileRole::from_filename(&member.name),
                weaver_model::files::FileRole::RarVolume { .. }
                    | weaver_model::files::FileRole::SevenZipArchive
                    | weaver_model::files::FileRole::SevenZipSplit { .. }
            );
            let result = if let Some(output) = output.filter(|_| !only_archives || route_archive) {
                Pipeline::extract_rar_member_to_sink(
                    &mut archive,
                    &volume_paths,
                    idx,
                    &options,
                    output,
                    &budget,
                    Arc::clone(&phase_counters),
                )
            } else {
                Pipeline::extract_rar_member_to_output(
                    &mut archive,
                    crate::pipeline::extraction::RarExtractionContext::new(
                        &volume_paths,
                        &event_tx,
                        job_id,
                        &set_name,
                        &output_dir,
                        &options,
                    )
                    .with_security(Arc::clone(&root), Arc::clone(&budget))
                    .with_phase_attempt(Some(Arc::new(
                        PhaseAttemptCounters::new(Arc::clone(&phase_counters)),
                    ))),
                    idx,
                )
            };
            match result {
                Ok((member_name, bytes_written, total_bytes)) => {
                    if validated_password.is_none() && member_password_required {
                        validated_password = selected_password.clone();
                    }
                    info!(job_id = job_id.0, member = %member_name, bytes_written, total_bytes, "member extracted");
                    let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                        job_id,
                        member: member_name.clone(),
                        bytes_written,
                        total_bytes,
                    });
                    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
                        job_id,
                        set_name: set_name.clone(),
                        member: member_name.clone(),
                    });
                    extracted_members.push(member_name);
                }
                Err(e) => {
                    let error = e.settle(&budget);
                    let _ = event_tx.send(PipelineEvent::ExtractionMemberFailed {
                        job_id,
                        set_name: set_name.clone(),
                        member: member.name.clone(),
                        error: error.message().to_string(),
                    });
                    tracing::warn!(member = %member.name, error = %error, "member extraction failed, continuing with remaining members");
                    failed_members.push((member.name.clone(), error));
                    if is_solid {
                        break;
                    }
                }
            }
        }

        Ok(FullSetExtractionOutcome {
            extracted: extracted_members,
            failed: failed_members,
            selected_password: validated_password,
        })
    }
}

pub(in crate::pipeline) struct PreparedSevenZip {
    pub job_id: JobId,
    pub set_name: String,
    pub part_candidates: Vec<(u32, PathBuf)>,
    pub uu_paths: HashSet<PathBuf>,
    pub passwords: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub root: Arc<ExtractionRoot>,
    pub budget: Arc<JobExtractionBudget>,
    pub event_tx: broadcast::Sender<PipelineEvent>,
    pub phase_counters: Arc<PhaseCounters>,
    pub sevenz_decode_memory: u64,
    pub sevenz_decode_threads: u32,
}

impl PreparedSevenZip {
    pub(in crate::pipeline) fn run_reader_to<
        O: ArchiveOutput,
        R: std::io::Read + std::io::Seek + Send + 'static,
        F: FnMut() -> Result<R, ExtractionError>,
    >(
        self,
        output: &O,
        mut open: F,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let mut header = [0u8; 32];
        std::io::Read::read_exact(&mut open()?, &mut header)
            .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
        let end_header_bytes =
            u64::from_le_bytes(header[20..28].try_into().expect("7z header width"));
        let context = SevenZipExtractionContext {
            job_id: self.job_id,
            set_name: self.set_name,
            root: self.root,
            budget: self.budget,
            password: sevenz_turbo::Password::empty(),
            event_tx: self.event_tx,
            phase_counters: self.phase_counters,
            decode_memory: SevenZipDecodeMemory::ReservedForFixedThreads {
                end_header_bytes,
                allowance_cap: self.sevenz_decode_memory,
            },
            decode_threads: self.sevenz_decode_threads,
        };
        password_candidates::extract_7z_with_password_candidates_to(
            context,
            &self.passwords,
            open,
            output,
        )
    }

    pub(in crate::pipeline) fn run_to<O: ArchiveOutput>(
        self,
        output: &O,
    ) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let Self {
            job_id,
            set_name,
            part_candidates,
            uu_paths,
            passwords,
            root,
            budget,
            event_tx,
            phase_counters,
            sevenz_decode_memory,
            sevenz_decode_threads,
        } = self;
        let parts = resolve_sevenz_parts(&part_candidates, &uu_paths).map_err(|error| {
            error.map_message(|message| format!("failed to read 7z archive: {message}"))
        })?;
        let end_header_bytes = sevenz_declared_end_header_bytes(&parts.paths[0]);

        let context = SevenZipExtractionContext {
            job_id,
            set_name,
            root,
            budget,
            password: sevenz_turbo::Password::empty(),
            event_tx,
            phase_counters,
            decode_memory: SevenZipDecodeMemory::ReservedForFixedThreads {
                end_header_bytes,
                allowance_cap: sevenz_decode_memory,
            },
            decode_threads: sevenz_decode_threads,
        };

        password_candidates::extract_7z_with_password_candidates_to(
            context,
            &passwords,
            || {
                parts.open().map_err(|error| {
                    ExtractionError::from_io(format!("failed to open 7z parts: {error}"), &error)
                })
            },
            output,
        )
    }
}

pub(in crate::pipeline) struct PreparedSimple {
    pub job_id: JobId,
    pub set_name: String,
    pub kind: SimpleArchiveKind,
    pub file_paths: Vec<PathBuf>,
    pub passwords: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub joined_output_already_present: Option<PathBuf>,
    pub root: Arc<ExtractionRoot>,
    pub budget: Arc<JobExtractionBudget>,
    pub event_tx: broadcast::Sender<PipelineEvent>,
    pub phase_counters: Arc<PhaseCounters>,
    pub xz_worker_threads: usize,
}

impl PreparedSimple {
    pub(in crate::pipeline) fn run(self) -> Result<FullSetExtractionOutcome, ExtractionError> {
        let Self {
            job_id,
            set_name,
            kind,
            file_paths,
            passwords,
            joined_output_already_present,
            root,
            budget,
            event_tx,
            phase_counters,
            xz_worker_threads,
        } = self;
        if file_paths.is_empty() {
            return Err(ExtractionError::terminal(format!(
                "no files found for set '{set_name}'"
            )));
        }
        let _memory_permit = simple_archive_task_memory_permit(
            kind,
            file_paths.first().map(PathBuf::as_path),
            &budget,
        )?;

        let mut selected_password = None;
        let extracted_members = match kind {
            SimpleArchiveKind::Zip => {
                let (members, password) = extract_zip(
                    &file_paths[0],
                    &root,
                    &budget,
                    &passwords,
                    &event_tx,
                    job_id,
                    &set_name,
                    Some(Arc::clone(&phase_counters)),
                )?;
                selected_password = password;
                members
            }
            SimpleArchiveKind::Tar => {
                extract_tar(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::TarGz => {
                extract_tar_gz(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::TarBz2 => {
                extract_tar_bz2(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::TarXz => {
                extract_tar_xz(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Gz => {
                extract_gz(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Deflate => {
                extract_deflate(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Brotli => {
                extract_brotli(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Zstd => {
                extract_zstd(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Bzip2 => {
                extract_bzip2(&file_paths[0], &root, &budget, &event_tx, job_id, &set_name)?
            }
            SimpleArchiveKind::Xz => extract_xz(
                &file_paths[0],
                &root,
                &budget,
                &event_tx,
                job_id,
                &set_name,
                xz_worker_threads,
            )?,
            SimpleArchiveKind::Split => extract_split(
                &file_paths,
                &root,
                &budget,
                &event_tx,
                job_id,
                &set_name,
                Some(Arc::clone(&phase_counters)),
                joined_output_already_present,
            )?,
        };

        Ok(FullSetExtractionOutcome {
            extracted: extracted_members,
            failed: Vec::new(),
            selected_password,
        })
    }
}
