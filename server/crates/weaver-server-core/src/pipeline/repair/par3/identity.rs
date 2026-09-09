//! Install an authenticated content identity without overwriting another file.

use super::*;
use crate::jobs::record::FileIdentitySource;

impl Pipeline {
    pub(super) async fn apply_par3_content_identity(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        let Some(found) = self
            .par3_runtime
            .as_mut()
            .expect("admitted job")
            .take_name_match(job_id)
            .map_err(|error| error.to_string())?
        else {
            self.schedule_job_completion_check(job_id);
            return Ok(());
        };
        let file_index =
            u32::try_from(found.source.0).map_err(|_| "unknown PAR3 content source")?;
        let id = NzbFileId { job_id, file_index };
        let state = self.jobs.get(&job_id).ok_or("missing PAR3 content job")?;
        let file = state.assembly.file(id).ok_or("missing PAR3 content file")?;
        if !file.is_complete() || self.par3_virtual_volume(id).is_some() {
            return Err("content identity requires a complete materialized source".into());
        }
        // Download identities currently use flat names. Never flatten an
        // authenticated nested path into a different protection description.
        if weaver_model::files::sanitize_download_filename(&found.name) != found.name {
            return Err("nested PAR3 content placement requires an output mapping".into());
        }
        let mut identity = self
            .effective_file_identity(job_id, id)
            .ok_or("missing file identity")?;
        let old_name = identity.current_filename.clone();
        let old_path = state.working_dir.join(&old_name);
        let target = state.working_dir.join(&found.name);
        if found.path.as_ref() != Some(&old_path) || old_path == target {
            return Err("PAR3 content source binding changed".into());
        }
        if state.assembly.files().any(|other| {
            other.file_id() != id && self.current_filename_for_file(job_id, other) == found.name
        }) {
            return Err("PAR3 content destination belongs to another job file".into());
        }
        if !std::fs::symlink_metadata(&old_path)
            .map_err(|error| error.to_string())?
            .file_type()
            .is_file()
        {
            return Err("PAR3 content source is not a regular file".into());
        }
        let old_non_rar_sets: Vec<String> = state.assembly.archive_topologies().iter()
            .filter(|(name, topology)| {
                !matches!(topology.archive_type, crate::jobs::assembly::ArchiveType::Rar)
                    && topology.volume_map.contains_key(&old_name)
                    && !state.assembly.files().any(|other| {
                        other.file_id() != id
                            && self.classified_archive_set_name_for_file(job_id, other)
                                .as_deref() == Some(name.as_str())
                    })
            })
            .map(|(name, _)| name.clone())
            .collect();
        let old_sets = self.rar_set_names_for_files(job_id, &[id]);
        // Keep the original name until the identity is durable. Hard-link
        // placement is exclusive and writes no clean payload bytes. A collision
        // (including a symlink) fails without changing either existing file.
        std::fs::hard_link(&old_path, &target)
            .map_err(|error| format!("cannot place {}: {error}", target.display()))?;
        identity.current_filename = found.name.clone();
        identity.canonical_filename = Some(found.name.clone());
        identity.classification =
            Self::canonical_archive_identity_from_filename(&found.name).or(identity.classification);
        identity.classification_source = FileIdentitySource::Par3;
        if let Err(error) = self.set_file_identity(job_id, identity) {
            let rollback = std::fs::remove_file(&target);
            return Err(format!("{error}; new-name rollback: {rollback:?}"));
        }
        // Identity persistence also updates the completed-file name in the
        // same transaction, preserving its existing checksum provenance.
        std::fs::remove_file(&old_path)
            .map_err(|error| format!("failed to retire old content name: {error}"))?;
        let touched = std::collections::HashSet::from([old_name]);
        for set in &old_sets {
            self.invalidate_archive_set_for_identity_rebind(job_id, set, &touched);
        }
        // The retired ZIP/7z/split roster must disappear before readiness
        // queues extraction. Its old names no longer designate source files.
        for set in old_non_rar_sets {
            self.jobs.get_mut(&job_id).expect("live job")
                .assembly.remove_archive_topology(&set);
            self.db.clear_extraction_chunks_for_set(job_id, &set)
                .map_err(|error| format!("failed to retire content extraction state: {error}"))?;
        }
        let role = weaver_model::files::FileRole::from_filename(&found.name);
        if !matches!(role, FileRole::RarVolume { .. })
            && let Some(set) = weaver_model::files::archive_base_name(&found.name, &role)
        {
            // A topology assembled before discovery may omit the formerly
            // obfuscated first part. Rebuild its roster from current identities.
            self.jobs
                .get_mut(&job_id)
                .expect("live job")
                .assembly
                .remove_archive_topology(&set);
        }
        self.refresh_archive_state_for_completed_file(job_id, id, false)
            .await;
        for set in old_sets {
            let _ = self.clear_archive_set_if_unreferenced_and_idle(job_id, &set);
        }
        self.refresh_par3_sources(job_id)
            .map_err(|error| error.to_string())?;
        self.mark_rar_unlock_priorities_dirty(job_id);
        self.release_direct_unpack_after_repair(job_id);
        self.schedule_job_completion_check(job_id);
        Ok(())
    }
}
