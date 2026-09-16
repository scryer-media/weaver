//! Install an authenticated content identity without overwriting another file.

use super::*;
use crate::jobs::record::FileIdentitySource;

impl Pipeline {
    /// Complete the durable intent before restore can publish a source or
    /// decide that an obfuscated embedded archive is an ordinary payload.
    pub(crate) fn restore_pending_par3_content_names(
        &self,
        job_id: JobId,
        directory: &std::path::Path,
        identities: &mut std::collections::HashMap<u32, crate::jobs::record::ActiveFileIdentity>,
    ) -> Result<(), String> {
        let pending: Vec<u32> = identities
            .values()
            .filter(|identity| identity.classification_source == FileIdentitySource::Par3Pending)
            .map(|identity| identity.file_index)
            .collect();
        for index in pending {
            let mut identity = identities[&index].clone();
            let replay = (|| {
                let name = identity
                    .canonical_filename
                    .clone()
                    .ok_or("missing PAR3 move target")?;
                if identities.values().any(|other| {
                    other.file_index != index
                        && (other.current_filename == name
                            || other.current_filename == identity.current_filename)
                }) {
                    return Err("PAR3 move collides with another job file".to_string());
                }
                finish_content_move(directory, &identity.current_filename, &name)
                    .map_err(|error| format!("cannot resume PAR3 content move: {error}"))?;
                Ok(name)
            })();
            match replay {
                Ok(name) => {
                    sync_content_directory(directory).map_err(|error| error.to_string())?;
                    identity.classification = Self::canonical_archive_identity_from_filename(&name)
                        .or(identity.classification);
                    identity.current_filename = name;
                    identity.classification_source = FileIdentitySource::Par3;
                }
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, file_index = index, %error,
                        "reverting unreplayable PAR3 name intent; source will be rediscovered");
                    identity.canonical_filename = None;
                    identity.classification =
                        Self::canonical_archive_identity_from_filename(&identity.current_filename);
                    identity.classification_source = FileIdentitySource::Declared;
                }
            }
            self.db
                .save_file_identity(job_id, &identity)
                .map_err(|error| {
                    format!("cannot commit restored PAR3 content identity: {error}")
                })?;
            identities.insert(index, identity);
        }
        Ok(())
    }

    /// Persist and install one validated name proposal. A failed exclusive
    /// move restores the exact previous identity before returning its error.
    pub(in crate::pipeline) fn install_par3_content_name(
        &mut self,
        job_id: JobId,
        mut identity: crate::jobs::record::ActiveFileIdentity,
        name: &str,
        directory: &std::path::Path,
    ) -> Result<(), String> {
        // All source IDs still designate the same files across this synchronous
        // rename. Keep their committed availability while identity changes fence
        // native generations and verification evidence as usual.
        let materialized = self
            .par3_runtime
            .as_mut()
            .and_then(|runtime| runtime.take_materialized_sources(job_id));
        let result = (|| {
            let old_name = identity.current_filename.clone();
            // Persist both names before changing the directory. Atomic exclusive
            // rename needs neither hard-link support nor a copy of the clean file.
            // A failed final database write leaves the durable intent replayable.
            let previous_identity = identity.clone();
            identity.canonical_filename = Some(name.to_owned());
            identity.classification_source = FileIdentitySource::Par3Pending;
            self.set_file_identity(job_id, identity.clone())?;
            crate::e2e_failpoint::maybe_trip("par3.content_name.intent");
            if let Err(error) = finish_content_move(directory, &old_name, name) {
                self.set_file_identity(job_id, previous_identity)
                .map_err(|rollback| {
                    format!(
                        "cannot place {}: {error}; cannot restore previous identity: {rollback}",
                        directory.join(name).display()
                    )
                })?;
                return Err(format!(
                    "cannot place {}: {error}",
                    directory.join(name).display()
                ));
            }
            // If durability fails after a successful move, keep the intent so a
            // restart can still find the actual target. Only a failed move rolls back.
            sync_content_directory(directory).map_err(|error| error.to_string())?;
            crate::e2e_failpoint::maybe_trip("par3.content_name.moved");
            identity.current_filename = name.to_owned();
            identity.classification =
                Self::canonical_archive_identity_from_filename(name).or(identity.classification);
            identity.classification_source = FileIdentitySource::Par3;
            self.set_file_identity(job_id, identity)?;
            crate::e2e_failpoint::maybe_trip("par3.content_name.persisted");
            Ok(())
        })();
        if let Some(materialized) = materialized
            && let Some(runtime) = self.par3_runtime.as_mut()
        {
            runtime.restore_materialized_sources(job_id, materialized);
        }
        result
    }

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
        let identity = self
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
        let old_non_rar_sets: Vec<String> = state
            .assembly
            .archive_topologies()
            .iter()
            .filter(|(name, topology)| {
                !matches!(
                    topology.archive_type,
                    crate::jobs::assembly::ArchiveType::Rar
                ) && topology.volume_map.contains_key(&old_name)
                    && !state.assembly.files().any(|other| {
                        other.file_id() != id
                            && self
                                .classified_archive_set_name_for_file(job_id, other)
                                .as_deref()
                                == Some(name.as_str())
                    })
            })
            .map(|(name, _)| name.clone())
            .collect();
        let old_sets = self.rar_set_names_for_files(job_id, &[id]);
        self.install_par3_content_name(
            job_id,
            identity,
            &found.name,
            old_path.parent().ok_or("missing content directory")?,
        )?;
        let touched = std::collections::HashSet::from([old_name]);
        for set in &old_sets {
            self.invalidate_archive_set_for_identity_rebind(job_id, set, &touched);
        }
        // The retired ZIP/7z/split roster must disappear before readiness
        // queues extraction. Its old names no longer designate source files.
        for set in old_non_rar_sets {
            self.jobs
                .get_mut(&job_id)
                .expect("live job")
                .assembly
                .remove_archive_topology(&set);
            self.db
                .clear_extraction_chunks_for_set(job_id, &set)
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

/// Replaying the filesystem half grants no checksum evidence. Restored bytes
/// must still pass native verification with their newly published generation.
fn finish_content_move(
    directory: &std::path::Path,
    old_name: &str,
    new_name: &str,
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    if [old_name, new_name].into_iter().any(|name| {
        name.is_empty() || weaver_model::files::sanitize_download_filename(name) != name
    }) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "unsafe PAR3 content name",
        ));
    }
    let old = directory.join(old_name);
    let target = directory.join(new_name);
    match std::fs::symlink_metadata(&old) {
        Ok(metadata) if metadata.file_type().is_file() => {
            crate::runtime::fs::rename_file_exclusive(&old, &target)?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            // The atomic move completed before the identity transaction.
            if !std::fs::symlink_metadata(&target)?.file_type().is_file() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "PAR3 move target is not a regular file",
                ));
            }
        }
        Ok(_) => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "PAR3 move source is not a regular file",
            ));
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn sync_content_directory(directory: &std::path::Path) -> std::io::Result<()> {
    // Keep post-move durability failures separate from failed exclusive moves.
    #[cfg(unix)]
    std::fs::File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[test]
    fn content_move_is_atomic_replayable_and_preserves_collisions() {
        use std::io::ErrorKind;
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("opaque.dat");
        let target = root.path().join("payload.bin");
        std::fs::write(&old, b"verified payload").unwrap();
        std::fs::write(&target, b"unrelated").unwrap();
        assert_eq!(
            finish_content_move(root.path(), "opaque.dat", "payload.bin")
                .unwrap_err()
                .kind(),
            ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&old).unwrap(), b"verified payload");
        assert_eq!(std::fs::read(&target).unwrap(), b"unrelated");
        std::fs::remove_file(&target).unwrap();
        let before = std::fs::metadata(&old).unwrap();
        finish_content_move(root.path(), "opaque.dat", "payload.bin").unwrap();
        assert!(!old.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                before.ino(),
                std::fs::metadata(&target).unwrap().ino(),
                "rename must not copy clean bytes"
            );
        }
        #[cfg(not(unix))]
        let _ = before;
        finish_content_move(root.path(), "opaque.dat", "payload.bin").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"verified payload");
        assert!(finish_content_move(root.path(), "../opaque.dat", "payload.bin").is_err());
        assert!(finish_content_move(root.path(), "opaque.dat", "../payload.bin").is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, &old).unwrap();
            assert_eq!(
                finish_content_move(root.path(), "opaque.dat", "payload.bin")
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidInput
            );
            std::fs::remove_file(&old).unwrap();
            std::fs::remove_file(&target).unwrap();
            std::os::unix::fs::symlink("missing", &target).unwrap();
            assert!(finish_content_move(root.path(), "opaque.dat", "payload.bin").is_err());
        }
    }

    #[test]
    fn content_move_supports_case_only_names_and_replay() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("PAYLOAD.BIN"), b"payload").unwrap();
        finish_content_move(root.path(), "PAYLOAD.BIN", "payload.bin").unwrap();
        finish_content_move(root.path(), "PAYLOAD.BIN", "payload.bin").unwrap();
        let names: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, [std::ffi::OsString::from("payload.bin")]);
        assert_eq!(
            std::fs::read(root.path().join("payload.bin")).unwrap(),
            b"payload"
        );
    }
}
