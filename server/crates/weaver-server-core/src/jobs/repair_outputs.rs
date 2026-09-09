//! Durable ownership of reconstructed files that have no NZB articles.

use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{StateError, jobs::ids::JobId, persistence::Database};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RepairOutput {
    pub file_index: u32,
    pub filename: String,
    pub expected_length: u64,
}

impl RepairOutput {
    pub fn validate(&self) -> Result<(), StateError> {
        if self.filename.is_empty()
            || self.filename.len() > 1024
            || self.filename.split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || weaver_model::files::sanitize_download_filename(part) != part
            })
            || self.expected_length > i64::MAX as u64
        {
            return Err(StateError::Database(
                "invalid reconstructed output identity".into(),
            ));
        }
        Ok(())
    }

    /// Preserve authenticated relative paths while rejecting non-directory
    /// ancestors. Native installation checks these components again at write time.
    pub fn destination(&self, directory: &std::path::Path) -> Result<std::path::PathBuf, String> {
        self.validate().map_err(|error| error.to_string())?;
        let mut path = directory.to_path_buf();
        let mut parts = self.filename.split('/').peekable();
        while let Some(part) = parts.next() {
            path.push(part);
            if parts.peek().is_none() {
                break;
            }
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(_) => return Err("reconstructed output ancestor is not a directory".into()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(path)
    }
}

impl Database {
    /// Reserve all destinations before native installation. Replays must agree.
    pub(crate) fn reserve_repair_outputs(
        &self,
        job: JobId,
        outputs: &[RepairOutput],
    ) -> Result<(), StateError> {
        if outputs.len() > 16_384 {
            return Err(StateError::Database(
                "too many reconstructed outputs".into(),
            ));
        }
        for output in outputs {
            output.validate()?;
        }
        let outputs = outputs.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "reserve_repair_outputs", |tx| {
                let outputs = outputs.clone();
                Box::pin(async move {
                    super::persistence::lock_active_job_for_write_tx(tx, job).await?;
                    if tx.fetch_optional("SELECT job_id FROM active_jobs WHERE job_id = {}", &[SqlArg::I64(job.0 as i64)]).await?.is_none() {
                        return Err(StateError::Database("reconstructed output job is gone".into()));
                    }
                    for output in outputs {
                        if let Some(row) = tx.fetch_optional(
                            "SELECT filename, expected_length FROM active_repair_outputs WHERE job_id = {} AND file_index = {}",
                            &[SqlArg::I64(job.0 as i64), SqlArg::I64(output.file_index as i64)],
                        ).await? {
                            if row.text("filename")? != output.filename || row.i64("expected_length")? != output.expected_length as i64 {
                                return Err(StateError::Database("contradictory reconstructed output identity".into()));
                            }
                            continue;
                        }
                        tx.execute("INSERT INTO active_repair_outputs (job_id, file_index, filename, expected_length) VALUES ({}, {}, {}, {})",
                            &[SqlArg::I64(job.0 as i64), SqlArg::I64(output.file_index as i64), SqlArg::Text(output.filename), SqlArg::I64(output.expected_length as i64)]).await?;
                    }
                    Ok(())
                })
            }).await
        })
    }

    pub(crate) fn load_repair_outputs(&self, job: JobId) -> Result<Vec<RepairOutput>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(),
                "SELECT file_index, filename, expected_length FROM active_repair_outputs WHERE job_id = {} ORDER BY file_index LIMIT 16385",
                &[SqlArg::I64(job.0 as i64)]).await?;
            if rows.len() > 16_384 { return Err(StateError::Database("too many reconstructed outputs".into())); }
            rows.into_iter().map(|row| {
                let output = RepairOutput {
                    file_index: u32::try_from(row.i64("file_index")?).map_err(|_| StateError::Database("invalid reconstructed file index".into()))?,
                    filename: row.text("filename")?,
                    expected_length: u64::try_from(row.i64("expected_length")?).map_err(|_| StateError::Database("invalid reconstructed file length".into()))?,
                };
                output.validate()?;
                Ok(output)
            }).collect()
        })
    }
}

pub(crate) fn identity(output: &RepairOutput) -> super::record::ActiveFileIdentity {
    super::record::ActiveFileIdentity {
        file_index: output.file_index,
        source_filename: output.filename.clone(),
        current_filename: output.filename.clone(),
        canonical_filename: Some(output.filename.clone()),
        classification: crate::pipeline::Pipeline::canonical_archive_identity_from_filename(
            &output.filename,
        ),
        classification_source: super::record::FileIdentitySource::Par3,
    }
}

/// Recreate output membership, not verification evidence. A regular disk image
/// at its declared length is available for fresh native verification on restore.
pub(crate) async fn restore_assembly(
    job: JobId,
    outputs: &[RepairOutput],
    directory: &std::path::Path,
    assembly: &mut super::assembly::JobAssembly,
    identities: &mut std::collections::HashMap<u32, super::record::ActiveFileIdentity>,
) -> Result<Vec<u32>, String> {
    let mut present = Vec::new();
    for output in outputs {
        output.validate().map_err(|error| error.to_string())?;
        let id = super::ids::NzbFileId {
            job_id: job,
            file_index: output.file_index,
        };
        if assembly.file(id).is_some()
            || assembly.files().any(|file| {
                identities
                    .get(&file.file_id().file_index)
                    .map_or(file.filename(), |identity| {
                        identity.current_filename.as_str()
                    })
                    == output.filename
            })
        {
            return Err("reconstructed output collides with an NZB file".into());
        }
        if let Some(existing) = identities.get(&output.file_index)
            && existing.current_filename != output.filename
        {
            return Err("reconstructed output name changed".into());
        }
        let mut file = super::assembly::FileAssembly::repair_output(id, output.filename.clone());
        match tokio::fs::symlink_metadata(output.destination(directory)?).await {
            Ok(metadata)
                if metadata.file_type().is_file() && metadata.len() == output.expected_length =>
            {
                file.mark_complete();
                present.push(output.file_index);
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err("reconstructed output is not a regular file".into());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
        identities
            .entry(output.file_index)
            .or_insert_with(|| identity(output));
        assembly.add_file(file);
    }
    Ok(present)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{
        assembly::{FileAssembly, JobAssembly},
        ids::NzbFileId,
        record::ActiveJob,
    };
    use std::collections::HashMap;

    fn job() -> ActiveJob {
        ActiveJob {
            job_id: JobId(1),
            nzb_hash: [3; 32],
            nzb_path: "input.nzb".into(),
            nzb_zstd: vec![],
            output_dir: "output".into(),
            created_at: 1,
            category: None,
            metadata: vec![],
            status: "queued",
            download_state: "queued",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
        }
    }

    #[test]
    fn output_reservation_is_atomic_replayable_and_cascades() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("state.sqlite");
        let db = Database::open(&path).unwrap();
        db.create_active_job(&job()).unwrap();
        let first = RepairOutput {
            file_index: 3,
            filename: "restored.bin".into(),
            expected_length: 7,
        };
        db.reserve_repair_outputs(JobId(1), std::slice::from_ref(&first))
            .unwrap();
        db.reserve_repair_outputs(JobId(1), std::slice::from_ref(&first))
            .unwrap();
        let mut conflicting = first.clone();
        conflicting.expected_length += 1;
        let second = RepairOutput {
            file_index: 4,
            filename: "other.bin".into(),
            expected_length: 0,
        };
        assert!(
            db.reserve_repair_outputs(JobId(1), &[second, conflicting])
                .is_err()
        );
        assert_eq!(
            db.load_repair_outputs(JobId(1)).unwrap(),
            vec![first.clone()]
        );
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_repair_outputs(JobId(1)).unwrap(),
            vec![first.clone()]
        );
        db.delete_active_job(JobId(1)).unwrap();
        assert!(db.load_repair_outputs(JobId(1)).unwrap().is_empty());
        assert!(db.reserve_repair_outputs(JobId(1), &[first]).is_err());
    }

    #[tokio::test]
    async fn output_restore_preserves_missing_members_without_download_progress() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(root.path().join("present.bin"), b"present")
            .await
            .unwrap();
        let outputs = vec![
            RepairOutput {
                file_index: 3,
                filename: "present.bin".into(),
                expected_length: 7,
            },
            RepairOutput {
                file_index: 4,
                filename: "missing.bin".into(),
                expected_length: 9,
            },
        ];
        let mut assembly = JobAssembly::new(JobId(1));
        let mut identities = HashMap::new();
        assert_eq!(
            restore_assembly(
                JobId(1),
                &outputs,
                root.path(),
                &mut assembly,
                &mut identities
            )
            .await
            .unwrap(),
            vec![3]
        );
        let present = assembly
            .file(NzbFileId {
                job_id: JobId(1),
                file_index: 3,
            })
            .unwrap();
        assert!(present.is_complete());
        assert!(present.is_repair_output());
        assert_eq!(present.total_bytes(), 0);
        assert_eq!(present.received_bytes(), 0);
        assert_eq!(present.total_segments(), 0);
        let missing = assembly
            .file_mut(NzbFileId {
                job_id: JobId(1),
                file_index: 4,
            })
            .unwrap();
        assert!(!missing.is_complete());
        assert_eq!(missing.progress(), 0.0);
        missing.mark_complete();
        assert!(missing.is_complete());
        missing.reset();
        assert!(!missing.is_complete());
        assert!(
            restore_assembly(
                JobId(1),
                &outputs,
                root.path(),
                &mut assembly,
                &mut identities
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn output_restore_cannot_replace_download_members() {
        let root = tempfile::tempdir().unwrap();
        let mut assembly = JobAssembly::new(JobId(1));
        assembly.add_file(FileAssembly::new(
            NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            "input.bin".into(),
            weaver_model::files::FileRole::Unknown,
            vec![10],
        ));
        let mut identities = HashMap::new();
        for output in [
            RepairOutput {
                file_index: 0,
                filename: "different.bin".into(),
                expected_length: 10,
            },
            RepairOutput {
                file_index: 3,
                filename: "input.bin".into(),
                expected_length: 10,
            },
            RepairOutput {
                file_index: 3,
                filename: "../outside.bin".into(),
                expected_length: 10,
            },
        ] {
            assert!(
                restore_assembly(
                    JobId(1),
                    &[output],
                    root.path(),
                    &mut assembly,
                    &mut identities
                )
                .await
                .is_err()
            );
        }
        assert_eq!(assembly.files().count(), 1);
    }

    #[tokio::test]
    async fn nested_outputs_restore_with_exact_relative_names() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("nested/deeper")).unwrap();
        std::fs::write(root.path().join("nested/deeper/output.bin"), b"output").unwrap();
        let output = RepairOutput {
            file_index: 5,
            filename: "nested/deeper/output.bin".into(),
            expected_length: 6,
        };
        let mut assembly = JobAssembly::new(JobId(1));
        let mut identities = HashMap::new();
        assert_eq!(
            restore_assembly(
                JobId(1),
                &[output],
                root.path(),
                &mut assembly,
                &mut identities
            )
            .await
            .unwrap(),
            vec![5]
        );
        assert_eq!(identities[&5].current_filename, "nested/deeper/output.bin");
        for name in [
            "/absolute.bin",
            "../outside.bin",
            "nested/../outside.bin",
            "nested//output.bin",
            "./output.bin",
            "nested/",
            "C:/output.bin",
            "nested\\outside.bin",
        ] {
            assert!(
                RepairOutput {
                    file_index: 6,
                    filename: name.into(),
                    expected_length: 0
                }
                .validate()
                .is_err(),
                "{name}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nested_output_restore_rejects_symbolic_link_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("output.bin"), b"output").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("nested")).unwrap();
        let output = RepairOutput {
            file_index: 5,
            filename: "nested/output.bin".into(),
            expected_length: 6,
        };
        assert!(output.destination(root.path()).is_err());
        let mut assembly = JobAssembly::new(JobId(1));
        let mut identities = HashMap::new();
        assert!(
            restore_assembly(
                JobId(1),
                &[output],
                root.path(),
                &mut assembly,
                &mut identities
            )
            .await
            .is_err()
        );
        assert_eq!(assembly.files().count(), 0);
        assert_eq!(
            std::fs::read(outside.path().join("output.bin")).unwrap(),
            b"output"
        );
    }
}
