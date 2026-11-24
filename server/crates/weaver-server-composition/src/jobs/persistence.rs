// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The active job writes moved to weaver-server-jobs, and the admission
// writes that also reach history, tags, outlines and scripts to
// weaver-server-postproc. The tests open the composed database.
pub use weaver_server_jobs::jobs::persistence::*;
pub use weaver_server_postproc::post_processing::admission_persistence::*;

#[cfg(test)]
use crate::jobs::model::{FieldUpdate, JobUpdate};
#[cfg(test)]
use crate::jobs::record::{ActiveFileIdentity, ActiveJob};
#[cfg(test)]
use crate::persistence::Database;
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(test)]
use weaver_server_base::jobs::ids::JobId;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::queries::JobsQueries;
    use crate::operations::recovery::credentials::StoredCredentialQueries;
    use crate::post_processing::effects::PostProcessingEffectsQueries;
    use std::path::PathBuf;

    fn sample_active_job(id: u64) -> ActiveJob {
        ActiveJob {
            job_id: JobId(id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("/tmp/queue_order_test_{id}.nzb")),
            nzb_zstd: Vec::new(),
            output_dir: PathBuf::from(format!("/tmp/queue_order_test_out_{id}")),
            created_at: 1_700_000_000 + id,
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
            supplied_password_candidates: Vec::new(),
        }
    }

    fn raw_archive_password(db: &Database, id: JobId) -> Option<String> {
        let datastore = db.datastore();
        db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT password FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(id.0 as i64)],
            )
            .await?
            .unwrap()
            .opt_text("password")
        })
        .unwrap()
    }

    fn replace_stored_archive_password(db: &Database, id: JobId, value: &str) {
        let datastore = db.datastore();
        let value = value.to_string();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                &[SqlArg::Text(value), SqlArg::I64(id.0 as i64)],
            )
            .await
            .map(|_| ())
        })
        .unwrap();
    }

    #[test]
    fn archive_password_is_encrypted_on_every_active_admission_path() {
        let db = Database::open_in_memory().unwrap();
        // A user's literal password may resemble a stored envelope. It must
        // still be encrypted, then restored exactly rather than interpreted.
        let plaintext = "enc:v1:literal archive password";
        for id in 1..=3 {
            let mut job = sample_active_job(id);
            job.password_override = Some(plaintext.into());
            match id {
                1 => db.create_active_job(&job).unwrap(),
                2 => db
                    .create_active_job_with_file_identities(&job, &[])
                    .unwrap(),
                _ => assert!(
                    db.materialize_admission(&job, &[], None, None, false)
                        .unwrap()
                ),
            }
            let stored = raw_archive_password(&db, job.job_id).unwrap();
            assert!(crate::persistence::encryption::is_encrypted(&stored));
            assert_ne!(stored, plaintext);
            assert_eq!(
                crate::persistence::encryption::decrypt_value(
                    db.encryption_key().unwrap(),
                    &stored
                )
                .unwrap(),
                plaintext
            );
            assert_eq!(
                db.load_active_jobs().unwrap()[&job.job_id]
                    .password_override
                    .as_deref(),
                Some(plaintext)
            );
        }
        assert!(db.has_encrypted_credentials().unwrap());
        db.validate_encrypted_credentials(db.encryption_key().unwrap())
            .unwrap();
    }

    #[test]
    fn metadata_edits_preserve_newer_script_parameters_and_refresh_effects() {
        use crate::post_processing::effects::JobScriptEffects;

        let db = Database::open_in_memory().unwrap();
        let mut job = sample_active_job(41);
        job.metadata = vec![("untouched".into(), "original".into())];
        db.create_active_job(&job).unwrap();
        let initial = JobScriptEffects {
            parameters: [
                ("Priority".into(), "NORMAL".into()),
                ("obsolete".into(), "remove me".into()),
                ("chain".into(), "preserve me".into()),
            ]
            .into(),
            directory: Some(PathBuf::from("fixture-output")),
            ..Default::default()
        };
        db.save_job_script_effects(job.job_id.0, &initial).unwrap();
        let update = JobUpdate {
            metadata_edits: vec![
                ("priority".into(), Some("HIGH".into())),
                ("OBSOLETE".into(), None),
            ],
            name: Some("renamed fixture".into()),
            ..Default::default()
        };
        // A directive commits after the intent was built but before its writer
        // runs. The edit must patch this current row rather than an old snapshot.
        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("later".into(), "new directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        db.update_active_job(job.job_id, &update).unwrap();

        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        for expected in [
            ("untouched", "original"),
            ("chain", "preserve me"),
            ("later", "new directive"),
            ("priority", "HIGH"),
        ] {
            assert!(
                metadata
                    .iter()
                    .any(|(key, value)| key == expected.0 && value == expected.1)
            );
        }
        assert!(
            !metadata
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
        );
        assert_eq!(
            metadata
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
                .count(),
            1
        );
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("renamed fixture")
        );
        let effects = db.job_script_effects(job.job_id.0).unwrap();
        assert_eq!(effects.directory, initial.directory);
        assert_eq!(effects.parameters["chain"], "preserve me");
        assert_eq!(effects.parameters["later"], "new directive");
        assert_eq!(effects.parameters["Priority"], "");
        assert_eq!(effects.parameters["priority"], "HIGH");
        let mut replayed = metadata.clone();
        effects.merge_parameters(&mut replayed);
        assert!(
            !replayed
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
        );
        assert_eq!(
            replayed
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
                .count(),
            1
        );

        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("next".into(), "another directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert!(metadata.contains(&("later".into(), "new directive".into())));
        assert!(metadata.contains(&("priority".into(), "HIGH".into())));
        assert!(metadata.contains(&("next".into(), "another directive".into())));
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("renamed fixture")
        );
    }

    #[test]
    fn rename_only_persists_metadata_without_replacing_other_fields() {
        let db = Database::open_in_memory().unwrap();
        let mut job = sample_active_job(42);
        job.metadata = vec![("preserved".into(), "value".into())];
        db.create_active_job(&job).unwrap();
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                name: Some("new title".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert!(metadata.contains(&("preserved".into(), "value".into())));
        assert_eq!(crate::ingest::renamed_title(&metadata), Some("new title"));
    }

    #[test]
    fn metadata_replacement_and_clear_apply_edits_after_the_replacement() {
        use crate::post_processing::effects::JobScriptEffects;

        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(43);
        db.create_active_job(&job).unwrap();
        db.save_job_script_effects(
            job.job_id.0,
            &JobScriptEffects {
                parameters: [("chain".into(), "old directive".into())].into(),
                ..Default::default()
            },
        )
        .unwrap();
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                metadata: FieldUpdate::Set(vec![("replacement".into(), "initial".into())]),
                metadata_edits: vec![("REPLACEMENT".into(), Some("final".into()))],
                name: Some("replacement title".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = db.load_active_jobs().unwrap()[&job.job_id].metadata.clone();
        assert_eq!(metadata.len(), 2);
        assert!(metadata.contains(&("REPLACEMENT".into(), "final".into())));
        assert_eq!(
            crate::ingest::renamed_title(&metadata),
            Some("replacement title")
        );
        assert_eq!(
            db.job_script_effects(job.job_id.0).unwrap().parameters["chain"],
            ""
        );
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                metadata: FieldUpdate::Clear,
                metadata_edits: vec![("after-clear".into(), Some("value".into()))],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id].metadata,
            vec![("after-clear".into(), "value".into())]
        );
    }

    #[test]
    fn archive_password_updates_preserve_null_empty_and_encrypted_overrides() {
        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(1);
        db.create_active_job(&job).unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id), None);
        for password in ["first-password", "enc:v1:another-literal-password"] {
            db.update_active_job(
                job.job_id,
                &JobUpdate {
                    password: FieldUpdate::Set(password.into()),
                    ..JobUpdate::default()
                },
            )
            .unwrap();
            assert_ne!(raw_archive_password(&db, job.job_id).unwrap(), password);
            assert_eq!(
                db.load_active_jobs().unwrap()[&job.job_id]
                    .password_override
                    .as_deref(),
                Some(password)
            );
        }
        let encrypted = raw_archive_password(&db, job.job_id);
        db.update_active_job(job.job_id, &JobUpdate::default())
            .unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id), encrypted);
        db.update_active_job(
            job.job_id,
            &JobUpdate {
                password: FieldUpdate::Clear,
                ..JobUpdate::default()
            },
        )
        .unwrap();
        assert_eq!(raw_archive_password(&db, job.job_id).as_deref(), Some(""));
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("")
        );
    }

    #[test]
    fn archive_password_rejects_missing_wrong_and_corrupt_keys_or_values() {
        use crate::persistence::encryption::EncryptionKey;
        assert!(encrypt_archive_password(None, Some("secret")).is_err());
        let mut db = Database::open_in_memory().unwrap();
        let key = db.encryption_key().unwrap().clone();
        let mut job = sample_active_job(1);
        job.password_override = Some("secret".into());
        db.create_active_job(&job).unwrap();
        let stored = raw_archive_password(&db, job.job_id).unwrap();
        assert!(decrypt_archive_password(None, Some(stored)).is_err());
        let wrong_key = EncryptionKey::generate();
        assert!(db.validate_encrypted_credentials(&wrong_key).is_err());
        db.set_encryption_key(wrong_key);
        assert!(db.load_active_jobs().is_err());
        db.set_encryption_key(key.clone());
        replace_stored_archive_password(&db, job.job_id, "enc:v1:corrupted");
        assert!(db.load_active_jobs().is_err());
        assert!(db.validate_encrypted_credentials(&key).is_err());
    }

    #[test]
    fn par3_content_identity_and_completed_name_survive_reopen() {
        use crate::jobs::record::FileIdentitySource;
        use weaver_server_base::jobs::ids::NzbFileId;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("content-identity.db");
        let job = sample_active_job(1);
        let identity = ActiveFileIdentity {
            file_index: 0,
            source_filename: "obfuscated.dat".into(),
            current_filename: "archive.zip".into(),
            canonical_filename: Some("archive.zip".into()),
            classification: None,
            classification_source: FileIdentitySource::Par3,
        };
        {
            let db = Database::open(&path).unwrap();
            db.create_active_job(&job).unwrap();
            db.complete_files(
                job.job_id,
                &[(0, "obfuscated.dat".into(), None)],
                CompletedHashProvenance::Verified,
            )
            .unwrap();
            let mut intent = identity.clone();
            intent.current_filename = intent.source_filename.clone();
            intent.classification_source = FileIdentitySource::Par3Pending;
            db.save_file_identity(job.job_id, &intent).unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            let restored = db.load_active_jobs().unwrap();
            let intent = &restored[&job.job_id].file_identities[&0];
            assert_eq!(
                intent.classification_source,
                FileIdentitySource::Par3Pending
            );
            assert_eq!(intent.current_filename, "obfuscated.dat");
            assert_eq!(intent.canonical_filename.as_deref(), Some("archive.zip"));
            db.save_file_identity(job.job_id, &identity).unwrap();
        }
        let db = Database::open(&path).unwrap();
        let jobs = db.load_active_jobs().unwrap();
        let restored = &jobs[&job.job_id];
        assert_eq!(restored.file_identities[&0], identity);
        assert!(restored.complete_files.contains(&NzbFileId {
            job_id: job.job_id,
            file_index: 0
        }));
        let datastore = db.datastore();
        let name = db
            .run_sql_blocking_read(async move {
                SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    "SELECT filename FROM active_files WHERE job_id = {} AND file_index = {}",
                    &[SqlArg::I64(1), SqlArg::I64(0)],
                )
                .await?
                .unwrap()
                .opt_text("filename")
            })
            .unwrap();
        assert_eq!(name.as_deref(), Some("archive.zip"));
    }

    #[test]
    fn archive_password_persistence_requires_the_same_key_after_reopen() {
        use crate::persistence::encryption::EncryptionKey;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("archive-password.db");
        let key = EncryptionKey::generate();
        let mut job = sample_active_job(1);
        job.password_override = Some("restart-password".into());
        {
            let mut db = Database::open(&path).unwrap();
            assert!(db.create_active_job(&job).is_err());
            assert!(db.load_active_jobs().unwrap().is_empty());
            db.set_encryption_key(key.clone());
            db.create_active_job(&job).unwrap();
        }
        let mut db = Database::open(&path).unwrap();
        assert!(db.has_encrypted_credentials().unwrap());
        assert!(db.load_active_jobs().is_err());
        db.validate_encrypted_credentials(&key).unwrap();
        db.set_encryption_key(key);
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("restart-password")
        );
    }

    #[test]
    fn legacy_archive_password_migration_is_encrypted_and_idempotent() {
        let db = Database::open_in_memory().unwrap();
        let job = sample_active_job(1);
        db.create_active_job(&job).unwrap();
        replace_stored_archive_password(&db, job.job_id, "legacy-password");
        assert!(!db.has_encrypted_credentials().unwrap());
        db.migrate_plaintext_credentials().unwrap();
        let stored = raw_archive_password(&db, job.job_id).unwrap();
        assert_ne!(stored, "legacy-password");
        assert!(db.has_encrypted_credentials().unwrap());
        assert_eq!(
            db.load_active_jobs().unwrap()[&job.job_id]
                .password_override
                .as_deref(),
            Some("legacy-password")
        );
        db.migrate_plaintext_credentials().unwrap();
        assert_eq!(
            raw_archive_password(&db, job.job_id).as_deref(),
            Some(stored.as_str())
        );
    }

    #[test]
    fn update_active_job_queue_positions_applies_case_update_to_all_rows() {
        let db = Database::open_in_memory().unwrap();
        for id in [1_u64, 2, 3] {
            db.create_active_job(&sample_active_job(id)).unwrap();
        }

        db.update_active_job_queue_positions(&[(JobId(1), 2), (JobId(2), 0), (JobId(3), 1)])
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, Some(2));
        assert_eq!(jobs[&JobId(2)].queue_position, Some(0));
        assert_eq!(jobs[&JobId(3)].queue_position, Some(1));
    }

    #[test]
    fn update_active_job_queue_positions_overwrites_previous_positions_in_one_statement() {
        let db = Database::open_in_memory().unwrap();
        for id in [1_u64, 2] {
            db.create_active_job(&sample_active_job(id)).unwrap();
        }
        db.update_active_job_queue_positions(&[(JobId(1), 0), (JobId(2), 1)])
            .unwrap();

        // A second batch (the full re-derived order) must fully overwrite the
        // positions from the first, in one statement.
        db.update_active_job_queue_positions(&[(JobId(1), 1), (JobId(2), 0)])
            .unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, Some(1));
        assert_eq!(jobs[&JobId(2)].queue_position, Some(0));
    }

    #[test]
    fn update_active_job_queue_positions_empty_slice_is_noop() {
        let db = Database::open_in_memory().unwrap();
        db.create_active_job(&sample_active_job(1)).unwrap();

        db.update_active_job_queue_positions(&[]).unwrap();

        let jobs = db.load_active_jobs().unwrap();
        assert_eq!(jobs[&JobId(1)].queue_position, None);
    }
}
