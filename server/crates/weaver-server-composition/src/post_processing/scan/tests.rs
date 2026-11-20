// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::post_processing::instances::PostProcessingInstancesQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::settings::service::SettingsServiceQueries;

#[test]
fn scan_only_applies_changed_environment_fields() {
    let mut context = context(None, vec![("priority".into(), "50".into())]);
    context.env = [
        ("NZBNP_PRIORITY".into(), "50".into()),
        ("NZBNP_TOP".into(), "0".into()),
        ("NZBNP_PAUSED".into(), "0".into()),
        ("NZBNP_DUPEKEY".into(), String::new()),
        ("NZBNP_DUPESCORE".into(), "0".into()),
        ("NZBNP_DUPEMODE".into(), "SCORE".into()),
    ]
    .into();
    let initial_env = context.env.clone();
    let mut options = SubmissionOptions::default();
    let original_options = options.clone();

    apply_scan_environment_changes(&mut context, &initial_env, &mut options);
    assert_eq!(
        context.facts.parameters,
        vec![("priority".into(), "50".into())]
    );
    assert_eq!(options, original_options);

    context.env.insert("NZBNP_PRIORITY".into(), "-50".into());
    context.env.insert("NZBNP_PAUSED".into(), "1".into());
    apply_scan_environment_changes(&mut context, &initial_env, &mut options);
    assert_eq!(
        context.facts.parameters,
        vec![("priority".into(), "LOW".into())]
    );
    assert!(options.add_paused);
}

#[test]
fn compressed_scan_input_stops_at_the_decoded_limit() {
    let compressed = zstd::stream::encode_all(&vec![b'x'; 4096][..], 3).unwrap();
    let source = zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).unwrap();
    let mut written = Vec::new();
    let error = copy_scan_input(source, &mut written, 32).unwrap_err();
    assert!(error.to_string().contains("scan input limit"));
    assert_eq!(written.len(), 33);
    let mut written = Vec::new();
    copy_scan_input(&b"1234"[..], &mut written, 4).unwrap();
    assert_eq!(written, b"1234");
}

#[test]
fn scan_rejects_incomplete_runs_but_ignores_completed_exit_codes() {
    use super::super::model::{ScriptAdapter, ScriptName, ScriptResult, ScriptStatus};
    let mut result = ScriptResult {
        script: ScriptName::new("scan.py").unwrap(),
        instance_id: None,
        instance_name: None,
        event: ScriptEventLabel::Scan,
        output_id: None,
        background: false,
        adapter: ScriptAdapter::Nzbget,
        status: ScriptStatus::Failed,
        exit_code: None,
        duration_ms: 0,
        output_tail: String::new(),
        output_truncated: false,
        error_message: None,
        finished_at_epoch_ms: 0,
    };
    assert!(scan_run_incomplete(&result));
    result.status = ScriptStatus::TimedOut;
    result.exit_code = Some(137);
    assert!(scan_run_incomplete(&result));
    result.status = ScriptStatus::Cancelled;
    assert!(scan_run_incomplete(&result));
    result.status = ScriptStatus::Succeeded;
    result.exit_code = Some(17);
    assert!(!scan_run_incomplete(&result));
}

#[tokio::test]
async fn scan_reader_rejects_input_when_execution_cannot_start() {
    use super::super::instances::{InstanceTrigger, ScriptInstanceDraft};
    use super::super::model::{PostProcessingSettings, ScriptName};
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    let scripts = db
        .initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    std::fs::write(scripts.join("scan.py"), "### NZBGET SCAN SCRIPT ###\n").unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    db.create_script_instance(ScriptInstanceDraft::new(
        ScriptName::new("scan.py").unwrap(),
        InstanceTrigger::Scan,
    ))
    .unwrap();
    let selected =
        super::super::listing::resolve_script(&scripts, &ScriptName::new("scan.py").unwrap())
            .unwrap();
    assert!(
        selected
            .manifest
            .kinds()
            .contains(&super::super::model::ScriptKind::Scan)
    );
    let metadata = vec![(
        SOURCE_URL_KEY.into(),
        "https://source.test/invalid\0nzb".into(),
    )];
    assert!(enabled(&db, None, &metadata).await.unwrap());
    let mut config = db.load_config().unwrap();
    config.data_dir = data.path().to_string_lossy().into_owned();
    let config = std::sync::Arc::new(tokio::sync::RwLock::new(config));
    // A malformed internal URL makes NZBNP_URL construction fail before
    // spawn. Unlike invalid NZBPR parameters, required event inputs cannot
    // be omitted. A configured scan must reject this infrastructure failure.
    let result = scan_reader(
        &db,
        &config,
        std::io::Cursor::new(b"unscanned input"),
        Some("input.nzb".into()),
        None,
        None,
        metadata,
        Default::default(),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("unexecuted scan accepted the input"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("scan script scan.py did not complete: failed")
    );
}

#[tokio::test]
async fn scratch_admission_is_held_until_directory_cleanup() {
    let admission = Arc::new(tokio::sync::Semaphore::new(1));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_path_buf();
    let scratch = ScanScratch(Arc::new(ScanScratchFiles {
        directory,
        _permit: admission.clone().acquire_owned().await.unwrap(),
    }));
    let pending = admission.acquire_owned();
    tokio::pin!(pending);
    tokio::select! {
        biased;
        _ = &mut pending => panic!("a retained scratch directory must occupy admission"),
        () = std::future::ready(()) => {},
    }
    assert!(path.is_dir());
    drop(scratch);
    let _permit = pending.await.unwrap();
    assert!(!path.exists());
}
