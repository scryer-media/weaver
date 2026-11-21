// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;

fn script_result(exit_code: i32) -> ScriptResult {
    ScriptResult {
        script: crate::post_processing::model::ScriptName::new("notify.sh").unwrap(),
        instance_id: None,
        instance_name: None,
        event: Default::default(),
        adapter: crate::post_processing::model::ScriptAdapter::Nzbget,
        status: crate::post_processing::model::ScriptStatus::Succeeded,
        exit_code: Some(exit_code),
        duration_ms: 5,
        output_tail: String::new(),
        output_id: None,
        background: false,
        output_truncated: false,
        error_message: None,
        finished_at_epoch_ms: 3,
    }
}

fn history_row(job_id: u64) -> crate::JobHistoryRow {
    crate::JobHistoryRow {
        job_id,
        job_hash: None,
        name: format!("Invented.Release.{job_id}"),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1_000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
        server_attribution: None,
        archived_at: None,
        protocol: crate::jobs::Protocol::Nzb,
    }
}

fn active_job(job_id: u64) -> crate::ActiveJob {
    crate::ActiveJob {
        job_id: crate::JobId(job_id),
        nzb_hash: [0xBC; 32],
        nzb_path: PathBuf::from(format!("/invented/job-{job_id}.nzb")),
        nzb_zstd: Vec::new(),
        output_dir: PathBuf::from(format!("/invented/job-{job_id}")),
        created_at: 1_700_000_000,
        category: None,
        metadata: Vec::new(),
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

fn set_results_json(db: &Database, table: &'static str, job_id: u64, json: Option<&str>) {
    let datastore = db.datastore();
    let json = json.map(str::to_string);
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.write_exec(),
            &format!("UPDATE {table} SET script_results_json = {{}} WHERE job_id = {{}}"),
            &[SqlArg::OptText(json), SqlArg::I64(job_id as i64)],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

// The batched read gives every job what the one-job read gives it,
// including a live row winning over its history row and an empty live
// result hiding a non-empty history one.
#[test]
fn batched_script_results_match_the_one_job_read() {
    let db = Database::open_in_memory().unwrap();
    let live_results = vec![script_result(1)];
    let history_results = vec![script_result(2), script_result(3)];
    let live_json = to_json(&live_results).unwrap();
    let history_json = to_json(&history_results).unwrap();

    // 1: live row only. 2: history row only. 3: both, both set.
    // 4: both, live unset. 5: both, live set to an empty list.
    // 6: neither row. 7: history row with nothing recorded.
    db.create_active_job(&active_job(1)).unwrap();
    set_results_json(&db, "active_jobs", 1, Some(&live_json));
    db.insert_job_history(&history_row(2)).unwrap();
    set_results_json(&db, "job_history", 2, Some(&history_json));
    for job_id in [3, 4, 5] {
        db.create_active_job(&active_job(job_id)).unwrap();
        db.insert_job_history(&history_row(job_id)).unwrap();
        set_results_json(&db, "job_history", job_id, Some(&history_json));
    }
    set_results_json(&db, "active_jobs", 3, Some(&live_json));
    set_results_json(&db, "active_jobs", 5, Some("[]"));
    db.insert_job_history(&history_row(7)).unwrap();

    let ids = [1, 2, 3, 4, 5, 6, 7, 3, 2];
    let batched = db.job_post_processing_results_many(&ids).unwrap();
    let mut one_by_one = std::collections::HashMap::new();
    for job_id in ids {
        let results = db.job_post_processing_results(job_id).unwrap();
        if !results.is_empty() {
            one_by_one.insert(job_id, results);
        }
    }
    assert_eq!(batched, one_by_one);
    assert_eq!(batched.get(&3), Some(&live_results));
    assert_eq!(batched.get(&4), Some(&history_results));
    assert!(!batched.contains_key(&5));
    assert!(db.job_post_processing_results_many(&[]).unwrap().is_empty());
}

#[test]
fn stored_settings_from_earlier_shapes_still_load() {
    let db = Database::open_in_memory().unwrap();
    // As 0.14 stored it: no event-script fields at all.
    db.set_setting(
        SETTINGS_KEY,
        r#"{"executionEnabled":true,"concurrency":2,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":["exe"]}"#,
    )
    .unwrap();
    let settings = db.post_processing_settings().unwrap();
    assert!(settings.execution_enabled);
    assert_eq!(settings.concurrency, 2);
    assert_eq!(settings.event_scripts.script_output_runs_per_job, 32);
    assert_eq!(settings.event_scripts.script_output_failed_runs_per_job, 8);

    // With output settings that no longer exist: they are ignored, and
    // the next save leaves them out.
    db.set_setting(
        SETTINGS_KEY,
        r#"{"eventScriptConcurrency":2,"eventScriptTimeoutSeconds":300,"fileDownloadedEventInterval":0,"scriptOutputCeilingBytes":1048576,"scriptOutputRunsPerJob":5,"scriptOutputRingBytes":67108864,"scriptOutputRunCapBytes":2097152,"executionEnabled":false,"concurrency":4,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":[],"globalScriptsRun":"always"}"#,
    )
    .unwrap();
    // So is the event-script concurrency the one concurrency setting
    // replaced.
    let settings = db.post_processing_settings().unwrap();
    assert_eq!(settings.event_scripts.script_output_runs_per_job, 5);
    assert_eq!(settings.event_scripts.script_output_failed_runs_per_job, 8);
    db.save_post_processing_settings(&settings).unwrap();
    let stored = db.get_setting(SETTINGS_KEY).unwrap().unwrap();
    assert!(!stored.contains("scriptOutputCeilingBytes"));
    assert!(!stored.contains("scriptOutputRingBytes"));
    assert!(!stored.contains("scriptOutputRunCapBytes"));
    assert!(!stored.contains("eventScriptConcurrency"));
    assert!(stored.contains(r#""concurrency":4"#));
    assert!(stored.contains(r#""scriptOutputFailedRunsPerJob":8"#));
}

#[test]
fn saving_the_extension_list_keeps_every_other_setting() {
    let db = Database::open_in_memory().unwrap();
    let mut settings = PostProcessingSettings {
        concurrency: 3,
        python_interpreter: Some("/usr/bin/python3".into()),
        ..PostProcessingSettings::default()
    };
    settings.event_scripts.script_output_runs_per_job = 7;
    db.save_post_processing_settings(&settings).unwrap();

    let saved = db
        .save_unacceptable_extensions(vec![" ISO ".into(), "exe".into(), "iso".into()])
        .unwrap();
    assert_eq!(saved.unacceptable_extensions, vec!["exe", "iso"]);
    let stored = db.post_processing_settings().unwrap();
    assert_eq!(stored.unacceptable_extensions, vec!["exe", "iso"]);
    assert_eq!(stored.concurrency, 3);
    assert_eq!(
        stored.python_interpreter.as_deref(),
        Some("/usr/bin/python3")
    );
    assert_eq!(stored.event_scripts.script_output_runs_per_job, 7);

    // An empty list turns the check off; a pattern it cannot read is refused.
    assert!(
        db.save_unacceptable_extensions(Vec::new())
            .unwrap()
            .unacceptable_extensions
            .is_empty()
    );
    assert!(db.save_unacceptable_extensions(vec!["a/b".into()]).is_err());
    assert!(
        db.post_processing_settings()
            .unwrap()
            .unacceptable_extensions
            .is_empty()
    );
}

#[test]
fn the_unwanted_extension_action_is_delete_files_until_one_is_saved() {
    let db = Database::open_in_memory().unwrap();
    // Nothing stored at all.
    assert_eq!(
        db.post_processing_settings()
            .unwrap()
            .unwanted_extension_action,
        UnwantedExtensionAction::DeleteFiles
    );
    // Stored before the action existed.
    db.set_setting(
        SETTINGS_KEY,
        r#"{"executionEnabled":false,"concurrency":2,"terminationGraceSeconds":10,"pythonInterpreter":null,"powershellInterpreter":null,"batchInterpreter":null,"unacceptableExtensions":["exe"]}"#,
    )
    .unwrap();
    let settings = db.post_processing_settings().unwrap();
    assert_eq!(
        settings.unwanted_extension_action,
        UnwantedExtensionAction::DeleteFiles
    );
    assert_eq!(settings.unacceptable_extensions, vec!["exe"]);
}

#[test]
fn only_the_delete_action_removes_a_listed_name() {
    let fail = PostProcessingSettings {
        unacceptable_extensions: vec!["exe".into()],
        unwanted_extension_action: UnwantedExtensionAction::FailJob,
        ..PostProcessingSettings::default()
    };
    let delete = PostProcessingSettings {
        unwanted_extension_action: UnwantedExtensionAction::DeleteFiles,
        ..fail.clone()
    };
    assert!(!fail.removes("setup.exe"));
    assert!(delete.removes("dir/SETUP.EXE"));
    assert!(!delete.removes("movie.mkv"));
    let off = PostProcessingSettings {
        unacceptable_extensions: Vec::new(),
        ..delete.clone()
    };
    assert!(!off.removes("setup.exe"));
}

#[test]
fn the_removed_reason_names_each_file_once_in_order() {
    use crate::post_processing::model::unwanted_files_removed_reason;
    assert_eq!(unwanted_files_removed_reason(&[]), None);
    assert_eq!(
        unwanted_files_removed_reason(&[
            "b\\setup.exe".into(),
            "a.scr".into(),
            "b/setup.exe".into(),
        ])
        .as_deref(),
        Some("removed for an unwanted extension: a.scr, b/setup.exe")
    );
    let many: Vec<String> = (0..25).map(|n| format!("f{n:02}.exe")).collect();
    let reason = unwanted_files_removed_reason(&many).unwrap();
    assert!(reason.ends_with("f19.exe, and 5 more"), "{reason}");
}

#[test]
fn saving_the_unwanted_extension_action_keeps_every_other_setting() {
    let db = Database::open_in_memory().unwrap();
    let settings = PostProcessingSettings {
        concurrency: 3,
        unacceptable_extensions: vec!["exe".into()],
        ..PostProcessingSettings::default()
    };
    db.save_post_processing_settings(&settings).unwrap();

    let saved = db
        .save_unwanted_extension_action(UnwantedExtensionAction::FailJob)
        .unwrap();
    assert_eq!(
        saved.unwanted_extension_action,
        UnwantedExtensionAction::FailJob
    );
    let stored = db.post_processing_settings().unwrap();
    assert_eq!(
        stored.unwanted_extension_action,
        UnwantedExtensionAction::FailJob
    );
    assert_eq!(stored.concurrency, 3);
    assert_eq!(stored.unacceptable_extensions, vec!["exe"]);
    assert!(
        db.get_setting(SETTINGS_KEY)
            .unwrap()
            .unwrap()
            .contains(r#""unwantedExtensionAction":"fail_job""#)
    );

    // The script settings save and the list save keep the action.
    db.save_post_processing_settings_preserving_extensions(
        PostProcessingSettings::default(),
        false,
    )
    .unwrap();
    db.save_unacceptable_extensions(vec!["iso".into()]).unwrap();
    assert_eq!(
        db.post_processing_settings()
            .unwrap()
            .unwanted_extension_action,
        UnwantedExtensionAction::FailJob
    );
}

#[test]
fn every_read_of_a_stored_value_that_does_not_parse_gets_the_defaults() {
    let db = Database::open_in_memory().unwrap();
    db.initialize_post_processing_script_directory(
        &std::env::temp_dir(),
        Some(&std::env::temp_dir()),
    )
    .unwrap();
    db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
    assert_eq!(
        db.post_processing_settings().unwrap(),
        PostProcessingSettings::default()
    );
    let (admitted, _) = db.post_processing_script_admission().unwrap();
    assert_eq!(admitted, PostProcessingSettings::default());
    // Reads leave the value as stored.
    assert_eq!(
        db.get_setting(SETTINGS_KEY).unwrap().as_deref(),
        Some("{unreadable")
    );
}

#[test]
fn a_save_replaces_a_stored_value_that_does_not_parse() {
    let db = Database::open_in_memory().unwrap();
    let settings = PostProcessingSettings {
        concurrency: 3,
        ..PostProcessingSettings::default()
    };
    db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
    db.save_post_processing_settings(&settings).unwrap();
    assert_eq!(db.post_processing_settings().unwrap().concurrency, 3);

    // The merging saves start from the defaults in its place.
    db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
    let saved = db.save_unacceptable_extensions(vec!["exe".into()]).unwrap();
    assert_eq!(
        saved,
        PostProcessingSettings {
            unacceptable_extensions: vec!["exe".into()],
            ..PostProcessingSettings::default()
        }
    );
    db.set_setting(SETTINGS_KEY, "{unreadable").unwrap();
    let saved = db
        .save_post_processing_settings_preserving_extensions(settings.clone(), true)
        .unwrap();
    assert_eq!(saved.concurrency, 3);
    assert_eq!(
        saved.unacceptable_extensions,
        PostProcessingSettings::default().unacceptable_extensions
    );
    assert_eq!(db.post_processing_settings().unwrap(), saved);
}
