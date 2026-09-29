#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use weaver_server_core::jobs::record::ActiveJob;
use weaver_server_core::post_processing::directives::{Directive, ScriptOutputEvent};
use weaver_server_core::post_processing::effects::apply_job_directive;
use weaver_server_core::post_processing::events::{EventContext, run_event};
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    PipelineOutcome, PostProcessingSettings, QueueEvent, ScriptEventLabel, ScriptList,
    ScriptListEntry, ScriptLists, ScriptName, ScriptStatus,
};
use weaver_server_core::post_processing::runner::{
    CompatibilityFacts, ExecutionDisposition, ExecutionSpec, InterpreterConfig,
    JobExecutionContext, execute_spec,
};
use weaver_server_core::{Database, JobId};

fn setup() -> (Database, tempfile::TempDir) {
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    db.initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    (db, data)
}

fn script(db: &Database, name: &str, body: &str) -> ScriptName {
    let root = db.post_processing_script_directory().unwrap();
    fs::write(root.join(name), body).unwrap();
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o755)).unwrap();
    ScriptName::new(name).unwrap()
}

fn select(db: &Database, scripts: &[ScriptName]) {
    db.save_post_processing_script_lists(&ScriptLists {
        global: ScriptList::new(scripts.iter().cloned().map(ScriptListEntry::new).collect())
            .unwrap(),
        ..Default::default()
    })
    .unwrap();
}

fn job(db: &Database, directory: &Path) -> JobExecutionContext {
    db.create_active_job(&ActiveJob {
        job_id: JobId(42),
        nzb_hash: [42; 32],
        nzb_path: directory.join("source.nzb"),
        nzb_zstd: Vec::new(),
        output_dir: directory.into(),
        created_at: 1,
        category: None,
        metadata: vec![("seed".into(), "input".into())],
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: Some("private-password".into()),
    })
    .unwrap();
    JobExecutionContext {
        job_id: 42,
        name: "sample".into(),
        nzb_filename: "sample.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: directory.into(),
        final_directory: directory.into(),
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: CompatibilityFacts {
            data_dir: Some(directory.into()),
            complete_dir: Some(directory.into()),
            password: Some("private-password".into()),
            ..Default::default()
        },
    }
}

fn jobless(directory: &Path, event: ScriptEventLabel, env: &[(&str, String)]) -> EventContext {
    EventContext {
        job_id: None,
        event,
        category: None,
        cwd: directory.into(),
        env: env
            .iter()
            .map(|(key, value)| ((*key).into(), value.clone()))
            .collect(),
        facts: Default::default(),
        scripts: None,
    }
}

fn supervisor() -> Option<PathBuf> {
    Some(PathBuf::from(env!("CARGO_BIN_EXE_weaver")))
}

#[tokio::test]
async fn scan_distinguishes_supervisor_launch_failure_from_script_exit_127() {
    let (db, data) = setup();
    let missing_interpreter = script(
        &db,
        "missing.py",
        "### NZBGET SCAN SCRIPT ###\nprint('must not run')\n",
    );
    let mut settings = db.post_processing_settings().unwrap();
    settings.python_interpreter = Some(
        data.path()
            .join("missing-interpreter")
            .to_string_lossy()
            .into_owned(),
    );
    db.save_post_processing_settings(&settings).unwrap();
    select(&db, &[missing_interpreter]);
    let mut event = jobless(data.path(), ScriptEventLabel::Scan, &[]);
    let failed = run_event(&db, &mut event, "scan-launch-failed", None, supervisor())
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].status, ScriptStatus::Failed);
    assert_eq!(failed[0].exit_code, None);
    assert!(
        failed[0]
            .error_message
            .as_deref()
            .unwrap()
            .contains("did not confirm script launch")
    );

    let completed_script = script(
        &db,
        "exit127.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf 'script ran\\n'\nexit 127\n",
    );
    select(&db, &[completed_script]);
    let completed = run_event(&db, &mut event, "scan-exit-127", None, supervisor())
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].status, ScriptStatus::Succeeded);
    assert_eq!(completed[0].exit_code, Some(127));
    assert_eq!(completed[0].output_tail, "script ran\n");
}

#[tokio::test]
async fn queue_parameters_stream_to_persistence_and_the_next_script() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let first = script(
        &db,
        "first.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_First=from-queue\\n' >&2\nexit 17\n",
    );
    let second = script(
        &db,
        "second.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_Second=%s/%s\\n' \"$NZBPR_FIRST\" \"$NZBPR_SEED\"\nexit 0\n",
    );
    select(&db, &[first, second]);
    let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
    let results = run_event(&db, &mut event, "queue-test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == ScriptStatus::Succeeded)
    );
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Second"],
        "from-queue/input"
    );
    assert_eq!(db.event_script_results(42).unwrap().len(), 2);
    assert_eq!(db.job_post_processing_results(42).unwrap().len(), 0);
}

#[tokio::test]
async fn cancellation_keeps_redacted_directives_emitted_before_the_kill() {
    let (db, data) = setup();
    let mut context = job(&db, data.path());
    let name = script(
        &db,
        "stream.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_Token=private-password\\n' >&2\nwhile :; do :; done\n",
    );
    let found = resolve_script(&db.post_processing_script_directory().unwrap(), &name).unwrap();
    let spec = ExecutionSpec {
        manifest: found.manifest,
        root: found.root,
        options: Vec::new(),
        cwd: data.path().into(),
        env: BTreeMap::new(),
        argv: Vec::new(),
        timeout: None,
        termination_grace: Duration::from_millis(1),
        kind: ScriptEventLabel::Queue(QueueEvent::NzbDownloaded),
        run_id: "cancel-test".into(),
        facts: context.compatibility.clone(),
        interpreters: InterpreterConfig::default(),
        supervisor_executable: supervisor(),
        output_ceiling: 1048576,
    };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let (cancel, cancellation) = tokio::sync::watch::channel(false);
    let (result, ()) = tokio::join!(
        execute_spec(spec, Some(cancellation), Some(sender)),
        async {
            let Some(ScriptOutputEvent::Directive(directive)) = receiver.recv().await else {
                panic!("expected streamed directive");
            };
            apply_job_directive(&db, &mut context, directive).unwrap();
            assert_eq!(
                db.job_script_effects(42).unwrap().parameters["Token"],
                "[REDACTED]"
            );
            cancel.send_replace(true);
            while receiver.recv().await.is_some() {}
        }
    );
    assert_eq!(result.unwrap().disposition, ExecutionDisposition::Cancelled);
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Token"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn scan_rewrites_input_and_category_reselects_the_remaining_scripts() {
    let (db, data) = setup();
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "scan.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf rewritten > \"$NZBNP_FILENAME\"\nprintf '[NZB] CATEGORY=movies\\n[NZB] PAUSED=1\\n[NZB] TOP=1\\n'\nexit 17\n",
    );
    let second = script(
        &db,
        "category.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf '[NZB] NZBPR_Category=%s\\n' \"$NZBNP_CATEGORY\"\nexit 0\n",
    );
    let mut lists = ScriptLists {
        global: ScriptList::new(vec![ScriptListEntry::new(first)]).unwrap(),
        ..Default::default()
    };
    lists.categories.insert(
        "movies".into(),
        ScriptList::new(vec![ScriptListEntry::new(second)]).unwrap(),
    );
    db.save_post_processing_script_lists(&lists).unwrap();
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );
    let results = run_event(&db, &mut context, "scan-test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(fs::read_to_string(path).unwrap(), "rewritten");
    assert_eq!(context.category.as_deref(), Some("movies"));
    assert_eq!(context.env["NZBNP_PAUSED"], "1");
    assert_eq!(context.env["NZBNP_TOP"], "1");
    assert!(
        context
            .facts
            .parameters
            .contains(&("Category".into(), "movies".into()))
    );
}

#[tokio::test]
async fn deleted_scan_input_skips_later_scripts() {
    let (db, data) = setup();
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "delete.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nrm \"$NZBNP_FILENAME\"\n",
    );
    let second = script(
        &db,
        "never.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf unexpected > unexpected\n",
    );
    select(&db, &[first, second]);
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );
    assert_eq!(
        run_event(&db, &mut context, "scan-delete", None, supervisor())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(!path.exists());
    assert!(!data.path().join("unexpected").exists());
}

#[tokio::test]
async fn feed_requires_93_and_still_runs_remaining_entries() {
    let (db, data) = setup();
    let path = data.path().join("feed.xml");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "failure.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nexit 0\n",
    );
    let second = script(
        &db,
        "rewrite.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nprintf rewritten > \"$NZBFP_FILENAME\"\nexit 93\n",
    );
    select(&db, &[first, second]);
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Feed(7),
        &[
            ("NZBFP_FILENAME", path.to_string_lossy().into_owned()),
            ("NZBFP_FEEDID", "7".into()),
        ],
    );
    let results = run_event(&db, &mut context, "feed-test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.status)
            .collect::<Vec<_>>(),
        [ScriptStatus::Failed, ScriptStatus::Succeeded]
    );
    assert_eq!(fs::read_to_string(path).unwrap(), "rewritten");
}

#[test]
fn directives_reject_foreign_markers_and_later_user_parameters_win() {
    let (db, data) = setup();
    let mut context = job(&db, data.path());
    let foreign = data.path().join("foreign");
    fs::create_dir(&foreign).unwrap();
    weaver_server_core::jobs::working_dir::mark_weaver_owned_working_dir(
        data.path(),
        &foreign,
        JobId(99),
    )
    .unwrap();
    assert!(
        apply_job_directive(
            &db,
            &mut context,
            Directive::FinalDirectory(foreign.to_string_lossy().into_owned())
        )
        .is_err()
    );
    apply_job_directive(
        &db,
        &mut context,
        Directive::Parameter {
            name: "value".into(),
            value: "script".into(),
        },
    )
    .unwrap();
    db.update_active_job(
        JobId(42),
        &weaver_server_core::JobUpdate {
            metadata: weaver_server_core::FieldUpdate::Set(vec![("value".into(), "user".into())]),
            ..Default::default()
        },
    )
    .unwrap();
    db.job_script_effects(42)
        .unwrap()
        .apply_to_context(&mut context);
    assert_eq!(
        context.compatibility.parameters,
        [("value".into(), "user".into())]
    );
}
