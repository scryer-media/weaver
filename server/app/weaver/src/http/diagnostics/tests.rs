// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[tokio::test]
async fn config_collection_redacts_and_refuses_unparseable_files() {
    let dir = tempfile::tempdir().expect("temp data dir");
    tokio::fs::write(
        dir.path().join("weaver.toml"),
        "data_dir = \"/var/lib/weaver\"\nencryption_key = \"0123456789abcdef\"\n",
    )
    .await
    .expect("write config");
    tokio::fs::write(
        dir.path().join("broken.toml"),
        "api_secret = \"hunter2-real\" = toml\n",
    )
    .await
    .expect("write broken config");
    tokio::fs::write(dir.path().join("notes.txt"), "ignored")
        .await
        .expect("write unrelated file");

    let components = collect_config_files(dir.path()).await;
    let names: Vec<&str> = components
        .iter()
        .map(|component| component.name.as_str())
        .collect();
    // A file is numbered by its sorted place: its own name is the operator's.
    assert_eq!(names, vec!["config/config-1.toml", "config/config-2.toml"]);

    let broken = &components[0];
    let error = broken
        .outcome
        .as_ref()
        .expect_err("an unparseable config is left out");
    // The parse error's kind is kept; the line it failed on is not.
    assert!(error.contains("could not be parsed"), "{error}");
    assert!(!error.contains("hunter2-real"), "{error}");
    assert!(!error.contains("api_secret"), "{error}");

    let good = components[1].outcome.as_ref().expect("redacted config");
    let text = String::from_utf8(good.clone()).expect("utf-8");
    assert!(text.contains("/var/lib/weaver"));
    assert!(!text.contains("0123456789abcdef"));
    assert!(text.contains(redact::REDACTED));
}

#[tokio::test]
async fn file_tail_reads_only_the_last_whole_lines() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("weaver.log");
    let log = b"first line\nSome.Secret.Name.mkv\nlast\n";
    tokio::fs::write(&path, log).await.expect("write log");

    let whole = read_file_tail(&path, 64).await.expect("whole file");
    assert_eq!(whole, log);

    // The last 15 bytes start inside the name; the tail starts after it, so
    // no fragment of it is left for the table to miss.
    let tail = read_file_tail(&path, 15).await.expect("tail");
    assert_eq!(tail, b"last\n");

    let single = dir.path().join("single.log");
    tokio::fs::write(&single, b"Some.Secret.Name.mkv")
        .await
        .expect("write log");
    let tail = read_file_tail(&single, 8).await.expect("tail");
    assert!(tail.is_empty(), "{tail:?}");
}

#[test]
fn every_collected_root_field_is_selectable_within_the_schema_limits() {
    use weaver_server_api::context::{GRAPHQL_MAX_COMPLEXITY, GRAPHQL_MAX_DEPTH};

    let shape = SchemaShape::current().expect("the shipped schema parses");

    // Every field the package reads.
    let fields = GRAPHQL_COMPONENTS
        .iter()
        .flat_map(|group| group.fields.iter().copied());
    for (field, arguments) in fields {
        let query = graphql::root_query(shape, field, arguments)
            .unwrap_or_else(|error| panic!("{field}: {error}"));

        let fields = graphql::field_count(&query);
        assert!(
            fields < GRAPHQL_MAX_COMPLEXITY,
            "{field} resolves {fields} fields, at or past the schema's complexity limit \
                 of {GRAPHQL_MAX_COMPLEXITY}; split it across files"
        );

        let depth = query
            .chars()
            .scan(0usize, |open, character| {
                match character {
                    '{' => *open += 1,
                    '}' => *open = open.saturating_sub(1),
                    _ => {}
                }
                Some(*open)
            })
            .max()
            .unwrap_or(0);
        assert!(
            depth <= GRAPHQL_MAX_DEPTH,
            "{field} nests {depth} deep, past the schema's limit of {GRAPHQL_MAX_DEPTH}"
        );
    }
}

#[test]
fn service_log_payload_flattens_to_plain_lines() {
    let payload = serde_json::json!({
        "serviceLogs": { "lines": ["first line", "second line"], "count": 2 }
    });
    assert_eq!(service_log_text(&payload), "first line\nsecond line\n");
    assert_eq!(service_log_text(&serde_json::json!({})), "");
}

// Unpacks a package into its members, with every gzip member decompressed, so
// a search sees the text a reader would.
fn unpack_package(archive: &[u8]) -> Vec<(String, Vec<u8>)> {
    use std::io::Read;

    let decoded = zstd::decode_all(archive).expect("archive decompresses");
    let mut reader = tar::Archive::new(decoded.as_slice());
    reader
        .entries()
        .expect("entries")
        .map(|entry| {
            let mut entry = entry.expect("entry");
            let name = entry.path().expect("path").display().to_string();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).expect("entry body");
            if name.ends_with(".gz") {
                let mut text = Vec::new();
                flate2::read::MultiGzDecoder::new(data.as_slice())
                    .read_to_end(&mut text)
                    .expect("gzip member decompresses");
                data = text;
            }
            (name, data)
        })
        .collect()
}

// A member of an unpacked package, as text.
fn member_text(members: &[(String, Vec<u8>)], name: &str) -> String {
    let data = &members
        .iter()
        .find(|(member, _)| member == name)
        .unwrap_or_else(|| panic!("{name} is in the package"))
        .1;
    String::from_utf8(data.clone()).expect("utf-8")
}

// Every member of an unpacked package, as one text.
fn package_text(members: &[(String, Vec<u8>)]) -> String {
    let everything: Vec<u8> = members
        .iter()
        .flat_map(|(_, data)| data.iter().copied())
        .collect();
    String::from_utf8_lossy(&everything).into_owned()
}

fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).expect("fixture parses")
}

// An NZB with one file, for seeding a job's persisted NZB.
fn seeded_nzb(title: &str, file: &str) -> Vec<u8> {
    let nzb = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<head><meta type="title">{title}</meta></head>
<file poster="poster@example.org" date="1" subject="&quot;{file}&quot; yEnc (1/1)">
<groups><group>alt.binaries.test</group></groups>
<segments><segment bytes="10" number="1">part1@example.org</segment></segments>
</file>
</nzb>
"#
    );
    weaver_server_core::ingest::compress_nzb_bytes(nzb.as_bytes()).expect("compress nzb")
}

// An active job with a persisted NZB.
fn seeded_job(
    id: u64,
    path: &str,
    nzb_zstd: Vec<u8>,
    output_dir: &str,
    category: Option<&str>,
    metadata: Vec<(String, String)>,
) -> weaver_server_core::jobs::ActiveJob {
    use weaver_server_core::jobs::{ActiveJob, JobId};
    ActiveJob {
        job_id: JobId(id),
        nzb_hash: [u8::try_from(id % 256).expect("byte"); 32],
        nzb_path: PathBuf::from(path),
        nzb_zstd,
        output_dir: PathBuf::from(output_dir),
        created_at: 1,
        category: category.map(str::to_string),
        metadata,
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

// Moves an active job to history.
fn archive_seeded(db: &Database, id: u64, name: &str, metadata: Option<String>) {
    db.archive_job(
        weaver_server_core::jobs::JobId(id),
        &weaver_server_core::JobHistoryRow {
            job_id: id,
            job_hash: Some(vec![u8::try_from(id % 256).expect("byte"); 32]),
            name: name.to_string(),
            status: "completed".to_string(),
            error_message: None,
            total_bytes: 10,
            downloaded_bytes: 10,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: Some(format!("/srv/olaf-nzbs/{name}.nzb")),
            created_at: 1,
            completed_at: 2,
            metadata,
            server_attribution: None,
        },
    )
    .expect("archive job");
}

// Reads the table sources off the runtime, as the handler does.
async fn stored_names(db: &Database) -> Result<sanitize::StoredNames, String> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || sanitize::read_stored_names(&db))
        .await
        .expect("the read runs")
}

// A table read as the endpoint reads it: an error is a failed read.
fn as_read(
    stored: Result<sanitize::StoredNames, String>,
) -> Result<sanitize::StoredNames, bounded::ReadRefused> {
    stored.map_err(bounded::ReadRefused::read_failed)
}

fn plain_local() -> sanitize::LocalNames {
    sanitize::LocalNames {
        data_dir: PathBuf::from("/srv/olaf-data"),
        intermediate_dir: PathBuf::from("/srv/olaf-data/intermediate"),
        complete_dir: PathBuf::from("/srv/olaf-complete"),
        watch_dir: None,
        log_file: None,
        home_dir: None,
        user: None,
        hostname: None,
    }
}

// The real path: every table source read once from a seeded database, the
// configuration read from disk, and the package built with its manifest.
#[tokio::test]
async fn a_package_takes_its_names_from_one_read() {
    use std::io::Write;
    use weaver_server_core::categories::CategoryConfig;
    use weaver_server_core::jobs::JobId;

    const ACTIVE: &str = "Olaf.Active.Show.S01E01";
    const ACTIVE_FILE: &str = "Olaf.Active.Show.S01E01.part01.rar";
    const MEMBER: &str = "Olaf.Active.Show.S01E01.Member.mkv";
    const HISTORY: &str = "Olaf.History.Film.1999";
    const HISTORY_FILE: &str = "Olaf.History.Film.1999.Posted.mkv";
    const HISTORY_ATTRIBUTE: &str = "Olaf Request Tag";
    const CATEGORY: &str = "olaf-category";
    const LOGIN: &str = "olaf-admin";
    const COMPLETE: &str = "/srv/olaf-complete";
    const PROVIDER: &str = "news.olaf-provider.example";

    let db = Database::open_in_memory().expect("database");
    db.create_active_job(&seeded_job(
        17,
        &format!("/srv/olaf-nzbs/{ACTIVE}.nzb"),
        seeded_nzb(ACTIVE, ACTIVE_FILE),
        &format!("{COMPLETE}/{ACTIVE}"),
        Some(CATEGORY),
        vec![("tvdb_id".to_string(), "81189".to_string())],
    ))
    .expect("active job");
    db.complete_file(JobId(17), 1, MEMBER, &[0; 16])
        .expect("member name");
    db.create_active_job(&seeded_job(
        23,
        &format!("/srv/olaf-nzbs/{HISTORY}.nzb"),
        seeded_nzb(HISTORY, HISTORY_FILE),
        &format!("{COMPLETE}/{HISTORY}"),
        None,
        Vec::new(),
    ))
    .expect("history job");
    archive_seeded(
        &db,
        23,
        HISTORY,
        Some(serde_json::to_string(&[("request", HISTORY_ATTRIBUTE)]).expect("metadata")),
    );
    db.insert_category(&CategoryConfig {
        id: 5,
        name: CATEGORY.to_string(),
        dest_dir: Some("/mnt/olaf-films".to_string()),
        aliases: String::new(),
    })
    .expect("category");
    db.set_auth_credentials(LOGIN, "not-a-real-hash")
        .expect("login");
    db.insert_server(&from_json(serde_json::json!({
        "id": 3, "host": PROVIDER, "port": 563, "tls": true,
        "username": null, "password": null, "connections": 8
    })))
    .expect("server");

    let data_dir = tempfile::tempdir().expect("data dir");
    tokio::fs::write(
        data_dir.path().join("weaver.toml"),
        format!("username = \"{LOGIN}\"\n\n[category_rules.{CATEGORY}]\nid = \"81189\"\n"),
    )
    .await
    .expect("write config");

    let mut components = vec![
        Component::bytes(
            "weaver.log",
            format!(
                "INFO {ACTIVE} wrote {ACTIVE_FILE}\n\
                 INFO {LOGIN} signed in on olafhost via {PROVIDER}:563\n"
            )
            .into_bytes(),
        ),
        Component::json(
            "queue.json",
            &serde_json::json!({
                "category": CATEGORY,
                "metadata": [
                    { "key": "tvdb_id", "value": "81189" },
                    { "key": "request", "value": HISTORY_ATTRIBUTE },
                ],
            }),
        ),
    ];
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(
        format!(
            "INFO {HISTORY} wrote {HISTORY_FILE}\n\
             INFO extracted {} to /mnt/olaf-films/x\n",
            MEMBER.to_uppercase(),
        )
        .as_bytes(),
    )
    .expect("gzip");
    components.push(Component::gzip(
        "weaver.log.1.gz",
        gzip.finish().expect("gzip"),
    ));
    components.extend(collect_config_files(data_dir.path()).await);

    let local = sanitize::LocalNames {
        data_dir: data_dir.path().to_path_buf(),
        hostname: Some("olafhost".to_string()),
        ..plain_local()
    };
    let (components, sanitized) =
        sanitize_components(components, as_read(stored_names(&db).await), local).await;
    assert!(sanitized.stand_ins >= 10, "{}", sanitized.stand_ins);
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);

    let everything = package_text(&members).to_lowercase();
    for real in ["olaf", "/srv", "/mnt", "81189"] {
        assert!(
            !everything.contains(real),
            "{real} left the host:\n{everything}"
        );
    }
    let member = |name: &str| member_text(&members, name);
    let log = member("weaver.log");
    assert!(
        log.contains("INFO job-17 wrote job-17/file-1.part01.rar\n"),
        "{log}"
    );
    assert!(
        log.contains("<login> signed in on <hostname> via provider-3\n"),
        "{log}"
    );
    let rotated = member("weaver.log.1.gz");
    assert!(
        rotated.contains("INFO job-23 wrote job-23/file-1.mkv\n"),
        "{rotated}"
    );
    assert!(
        rotated.contains("extracted job-17/file-2.mkv to dir-1/x\n"),
        "{rotated}"
    );
    // A category or metadata value is replaced where it is a whole value.
    let queue = member("queue.json");
    for stand_in in ["category-5", "job-17/meta-1", "job-23/meta-1"] {
        assert!(queue.contains(&format!("\"{stand_in}\"")), "{queue}");
    }

    // The manifest counts the whole history.
    let manifest: archive::Manifest =
        serde_json::from_str(&member(archive::MANIFEST_NAME)).expect("manifest parses");
    assert_eq!(manifest.sanitized.history_jobs_total, Some(1));
}

// The persisted NZBs are read a page at a time, past the first page, for
// the active jobs and the finished ones the read takes.
#[tokio::test]
async fn persisted_nzbs_are_read_a_page_at_a_time() {
    use weaver_server_core::jobs::diagnostic_names::DIAGNOSTIC_NZB_PAGE;

    let db = Database::open_in_memory().expect("database");
    let jobs = u64::try_from(DIAGNOSTIC_NZB_PAGE).expect("page") + 6;
    for id in 1..=jobs {
        db.create_active_job(&seeded_job(
            id,
            &format!("/srv/nzbs/Job.{id}.nzb"),
            seeded_nzb(&format!("Title {id}"), &format!("File.{id}.mkv")),
            &format!("/srv/complete/Job.{id}"),
            None,
            Vec::new(),
        ))
        .expect("job");
    }
    archive_seeded(&db, jobs, &format!("Job.{jobs}"), None);

    let stored = stored_names(&db).await.expect("names read");
    for id in 1..=jobs {
        assert_eq!(
            stored.posted[&id].file_names,
            vec![format!("File.{id}.mkv")],
            "job {id}"
        );
    }
    assert_eq!(stored.history_total, 1);
}

// An NZB the read cannot parse fails the table, naming its job.
#[tokio::test]
async fn an_unreadable_nzb_fails_the_table_and_names_its_job() {
    let db = Database::open_in_memory().expect("database");
    let mut broken = vec![0x28, 0xB5, 0x2F, 0xFD];
    broken.extend_from_slice(b"not a zstd frame at all");
    db.create_active_job(&seeded_job(
        31,
        "/srv/olaf-nzbs/Olaf.Broken.nzb",
        broken,
        "/srv/olaf-complete/Olaf.Broken",
        None,
        Vec::new(),
    ))
    .expect("active job");

    let error = stored_names(&db).await.expect_err("the table fails");
    assert!(error.contains("the NZB of job 31"), "{error}");
}

// A database the table cannot read leaves every component out, and the
// manifest names them through the host's own names: a log named after the
// host leaves as `weaver.log`.
#[tokio::test]
async fn a_database_the_table_cannot_read_leaves_every_component_out() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("weaver.db");
    let db = Database::open(&path).expect("database");
    // Overwritten in place, never truncated: Windows refuses to truncate a
    // file the open database has mapped. The shared-memory index is left
    // whole, since Windows also refuses writes to the bytes SQLite holds
    // locked there; it still points every read into the garbage.
    for entry in std::fs::read_dir(dir.path()).expect("dir") {
        let file = entry.expect("entry").path();
        if file.to_string_lossy().ends_with("-shm") {
            continue;
        }
        let len = std::fs::metadata(&file).expect("size").len();
        let mut handle = std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .expect("open the database file");
        std::io::Write::write_all(&mut handle, &vec![0x5a; len.max(64 * 1024) as usize])
            .expect("corrupt the database");
    }

    let stored = stored_names(&db).await;
    let error = stored.as_ref().expect_err("the read fails");
    assert!(error.contains("the servers"), "{error}");

    let local = sanitize::LocalNames {
        log_file: Some(PathBuf::from("/srv/olafhost/olafhost.log")),
        hostname: Some("olafhost".to_string()),
        ..plain_local()
    };
    let components = vec![Component::bytes(
        "olafhost.log",
        b"INFO Olaf.Secret.Name done\n".to_vec(),
    )];
    let (components, sanitized) = sanitize_components(components, as_read(stored), local).await;
    assert_eq!(sanitized.stand_ins, 0);
    assert_eq!(sanitized.history_jobs_total, None);
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);
    let manifest = member_text(&members, archive::MANIFEST_NAME);
    assert!(!manifest.to_lowercase().contains("olaf"), "{manifest}");
    assert!(manifest.contains("\"weaver.log\""), "{manifest}");
    assert!(manifest.contains("the servers"), "{manifest}");
    assert_eq!(members.len(), 1, "only the manifest is carried");
}

// The names a job's file identities and graph members hold reach the table.
#[tokio::test]
async fn file_identity_and_graph_member_names_go_through_the_filter() {
    use weaver_server_core::jobs::{ActiveFileIdentity, FileIdentitySource, JobId};

    let db = Database::open_in_memory().expect("database");
    db.create_active_job(&seeded_job(
        9,
        "/srv/olaf-nzbs/Olaf.Job.nzb",
        seeded_nzb("Olaf Job", "Olaf.Posted.Name.bin"),
        "/srv/olaf-complete/Olaf.Job",
        None,
        Vec::new(),
    ))
    .expect("job");
    db.save_file_identity(
        JobId(9),
        &ActiveFileIdentity {
            file_index: 0,
            source_filename: "Olaf.Posted.Name.bin".to_string(),
            current_filename: "Olaf.Current.Name.bin".to_string(),
            canonical_filename: Some("Olaf.Canonical.Name.mkv".to_string()),
            classification: None,
            classification_source: FileIdentitySource::Probe,
        },
    )
    .expect("file identity");

    let mut stored = stored_names(&db).await.expect("names read");
    // The engine alone writes graph members; the read's own test covers
    // that table.
    stored
        .jobs
        .get_mut(&9)
        .expect("the job is read")
        .files
        .extend([
            "Olaf.Graph.Member.mkv".to_string(),
            "Olaf.Graph.Output.mkv".to_string(),
        ]);

    let components = vec![Component::bytes(
        "weaver.log",
        b"INFO renamed Olaf.Current.Name.bin to Olaf.Canonical.Name.mkv\n\
          INFO member Olaf.Graph.Member.mkv wrote Olaf.Graph.Output.mkv\n"
            .to_vec(),
    )];
    let (components, _) = sanitize_components(components, Ok(stored), plain_local()).await;
    let log = String::from_utf8(components[0].outcome.clone().expect("sanitized")).expect("utf-8");
    assert_eq!(
        log,
        "INFO renamed job-9/file-2.bin to job-9/file-3.mkv\n\
         INFO member job-9/file-4.mkv wrote job-9/file-5.mkv\n"
    );
}

// A queued job as the queue's shared rows hold it.
fn queued_info(id: u64, status: weaver_server_core::JobStatus) -> weaver_server_core::JobInfo {
    use weaver_server_core::{DownloadState, JobId, JobStatus, PostState, RunState};
    let (download_state, post_state, run_state) = match &status {
        JobStatus::Downloading => (
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Active,
        ),
        JobStatus::Repairing => (
            DownloadState::Complete,
            PostState::Repairing,
            RunState::Active,
        ),
        JobStatus::Extracting => (
            DownloadState::Complete,
            PostState::Extracting,
            RunState::Active,
        ),
        JobStatus::Paused => (DownloadState::Queued, PostState::Idle, RunState::Paused),
        _ => (DownloadState::Queued, PostState::Idle, RunState::Active),
    };
    weaver_server_core::JobInfo {
        job_id: JobId(id),
        job_hash: None,
        name: format!("Synthetic.Job.{id}"),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        status,
        download_state,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state,
        run_state,
        progress: 0.0,
        total_bytes: 10,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 1,
        completed_files: 0,
        remaining_par_files: 0,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 10,
        wanted_bytes: 10,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        created_at_epoch_ms: 0.0,
        server_attribution: Vec::new(),
    }
}

// A handle over a queue holding `jobs`, with no pipeline behind it: the
// reports must be made without one answering.
fn queue_handle(jobs: Vec<weaver_server_core::JobInfo>) -> SchedulerHandle {
    use weaver_server_core::{PipelineMetrics, SharedPipelineState};
    let (event_tx, _) = tokio::sync::broadcast::channel(4);
    let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
    state.publish_jobs(jobs);
    SchedulerHandle::new(event_tx, state)
}

// Seeds an active job row whose stored NZB names the synthetic fixture.
fn seed_report_job(db: &Database, id: u64) {
    db.create_active_job(&seeded_job(
        id,
        &format!("/srv/synthetic-nzbs/Synthetic.Job.{id}.nzb"),
        seeded_nzb(
            &format!("Synthetic Job {id}"),
            &format!("Synthetic.File.{id}.mkv"),
        ),
        &format!("/srv/synthetic-complete/Synthetic.Job.{id}"),
        None,
        Vec::new(),
    ))
    .expect("active job");
}

const REPORT_NOW: u64 = 1_767_225_600;

// Far past any real run: the budget is not what these tests exercise.
const UNBOUNDED: Duration = Duration::from_secs(365 * 24 * 3_600);

fn nzb_reports_json(component: &Component) -> serde_json::Value {
    assert_eq!(component.name, nzb_reports::FILE);
    let data = component.outcome.as_ref().expect("the reports are written");
    serde_json::from_slice(data).expect("the reports are JSON")
}

fn entry_ids(reports: &serde_json::Value) -> Vec<u64> {
    reports["jobs"]
        .as_array()
        .expect("jobs")
        .iter()
        .map(|entry| entry["job_id"].as_u64().expect("job id"))
        .collect()
}

// Only jobs Weaver has started work on get a report, post-processing first,
// each keyed by the id queue.json lists it under, and each report is the
// support report the report code makes for that job.
#[tokio::test]
async fn nzb_reports_cover_exactly_the_active_jobs() {
    use weaver_server_core::JobStatus;
    use weaver_server_core::support::{ReportEnvironment, build_job_support_report};

    let db = Database::open_in_memory().expect("database");
    for id in 1..=7 {
        seed_report_job(&db, id);
    }
    let jobs = vec![
        queued_info(1, JobStatus::Queued),
        queued_info(2, JobStatus::Downloading),
        queued_info(3, JobStatus::Paused),
        queued_info(4, JobStatus::Repairing),
        queued_info(5, JobStatus::Queued),
        queued_info(6, JobStatus::Paused),
        queued_info(7, JobStatus::AwaitingQueueScripts),
    ];
    let handle = queue_handle(jobs.clone());

    let component =
        nzb_reports::collect_nzb_reports_within(&db, &handle, 25, UNBOUNDED, REPORT_NOW).await;
    let reports = nzb_reports_json(&component);
    assert_eq!(entry_ids(&reports), vec![4, 2], "{reports:#}");
    assert_eq!(reports["qualified"], 2);
    assert_eq!(reports["written"], 2);
    assert_eq!(reports["failed"], 0);
    assert!(reports["cut_short"].is_null());

    let environment = ReportEnvironment::current(&db, &handle);
    for (entry, (id, group, status)) in reports["jobs"].as_array().expect("jobs").iter().zip([
        (4, "post_processing", "repairing"),
        (2, "downloading", "downloading"),
    ]) {
        assert_eq!(entry["job"], format!("job-{id}"));
        assert_eq!(entry["group"], group);
        assert_eq!(entry["status"], status);
        let info = jobs[usize::try_from(id - 1).expect("index")].clone();
        let expected = build_job_support_report(&db, Some(info), id, &environment, REPORT_NOW)
            .expect("the report code reports the job");
        let expected: serde_json::Value =
            serde_json::from_str(&expected.json).expect("report JSON");
        assert_eq!(entry["report"], expected, "job {id}");
    }
}

// The cap keeps the first reports in report order and the file says how many
// qualified, so a cut set is visible.
#[tokio::test]
async fn nzb_reports_stop_at_the_cap_and_count_what_qualified() {
    use weaver_server_core::JobStatus;

    let db = Database::open_in_memory().expect("database");
    for id in 1..=6 {
        seed_report_job(&db, id);
    }
    let handle = queue_handle(vec![
        queued_info(1, JobStatus::Downloading),
        queued_info(2, JobStatus::Downloading),
        queued_info(3, JobStatus::Extracting),
        queued_info(4, JobStatus::Downloading),
        queued_info(5, JobStatus::Repairing),
        queued_info(6, JobStatus::Queued),
    ]);

    let component =
        nzb_reports::collect_nzb_reports_within(&db, &handle, 3, UNBOUNDED, REPORT_NOW).await;
    let reports = nzb_reports_json(&component);
    assert_eq!(entry_ids(&reports), vec![3, 5, 1], "{reports:#}");
    assert_eq!(reports["qualified"], 5);
    assert_eq!(reports["limit"], 3);
    assert_eq!(reports["written"], 3);
}

// A job whose NZB cannot be read gets an entry with a stable reason, the
// other jobs keep their reports, and the package still builds, even though
// the stand-in table refuses the same NZB.
#[tokio::test]
async fn an_unreadable_nzb_gets_an_entry_and_the_package_still_builds() {
    use weaver_server_core::JobStatus;

    let db = Database::open_in_memory().expect("database");
    seed_report_job(&db, 1);
    let mut broken = vec![0x28, 0xB5, 0x2F, 0xFD];
    broken.extend_from_slice(b"not a zstd frame at all");
    db.create_active_job(&seeded_job(
        2,
        "/srv/synthetic-nzbs/Synthetic.Broken.nzb",
        broken,
        "/srv/synthetic-complete/Synthetic.Broken",
        None,
        Vec::new(),
    ))
    .expect("broken job");
    let handle = queue_handle(vec![
        queued_info(1, JobStatus::Downloading),
        queued_info(2, JobStatus::Repairing),
    ]);

    let component =
        nzb_reports::collect_nzb_reports_within(&db, &handle, 25, UNBOUNDED, REPORT_NOW).await;
    let reports = nzb_reports_json(&component);
    assert_eq!(entry_ids(&reports), vec![2, 1], "{reports:#}");
    assert_eq!(reports["written"], 2);
    assert_eq!(reports["failed"], 1);
    let broken = &reports["jobs"][0];
    assert_eq!(broken["error"]["reason"], "nzb_unreadable", "{broken:#}");
    assert!(broken.get("report").is_none(), "{broken:#}");
    assert!(reports["jobs"][1]["report"].is_object());

    let archive = archive::build_archive(
        vec![component],
        "0.0.0-test",
        "2000-01-01T00:00:00Z",
        archive::ManifestSanitized {
            stand_ins: 0,
            history_jobs_total: None,
            note: String::new(),
        },
    )
    .expect("archive builds");
    let members = unpack_package(&archive);
    let manifest: archive::Manifest =
        serde_json::from_str(&member_text(&members, archive::MANIFEST_NAME))
            .expect("manifest parses");
    assert!(manifest.errors.is_empty(), "{:?}", manifest.errors);
    assert!(
        manifest
            .files
            .iter()
            .any(|file| file.name == nzb_reports::FILE)
    );
}

// A spent budget writes what was made and says the set was cut short.
#[tokio::test]
async fn a_spent_budget_writes_what_was_made_and_says_so() {
    use weaver_server_core::JobStatus;

    let db = Database::open_in_memory().expect("database");
    seed_report_job(&db, 1);
    let handle = queue_handle(vec![queued_info(1, JobStatus::Downloading)]);

    let component =
        nzb_reports::collect_nzb_reports_within(&db, &handle, 25, Duration::ZERO, REPORT_NOW).await;
    let reports = nzb_reports_json(&component);
    assert_eq!(reports["qualified"], 1);
    assert_eq!(reports["written"], 0);
    assert_eq!(reports["cut_short"], "time_budget_spent");
}

fn package_member_json(members: &[(String, Vec<u8>)], name: &str) -> serde_json::Value {
    serde_json::from_str(&member_text(members, name)).expect("member is JSON")
}

// Archives `components` as the endpoint does, through the stand-in pass.
async fn archive_through_the_pass(components: Vec<Component>) -> Vec<(String, Vec<u8>)> {
    let db = Database::open_in_memory().expect("database");
    let (components, sanitized) =
        sanitize_components(components, as_read(stored_names(&db).await), plain_local()).await;
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    unpack_package(&archive)
}

// A pipeline that does not answer still leaves a pipeline-internals.json:
// it says the pipeline did not answer, what the turn in progress is and how
// long it has been busy by the clock the reader is given, and the counts the
// shared state holds. The manifest still names the full snapshot as missing.
#[tokio::test]
async fn an_unanswered_pipeline_leaves_what_could_be_read_without_it() {
    use weaver_server_core::operations::instrumentation::PipelinePhase;
    use weaver_server_core::{JobStatus, PipelineMetrics, SharedPipelineState};

    let metrics = PipelineMetrics::new();
    let stamp = &metrics.pipeline_turn;
    let origin = stamp.origin();
    stamp.ended(origin + Duration::from_millis(400));
    stamp.end_of_turn(5);
    stamp.ended(origin + Duration::from_millis(900));
    stamp.busy(PipelinePhase::DecodeDone, origin + Duration::from_secs(2));
    metrics
        .repair_active
        .store(3, std::sync::atomic::Ordering::Relaxed);
    let (event_tx, _) = tokio::sync::broadcast::channel(4);
    let state = SharedPipelineState::new(metrics.clone(), Vec::new());
    state.publish_jobs(vec![
        queued_info(1, JobStatus::Queued),
        queued_info(2, JobStatus::Queued),
        queued_info(3, JobStatus::Repairing),
    ]);
    // No pipeline runs behind this handle, so the request is never answered.
    let handle = SchedulerHandle::new(event_tx, state);

    let components =
        pipeline_internals::collect_pipeline_internals_within(&handle, Duration::ZERO, || {
            origin + Duration::from_secs(65)
        })
        .await;
    let members = archive_through_the_pass(components).await;

    let internals = package_member_json(&members, pipeline_internals::FILE);
    assert_eq!(internals["answered"], false, "{internals:#}");
    assert_eq!(internals["reason"], "timed_out");
    assert_eq!(internals["timeout_ms"], 0);
    let turn = &internals["turn"];
    assert_eq!(turn["phase"], "decode_done", "{internals:#}");
    assert_eq!(turn["in_progress"], true);
    assert_eq!(turn["busy_ms"], 63_000);
    assert_eq!(turn["turns_completed"], 2);
    assert_eq!(turn["since_last_turn_ended_ms"], 64_100);
    assert_eq!(turn["installed_jobs"], 5);
    let queue = &internals["queue"];
    assert_eq!(queue["jobs"], 3);
    assert_eq!(queue["jobs_by_status"]["queued"], 2);
    assert_eq!(queue["jobs_by_status"]["repairing"], 1);
    assert_eq!(queue["jobs_by_status"]["downloading"], 0);
    assert_eq!(queue["queue_window"], 100);
    assert_eq!(internals["work"]["jobs_repairing"], 3);

    let manifest: archive::Manifest =
        serde_json::from_str(&member_text(&members, archive::MANIFEST_NAME))
            .expect("manifest parses");
    assert!(
        manifest
            .files
            .iter()
            .any(|file| file.name == pipeline_internals::FILE),
        "{:?}",
        manifest.files
    );
    assert_eq!(
        manifest
            .errors
            .iter()
            .filter(|error| error.name == pipeline_internals::FILE)
            .count(),
        1,
        "{:?}",
        manifest.errors
    );
}

// A pipeline that answers is written as it answered: one component, the
// pipeline's own snapshot, and nothing in the manifest's errors.
#[tokio::test]
async fn an_answering_pipeline_writes_its_own_snapshot() {
    let root = tempfile::tempdir().expect("temporary directory");
    let config: SharedConfig = Arc::new(tokio::sync::RwLock::new(
        weaver_server_core::settings::model::Config {
            data_dir: root.path().to_string_lossy().into_owned(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        },
    ));
    let (mut pipeline, handle, _) = weaver_server_core::pipeline::test_support::idle_pipeline(
        Database::open_in_memory().expect("database"),
        config,
    )
    .await;
    let task = tokio::spawn(async move { pipeline.run().await });

    let components = pipeline_internals::collect_pipeline_internals_within(
        &handle,
        UNBOUNDED,
        std::time::Instant::now,
    )
    .await;
    assert_eq!(components.len(), 1);
    let component = &components[0];
    assert_eq!(component.name, pipeline_internals::FILE);
    assert_eq!(component.form, archive::Form::Json);
    let written = component.outcome.as_ref().expect("the snapshot is written");
    let snapshot: weaver_server_core::pipeline::diagnostics::PipelineDiagnostics =
        serde_json::from_slice(written).expect("the pipeline's own snapshot");
    // Byte for byte what the snapshot serializes to.
    assert_eq!(
        written,
        &serde_json::to_vec_pretty(&snapshot).expect("snapshot serializes")
    );

    handle.shutdown().await.expect("shutdown");
    task.await.expect("pipeline stops");
}

// Errors in `database.json` as (value, reason) pairs.
fn database_value_errors(database: &serde_json::Value) -> Vec<(String, String)> {
    database["errors"]
        .as_array()
        .expect("errors list")
        .iter()
        .map(|error| {
            (
                error["value"].as_str().expect("value name").to_string(),
                error["reason"].as_str().expect("reason code").to_string(),
            )
        })
        .collect()
}

fn database_manifest_errors(members: &[(String, Vec<u8>)]) -> usize {
    let manifest: archive::Manifest =
        serde_json::from_str(&member_text(members, archive::MANIFEST_NAME))
            .expect("manifest parses");
    assert!(
        manifest
            .files
            .iter()
            .any(|file| file.name == database_facts::FILE),
        "{:?}",
        manifest.files
    );
    manifest
        .errors
        .iter()
        .filter(|error| error.name == database_facts::FILE)
        .count()
}

fn synthetic_database_sizes() -> weaver_server_core::persistence::connection::SqliteFileSizes {
    use weaver_server_core::persistence::connection::{SqliteFileSize, SqliteFileSizes};
    SqliteFileSizes {
        database_file: SqliteFileSize::Present(8_192_000),
        wal_file: SqliteFileSize::Absent,
        shm_file: SqliteFileSize::Unreadable("permission denied".to_string()),
    }
}

// The database's storage facts are written as fields. A value that could not
// be read is null in the file, named with a reason code in its `errors`, and
// an error entry in the manifest; the file itself still goes into the
// package through the stand-in pass.
#[tokio::test]
async fn database_facts_keep_what_was_read_and_name_what_was_not() {
    use weaver_server_core::persistence::connection::SqlitePageFacts;

    let pages = database_facts::PageOutcome::Read(SqlitePageFacts {
        page_size: Ok(4096),
        page_count: Err("database is locked".to_string()),
        freelist_count: Ok(12),
        journal_mode: Ok("wal".to_string()),
    });
    let members = archive_through_the_pass(database_facts::database_components(
        &synthetic_database_sizes(),
        &pages,
    ))
    .await;

    let database = package_member_json(&members, database_facts::FILE);
    assert_eq!(database["engine"], "sqlite");
    assert_eq!(database["files"]["database"]["state"], "present");
    assert_eq!(database["files"]["database"]["bytes"], 8_192_000);
    assert_eq!(database["files"]["wal"]["state"], "absent");
    assert!(database["files"]["wal"]["bytes"].is_null());
    assert_eq!(database["files"]["shm"]["state"], "unreadable");
    assert_eq!(database["page_size"], 4096);
    assert!(database["page_count"].is_null());
    assert_eq!(database["freelist_count"], 12);
    assert_eq!(database["journal_mode"], "wal");
    assert_eq!(
        database_value_errors(&database),
        [
            (
                "shm_bytes".to_string(),
                database_facts::REASON_UNREADABLE.to_string()
            ),
            (
                "page_count".to_string(),
                bounded::REASON_READ_FAILED.to_string()
            ),
        ]
    );
    assert_eq!(database_manifest_errors(&members), 2);
}

// A database whose read executor does not answer still leaves its file
// sizes: the page read is abandoned at the wait, its four values are null
// with `timed_out`, and the package is built.
#[tokio::test]
async fn a_stalled_database_still_leaves_its_file_sizes() {
    let never_answers = std::future::pending();
    let components =
        database_facts::collect_within(synthetic_database_sizes(), never_answers, Duration::ZERO)
            .await;
    let members = archive_through_the_pass(components).await;

    let database = package_member_json(&members, database_facts::FILE);
    assert_eq!(database["files"]["database"]["bytes"], 8_192_000);
    assert_eq!(database["files"]["wal"]["state"], "absent");
    for value in ["page_size", "page_count", "freelist_count", "journal_mode"] {
        assert!(database[value].is_null(), "{value}");
    }
    let timed_out = bounded::REASON_TIMED_OUT.to_string();
    assert_eq!(
        database_value_errors(&database),
        [
            (
                "shm_bytes".to_string(),
                database_facts::REASON_UNREADABLE.to_string()
            ),
            ("page_size".to_string(), timed_out.clone()),
            ("page_count".to_string(), timed_out.clone()),
            ("freelist_count".to_string(), timed_out.clone()),
            ("journal_mode".to_string(), timed_out),
        ]
    );
    assert_eq!(database_manifest_errors(&members), 5);
}

fn manifest_of(members: &[(String, Vec<u8>)]) -> archive::Manifest {
    serde_json::from_str(&member_text(members, archive::MANIFEST_NAME)).expect("manifest parses")
}

// GraphQL fields run together until one deadline. A field that does not
// answer by then is named in its file, and its file is recorded in the
// manifest as `timed_out`; a file with nothing that answered is left out
// with that reason; fields that answered are written as before.
#[tokio::test]
async fn graphql_fields_that_do_not_answer_in_time_are_recorded_as_timed_out() {
    const GROUPS: &[GraphqlComponent] = &[
        GraphqlComponent {
            file: "answered.json",
            fields: &[("answers", None)],
            redact: true,
        },
        GraphqlComponent {
            file: "partial.json",
            fields: &[("answers", None), ("stalls", None)],
            redact: true,
        },
        GraphqlComponent {
            file: "stalled.json",
            fields: &[("stalls", None)],
            redact: true,
        },
        GraphqlComponent {
            file: SERVICE_LOG_FILE,
            fields: &[("stalls", None)],
            redact: false,
        },
    ];
    // The deadline has already passed: what answers at once is kept, and
    // what waits is given up on without the test waiting for anything.
    let components = collect_graphql_within(
        GROUPS,
        tokio::time::Instant::now(),
        |field: &'static str, _arguments| async move {
            if field == "stalls" {
                std::future::pending::<()>().await;
            }
            Ok((serde_json::json!({ "count": 7 }), Vec::new()))
        },
    )
    .await;
    let members = archive_through_the_pass(components).await;

    let answered = package_member_json(&members, "answered.json");
    assert_eq!(answered, serde_json::json!({ "answers": { "count": 7 } }));
    let partial = package_member_json(&members, "partial.json");
    assert_eq!(partial["data"]["answers"]["count"], 7);
    assert_eq!(partial["timed_out"], serde_json::json!(["stalls"]));

    let manifest = manifest_of(&members);
    let reason_of = |name: &str| -> Vec<Option<String>> {
        manifest
            .errors
            .iter()
            .filter(|error| error.name == name)
            .map(|error| error.reason.clone())
            .collect()
    };
    let timed_out = Some(bounded::REASON_TIMED_OUT.to_string());
    assert!(reason_of("answered.json").is_empty());
    assert_eq!(reason_of("partial.json"), std::slice::from_ref(&timed_out));
    assert_eq!(reason_of("stalled.json"), std::slice::from_ref(&timed_out));
    assert_eq!(reason_of(SERVICE_LOG_FILE), [timed_out]);
    for name in ["stalled.json", SERVICE_LOG_FILE] {
        assert!(
            !manifest.files.iter().any(|file| file.name == name),
            "{name} has no file"
        );
    }
}

// A stand-in table that cannot be read in time is treated as one that cannot
// be read at all: every component is left out, none ships with less
// sanitizing, each carries `timed_out`, and the package is still built.
#[tokio::test]
async fn a_stand_in_table_that_does_not_answer_in_time_leaves_every_component_out() {
    let stored = bounded::within(tokio::time::Instant::now(), std::future::pending()).await;
    let components = vec![
        Component::bytes("weaver.log", b"INFO Olaf.Secret.Name done\n".to_vec()),
        Component::json(
            "queue.json",
            &serde_json::json!({ "name": "Olaf.Secret.Name" }),
        ),
    ];
    let (components, sanitized) = sanitize_components(components, stored, plain_local()).await;
    assert_eq!(sanitized.stand_ins, 0);
    assert_eq!(sanitized.history_jobs_total, None);
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);

    let manifest = manifest_of(&members);
    assert!(
        manifest
            .files
            .iter()
            .all(|file| file.name == archive::MANIFEST_NAME),
        "{:?}",
        manifest.files
    );
    let mut left_out: Vec<&str> = manifest
        .errors
        .iter()
        .map(|error| error.name.as_str())
        .collect();
    left_out.sort_unstable();
    assert_eq!(left_out, ["queue.json", "stand-in-table", "weaver.log"]);
    assert!(
        manifest
            .errors
            .iter()
            .all(|error| error.reason.as_deref() == Some(bounded::REASON_TIMED_OUT)),
        "{:?}",
        manifest.errors
    );
    for (_, data) in &members {
        assert!(!String::from_utf8_lossy(data).contains("Olaf"));
    }
}

// Archives `components` as the endpoint does when the stand-in table did not
// answer in time.
async fn archive_without_the_table(components: Vec<Component>) -> Vec<(String, Vec<u8>)> {
    let (components, sanitized) = sanitize_components(
        components,
        Err(bounded::ReadRefused::timed_out()),
        plain_local(),
    )
    .await;
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    unpack_package(&archive)
}

const PLANTED_NAME: &str = "Olaf.Secret.Name";

fn named_components() -> Vec<Component> {
    vec![
        Component::bytes(
            "weaver.log",
            format!("INFO {PLANTED_NAME} done\n").into_bytes(),
        ),
        Component::json("queue.json", &serde_json::json!({ "name": PLANTED_NAME })),
    ]
}

fn assert_left_out_with_their_reasons(
    members: &[(String, Vec<u8>)],
    shipped: &str,
    own_reasons: &[&str],
) {
    let manifest = manifest_of(members);
    let mut files: Vec<&str> = manifest
        .files
        .iter()
        .map(|file| file.name.as_str())
        .collect();
    files.sort_unstable();
    let mut expected = [archive::MANIFEST_NAME, shipped];
    expected.sort_unstable();
    assert_eq!(files, expected);
    let timed_out = Some(bounded::REASON_TIMED_OUT.to_string());
    for name in ["queue.json", "weaver.log", "stand-in-table"] {
        let entries: Vec<_> = manifest
            .errors
            .iter()
            .filter(|error| error.name == name)
            .collect();
        assert_eq!(entries.len(), 1, "{name}: {:?}", manifest.errors);
        assert_eq!(entries[0].reason, timed_out, "{name}");
    }
    let mut shipped_reasons: Vec<String> = manifest
        .errors
        .iter()
        .filter(|error| error.name == shipped)
        .map(|error| error.reason.clone().expect("a reason code"))
        .collect();
    shipped_reasons.sort_unstable();
    let mut own_reasons: Vec<String> = own_reasons
        .iter()
        .map(|reason| reason.to_string())
        .collect();
    own_reasons.sort_unstable();
    assert_eq!(shipped_reasons, own_reasons);
    for (member, data) in members {
        assert!(
            !String::from_utf8_lossy(data).contains(PLANTED_NAME),
            "{member} carries the planted name"
        );
    }
}

// With no stand-in table, `database.json` still ships with its numbers and
// codes; the OS and database error text, which can hold a path, is replaced
// by a fixed sentence per reason; every other component is left out with
// `timed_out`.
#[tokio::test]
async fn database_facts_ship_without_the_table_and_drop_error_text() {
    use weaver_server_core::persistence::connection::{
        SqliteFileSize, SqliteFileSizes, SqlitePageFacts,
    };

    let sizes = SqliteFileSizes {
        database_file: SqliteFileSize::Present(8_192_000),
        wal_file: SqliteFileSize::Present(4_096),
        shm_file: SqliteFileSize::Unreadable(format!(
            "/srv/{PLANTED_NAME}/weaver.db-shm: permission denied"
        )),
    };
    let pages = database_facts::PageOutcome::Read(SqlitePageFacts {
        page_size: Ok(4096),
        page_count: Err(format!("unable to open /srv/{PLANTED_NAME}/weaver.db")),
        freelist_count: Ok(12),
        journal_mode: Ok("wal".to_string()),
    });
    let mut components = named_components();
    components.extend(database_facts::database_components(&sizes, &pages));
    let members = archive_without_the_table(components).await;

    let database = package_member_json(&members, database_facts::FILE);
    assert_eq!(database["files"]["database"]["bytes"], 8_192_000);
    assert_eq!(database["files"]["wal"]["bytes"], 4_096);
    assert_eq!(database["files"]["shm"]["state"], "unreadable");
    assert_eq!(database["page_size"], 4096);
    assert!(database["page_count"].is_null());
    assert_eq!(database["freelist_count"], 12);
    assert_eq!(database["journal_mode"], "wal");
    assert_eq!(
        database_value_errors(&database),
        [
            (
                "shm_bytes".to_string(),
                database_facts::REASON_UNREADABLE.to_string()
            ),
            (
                "page_count".to_string(),
                bounded::REASON_READ_FAILED.to_string()
            ),
        ]
    );
    assert_left_out_with_their_reasons(
        &members,
        database_facts::FILE,
        &[
            database_facts::REASON_UNREADABLE,
            bounded::REASON_READ_FAILED,
        ],
    );
}

// A journal mode SQLite does not define is database text, so without the
// table it is not shipped; it is null and named as unread in the file.
#[tokio::test]
async fn an_unknown_journal_mode_does_not_ship_without_the_table() {
    use weaver_server_core::persistence::connection::SqlitePageFacts;

    let pages = database_facts::PageOutcome::Read(SqlitePageFacts {
        page_size: Ok(4096),
        page_count: Ok(2000),
        freelist_count: Ok(0),
        journal_mode: Ok(PLANTED_NAME.to_string()),
    });
    let mut components = named_components();
    components.extend(database_facts::database_components(
        &synthetic_database_sizes(),
        &pages,
    ));
    let members = archive_without_the_table(components).await;

    let database = package_member_json(&members, database_facts::FILE);
    assert!(database["journal_mode"].is_null());
    assert_eq!(database["page_count"], 2000);
    // Named in the file's own errors; the manifest keeps the entries the
    // file has with the table.
    assert_eq!(
        database_value_errors(&database),
        [
            (
                "shm_bytes".to_string(),
                database_facts::REASON_UNREADABLE.to_string()
            ),
            (
                "journal_mode".to_string(),
                bounded::REASON_READ_FAILED.to_string()
            ),
        ]
    );
    assert_left_out_with_their_reasons(
        &members,
        database_facts::FILE,
        &[database_facts::REASON_UNREADABLE],
    );
}

// With no stand-in table, the unanswered pipeline fallback still ships as it
// was written, its manifest entry says only what its code says, and every
// other component is left out.
#[tokio::test]
async fn the_unanswered_pipeline_fallback_ships_without_the_table() {
    use weaver_server_core::{JobStatus, PipelineMetrics, SharedPipelineState};

    let metrics = PipelineMetrics::new();
    let origin = metrics.pipeline_turn.origin();
    let (event_tx, _) = tokio::sync::broadcast::channel(4);
    let state = SharedPipelineState::new(metrics.clone(), Vec::new());
    state.publish_jobs(vec![
        queued_info(1, JobStatus::Queued),
        queued_info(2, JobStatus::Repairing),
    ]);
    // No pipeline runs behind this handle, so the request is never answered.
    let handle = SchedulerHandle::new(event_tx, state);
    let fallback =
        pipeline_internals::collect_pipeline_internals_within(&handle, Duration::ZERO, || {
            origin + Duration::from_secs(5)
        })
        .await;
    let written = fallback
        .iter()
        .find_map(|component| component.outcome.as_ref().ok())
        .expect("the fallback is written")
        .clone();
    let mut components = named_components();
    components.extend(fallback);
    let members = archive_without_the_table(components).await;

    assert_eq!(
        member_text(&members, pipeline_internals::FILE).as_bytes(),
        written
    );
    let internals = package_member_json(&members, pipeline_internals::FILE);
    assert_eq!(internals["answered"], false);
    assert_eq!(internals["queue"]["jobs"], 2);
    assert_left_out_with_their_reasons(
        &members,
        pipeline_internals::FILE,
        &[bounded::REASON_TIMED_OUT],
    );
}

// With a table that builds, marking `database.json` name-free changes
// nothing: it leaves the pass byte for byte as it did unmarked.
#[tokio::test]
async fn a_healthy_table_leaves_database_facts_unchanged() {
    use weaver_server_core::persistence::connection::SqlitePageFacts;

    let pages = || {
        database_facts::PageOutcome::Read(SqlitePageFacts {
            page_size: Ok(4096),
            page_count: Err("database is locked".to_string()),
            freelist_count: Ok(12),
            journal_mode: Ok("wal".to_string()),
        })
    };
    let marked = database_facts::database_components(&synthetic_database_sizes(), &pages());
    assert!(
        marked
            .iter()
            .all(|component| component.without_table.is_some())
    );
    let unmarked: Vec<Component> =
        database_facts::database_components(&synthetic_database_sizes(), &pages())
            .into_iter()
            .map(|component| Component {
                without_table: None,
                ..component
            })
            .collect();
    let marked = archive_through_the_pass(marked).await;
    let unmarked = archive_through_the_pass(unmarked).await;
    assert_eq!(marked, unmarked);
    assert!(member_text(&marked, database_facts::FILE).contains("permission denied"));
}
