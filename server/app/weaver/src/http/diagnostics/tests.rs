// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_core::auth::repository::AuthRepositoryQueries;
use weaver_server_core::categories::persistence::CategoriesPersistenceQueries;
use weaver_server_core::categories::queries::CategoriesQueries;
use weaver_server_core::jobs::diagnostic_names::JobsDiagnosticNamesDiagnosticRecentHistoryIdsQueries;
use weaver_server_core::jobs::diagnostic_names::reference::JobsDiagnosticNamesDiagnosticJobNamesWholeQueries;
use weaver_server_core::jobs::persistence::JobsPersistenceQueries;
use weaver_server_core::jobs::repository::JobsRepositoryQueries;
use weaver_server_core::post_processing::instances::PostProcessingInstancesQueries;
use weaver_server_core::proxies::ProxiesDnsPersistenceQueries;
use weaver_server_core::proxies::ProxiesNetworkPersistenceQueries;
use weaver_server_core::proxies::ProxiesPersistenceQueries;
use weaver_server_core::rss::RssQueries;
use weaver_server_core::servers::persistence::ServersPersistenceQueries;
use weaver_server_core::servers::queries::ServersQueries;
use weaver_server_core::settings::SettingsSchedulesQueries;

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
            protocol: Default::default(),
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
            archived_at: None,
        },
    )
    .expect("archive job");
}

// Runs `read` over `db` off the runtime to its end, and takes what it read.
async fn run_read(db: &Database, read: stored_read::NameRead) -> stored_read::ReadState {
    let db = db.clone();
    let read = Arc::new(read);
    let reader = read.clone();
    tokio::task::spawn_blocking(move || stored_read::read_stored_names(&db, &reader))
        .await
        .expect("the read runs");
    read.finish()
}

// Reads the table sources off the runtime, as the handler does, to the end,
// with nothing in memory.
async fn stored_names(db: &Database) -> stored_read::TableNames {
    stored_read::TableNames::from_read(
        run_read(db, stored_read::NameRead::new()).await,
        stored_read::MemoryNames::default(),
    )
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
        volumes: Vec::new(),
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
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
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
                "downloadWaitReason": format!("login_refused:{PROVIDER}:563"),
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
    let names = stored_names(&db).await;
    assert!(names.manifest.complete, "{:?}", names.manifest);
    assert!(names.guard.is_none());
    let (components, sanitized) = sanitize_components(components, names, local).await;
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
        log.contains("INFO job-17 wrote job-17.part01.rar\n"),
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
        rotated.contains("extracted job-17/file-1.mkv to dir-1/name-1\n"),
        "{rotated}"
    );
    // A category or metadata value is replaced where it is a whole value.
    let queue = member("queue.json");
    for stand_in in ["category-5", "job-17/meta-1", "job-23/meta-1"] {
        assert!(queue.contains(&format!("\"{stand_in}\"")), "{queue}");
    }
    // A job waiting on a server refusing the login names the server by its
    // stand-in.
    assert!(queue.contains("\"login_refused:provider-3\""), "{queue}");

    // The manifest counts the whole history.
    let manifest: archive::Manifest =
        serde_json::from_str(&member(archive::MANIFEST_NAME)).expect("manifest parses");
    assert_eq!(manifest.sanitized.history_jobs_total, Some(1));
}

// The persisted NZBs are read in batches, past the first, for the active
// jobs and the finished ones the read takes.
#[tokio::test]
async fn persisted_nzbs_are_read_in_batches() {
    let db = Database::open_in_memory().expect("database");
    let jobs = u64::try_from(stored_read::NZB_BATCH * 2).expect("batch") + 6;
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

    let names = stored_names(&db).await;
    assert!(names.manifest.complete, "{:?}", names.manifest);
    let stored = names.stored;
    for id in 1..=jobs {
        assert_eq!(
            stored.posted[&id].file_names,
            vec![format!("File.{id}.mkv")],
            "job {id}"
        );
    }
    assert_eq!(stored.history_total, 1);
}

// An NZB the read cannot parse costs only that job's posted names: its step
// is partial and names the job, and the job is under the guard.
#[tokio::test]
async fn an_unreadable_nzb_costs_only_its_own_job() {
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

    seed_report_job(&db, 32);
    let names = stored_names(&db).await;
    let step = names
        .manifest
        .steps
        .iter()
        .find(|step| step.step == stored_read::STEP_ACTIVE_NZBS)
        .expect("the step is listed");
    assert_eq!(step.state, stored_read::STATE_PARTIAL, "{step:?}");
    assert_eq!((step.read, step.of), (Some(1), Some(2)), "{step:?}");
    let error = step.error.as_deref().expect("an error");
    assert!(error.contains("the NZB of job 31"), "{error}");
    assert!(names.stored.posted.contains_key(&32));
    let guard = names.guard.expect("a guard");
    assert_eq!(guard.jobs, Some(std::collections::BTreeSet::from([31])));
}

// Corrupts every file of an open database in place, so every read of it
// fails. Overwritten, never truncated: Windows refuses to truncate a file the
// open database has mapped. The shared-memory index is left whole, since
// Windows also refuses writes to the bytes SQLite holds locked there; it
// still points every read into the garbage.
fn corrupt_database(dir: &Path) {
    for entry in std::fs::read_dir(dir).expect("dir") {
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
}

// The files a package holds whatever the database does, each naming a seeded
// job, file, server, host, user or category as the real ones would.
fn package_without_the_database() -> Vec<Component> {
    vec![
        Component::bytes(
            "olafhost.log",
            b"INFO job_id=41 wrote Olaf.Seeded.Job.S01E01.part01.rar\n\
              INFO job_id=41 extracted Olaf.Seeded.Member.mkv\n\
              INFO connected to news.olaf-provider.example:563 as olaf-user\n\
              INFO queued Olaf.Seeded.Job.S01E01\n\
              INFO pipeline turn took 12 ms\n"
                .to_vec(),
        ),
        Component::text(
            "metrics-1.txt",
            "weaver_job_bytes{job=\"Olaf.Seeded.Job.S01E01\"} 5\nweaver_turns_total 9\n",
        ),
        Component::json(
            "settings.json",
            &serde_json::json!({ "settings": { "completeDir": "/srv/olaf-complete", "maxRetries": 3 } }),
        ),
        Component::json(
            "queue.json",
            &serde_json::json!({ "queueSnapshot": { "jobs": [
                { "id": 41, "name": "Olaf.Seeded.Job.S01E01", "category": "olaf-films",
                  "error": "Olaf.Seeded.Member.mkv failed its CRC" }
            ] } }),
        ),
        Component::json(
            "servers.json",
            &serde_json::json!({ "servers": [ { "id": 3, "host": "news.olaf-provider.example" } ] }),
        ),
        Component::json(
            "pipeline-internals.json",
            &serde_json::json!({ "answered": true, "jobs": [ { "job_id": 41, "files": 3 } ] }),
        ),
    ]
}

const PACKAGE_FILES: [&str; 6] = [
    "weaver.log",
    "metrics-1.txt",
    "settings.json",
    "queue.json",
    "servers.json",
    "pipeline-internals.json",
];

fn host_with_names() -> sanitize::LocalNames {
    sanitize::LocalNames {
        log_file: Some(PathBuf::from("/srv/olafhost/olafhost.log")),
        hostname: Some("olafhost".to_string()),
        user: Some("olaf-user".to_string()),
        ..plain_local()
    }
}

// Every package file is there, and no seeded name is anywhere in it.
fn assert_every_file_and_no_name(members: &[(String, Vec<u8>)]) -> archive::Manifest {
    let manifest = manifest_of(members);
    for name in PACKAGE_FILES {
        assert!(
            members.iter().any(|(member, _)| member == name),
            "{name} is missing: {:?}",
            manifest.errors
        );
    }
    let everything = package_text(members).to_lowercase();
    assert!(
        !everything.contains("olaf"),
        "a name left the host:\n{everything}"
    );
    manifest
}

// Every database read fails: the package still comes back with every file
// that does not come from the database, the steps say each failed, and no
// seeded name is in it.
#[tokio::test]
async fn a_database_every_read_of_which_fails_still_ships_every_other_file() {
    let dir = tempfile::tempdir().expect("dir");
    let db = Database::open(&dir.path().join("weaver.db")).expect("database");
    corrupt_database(dir.path());

    let names = stored_names(&db).await;
    assert!(!names.manifest.complete);
    let (components, sanitized) =
        sanitize_components(package_without_the_database(), names, host_with_names()).await;
    assert_eq!(sanitized.history_jobs_total, None);
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);
    let manifest = assert_every_file_and_no_name(&members);
    let failed: Vec<&str> = manifest
        .sanitized
        .table
        .steps
        .iter()
        .filter(|step| step.state == stored_read::STATE_FAILED)
        .map(|step| step.step.as_str())
        .collect();
    assert!(failed.contains(&stored_read::STEP_SERVERS), "{manifest:#?}");
    assert!(
        failed.contains(&stored_read::STEP_ACTIVE_NAMES),
        "{manifest:#?}"
    );
    let log = manifest
        .files
        .iter()
        .find(|file| file.name == "weaver.log")
        .expect("the log ships");
    assert!(log.partial.is_some(), "{log:?}");
    assert!(member_text(&members, "weaver.log").contains("INFO pipeline turn took 12 ms\n"));
}

// A database that never answers: the read is stuck in its first statement.
// The package is still built, with every file, from memory and the guard,
// and no seeded name is in it.
#[tokio::test]
async fn a_database_that_never_answers_still_ships_every_file_and_no_name() {
    use weaver_server_core::JobStatus;

    let db = Database::open_in_memory().expect("database");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let entered_tx = std::sync::Mutex::new(entered_tx);
    let read = Arc::new(stored_read::NameRead::with_cut(move |_, _| {
        let _ = entered_tx.lock().expect("lock").send(());
        // Blocks until the test ends: a statement that never returns.
        let _ = release_rx.lock().expect("lock").recv();
        None
    }));
    let reader = read.clone();
    let reading = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || stored_read::read_stored_names(&db, &reader))
    };
    tokio::task::spawn_blocking(move || entered_rx.recv().expect("the read starts"))
        .await
        .expect("the read starts");
    let state = read.finish();
    assert!(
        state
            .steps
            .iter()
            .all(|step| step.state == stored_read::STATE_NOT_REACHED)
    );

    let mut job = queued_info(41, JobStatus::Downloading);
    job.name = "Olaf.Seeded.Job.S01E01".to_string();
    job.category = Some("olaf-films".to_string());
    let handle = queue_handle(vec![job]);
    let config: SharedConfig = Arc::new(tokio::sync::RwLock::new(
        weaver_server_core::settings::model::Config {
            servers: vec![from_json(serde_json::json!({
                "id": 3, "host": "news.olaf-provider.example", "port": 563, "tls": true,
                "username": null, "password": null, "connections": 8
            }))],
            categories: vec![weaver_server_core::categories::CategoryConfig {
                id: 5,
                name: "olaf-films".to_string(),
                dest_dir: None,
                aliases: String::new(),
                seeding: None,
                torrent_route: false,
                unpack: None,
                upload_cap: Default::default(),
            }],
            ..empty_config("/srv/olaf-data")
        },
    ));
    let memory = stored_read::MemoryNames::current(&handle, &config).await;
    let names = stored_read::TableNames::from_read(state, memory);
    assert_eq!(names.manifest.jobs_from_memory, 1);
    let (components, sanitized) =
        sanitize_components(package_without_the_database(), names, host_with_names()).await;
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);
    let manifest = assert_every_file_and_no_name(&members);
    assert!(!manifest.sanitized.table.complete);
    assert_eq!(manifest.sanitized.table.jobs_not_fully_read, 1);
    assert!(manifest.sanitized.table.fallback.is_some());
    // The job memory names keeps its stand-in where a field names it.
    let queue = member_text(&members, "queue.json");
    assert!(queue.contains("\"job-41\""), "{queue}");
    assert!(queue.contains("\"category-5\""), "{queue}");
    let servers = member_text(&members, "servers.json");
    assert!(servers.contains("provider-3"), "{servers}");

    drop(release_tx);
    reading.await.expect("the read ends once released");
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

    let mut names = stored_names(&db).await;
    // The engine alone writes graph members; the read's own test covers
    // that table.
    names
        .stored
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
    let (components, _) = sanitize_components(components, names, plain_local()).await;
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
        protocol: weaver_server_core::Protocol::Nzb,
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

// A configuration with nothing set but its data directory.
fn empty_config(data_dir: &str) -> weaver_server_core::settings::model::Config {
    weaver_server_core::settings::model::Config {
        data_dir: data_dir.to_string(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        bandwidth: Default::default(),
        seeding: None,
        torrent: Default::default(),
        upload_cap: None,
        cleanup_after_extract: None,
        delete_failed_job_files: None,
        history_retention_days: None,
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
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
        let expected =
            build_job_support_report(&db, Some(info), None, id, &environment, REPORT_NOW)
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
            table: Default::default(),
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
    let mut names = stored_names(&db).await;
    // The read's own duration is the one figure two runs do not share.
    names.manifest.read_ms = 0;
    let (components, sanitized) = sanitize_components(components, names, plain_local()).await;
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
    let config: SharedConfig = Arc::new(tokio::sync::RwLock::new(empty_config(
        &root.path().to_string_lossy(),
    )));
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

fn synthetic_tables() -> database_facts::TableOutcome {
    use weaver_server_core::jobs::diagnostic_tables::DiagnosticTableSize;
    database_facts::TableOutcome::Read(vec![
        DiagnosticTableSize {
            table: "job_events".to_string(),
            rows_upper_bound: Some(1_250_000),
            rows_estimated_at_last_analyze: Some(1_100_000),
        },
        DiagnosticTableSize {
            table: "settings".to_string(),
            rows_upper_bound: None,
            rows_estimated_at_last_analyze: None,
        },
    ])
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
        &synthetic_tables(),
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
    assert_eq!(
        database["tables"],
        serde_json::json!([
            {
                "table": "job_events",
                "rows_upper_bound": 1_250_000,
                "rows_estimated_at_last_analyze": 1_100_000,
            },
            {
                "table": "settings",
                "rows_upper_bound": null,
                "rows_estimated_at_last_analyze": null,
            },
        ])
    );
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
    let components = database_facts::collect_within(
        synthetic_database_sizes(),
        std::future::pending(),
        std::future::pending(),
        Duration::ZERO,
    )
    .await;
    let members = archive_through_the_pass(components).await;

    let database = package_member_json(&members, database_facts::FILE);
    assert_eq!(database["files"]["database"]["bytes"], 8_192_000);
    assert_eq!(database["files"]["wal"]["state"], "absent");
    for value in [
        "page_size",
        "page_count",
        "freelist_count",
        "journal_mode",
        "tables",
    ] {
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
            ("journal_mode".to_string(), timed_out.clone()),
            ("tables".to_string(), timed_out),
        ]
    );
    assert_eq!(database_manifest_errors(&members), 6);
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

// Archives `components` as the endpoint does when the stand-in read did not
// reach a single step and memory holds nothing.
async fn archive_without_the_table(components: Vec<Component>) -> Vec<(String, Vec<u8>)> {
    let names = stored_read::TableNames::from_read(
        stored_read::NameRead::new().finish(),
        stored_read::MemoryNames::default(),
    );
    let (components, sanitized) = sanitize_components(components, names, plain_local()).await;
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

// Every component ships through the guard: the named ones, `shipped` with
// only `own_reasons` in the manifest's errors, and the planted name nowhere.
fn assert_shipped_under_the_guard(
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
    let mut expected = [archive::MANIFEST_NAME, "queue.json", "weaver.log", shipped];
    expected.sort_unstable();
    assert_eq!(files, expected);
    assert!(!manifest.sanitized.table.complete);
    let mut shipped_reasons: Vec<String> = manifest
        .errors
        .iter()
        .map(|error| {
            assert_eq!(error.name, shipped, "{:?}", manifest.errors);
            error.reason.clone().expect("a reason code")
        })
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

// With a read that reached nothing, `database.json` still ships with its
// numbers and codes, its error text through the guard, and every other
// component ships beside it.
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
    components.extend(database_facts::database_components(
        &sizes,
        &pages,
        &synthetic_tables(),
    ));
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
    assert_shipped_under_the_guard(
        &members,
        database_facts::FILE,
        &[
            database_facts::REASON_UNREADABLE,
            bounded::REASON_READ_FAILED,
        ],
    );
}

// A journal mode SQLite does not define is database text, so with a read
// that reached nothing, the guard keeps it out of the file.
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
        &synthetic_tables(),
    ));
    let members = archive_without_the_table(components).await;

    let database = package_member_json(&members, database_facts::FILE);
    assert_eq!(database["journal_mode"], "name-1");
    assert_eq!(database["page_count"], 2000);
    assert_shipped_under_the_guard(
        &members,
        database_facts::FILE,
        &[database_facts::REASON_UNREADABLE],
    );
}

// With a read that reached nothing, the unanswered pipeline fallback still
// ships with what it says, its manifest entry says only what its code says,
// and every other component ships beside it.
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

    let written: serde_json::Value = serde_json::from_slice(&written).expect("JSON");
    let internals = package_member_json(&members, pipeline_internals::FILE);
    assert_eq!(internals, written);
    assert_eq!(internals["answered"], false);
    assert_eq!(internals["queue"]["jobs"], 2);
    assert_shipped_under_the_guard(
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
    let marked = database_facts::database_components(
        &synthetic_database_sizes(),
        &pages(),
        &synthetic_tables(),
    );
    assert!(
        marked
            .iter()
            .all(|component| component.without_table.is_some())
    );
    let unmarked: Vec<Component> = database_facts::database_components(
        &synthetic_database_sizes(),
        &pages(),
        &synthetic_tables(),
    )
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

// The read as it was before it was staged, kept for comparison: every job's
// names in one snapshot, the stored NZBs a page at a time, each opened with
// a new zstd decoder, one after another.
fn old_stored_names(db: &Database) -> sanitize::StoredNames {
    use weaver_server_core::jobs::diagnostic_names::reference::DiagnosticNzbSource;

    let mut posted = std::collections::BTreeMap::new();
    for source in [DiagnosticNzbSource::Active, DiagnosticNzbSource::History] {
        let mut after = 0;
        loop {
            let page = db.diagnostic_persisted_nzbs(source, after).expect("page");
            let Some(last) = page.last().map(|nzb| nzb.job_id) else {
                break;
            };
            for nzb in page {
                if let Some(document) = nzb.nzb.filter(|document| !document.is_empty()) {
                    posted.insert(nzb.job_id, old_posted_names(&document));
                }
            }
            after = last;
        }
    }
    let script_dir = db.diagnostic_script_directory().expect("scripts");
    let mut scripts = Vec::new();
    for instance in db.script_instances().expect("instances") {
        scripts.push(instance.script.as_str().to_string());
        scripts.push(instance.name);
    }
    sanitize::StoredNames {
        jobs: db.diagnostic_job_names_whole().expect("names"),
        posted,
        history_total: db.diagnostic_history_total().expect("total"),
        categories: db.list_categories().expect("categories"),
        logins: db
            .get_auth_credentials()
            .expect("login")
            .map(|credentials| credentials.username)
            .into_iter()
            .collect(),
        servers: db.list_servers().expect("servers"),
        schedules: db.list_schedules().expect("schedules"),
        proxies: db.list_proxy_profiles().expect("proxies"),
        egress: db.list_egress_interfaces().expect("egress"),
        pools: db.list_proxy_pools().expect("pools"),
        dns_lists: db.list_dns_server_lists().expect("lists"),
        dns_profiles: db.list_dns_profiles().expect("profiles"),
        feeds: db.list_rss_feeds().expect("feeds"),
        script_dirs: script_dir.into_iter().collect(),
        scripts,
    }
}

fn old_posted_names(nzb: &[u8]) -> weaver_nzb::NzbNames {
    if nzb.starts_with(&stored_read::ZSTD_MAGIC) {
        let decoder = zstd::stream::read::Decoder::new(nzb).expect("decoder");
        weaver_nzb::parse_nzb_names_reader(std::io::BufReader::new(decoder)).expect("names")
    } else {
        weaver_nzb::parse_nzb_names_reader(nzb).expect("names")
    }
}

// A seeded instance: `active` active jobs (even ids), `history` finished
// ones (odd ids), `files` file rows per job in each file table, a stored
// NZB for every job, a server, a category and a login.
fn seeded_instance(active: u64, history: u64, files: u64) -> Database {
    use weaver_server_core::jobs::diagnostic_names::reference::diagnostic_seed_script;

    let db = Database::open_in_memory().expect("database");
    db.diagnostic_seed_sql(diagnostic_seed_script(active, history, files))
        .expect("seed");
    let nzbs = (1..=(active * 2).max(history * 2))
        .filter(|id| {
            if id % 2 == 0 {
                *id <= active * 2
            } else {
                *id < history * 2
            }
        })
        .map(|id| {
            (
                id,
                seeded_nzb(
                    &format!("Seed Title {id}"),
                    &format!("Seed.Posted.{id}.mkv"),
                ),
            )
        })
        .collect();
    db.diagnostic_seed_nzbs(nzbs).expect("nzbs");
    db.insert_server(&from_json(serde_json::json!({
        "id": 3, "host": "news.seed-provider.example", "port": 563, "tls": true,
        "username": "seed-account", "password": null, "connections": 8
    })))
    .expect("server");
    db.insert_category(&weaver_server_core::categories::CategoryConfig {
        id: 5,
        name: "seedcat".to_string(),
        dest_dir: Some("/mnt/seed-films".to_string()),
        aliases: String::new(),
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
    })
    .expect("category");
    db.set_auth_credentials("seed-admin", "not-a-real-hash")
        .expect("login");
    db
}

// A package naming seeded jobs of the first page, of a later page and of
// the history, in fields and in running text.
fn seeded_package(late: u64) -> Vec<Component> {
    vec![
        Component::bytes(
            "weaver.log",
            format!(
                "INFO job_id=2 wrote Seed.Job.2.part0.rar\n\
                 INFO job_id={late} wrote Seed.Job.{late}.part0.rar and Seed.Posted.{late}.mkv\n\
                 INFO finished Seed.History.4199 into Seed.Posted.4199.mkv\n\
                 INFO connected to news.seed-provider.example:563 as seed-account\n\
                 INFO pipeline turn took 12 ms\n"
            )
            .into_bytes(),
        ),
        Component::text("metrics-1.txt", "weaver_turns_total 9\n"),
        Component::json(
            "settings.json",
            &serde_json::json!({ "settings": { "completeDir": "/srv/seed-complete" } }),
        ),
        Component::json(
            "queue.json",
            &serde_json::json!({ "jobs": [
                { "id": 2, "name": "Seed.Job.2", "category": "seedcat" },
                { "id": late, "name": format!("Seed.Job.{late}"), "category": "seedcat" },
            ] }),
        ),
        Component::json(
            "servers.json",
            &serde_json::json!({ "servers": [ { "host": "news.seed-provider.example" } ] }),
        ),
        Component::json(
            "pipeline-internals.json",
            &serde_json::json!({ "answered": true, "turns": 4 }),
        ),
    ]
}

async fn package_of(
    components: Vec<Component>,
    mut names: stored_read::TableNames,
) -> (Vec<(String, Vec<u8>)>, archive::Manifest) {
    names.manifest.read_ms = 0;
    let (components, sanitized) = sanitize_components(components, names, plain_local()).await;
    let archive =
        archive::build_archive(components, "0.0.0-test", "2000-01-01T00:00:00Z", sanitized)
            .expect("archive builds");
    let members = unpack_package(&archive);
    let manifest = manifest_of(&members);
    let members = members
        .into_iter()
        .filter(|(name, _)| name != archive::MANIFEST_NAME)
        .collect();
    (members, manifest)
}

// The staged read reads what the old read read, over a seeded queue and
// history, and the package it builds is the old package byte for byte.
#[tokio::test]
async fn the_staged_read_builds_the_package_the_old_read_built() {
    let db = seeded_instance(1_200, 2_100, 3);
    let old = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || old_stored_names(&db))
            .await
            .expect("the old read runs")
    };
    let new = stored_names(&db).await;
    assert!(new.manifest.complete, "{:#?}", new.manifest.steps);
    assert!(new.guard.is_none());
    assert_eq!(new.stored.jobs, old.jobs);
    assert_eq!(new.stored.posted, old.posted);
    assert_eq!(format!("{:?}", new.stored), format!("{old:?}"));
    assert_eq!(new.stored.jobs.len(), 1_200 + 2_000);

    let (new_members, new_manifest) = package_of(seeded_package(2_200), new).await;
    let (old_members, old_manifest) =
        package_of(seeded_package(2_200), stored_read::TableNames::whole(old)).await;
    assert_eq!(new_members, old_members);
    assert_eq!(
        new_manifest.sanitized.stand_ins,
        old_manifest.sanitized.stand_ins
    );
    let listed = |manifest: &archive::Manifest| {
        serde_json::to_value(
            manifest
                .files
                .iter()
                .filter(|file| file.name != archive::MANIFEST_NAME)
                .collect::<Vec<_>>(),
        )
        .unwrap()
    };
    assert_eq!(listed(&new_manifest), listed(&old_manifest));
    assert_eq!(
        serde_json::to_value(&new_manifest.errors).unwrap(),
        serde_json::to_value(&old_manifest.errors).unwrap()
    );
    let log = member_text(&new_members, "weaver.log");
    assert!(
        log.contains(
            "INFO job_id=2 wrote job-2.part0.rar
"
        ) && !log.contains("Seed."),
        "{log}"
    );
}

// A read cut after its first page of active jobs keeps that page: those jobs
// keep the stand-ins a whole read gives them, the jobs of later pages are
// nowhere in the package, and the manifest says how far the read got.
#[tokio::test]
async fn a_read_cut_after_its_first_page_keeps_that_page() {
    let db = seeded_instance(1_200, 10, 1);
    let late = 2 * 1_100;
    let cut = run_read(
        &db,
        stored_read::NameRead::with_cut(|step, ran| {
            (step == stored_read::STEP_ACTIVE_NAMES && ran > 0).then_some(stored_read::Cut::Stop)
        }),
    )
    .await;
    let cut = stored_read::TableNames::from_read(cut, stored_read::MemoryNames::default());
    let step = |name: &str| {
        cut.manifest
            .steps
            .iter()
            .find(|step| step.step == name)
            .cloned()
            .expect("listed")
    };
    let active = step(stored_read::STEP_ACTIVE_NAMES);
    assert_eq!(active.state, stored_read::STATE_PARTIAL, "{active:?}");
    assert_eq!(active.read, Some(500));
    assert_eq!(active.reason.as_deref(), Some("stopped"));
    for later in [
        stored_read::STEP_HISTORY_NAMES,
        stored_read::STEP_ACTIVE_NZBS,
        stored_read::STEP_HISTORY_NZBS,
    ] {
        assert_eq!(step(later).state, stored_read::STATE_NOT_REACHED, "{later}");
    }
    assert_eq!(
        step(stored_read::STEP_SERVERS).state,
        stored_read::STATE_COMPLETE
    );
    assert!(!cut.manifest.complete);
    assert_eq!(cut.manifest.jobs_not_fully_read, 500 + 10);

    let (cut_members, cut_manifest) = package_of(seeded_package(late), cut).await;
    let (whole_members, _) = package_of(seeded_package(late), stored_names(&db).await).await;
    for name in PACKAGE_FILES {
        assert!(
            cut_members.iter().any(|(member, _)| member == name),
            "{name}: {:?}",
            cut_manifest.errors
        );
    }
    let cut_text = package_text(&cut_members);
    for real in [
        format!("Seed.Job.{late}"),
        format!("Seed.Posted.{late}"),
        "seed-".into(),
    ] {
        assert!(
            !cut_text.contains(&real),
            "{real} left the host:\n{cut_text}"
        );
    }
    // A page-one job keeps its stand-in where a field names it.
    let cut_queue = package_member_json(&cut_members, "queue.json");
    let whole_queue = package_member_json(&whole_members, "queue.json");
    assert_eq!(cut_queue["jobs"][0], whole_queue["jobs"][0]);
    assert_eq!(cut_queue["jobs"][0]["name"], "job-2");
    // Lines naming a job not read whole are left out, and the log says so.
    let log = member_text(&cut_members, "weaver.log");
    assert!(log.contains(sanitize::GUARDED_LINE), "{log}");
    assert!(log.contains("INFO pipeline turn took 12 ms\n"), "{log}");
    let log_entry = cut_manifest
        .files
        .iter()
        .find(|file| file.name == "weaver.log")
        .expect("the log ships");
    assert!(log_entry.partial.is_some(), "{log_entry:?}");
    assert!(!cut_manifest.sanitized.table.complete);
}

// Each step of the read failed in turn: every other step completes, the
// manifest names only that one as failed, and every file ships.
#[tokio::test]
async fn each_failed_step_costs_only_its_own_names() {
    let db = seeded_instance(30, 10, 1);
    for failing in stored_read::STEPS {
        let state = run_read(
            &db,
            stored_read::NameRead::with_cut(move |step, _| {
                (step == failing).then_some(stored_read::Cut::Fail("planted failure"))
            }),
        )
        .await;
        let names = stored_read::TableNames::from_read(state, stored_read::MemoryNames::default());
        let states: Vec<(&str, &str)> = names
            .manifest
            .steps
            .iter()
            .map(|step| (step.step.as_str(), step.state.as_str()))
            .collect();
        let expected: Vec<(&str, &str)> = stored_read::STEPS
            .iter()
            .map(|step| {
                (
                    *step,
                    if *step == failing {
                        stored_read::STATE_FAILED
                    } else {
                        stored_read::STATE_COMPLETE
                    },
                )
            })
            .collect();
        assert_eq!(states, expected, "{failing}");
        let (members, manifest) = package_of(seeded_package(60), names).await;
        for name in PACKAGE_FILES {
            assert!(
                members.iter().any(|(member, _)| member == name),
                "{failing}: {name} is missing: {:?}",
                manifest.errors
            );
        }
        assert!(
            manifest.errors.is_empty(),
            "{failing}: {:?}",
            manifest.errors
        );
        let text = package_text(&members).to_lowercase();
        assert!(
            !text.contains("seed."),
            "{failing}: a name left the host:\n{text}"
        );
    }
}

// Each collected file failed in turn: every other file ships, and the
// manifest names only that one.
#[tokio::test]
async fn each_failed_file_costs_only_itself() {
    let db = seeded_instance(30, 10, 1);
    for failing in PACKAGE_FILES {
        let components: Vec<Component> = seeded_package(60)
            .into_iter()
            .map(|component| {
                if component.name == failing {
                    Component::failed(failing, "planted failure")
                } else {
                    component
                }
            })
            .collect();
        let (members, manifest) = package_of(components, stored_names(&db).await).await;
        let shipped: Vec<&str> = members.iter().map(|(name, _)| name.as_str()).collect();
        let expected: Vec<&str> = PACKAGE_FILES
            .iter()
            .copied()
            .filter(|name| *name != failing)
            .collect();
        assert_eq!(shipped, expected, "{failing}");
        let errors: Vec<&str> = manifest
            .errors
            .iter()
            .map(|error| error.name.as_str())
            .collect();
        assert_eq!(errors, [failing]);
    }
}

// An NZB of `files` files of `segments` segments each, named after job `id`.
// About 95 bytes of XML per segment: 60,000 segments make a 5.7 MB document.
fn sized_nzb(id: u64, files: u64, segments: u64) -> Vec<u8> {
    use std::fmt::Write as _;

    let mut nzb = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n\
         <head><meta type=\"title\">Seed Title {id}</meta></head>\n"
    );
    for file in 0..files {
        write!(
            nzb,
            "<file poster=\"poster@example.org\" date=\"1\" \
             subject=\"&quot;Seed.Posted.{id}.part{file:03}.rar&quot; yEnc (1/{segments})\">\n\
             <groups><group>alt.binaries.test</group></groups>\n<segments>\n"
        )
        .expect("write");
        for number in 1..=segments {
            writeln!(
                nzb,
                "<segment bytes=\"768000\" number=\"{number}\">\
                 part{number}of{segments}.{id}.{file}@seed-poster.example.org</segment>"
            )
            .expect("write");
        }
        nzb.push_str("</segments>\n</file>\n");
    }
    nzb.push_str("</nzb>\n");
    weaver_server_core::ingest::compress_nzb_bytes(nzb.as_bytes()).expect("compress nzb")
}

// The jobs whose stored NZBs are real-size, and the segments per file each
// one gets (100 files each): about 5.7, 8.6 and 11.4 MB of XML.
const LARGE_NZBS: [(u64, u64); 3] = [(2, 600), (20_000, 900), (4_999, 1_200)];

// 20,000 active jobs (even ids) of 20 files, 2,500 finished ones (odd ids),
// a stored NZB for each (20 files of 4 segments, three of them real-size),
// a server, a category and a login.
fn queue_of_20000() -> Database {
    use weaver_server_core::jobs::diagnostic_names::reference::diagnostic_seed_script;

    let db = Database::open_in_memory().expect("database");
    db.diagnostic_seed_sql(diagnostic_seed_script(20_000, 2_500, 20))
        .expect("seed");
    let ids: Vec<u64> = (1..=40_000)
        .filter(|id| id % 2 == 0 || *id < 5_000)
        .collect();
    for chunk in ids.chunks(2_000) {
        let nzbs = chunk
            .iter()
            .map(|id| {
                let segments = LARGE_NZBS
                    .iter()
                    .find(|(large, _)| large == id)
                    .map(|(_, segments)| *segments);
                match segments {
                    Some(segments) => (*id, sized_nzb(*id, 100, segments)),
                    None => (*id, sized_nzb(*id, 20, 4)),
                }
            })
            .collect();
        db.diagnostic_seed_nzbs(nzbs).expect("nzbs");
    }
    db
}

// The most statements a whole read may run over `active` active jobs and
// `history` read finished ones: the fixed steps, one statement per side
// table per page of names (and one empty page to end the active pages), one
// size statement per page of ids, and one fetch per batch of NZBs (batches
// are cut within a page of ids), plus one
// per real-size NZB that fills a batch alone. Nothing in it grows with the
// files of a job.
fn statement_bound(active: u64, history: u64, per_page: u64, large: u64) -> u64 {
    let page = weaver_server_core::jobs::diagnostic_names::DIAGNOSTIC_NAME_PAGE as u64;
    let pages = active.div_ceil(page) + 1 + history.div_ceil(page);
    let fixed = stored_read::STEPS.len() as u64;
    let batch = stored_read::NZB_BATCH as u64;
    let batches = |jobs: u64| (jobs / page) * page.div_ceil(batch) + (jobs % page).div_ceil(batch);
    fixed
        + pages * per_page
        + active.div_ceil(page)
        + history.div_ceil(page)
        + batches(active)
        + batches(history)
        + 2 * large
}

// The statements one page of names runs, read from a single seeded job.
async fn statements_per_page() -> u64 {
    use weaver_server_core::jobs::diagnostic_names::reference::diagnostic_seed_script;

    let db = Database::open_in_memory().expect("database");
    db.diagnostic_seed_sql(diagnostic_seed_script(1, 0, 1))
        .expect("seed");
    let page = tokio::task::spawn_blocking(move || {
        db.diagnostic_job_names_page(
            weaver_server_core::jobs::diagnostic_names::DiagnosticJobs::ActiveAfter(0),
        )
    })
    .await
    .expect("the page runs")
    .expect("the page reads");
    page.statements.len() as u64
}

// A queue of 20,000 jobs reads whole: every job named, every stored NZB
// opened, in a number of statements set by pages, not by jobs or files.
#[tokio::test]
async fn a_queue_of_20000_jobs_reads_whole_in_bounded_statements() {
    let db = queue_of_20000();
    let state = run_read(&db, stored_read::NameRead::new()).await;
    let per_page = statements_per_page().await;
    let bound = statement_bound(20_000, 2_000, per_page, LARGE_NZBS.len() as u64);
    assert!(
        state.statements <= bound,
        "{} statements, bound {bound}",
        state.statements
    );
    assert_eq!(state.stored.jobs.len(), 22_000);
    assert_eq!(state.stored.posted.len(), 22_000);
    let large = &state.stored.posted[&4_999];
    assert_eq!(large.file_names.len(), 100, "{large:?}");
    let names = stored_read::TableNames::from_read(state, stored_read::MemoryNames::default());
    assert!(names.manifest.complete, "{:#?}", names.manifest.steps);
    assert_eq!(names.manifest.names_from_table, 0, "counted at sanitizing");
    assert!(names.guard.is_none());
}

// Measures the old read and the staged read over the 20,000-job queue, a
// step at a time, and checks they read the same names. Run by hand:
// cargo nextest run --locked -p weaver --cargo-profile matrix --run-ignored only \
//   http::diagnostics::tests::measure_the_read_of_20000_jobs --no-capture
#[tokio::test]
#[ignore = "a measurement, run by hand"]
async fn measure_the_read_of_20000_jobs() {
    use std::time::Instant;
    use weaver_server_core::jobs::diagnostic_names::reference::DiagnosticNzbSource;

    let seeded = Instant::now();
    let db = queue_of_20000();
    eprintln!("seed: {} ms", seeded.elapsed().as_millis());

    let measured = db.clone();
    tokio::task::spawn_blocking(move || {
        let db = measured;
        let names = Instant::now();
        let whole = db.diagnostic_job_names_whole().expect("names");
        eprintln!(
            "old names, one snapshot of whole tables: {} ms, {} jobs",
            names.elapsed().as_millis(),
            whole.len()
        );

        let fetched = Instant::now();
        let mut blobs = Vec::new();
        for source in [DiagnosticNzbSource::Active, DiagnosticNzbSource::History] {
            let mut after = 0;
            loop {
                let page = db.diagnostic_persisted_nzbs(source, after).expect("page");
                let Some(last) = page.last().map(|nzb| nzb.job_id) else {
                    break;
                };
                blobs.extend(page.into_iter().filter_map(|nzb| nzb.nzb));
                after = last;
            }
        }
        let stored: usize = blobs.iter().map(Vec::len).sum();
        eprintln!(
            "old NZB fetch: {} ms, {} NZBs, {} stored bytes",
            fetched.elapsed().as_millis(),
            blobs.len(),
            stored
        );

        let decoded = Instant::now();
        let mut documents = Vec::with_capacity(blobs.len());
        for blob in &blobs {
            let mut out = Vec::new();
            let mut decoder = zstd::stream::read::Decoder::new(blob.as_slice()).expect("decoder");
            std::io::copy(&mut decoder, &mut out).expect("decode");
            documents.push(out);
        }
        let xml: usize = documents.iter().map(Vec::len).sum();
        eprintln!(
            "decode, a new decoder per NZB: {} ms, {} bytes of XML",
            decoded.elapsed().as_millis(),
            xml
        );

        let reused = Instant::now();
        let mut context = zstd::zstd_safe::DCtx::create();
        let mut out = Vec::new();
        for blob in &blobs {
            out.clear();
            let mut decoder =
                zstd::stream::read::Decoder::with_context(blob.as_slice(), &mut context);
            std::io::copy(&mut decoder, &mut out).expect("decode");
        }
        eprintln!(
            "decode, one reused context: {} ms",
            reused.elapsed().as_millis()
        );

        let parsed = Instant::now();
        for document in &documents {
            weaver_nzb::parse_nzb_names_reader(document.as_slice()).expect("names");
        }
        eprintln!(
            "parse of decoded XML, one thread: {} ms",
            parsed.elapsed().as_millis()
        );

        let serial = Instant::now();
        let mut context = zstd::zstd_safe::DCtx::create();
        for blob in &blobs {
            stored_read::posted_names_with(blob, &mut context).expect("names");
        }
        eprintln!(
            "decode and parse, one thread, reused context: {} ms",
            serial.elapsed().as_millis()
        );

        let old = Instant::now();
        let old_names = old_stored_names(&db);
        eprintln!("old read, whole: {} ms", old.elapsed().as_millis());
        old_names
    })
    .await
    .map(|old_names| {
        let db = db.clone();
        async move {
            let new = std::time::Instant::now();
            let state = run_read(&db, stored_read::NameRead::new()).await;
            eprintln!(
                "staged read: {} ms wall ({} ms in the read), {} statements, {} rows, \
                 {} stored bytes, {} decoded bytes, {} parse workers",
                new.elapsed().as_millis(),
                state.read_ms,
                state.statements,
                state.rows,
                state.nzb_bytes,
                state.decoded_bytes,
                stored_read::parse_workers()
            );
            assert_eq!(state.stored.jobs, old_names.jobs);
            assert_eq!(state.stored.posted, old_names.posted);
            let names_only = std::time::Instant::now();
            let names = run_read(
                &db,
                stored_read::NameRead::with_cut(|step, _| {
                    (step == stored_read::STEP_ACTIVE_NZBS).then_some(stored_read::Cut::Stop)
                }),
            )
            .await;
            eprintln!(
                "staged read, names steps only: {} ms, {} statements, {} rows",
                names_only.elapsed().as_millis(),
                names.statements,
                names.rows
            );
        }
    })
    .expect("the old read runs")
    .await;
}
