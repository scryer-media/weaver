// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const MIB: u64 = 1024 * 1024;

fn plan(job_display_name: &str) -> DeliveryNamingPlan {
    DeliveryNamingPlan {
        job_display_name: job_display_name.to_string(),
        srrdb: None,
    }
}

fn plan_with_srrdb(job_display_name: &str, member: &str, crc32: u32) -> DeliveryNamingPlan {
    DeliveryNamingPlan {
        job_display_name: job_display_name.to_string(),
        srrdb: Some(SrrdbInputs {
            // Loopback on a port nothing can bind: the request is refused
            // by the kernel, with no name resolution and nothing leaving
            // the machine.
            base_url: "http://127.0.0.1:1/v1".to_string(),
            crc32_by_member_path: HashMap::from([(member.to_ascii_lowercase(), crc32)]),
            proxies: Some(crate::proxies::direct_runtime()),
        }),
    }
}

fn write_file(root: &Path, relative_path: &str, bytes: u64) {
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(bytes).unwrap();
}

// A sparse file of `bytes` whose head is `head`.
fn write_file_with_head(root: &Path, relative_path: &str, bytes: u64, head: &[u8]) {
    use std::io::Write;

    write_file(root, relative_path, bytes);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(relative_path))
        .unwrap();
    file.write_all(head).unwrap();
}

// The start of a Matroska EBML header.
const MATROSKA_HEAD: &[u8] =
    b"\x1A\x45\xDF\xA3\xA3\x42\x86\x81\x01\x42\x82\x88matroska\x42\x87\x81\x04";

fn delivered_names(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = scan_delivery(root)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    names.sort();
    names
}

// ── srrdb URL and response parsing (never touches the network) ───────

#[test]
fn search_url_renders_the_checksum_as_eight_lowercase_hex_digits() {
    assert_eq!(
        srrdb_search_url("https://api.example.test/v1", 0x0a1b_2c3d),
        "https://api.example.test/v1/search/archive-crc:0a1b2c3d"
    );
    // Leading zeroes are significant to the index.
    assert_eq!(
        srrdb_search_url("https://api.example.test/v1/", 0x0000_00ff),
        "https://api.example.test/v1/search/archive-crc:000000ff"
    );
}

#[test]
fn a_single_result_yields_its_release_name() {
    let body = r#"{"resultsCount":"1","results":[
            {"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW","date":"2024-04-01"}
        ]}"#;
    assert_eq!(
        parse_srrdb_release(body).as_deref(),
        Some("Silver.Horizon.2024.1080p.WEB-DL.x264-CREW")
    );
}

#[test]
fn a_miss_or_an_ambiguity_yields_nothing() {
    assert_eq!(parse_srrdb_release(r#"{"results":[]}"#), None);
    assert_eq!(
        parse_srrdb_release(
            r#"{"results":[{"release":"Silver.Horizon.2024"},{"release":"Quiet.Harbour.2021"}]}"#
        ),
        None
    );
    assert_eq!(parse_srrdb_release(r#"{"results":[{"release":""}]}"#), None);
    assert_eq!(parse_srrdb_release("not json at all"), None);
    assert_eq!(parse_srrdb_release("{}"), None);
}

#[tokio::test]
async fn a_large_release_index_response_is_streamed_without_rejection() {
    use axum::Router;
    use axum::extract::State;
    use axum::routing::get;

    async fn search(State(body): State<String>) -> String {
        body
    }

    let mut body =
        r#"{"results":[{"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW"}]}"#.to_string();
    body.push_str(&" ".repeat(128 * 1024));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/search/{query}", get(search))
        .with_state(body);
    let served = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let release =
        fetch_srrdb_release(&crate::proxies::direct_runtime(), &base_url, 0x0a1b_2c3d).await;
    served.abort();

    assert_eq!(
        release.as_deref(),
        Some("Silver.Horizon.2024.1080p.WEB-DL.x264-CREW")
    );
}

#[tokio::test]
async fn a_release_index_response_over_one_mib_is_rejected() {
    use axum::Router;
    use axum::extract::State;
    use axum::routing::get;

    async fn search(State(body): State<String>) -> String {
        body
    }

    let mut body =
        r#"{"results":[{"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW"}]}"#.to_string();
    body.push_str(&" ".repeat(MAX_SRRDB_RESPONSE_BYTES));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/search/{query}", get(search))
        .with_state(body);
    let served = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let release =
        fetch_srrdb_release(&crate::proxies::direct_runtime(), &base_url, 0x0a1b_2c3d).await;
    served.abort();

    assert_eq!(release, None);
}

// ── the pass over a real directory ──────────────────────────────────

#[tokio::test]
async fn an_obfuscated_member_takes_the_job_name() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
    write_file(root.path(), "Yb5drZSkNi20UCMkb-sample.mkv", 2 * MIB);
    write_file(root.path(), "Yb5drZSkNi20UCMkb.dut.srt", 4096);

    let renamed = rename_obfuscated_members(
        JobId(1),
        root.path(),
        &plan("Silver Horizon 2024"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 3);
    assert_eq!(
        delivered_names(root.path()),
        vec![
            "Silver Horizon 2024-sample.mkv".to_string(),
            "Silver Horizon 2024.dut.srt".to_string(),
            "Silver Horizon 2024.mkv".to_string(),
        ]
    );
}

#[tokio::test]
async fn a_member_named_after_a_cleaned_job_name_reports_the_full_name() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(11),
        root.path(),
        &plan("Silver Horizon: Part 2"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(
        renamed,
        vec![FileRename {
            from: "Yb5drZSkNi20UCMkb.mkv".to_string(),
            to: "Silver Horizon_ Part 2.mkv".to_string(),
            full_name: "Silver Horizon: Part 2.mkv".to_string(),
        }]
    );
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon_ Part 2.mkv".to_string()]
    );
}

// The renames the pass applied before a full name over the record's cap
// stopped being copied into each one: the plan built with the job's full
// name, every rename carrying its `full_to`.
fn reference_renames(root: &Path, job_display_name: &str) -> Vec<FileRename> {
    let files = scan_delivery(root);
    let extensions: Vec<_> = files
        .iter()
        .map(|(relative_path, _)| content_extension(root, relative_path))
        .collect();
    let entries: Vec<DeliveredFile<'_>> = files
        .iter()
        .zip(&extensions)
        .map(|((relative_path, bytes), extension)| DeliveredFile {
            relative_path,
            bytes: *bytes,
            content_extension: *extension,
        })
        .collect();
    let candidate = weaver_nzb::select_rename_candidate(&entries, |_| false).unwrap();
    let target = crate::jobs::working_dir::sanitize_dirname(job_display_name);
    let mut renames =
        weaver_nzb::plan_renames(&entries, candidate, &target, job_display_name.trim());
    renames.extend(weaver_nzb::plan_content_extensions(&entries, &renames));
    renames
        .into_iter()
        .map(|rename| FileRename {
            from: rename.from,
            to: rename.to,
            full_name: rename.full_to,
        })
        .collect()
}

fn lay_out_full_name_delivery(root: &Path) {
    write_file(root, "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
    write_file(root, "Yb5drZSkNi20UCMkb-sample.mkv", 2 * MIB);
    write_file(root, "Yb5drZSkNi20UCMkb.dut.srt", 4096);
    // An extensionless member whose content name is taken, so its rename
    // is numbered and records a full name of its own.
    write_file(root, "extras/Quiet Harbour.mkv", 4096);
    write_file_with_head(root, "extras/Quiet Harbour", 2 * MIB, MATROSKA_HEAD);
}

#[tokio::test]
async fn full_names_record_as_before_and_an_over_cap_title_is_not_copied_per_file() {
    let over_cap = "Silver Horizon ".repeat(70 * 1024 / 15);
    for title in ["Silver Horizon: Part 2", over_cap.as_str()] {
        let expected_root = tempfile::tempdir().unwrap();
        lay_out_full_name_delivery(expected_root.path());
        let expected = reference_renames(expected_root.path(), title);

        let root = tempfile::tempdir().unwrap();
        lay_out_full_name_delivery(root.path());
        let renamed = rename_obfuscated_members(
            JobId(31),
            root.path(),
            &plan(title),
            &PostProcessingSettings::default(),
        )
        .await;

        let moves = |renames: &[FileRename]| {
            renames
                .iter()
                .map(|rename| (rename.from.clone(), rename.to.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(moves(&renamed), moves(&expected));
        assert_eq!(renamed.len(), 4);

        // The record a job holds afterwards is the same, byte for byte,
        // starting from a record that already holds an entry.
        let seeded = crate::ingest::append_full_file_names_metadata(
            Vec::new(),
            [("Notes: Part 2.nfo", "Notes_ Part 2.nfo")],
        );
        let mut expected_metadata = seeded.clone();
        crate::ingest::record_file_renames(&mut expected_metadata, &expected);
        let mut metadata = seeded.clone();
        crate::ingest::record_file_renames(&mut metadata, &renamed);
        assert_eq!(metadata, expected_metadata);

        let full_name_bytes = |renames: &[FileRename]| {
            renames
                .iter()
                .map(|rename| rename.full_name.len())
                .sum::<usize>()
        };
        if title.len() > 64 * 1024 {
            // The over-cap title was copied into each of the three renames
            // named after the job; now none holds it.
            assert!(full_name_bytes(&expected) > 3 * title.len());
            assert_eq!(full_name_bytes(&renamed), 0);
            assert_eq!(
                crate::ingest::full_file_names(&metadata),
                crate::ingest::full_file_names(&seeded)
            );
        } else {
            assert_eq!(renamed, expected);
            // The seeded entry, the three renames named after the job and
            // the numbered content rename.
            assert_eq!(crate::ingest::full_file_names(&metadata).len(), 5);
        }
    }
}

#[tokio::test]
async fn a_readable_member_is_left_alone() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Silver.Horizon.2024.1080p.mkv", 64 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(2),
        root.path(),
        &plan("Quiet Harbour"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver.Horizon.2024.1080p.mkv".to_string()]
    );
}

#[tokio::test]
async fn an_obfuscated_job_name_refuses_the_pass() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(3),
        root.path(),
        &plan("2c0837e5fa42c8cfb5d5e583168a2af4"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Yb5drZSkNi20UCMkb.mkv".to_string()]
    );
}

#[tokio::test]
async fn a_disc_structure_delivery_is_never_touched() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "VIDEO_TS/VTS_01_1.VOB", 64 * MIB);
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 32 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(4),
        root.path(),
        &plan("Silver Horizon"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec![
            "VIDEO_TS/VTS_01_1.VOB".to_string(),
            "Yb5drZSkNi20UCMkb.mkv".to_string(),
        ]
    );
}

#[tokio::test]
async fn an_unreachable_release_index_falls_through_to_the_job_name() {
    // The endpoint resolves nowhere, which is the same ladder rung as a
    // miss, a timeout or a malformed answer: the job name still wins and
    // the job still completes.
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(5),
        root.path(),
        &plan_with_srrdb("Silver Horizon 2024", "Yb5drZSkNi20UCMkb.mkv", 0x1234_5678),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 1);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon 2024.mkv".to_string()]
    );
}

// The whole rung, against a canned index bound to loopback: nothing leaves
// the machine, and the assertion covers what the request carried as well as
// what the answer did to the name.
#[tokio::test]
async fn a_release_index_hit_outranks_the_job_name() {
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::{Path as AxumPath, State};
    use axum::routing::get;

    #[derive(Clone)]
    struct Index {
        requests: StdArc<std::sync::Mutex<Vec<String>>>,
        calls: StdArc<AtomicUsize>,
    }

    async fn search(State(index): State<Index>, AxumPath(query): AxumPath<String>) -> String {
        index.calls.fetch_add(1, Ordering::SeqCst);
        index.requests.lock().unwrap().push(query);
        r#"{"resultsCount":"1","results":[
                {"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW"}
            ]}"#
        .to_string()
    }

    let index = Index {
        requests: StdArc::new(std::sync::Mutex::new(Vec::new())),
        calls: StdArc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/v1/search/{query}", get(search))
        .with_state(index.clone());
    let served = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "feature/Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
    write_file(root.path(), "feature/Yb5drZSkNi20UCMkb.eng.srt", 4096);

    let mut plan = plan_with_srrdb(
        "Quiet Harbour 2021",
        "feature/Yb5drZSkNi20UCMkb.mkv",
        0x0a1b_2c3d,
    );
    plan.srrdb.as_mut().unwrap().base_url = base_url;

    let renamed = rename_obfuscated_members(
        JobId(8),
        root.path(),
        &plan,
        &PostProcessingSettings::default(),
    )
    .await;
    served.abort();

    assert_eq!(
        index.requests.lock().unwrap().as_slice(),
        ["archive-crc:0a1b2c3d".to_string()],
        "the request carries the checksum and nothing else"
    );
    assert_eq!(index.calls.load(Ordering::SeqCst), 1, "exactly one attempt");
    assert_eq!(renamed.len(), 2);
    assert_eq!(
        delivered_names(root.path()),
        vec![
            "feature/Silver.Horizon.2024.1080p.WEB-DL.x264-CREW.eng.srt".to_string(),
            "feature/Silver.Horizon.2024.1080p.WEB-DL.x264-CREW.mkv".to_string(),
        ],
        "the release name outranks the job name, and the helper follows the payload"
    );
}

#[tokio::test]
async fn a_job_name_that_is_already_the_member_name_plans_nothing() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);

    let renamed = rename_obfuscated_members(
        JobId(6),
        root.path(),
        &plan("Yb5drZSkNi20UCMkb"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
}

#[tokio::test]
async fn an_executable_payload_keeps_its_name_with_the_policy_off() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.exe", 64 * MIB);
    let policy_off = PostProcessingSettings {
        unacceptable_extensions: Vec::new(),
        ..PostProcessingSettings::default()
    };

    let renamed =
        rename_obfuscated_members(JobId(9), root.path(), &plan("Silver Horizon"), &policy_off)
            .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Yb5drZSkNi20UCMkb.exe".to_string()]
    );
}

#[tokio::test]
async fn a_payload_the_policy_refuses_keeps_its_name() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.iso", 64 * MIB);
    let policy = PostProcessingSettings {
        unacceptable_extensions: vec!["iso".into()],
        ..PostProcessingSettings::default()
    };

    let renamed =
        rename_obfuscated_members(JobId(10), root.path(), &plan("Silver Horizon"), &policy).await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Yb5drZSkNi20UCMkb.iso".to_string()]
    );
}

#[tokio::test]
async fn an_obfuscated_extensionless_member_keeps_its_name_when_the_policy_refuses_its_content() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(root.path(), "Yb5drZSkNi20UCMkb", 64 * MIB, MATROSKA_HEAD);
    write_file(root.path(), "Yb5drZSkNi20UCMkb.nfo", 2048);
    let policy = PostProcessingSettings {
        unacceptable_extensions: vec!["MKV".into()],
        ..PostProcessingSettings::default()
    };

    let renamed = rename_obfuscated_members(
        JobId(14),
        root.path(),
        &plan("Silver Horizon S01E03"),
        &policy,
    )
    .await;

    // Neither the payload nor its helper moves: the policy would refuse
    // the name the payload was about to take.
    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec![
            "Yb5drZSkNi20UCMkb".to_string(),
            "Yb5drZSkNi20UCMkb.nfo".to_string()
        ]
    );
}

#[tokio::test]
async fn a_readable_extensionless_member_keeps_its_name_when_the_policy_refuses_its_content() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(
        root.path(),
        "Silver Horizon S01E03",
        64 * MIB,
        MATROSKA_HEAD,
    );
    let policy = PostProcessingSettings {
        unacceptable_extensions: vec!["mkv".into()],
        ..PostProcessingSettings::default()
    };

    let renamed =
        rename_obfuscated_members(JobId(15), root.path(), &plan("Quiet Harbour"), &policy).await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon S01E03".to_string()]
    );
}

#[tokio::test]
async fn an_extensionless_member_takes_the_job_name_and_its_content_extension() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(root.path(), "Yb5drZSkNi20UCMkb", 64 * MIB, MATROSKA_HEAD);

    let renamed = rename_obfuscated_members(
        JobId(12),
        root.path(),
        &plan("Silver Horizon S01E03"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(
        renamed,
        vec![FileRename {
            from: "Yb5drZSkNi20UCMkb".to_string(),
            to: "Silver Horizon S01E03.mkv".to_string(),
            full_name: "Silver Horizon S01E03.mkv".to_string(),
        }]
    );
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon S01E03.mkv".to_string()]
    );
}

#[tokio::test]
async fn a_readable_extensionless_member_takes_its_content_extension() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(
        root.path(),
        "Silver Horizon S01E03",
        64 * MIB,
        MATROSKA_HEAD,
    );

    let renamed = rename_obfuscated_members(
        JobId(13),
        root.path(),
        &plan("Quiet Harbour"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 1);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon S01E03.mkv".to_string()]
    );
}

#[tokio::test]
async fn an_extensionless_member_of_unknown_content_keeps_its_name() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(
        root.path(),
        "Silver Horizon S01E03",
        64 * MIB,
        b"nothing a sniff knows",
    );

    let renamed = rename_obfuscated_members(
        JobId(14),
        root.path(),
        &plan("Quiet Harbour"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver Horizon S01E03".to_string()]
    );
}

#[tokio::test]
async fn a_member_with_a_real_extension_keeps_it_whatever_its_content() {
    let root = tempfile::tempdir().unwrap();
    write_file_with_head(
        root.path(),
        "Silver.Horizon.S01E03.avi",
        64 * MIB,
        MATROSKA_HEAD,
    );

    let renamed = rename_obfuscated_members(
        JobId(15),
        root.path(),
        &plan("Quiet Harbour"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 0);
    assert_eq!(
        delivered_names(root.path()),
        vec!["Silver.Horizon.S01E03.avi".to_string()]
    );
}

#[tokio::test]
async fn weaver_bookkeeping_is_not_part_of_the_delivery() {
    let root = tempfile::tempdir().unwrap();
    write_file(root.path(), "Yb5drZSkNi20UCMkb.mkv", 64 * MIB);
    write_file(root.path(), OUTPUT_DIR_MARKER, 32);
    write_file(root.path(), ".hidden-scratch", 512);

    let renamed = rename_obfuscated_members(
        JobId(7),
        root.path(),
        &plan("Silver Horizon"),
        &PostProcessingSettings::default(),
    )
    .await;

    assert_eq!(renamed.len(), 1);
    assert!(root.path().join(OUTPUT_DIR_MARKER).is_file());
    assert!(root.path().join(".hidden-scratch").is_file());
}

#[tokio::test]
async fn the_release_index_lookup_goes_through_the_outbound_proxy() {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new().route(
        "/v1/search/{query}",
        get(|| async {
            r#"{"results":[{"release":"Silver.Horizon.2024.1080p.WEB-DL.x264-CREW"}]}"#
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let served = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let proxy = weaver_tunnel::test_support::proxy::ProxyServerDouble::start(
        weaver_tunnel::transport::TransportKind::HttpConnect,
        None,
        [(("srrdb.invalid".to_owned(), 80), origin)].into(),
    )
    .await;
    let runtime = crate::proxies::outbound_only_through(
        crate::proxies::ProxyKind::HttpConnect,
        proxy.addr.port(),
    );

    let release = fetch_srrdb_release(&runtime, "http://srrdb.invalid/v1", 0x0a1b_2c3d).await;

    assert_eq!(
        release.as_deref(),
        Some("Silver.Horizon.2024.1080p.WEB-DL.x264-CREW")
    );
    assert_eq!(
        proxy.targets.lock().unwrap().clone(),
        [("srrdb.invalid".to_owned(), 80)]
    );
    runtime.stop_all().await;
    served.abort();
}
