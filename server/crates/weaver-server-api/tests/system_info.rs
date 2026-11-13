// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;
use weaver_server_core::categories::CategoryConfig;

const SYSTEM_INFO_QUERY: &str = r#"
{
  systemInfo {
    version
    uptimeSeconds
    deployment
    operatingSystem
    architecture
    databaseEngine
    compute {
      physicalCores
      logicalCores
      cgroupLimit
      decoderTier
      simdFeatures
      kernels { component library ladder kernel pinnedBy }
    }
    memory {
      totalBytes
      availableAtStartupBytes
      cgroupLimitBytes
      effectiveLimitBytes
    }
    primaryStorage {
      storageClass
      filesystem
      startupRandomReadIops
    }
    configuredStorage {
      labels
      path
      error
      capacity { totalBytes usedBytes freeBytes }
    }
  }
}
"#;

#[tokio::test]
async fn system_info_exposes_safe_runtime_profile_to_read_scope() {
    let h = TestHarness::new().await;
    let response = h.execute_as(SYSTEM_INFO_QUERY, CallerScope::Read).await;
    assert_no_errors(&response);
    let info = &response_data(&response)["systemInfo"];

    assert!(!info["version"].as_str().unwrap().is_empty());
    assert!(info["uptimeSeconds"].as_f64().unwrap() >= 0.0);
    assert!(matches!(
        info["deployment"].as_str().unwrap(),
        "NATIVE" | "DOCKER" | "CONTAINER"
    ));
    assert!(matches!(
        info["operatingSystem"].as_str().unwrap(),
        "LINUX" | "MACOS" | "WINDOWS" | "UNKNOWN"
    ));
    assert!(!info["architecture"].as_str().unwrap().is_empty());
    assert_eq!(info["databaseEngine"].as_str().unwrap(), "SQLITE");
    assert_eq!(info["compute"]["physicalCores"].as_u64().unwrap(), 4);
    assert_eq!(info["compute"]["logicalCores"].as_u64().unwrap(), 8);
    let kernels = info["compute"]["kernels"].as_array().unwrap();
    let components: Vec<&str> = kernels
        .iter()
        .map(|kernel| kernel["component"].as_str().unwrap())
        .collect();
    assert_eq!(
        components,
        [
            "YENC_DECODE",
            "YENC_CRC32",
            "PAR2_REPAIR",
            "PAR2_MD5",
            "PAR2_CRC32",
            "RAR_RECOVERY",
            "RAR_CRC32",
            "RAR_SHA1",
            "RAR_AES",
        ]
    );
    for kernel in kernels {
        let selected = kernel["kernel"].as_str().unwrap();
        assert!(
            kernel["ladder"]
                .as_array()
                .unwrap()
                .iter()
                .any(|rung| rung.as_str() == Some(selected)),
            "{kernel} selected a kernel off its ladder"
        );
        assert!(!kernel["library"].as_str().unwrap().is_empty());
    }
    assert_eq!(
        info["memory"]["effectiveLimitBytes"].as_u64().unwrap(),
        8 * 1024 * 1024 * 1024
    );
}

#[tokio::test]
async fn configured_storage_merges_equal_paths_and_keeps_unavailable_paths() {
    let h = TestHarness::new().await;
    let (data_dir, complete_dir, intermediate_dir, category_dir, missing_dir) = {
        let mut config = h.config.write().await;
        let data_dir = std::path::PathBuf::from(&config.data_dir);
        let complete_dir = data_dir.join("complete-custom");
        let intermediate_dir = data_dir.join("intermediate-custom");
        let category_dir = complete_dir.join("TV");
        let missing_dir = data_dir.join("missing-category");
        config.complete_dir = Some(complete_dir.display().to_string());
        config.intermediate_dir = Some(intermediate_dir.display().to_string());
        config.categories = vec![
            CategoryConfig {
                id: 1,
                name: "Movies".to_string(),
                dest_dir: Some(complete_dir.display().to_string()),
                aliases: String::new(),
            },
            CategoryConfig {
                id: 2,
                name: "TV".to_string(),
                dest_dir: None,
                aliases: String::new(),
            },
            CategoryConfig {
                id: 3,
                name: "Unavailable".to_string(),
                dest_dir: Some(missing_dir.display().to_string()),
                aliases: String::new(),
            },
        ];
        (
            data_dir,
            complete_dir,
            intermediate_dir,
            category_dir,
            missing_dir,
        )
    };
    for directory in [&data_dir, &complete_dir, &intermediate_dir, &category_dir] {
        std::fs::create_dir_all(directory).unwrap();
    }
    // The folders are set as a settings change sets them, and the answer is
    // the samplers' reading of them.
    h.handle
        .update_runtime_paths(
            data_dir.clone(),
            intermediate_dir.clone(),
            complete_dir.clone(),
        )
        .await
        .unwrap();
    wait_for_storage_readings(&h, &data_dir, &intermediate_dir, &complete_dir).await;

    let response = h.execute(SYSTEM_INFO_QUERY).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let storage = data["systemInfo"]["configuredStorage"].as_array().unwrap();

    let complete = storage
        .iter()
        .find(|entry| entry["path"].as_str() == Some(complete_dir.to_str().unwrap()))
        .unwrap();
    assert_eq!(
        complete["labels"].as_array().unwrap(),
        &["Complete library", "Category: Movies"]
    );
    assert!(complete["capacity"].is_object());
    assert!(complete["error"].is_null());

    let category = storage
        .iter()
        .find(|entry| entry["path"].as_str() == Some(category_dir.to_str().unwrap()))
        .unwrap();
    assert_eq!(category["labels"].as_array().unwrap(), &["Category: TV"]);
    assert!(category["capacity"].is_object());

    let missing = storage
        .iter()
        .find(|entry| entry["path"].as_str() == Some(missing_dir.to_str().unwrap()))
        .unwrap();
    assert!(missing["capacity"].is_null());
    assert!(missing["error"].as_str().unwrap().contains("unavailable"));
}

// Block, off the runtime, until each root is read at its directory.
async fn wait_for_storage_readings(
    h: &TestHarness,
    data_dir: &std::path::Path,
    intermediate_dir: &std::path::Path,
    complete_dir: &std::path::Path,
) {
    use weaver_server_core::operations::StorageRoot;
    let storage = h
        .shared_state
        .storage_capacity()
        .expect("the pipeline's samplers");
    let dirs = [
        (StorageRoot::Data, data_dir.to_path_buf()),
        (StorageRoot::Intermediate, intermediate_dir.to_path_buf()),
        (StorageRoot::Complete, complete_dir.to_path_buf()),
    ];
    tokio::task::spawn_blocking(move || {
        for (root, dir) in &dirs {
            storage.wait_for_root_read(*root, dir);
        }
    })
    .await
    .unwrap();
}

// A root's directory is answered from its sampler's reading, with no look
// on the request; any other configured folder is looked at on demand. The
// numbers are each folder's filesystem's.
#[tokio::test]
async fn storage_queries_answer_configured_folders_from_the_cache() {
    use weaver_server_core::operations::disk::disk_looks_for_test;
    let h = TestHarness::new().await;
    let (data_dir, intermediate_dir, complete_dir, category_dir) = {
        let mut config = h.config.write().await;
        let data_dir = std::path::PathBuf::from(&config.data_dir);
        let complete_dir = std::path::PathBuf::from(config.complete_dir());
        config.categories = vec![CategoryConfig {
            id: 1,
            name: "TV".to_string(),
            dest_dir: None,
            aliases: String::new(),
        }];
        (
            data_dir,
            std::path::PathBuf::from(config.intermediate_dir()),
            complete_dir.clone(),
            complete_dir.join("TV"),
        )
    };
    std::fs::create_dir_all(&category_dir).unwrap();
    wait_for_storage_readings(&h, &data_dir, &intermediate_dir, &complete_dir).await;
    // The roots' readings stay current however slowly the test runs.
    h.shared_state
        .storage_capacity()
        .expect("the pipeline's samplers")
        .stop_clock_for_test();

    let mut variables = async_graphql::Variables::default();
    variables.insert(
        async_graphql::Name::new("path"),
        async_graphql::Value::String(complete_dir.display().to_string()),
    );
    let looks = disk_looks_for_test();
    let usage = h
        .execute("{ diskUsage { label path totalBytes usedBytes freeBytes } }")
        .await;
    let picked = h
        .execute_with_variables(
            "query($path: String!) { pathStorage(path: $path) { path error capacity { totalBytes usedBytes freeBytes } } }",
            variables,
        )
        .await;
    assert_eq!(
        disk_looks_for_test(),
        looks,
        "the roots are answered with no look"
    );
    let info = h.execute(SYSTEM_INFO_QUERY).await;
    assert!(
        disk_looks_for_test() > looks,
        "the category folder is looked at on demand"
    );
    for response in [&info, &usage, &picked] {
        assert_no_errors(response);
    }

    let direct_total = |path: &std::path::Path| {
        weaver_server_core::operations::disk::read_space_for_test(path)
            .unwrap()
            .total_bytes
    };
    let info = response_data(&info);
    let storage = info["systemInfo"]["configuredStorage"].as_array().unwrap();
    for dir in [&data_dir, &intermediate_dir, &complete_dir, &category_dir] {
        let row = storage
            .iter()
            .find(|row| row["path"].as_str() == dir.to_str())
            .unwrap_or_else(|| panic!("a row for {}", dir.display()));
        assert!(row["error"].is_null(), "{row}");
        let capacity = &row["capacity"];
        let total = capacity["totalBytes"].as_u64().unwrap();
        assert_eq!(total, direct_total(dir), "{row}");
        assert_eq!(
            capacity["usedBytes"].as_u64().unwrap() + capacity["freeBytes"].as_u64().unwrap(),
            total
        );
    }
    let usage = response_data(&usage);
    let rows = usage["diskUsage"].as_array().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row["label"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["Data", "Intermediate downloads", "Complete library"]
    );
    for row in rows {
        let total = row["totalBytes"].as_u64().unwrap();
        let path = std::path::PathBuf::from(row["path"].as_str().unwrap());
        assert_eq!(total, direct_total(&path), "{row}");
        assert_eq!(
            row["usedBytes"].as_u64().unwrap() + row["freeBytes"].as_u64().unwrap(),
            total
        );
    }
    let picked = response_data(&picked);
    let picked = &picked["pathStorage"];
    assert_eq!(picked["path"].as_str(), complete_dir.to_str());
    assert!(picked["error"].is_null(), "{picked}");
    assert_eq!(
        picked["capacity"]["totalBytes"].as_u64().unwrap(),
        direct_total(&complete_dir)
    );
}

// A configured folder that does not exist yet: `systemInfo` reports its
// latest look failed, and `pathStorage` reports the disk it would be
// created on, its nearest existing parent's, as each always did.
#[tokio::test]
async fn a_configured_folder_not_made_yet_answers_as_before() {
    let h = TestHarness::new().await;
    let (data_dir, intermediate_dir, complete_dir, missing) = {
        let mut config = h.config.write().await;
        let data_dir = std::path::PathBuf::from(&config.data_dir);
        let missing = data_dir.join("not-made");
        config.categories = vec![CategoryConfig {
            id: 1,
            name: "Later".to_string(),
            dest_dir: Some(missing.display().to_string()),
            aliases: String::new(),
        }];
        (
            data_dir,
            std::path::PathBuf::from(config.intermediate_dir()),
            std::path::PathBuf::from(config.complete_dir()),
            missing,
        )
    };
    for dir in [&data_dir, &intermediate_dir, &complete_dir] {
        std::fs::create_dir_all(dir).unwrap();
    }
    wait_for_storage_readings(&h, &data_dir, &intermediate_dir, &complete_dir).await;

    let info = h.execute(SYSTEM_INFO_QUERY).await;
    assert_no_errors(&info);
    let info = response_data(&info);
    let row = info["systemInfo"]["configuredStorage"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["path"].as_str() == missing.to_str())
        .expect("a row for the folder")
        .clone();
    assert_eq!(row["labels"].as_array().unwrap(), &["Category: Later"]);
    assert!(row["capacity"].is_null(), "{row}");
    assert!(
        row["error"]
            .as_str()
            .unwrap()
            .starts_with("Filesystem capacity is unavailable for this path: "),
        "{row}"
    );

    let mut variables = async_graphql::Variables::default();
    variables.insert(
        async_graphql::Name::new("path"),
        async_graphql::Value::String(missing.display().to_string()),
    );
    let picked = h
        .execute_with_variables(
            "query($path: String!) { pathStorage(path: $path) { path error capacity { totalBytes usedBytes freeBytes } } }",
            variables,
        )
        .await;
    assert_no_errors(&picked);
    let picked = response_data(&picked);
    let picked = &picked["pathStorage"];
    assert_eq!(picked["path"].as_str(), missing.to_str());
    assert!(picked["error"].is_null(), "{picked}");
    assert_eq!(
        picked["capacity"]["totalBytes"].as_u64().unwrap(),
        weaver_server_core::operations::disk::read_space_for_test(&data_dir)
            .unwrap()
            .total_bytes
    );
}

#[tokio::test]
async fn path_storage_reports_the_disk_a_missing_folder_would_be_created_on() {
    let h = TestHarness::new().await;
    let data_dir = std::path::PathBuf::from(&h.config.read().await.data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let missing = data_dir.join("not-yet").join("complete");

    let mut variables = async_graphql::Variables::default();
    variables.insert(
        async_graphql::Name::new("path"),
        async_graphql::Value::String(missing.display().to_string()),
    );
    let response = h
        .execute_with_variables(
            "query($path: String!) { pathStorage(path: $path) { path error capacity { totalBytes freeBytes } } }",
            variables,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let storage = &data["pathStorage"];
    assert_eq!(storage["path"].as_str(), missing.to_str());
    assert!(storage["error"].is_null(), "{storage}");
    assert!(storage["capacity"]["totalBytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn path_storage_is_admin_only() {
    let h = TestHarness::new().await;
    let response = h
        .execute_as(r#"{ pathStorage(path: "/") { path } }"#, CallerScope::Read)
        .await;
    assert!(!response.errors.is_empty());
}

#[tokio::test]
async fn service_logs_carry_each_record_and_its_text_line() {
    use std::io::Write;
    let h = TestHarness::new().await;
    let record = r#"{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs","fields":{"message":"job added","job_id":7}}"#;
    let mut buffer = h.log_buffer.clone();
    buffer.write_all(format!("{record}\n").as_bytes()).unwrap();

    let response = h.execute("{ serviceLogs { records lines count } }").await;
    assert_no_errors(&response);
    let logs = &response_data(&response)["serviceLogs"];
    assert_eq!(logs["count"], 1);
    assert_eq!(logs["records"][0].as_str(), Some(record));
    assert_eq!(
        logs["lines"][0].as_str(),
        Some("2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: job added job_id=7")
    );
}
