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
