mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

#[tokio::test]
async fn browse_specific_path() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"{ browseDirectories(path: "/tmp") { currentPath parentPath entries { name path } } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["browseDirectories"]["currentPath"].as_str().unwrap(), "/tmp");
}

#[tokio::test]
async fn browse_nonexistent_path() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"{ browseDirectories(path: "/nonexistent_weaver_test_path") { currentPath } }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn browse_file_not_dir() {
    let tempfile = tempfile::NamedTempFile::new().expect("failed to create temp file");
    let path = tempfile.path().to_string_lossy().to_string();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"{{ browseDirectories(path: "{path}") {{ currentPath }} }}"#
        ))
        .await;
    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(
        err_msg.contains("not a directory"),
        "expected 'not a directory' error, got: {err_msg}"
    );
}

#[tokio::test]
async fn browse_has_parent_path() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let subdir = tempdir.path().join("child");
    std::fs::create_dir(&subdir).expect("failed to create subdir");
    let subdir_path = subdir.to_string_lossy().to_string();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"{{ browseDirectories(path: "{subdir_path}") {{ currentPath parentPath }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let parent = data["browseDirectories"]["parentPath"].as_str();
    assert!(parent.is_some(), "browsing a subdirectory should have a parentPath");
    assert_eq!(
        parent.unwrap(),
        tempdir.path().to_string_lossy().as_ref()
    );
}

#[tokio::test]
async fn browse_entries_sorted() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    std::fs::create_dir(tempdir.path().join("banana")).unwrap();
    std::fs::create_dir(tempdir.path().join("apple")).unwrap();
    std::fs::create_dir(tempdir.path().join("cherry")).unwrap();
    let dir_path = tempdir.path().to_string_lossy().to_string();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"{{ browseDirectories(path: "{dir_path}") {{ entries {{ name }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let entries = data["browseDirectories"]["entries"].as_array().unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["apple", "banana", "cherry"]);
}
