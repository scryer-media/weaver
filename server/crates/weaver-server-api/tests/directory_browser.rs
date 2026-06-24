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
    assert_eq!(
        data["browseDirectories"]["currentPath"].as_str().unwrap(),
        "/tmp"
    );
}

#[tokio::test]
async fn browse_nonexistent_path() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let missing_child = tempdir.path().join("missing").join("nested");

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"{{ browseDirectories(path: "{}") {{ currentPath parentPath }} }}"#,
            missing_child.to_string_lossy()
        ))
        .await;

    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(
        err_msg.contains("Directory does not exist"),
        "expected missing-directory error, got: {err_msg}"
    );
    assert!(
        err_msg.contains("INVALID_INPUT"),
        "expected INVALID_INPUT error, got: {err_msg}"
    );
}

#[tokio::test]
async fn browse_relative_path_rejected() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"{ browseDirectories(path: "relative/path") { currentPath } }"#)
        .await;

    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(
        err_msg.contains("path must be absolute"),
        "expected absolute-path error, got: {err_msg}"
    );
    assert!(
        err_msg.contains("INVALID_INPUT"),
        "expected INVALID_INPUT error, got: {err_msg}"
    );
}

#[tokio::test]
async fn browse_default_relative_complete_dir_is_resolved() {
    let cwd = std::env::current_dir().expect("failed to read current dir");
    let complete_dir = tempfile::tempdir_in(&cwd).expect("failed to create relative complete dir");
    let relative_complete_dir = complete_dir
        .path()
        .strip_prefix(&cwd)
        .expect("tempdir should be under cwd")
        .to_string_lossy()
        .to_string();

    let h = TestHarness::new().await;
    h.config.write().await.complete_dir = Some(relative_complete_dir);

    let resp = h.execute(r#"{ browseDirectories { currentPath } }"#).await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["browseDirectories"]["currentPath"].as_str().unwrap(),
        complete_dir.path().to_string_lossy().as_ref()
    );
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
    assert!(
        err_msg.contains("INVALID_INPUT"),
        "expected INVALID_INPUT error, got: {err_msg}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn browse_unreadable_path_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let unreadable = tempdir.path().join("unreadable");
    std::fs::create_dir(&unreadable).expect("failed to create unreadable dir");
    let mut permissions = std::fs::metadata(&unreadable).unwrap().permissions();
    permissions.set_mode(0o000);
    std::fs::set_permissions(&unreadable, permissions).unwrap();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"{{ browseDirectories(path: "{}") {{ currentPath }} }}"#,
            unreadable.to_string_lossy()
        ))
        .await;

    let mut permissions = std::fs::metadata(&unreadable).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&unreadable, permissions).unwrap();

    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(
        err_msg.contains("Directory is not readable"),
        "expected unreadable-directory error, got: {err_msg}"
    );
    assert!(
        err_msg.contains("INVALID_INPUT"),
        "expected INVALID_INPUT error, got: {err_msg}"
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
    assert!(
        parent.is_some(),
        "browsing a subdirectory should have a parentPath"
    );
    assert_eq!(parent.unwrap(), tempdir.path().to_string_lossy().as_ref());
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
    let names: Vec<&str> = entries
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["apple", "banana", "cherry"]);
}

#[tokio::test]
async fn create_directory_creates_folder_and_returns_listing() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let parent_path = tempdir.path().to_string_lossy().to_string();
    let created_path = tempdir.path().join("alpha");

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"mutation {{ createDirectory(path: "{parent_path}", name: "alpha") {{ currentPath parentPath entries {{ name path }} }} }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["createDirectory"]["currentPath"].as_str().unwrap(),
        created_path.to_string_lossy().as_ref()
    );
    assert_eq!(
        data["createDirectory"]["parentPath"].as_str().unwrap(),
        tempdir.path().to_string_lossy().as_ref()
    );
    assert!(
        data["createDirectory"]["entries"]
            .as_array()
            .is_some_and(|entries| entries.is_empty())
    );
    assert!(created_path.is_dir());
}

#[tokio::test]
async fn create_directory_rejects_duplicate_name() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    std::fs::create_dir(tempdir.path().join("alpha")).expect("failed to create existing dir");
    let parent_path = tempdir.path().to_string_lossy().to_string();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"mutation {{ createDirectory(path: "{parent_path}", name: "alpha") {{ currentPath }} }}"#
        ))
        .await;

    assert_has_errors(&resp);
    let err_msg = resp
        .errors
        .first()
        .map(|error| error.message.as_str())
        .unwrap_or("");
    assert!(
        err_msg.contains("already exists"),
        "expected duplicate directory error, got: {err_msg}"
    );
}

#[tokio::test]
async fn create_directory_rejects_invalid_name() {
    let tempdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let parent_path = tempdir.path().to_string_lossy().to_string();

    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            r#"mutation {{ createDirectory(path: "{parent_path}", name: "nested/child") {{ currentPath }} }}"#
        ))
        .await;

    assert_has_errors(&resp);
    let err_msg = resp
        .errors
        .first()
        .map(|error| error.message.as_str())
        .unwrap_or("");
    assert!(
        err_msg.contains("path separators"),
        "expected invalid folder name error, got: {err_msg}"
    );
}
