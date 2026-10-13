use std::fs;

use super::instances::{InstanceTrigger, ScriptInstanceDraft};
use super::model::ScriptName;
use super::settings::{ScriptDirectoryError, normalize_script_directory};
use crate::Database;

#[test]
fn script_directory_seeds_once_and_the_database_wins_afterward() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let data_dir = root.path().join("data");
    let from_env = root.path().join("from-env");
    let later_env = root.path().join("later-env");

    let first = db
        .initialize_post_processing_script_directory(&data_dir, Some(&from_env))
        .unwrap();
    assert_eq!(first, fs::canonicalize(&from_env).unwrap());
    assert!(first.is_dir());

    let second = db
        .initialize_post_processing_script_directory(&data_dir, Some(&later_env))
        .unwrap();
    assert_eq!(second, first);
    assert!(!later_env.exists());
}

#[test]
fn changing_script_directory_turns_every_instance_off_and_leaves_files_alone() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let old_root = normalize_script_directory(&root.path().join("old")).unwrap();
    let new_root = normalize_script_directory(&root.path().join("new")).unwrap();
    fs::write(old_root.join("notify.sh"), "#!/bin/sh\n").unwrap();
    let script = ScriptName::new("notify.sh").unwrap();
    db.replace_post_processing_script_directory(&old_root)
        .unwrap();
    let instance = db
        .create_script_instance(
            ScriptInstanceDraft::new(script, InstanceTrigger::PostProcessing)
                .input("token", "configured"),
        )
        .unwrap();
    assert!(instance.enabled);

    assert!(
        db.replace_post_processing_script_directory(&new_root)
            .unwrap()
    );
    assert_eq!(db.post_processing_script_directory().unwrap(), new_root);
    let (_, admitted_root) = db.post_processing_script_admission().unwrap();
    assert_eq!(admitted_root, new_root);
    // A name means a different file under a different directory, so nothing
    // runs until the operator turns it back on. What was set up is kept.
    let kept = db.script_instance(&instance.id).unwrap().unwrap();
    assert!(!kept.enabled);
    assert_eq!(kept.inputs, instance.inputs);
    assert!(old_root.join("notify.sh").is_file());

    assert!(
        !db.replace_post_processing_script_directory(&new_root)
            .unwrap()
    );
}

#[test]
fn scripts_directory_must_be_absolute_and_a_directory() {
    assert!(matches!(
        normalize_script_directory(std::path::Path::new("relative/scripts")),
        Err(ScriptDirectoryError::RelativePath)
    ));

    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("not-a-directory");
    fs::write(&file, "nope").unwrap();
    assert!(matches!(
        normalize_script_directory(&file),
        Err(ScriptDirectoryError::Io(_)) | Err(ScriptDirectoryError::NotDirectory(_))
    ));
}
