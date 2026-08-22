use std::fs;

use super::model::{
    OptionName, OptionValue, ResolvedOption, ScriptList, ScriptListEntry, ScriptLists, ScriptName,
};
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
fn changing_script_directory_clears_name_based_configuration_but_not_files() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let old_root = normalize_script_directory(&root.path().join("old")).unwrap();
    let new_root = normalize_script_directory(&root.path().join("new")).unwrap();
    fs::write(old_root.join("notify.sh"), "#!/bin/sh\n").unwrap();
    let script = ScriptName::new("notify.sh").unwrap();
    db.replace_post_processing_script_directory(&old_root)
        .unwrap();
    db.save_post_processing_script_lists(&ScriptLists {
        global: ScriptList::new(vec![ScriptListEntry::new(script.clone())]).unwrap(),
        categories: Default::default(),
    })
    .unwrap();
    db.save_post_processing_script_options(
        &script,
        &[ResolvedOption::new(
            OptionName::new("token").unwrap(),
            OptionValue::String("configured".to_string()),
        )],
    )
    .unwrap();

    assert!(
        db.replace_post_processing_script_directory(&new_root)
            .unwrap()
    );
    assert_eq!(db.post_processing_script_directory().unwrap(), new_root);
    let (_, admitted_lists, admitted_root, _) = db.post_processing_script_admission().unwrap();
    assert_eq!(admitted_root, new_root);
    assert!(admitted_lists.global.entries().is_empty());
    assert!(
        db.post_processing_script_lists()
            .unwrap()
            .global
            .entries()
            .is_empty()
    );
    assert!(
        db.post_processing_script_options(&script)
            .unwrap()
            .is_empty()
    );
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
