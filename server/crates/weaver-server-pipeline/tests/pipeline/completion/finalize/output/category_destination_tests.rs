// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn category(
    name: &str,
    dest_dir: Option<&str>,
) -> weaver_server_settings::categories::CategoryConfig {
    weaver_server_settings::categories::CategoryConfig {
        upload_cap: Default::default(),
        seeding: None,
        torrent_route: false,
        unpack: None,
        id: 1,
        name: name.to_string(),
        dest_dir: dest_dir.map(str::to_string),
        aliases: String::new(),
    }
}

#[test]
fn safe_categories_and_collision_suffixes_remain_beneath_complete_dir() {
    let complete = std::path::Path::new("/downloads/complete");
    let parent = complete_parent_for_category(complete, &[], Some("tv-hd")).unwrap();
    assert_eq!(parent, complete.join("tv-hd"));

    let collision = parent.join(weaver_model::files::path_component_with_suffix(
        "release", ".#42.1",
    ));
    assert!(collision.starts_with(complete));
}

#[tokio::test]
async fn script_destination_worker_refuses_foreign_ownership_and_path_changes() {
    use crate::post_processing::model::PipelineOutcome;
    use crate::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

    let temp = tempfile::tempdir().unwrap();
    let complete = temp.path().canonicalize().unwrap();
    let source = complete.join("source");
    let destination = complete.join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(source.join("payload.bin"), b"payload").unwrap();
    let db = crate::Database::open_in_memory().unwrap();
    let context = JobExecutionContext {
        job_id: 1,
        name: "destination-check".into(),
        nzb_filename: "destination-check.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: source.clone(),
        final_directory: destination.clone(),
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: CompatibilityFacts {
            complete_dir: Some(complete.clone()),
            ..Default::default()
        },
    };
    validate_script_move_destination(db.clone(), context.clone(), &destination)
        .await
        .unwrap();
    let missing = complete.join("missing");
    let error = validate_script_move_destination(db.clone(), context.clone(), &missing)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MoveToCompleteFailure::Operational(message) if message.contains("does not exist"))
    );
    crate::jobs::working_dir::mark_weaver_owned_working_dir(&complete, &destination, JobId(2))
        .unwrap();
    std::fs::write(destination.join("foreign.bin"), b"foreign").unwrap();
    let error = validate_script_move_destination(db.clone(), context.clone(), &destination)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MoveToCompleteFailure::Security(message) if message.contains("foreign ownership marker"))
    );
    assert_eq!(
        std::fs::read(source.join("payload.bin")).unwrap(),
        b"payload"
    );
    assert_eq!(
        std::fs::read(destination.join("foreign.bin")).unwrap(),
        b"foreign"
    );
    assert!(!destination.join("payload.bin").exists());

    #[cfg(unix)]
    {
        let actual = complete.join("actual");
        let alias = complete.join("alias");
        std::fs::create_dir(&actual).unwrap();
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        let error = validate_script_move_destination(db, context, &alias)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MoveToCompleteFailure::Security(message) if message.contains("changed after reservation"))
        );
        assert!(actual.exists());
        assert!(
            std::fs::symlink_metadata(alias)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}

#[test]
fn configured_destination_override_remains_trusted_admin_input() {
    let complete = std::path::Path::new("/downloads/complete");
    let categories = vec![category("custom/name", Some("/mnt/admin-selected"))];

    assert_eq!(
        complete_parent_for_category(complete, &categories, Some("custom/name")).unwrap(),
        PathBuf::from("/mnt/admin-selected")
    );
}

#[test]
fn malicious_or_legacy_unsafe_categories_fail_before_a_destination_is_returned() {
    let complete = std::path::Path::new("/downloads/complete");
    for unsafe_category in ["/tmp", "../../outside", "nested/path", "C:\\outside"] {
        assert!(
            complete_parent_for_category(complete, &[], Some(unsafe_category)).is_err(),
            "accepted {unsafe_category:?}"
        );
    }

    let legacy = vec![category("legacy/unsafe", None)];
    assert!(complete_parent_for_category(complete, &legacy, Some("legacy/unsafe")).is_err());
}
