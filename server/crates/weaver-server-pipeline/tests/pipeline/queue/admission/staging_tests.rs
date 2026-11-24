// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn one_device_stages_nothing() {
    let root = tempfile::tempdir().expect("temp dir");
    let intermediate = root.path().join("intermediate");
    let complete = root.path().join("complete");
    assert_eq!(
        delivery_staging_dir(&intermediate, &complete, JobId(7)),
        None
    );
}

#[test]
fn two_devices_stage_under_the_destination() {
    let root = tempfile::tempdir().expect("temp dir");
    let intermediate = root.path().join("intermediate");
    let complete = root.path().join("complete");
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    assert_eq!(
        delivery_staging_dir(&intermediate, &complete, JobId(7)),
        Some(complete.join(".weaver-staging").join("7"))
    );
}

fn category(
    name: &str,
    dest_dir: Option<&Path>,
) -> weaver_server_settings::categories::CategoryConfig {
    weaver_server_settings::categories::CategoryConfig {
        id: 1,
        name: name.to_string(),
        dest_dir: dest_dir.map(|dir| dir.display().to_string()),
        aliases: String::new(),
        upload_cap: Default::default(),
        seeding: None,
        torrent_route: false,
        unpack: None,
    }
}

// A category with its own folder on another device stages there, never
// on the complete directory's device; one on the intermediate device
// stages nothing.
#[test]
fn a_category_folder_decides_where_the_job_stages() {
    let root = tempfile::tempdir().expect("temp dir");
    let intermediate = root.path().join("intermediate");
    let complete = root.path().join("complete");
    let movies = root.path().join("movies");
    let beside = root.path().join("beside");
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
    weaver_server_disk::runtime::fs::test_devices::assign(&beside, 1);
    let categories = vec![
        category("movies", Some(&movies)),
        category("beside", Some(&beside)),
        category("tv", None),
    ];
    let stage = |name: Option<&str>| {
        delivery_staging_dir(
            &intermediate,
            &destination_root(&complete, &categories, name),
            JobId(7),
        )
    };
    assert_eq!(
        stage(Some("movies")),
        Some(movies.join(".weaver-staging").join("7"))
    );
    assert_eq!(stage(Some("beside")), None);
    assert_eq!(
        stage(Some("tv")),
        Some(complete.join("tv").join(".weaver-staging").join("7"))
    );
    assert_eq!(
        stage(None),
        Some(complete.join(".weaver-staging").join("7"))
    );
    // A category that cannot be resolved stages under the complete
    // directory.
    assert_eq!(
        stage(Some("../outside")),
        Some(complete.join(".weaver-staging").join("7"))
    );
    assert_eq!(
        destination_roots(&complete, &categories),
        vec![complete.clone(), movies, beside, complete.join("tv")]
    );
}

#[test]
fn a_staging_directory_is_made_and_marked_the_job_s() {
    let complete = tempfile::tempdir().expect("temp dir");
    let staging = staging_dir_for(complete.path(), JobId(9));
    make_staging_dir(&staging, JobId(9)).expect("staging");
    assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
        &staging
    ));
    assert!(
        crate::jobs::working_dir::is_job_working_dir(
            &complete.path().join(".weaver-staging"),
            &staging,
            JobId(9)
        )
        .expect("marker")
    );
}
