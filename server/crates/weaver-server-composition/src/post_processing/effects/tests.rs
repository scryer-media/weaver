// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::post_processing::model::PipelineOutcome;
use crate::post_processing::runner::CompatibilityFacts;
use weaver_server_settings::categories::CategoryConfig;
use weaver_server_settings::categories::persistence::CategoriesPersistenceQueries;

#[test]
fn directory_directives_cannot_claim_shared_destination_roots() {
    let data = tempfile::tempdir().unwrap();
    let complete = data.path().join("complete");
    let category = complete.join("shared").join("tv");
    let default_category = complete.join("movies");
    let future_parent = complete.join("future");
    std::fs::create_dir_all(&future_parent).unwrap();
    let job_output = category.join("job");
    std::fs::create_dir_all(&job_output).unwrap();
    std::fs::create_dir_all(&default_category).unwrap();
    let db = Database::open_in_memory().unwrap();
    for category in [
        CategoryConfig {
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: false,
            unpack: None,
            id: 1,
            name: "tv".into(),
            dest_dir: Some(category.to_string_lossy().into_owned()),
            aliases: String::new(),
        },
        CategoryConfig {
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: false,
            unpack: None,
            id: 2,
            name: "movies".into(),
            dest_dir: None,
            aliases: String::new(),
        },
        CategoryConfig {
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: false,
            unpack: None,
            id: 3,
            name: "future".into(),
            dest_dir: Some(future_parent.join("tv").to_string_lossy().into_owned()),
            aliases: String::new(),
        },
    ] {
        db.insert_category(&category).unwrap();
    }
    let context = JobExecutionContext {
        job_id: 1,
        name: "job".into(),
        nzb_filename: "job.nzb".into(),
        category: Some("tv".into()),
        group: None,
        source_url: None,
        working_directory: data.path().into(),
        final_directory: data.path().into(),
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: CompatibilityFacts {
            complete_dir: Some(complete.clone()),
            ..Default::default()
        },
    };
    for shared in [
        &complete,
        &category,
        &default_category,
        &complete.join("shared"),
        &future_parent,
    ] {
        assert!(
            validate_directory(&db, &context, shared)
                .unwrap_err()
                .contains("configured complete or category root"),
            "shared root must never become job-owned: {}",
            shared.display()
        );
    }
    assert_eq!(
        validate_directory(&db, &context, &job_output).unwrap(),
        job_output.canonicalize().unwrap()
    );
    let current = category.join("current-job");
    let relocated = category.join("relocated-job");
    std::fs::create_dir(&current).unwrap();
    std::fs::write(current.join("job.bin"), b"job").unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&current, crate::JobId(1)).unwrap();
    let mut context = context;
    context.working_directory = current.clone();
    context.final_directory = current.clone();
    std::fs::write(job_output.join("unrelated.bin"), b"unrelated").unwrap();
    for directive in [
        Directive::Directory(job_output.to_string_lossy().into_owned()),
        Directive::FinalDirectory(job_output.to_string_lossy().into_owned()),
    ] {
        assert!(
            apply_job_directive(&db, &mut context, directive)
                .unwrap_err()
                .contains("current or relocated owned output")
        );
        assert_eq!(context.working_directory, current);
        assert!(context.compatibility.final_directory_override.is_none());
        assert_eq!(
            std::fs::read(job_output.join("unrelated.bin")).unwrap(),
            b"unrelated"
        );
        assert!(
            !job_output
                .join(crate::jobs::working_dir::OUTPUT_DIR_MARKER)
                .exists()
        );
    }
    std::fs::rename(&current, &relocated).unwrap();
    assert_eq!(
        validate_postprocessing_directory(&db, &context, &relocated).unwrap(),
        relocated.canonicalize().unwrap()
    );
    assert_eq!(
        crate::jobs::working_dir::output_dir_owner(&relocated),
        Some(crate::jobs::working_dir::OutputDirOwner::Job(crate::JobId(
            1
        )))
    );
    assert_eq!(std::fs::read(relocated.join("job.bin")).unwrap(), b"job");
}
