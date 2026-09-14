//! Chasing the compressed member of the existing RARLAB mixed-member fixture.

use super::*;
use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;

#[tokio::test]
async fn mixed_chase_extracts_only_compressed_members_before_the_job_finishes() {
    run_chase(DirectStoreGate::Enabled, false).await;
}

#[tokio::test]
async fn conventional_chase_extracts_the_whole_set_before_the_job_finishes() {
    run_chase(DirectStoreGate::Disabled, false).await;
}

#[tokio::test]
async fn mixed_chase_invalidated_by_repair_uses_conventional_fallback() {
    run_chase(DirectStoreGate::Enabled, true).await;
}

#[tokio::test]
async fn solid_members_demote_direct_store_before_mixed_chase_can_skip_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let volumes = vec![("solid.rar".into(), rar5_fixture_bytes("rar5_solid.rar"))];
    let par2_bytes = par2_index_over_volumes(&volumes);
    let (spec, _) = par2_bearing_job_spec("Solid admission", &volumes, &par2_bytes);
    let job_id = JobId(41953);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for segment in 0..2 {
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .expect("candidate set was admitted")
            .is_demoted()
    );
}

#[tokio::test]
async fn dropping_the_pipeline_wakes_a_partial_rar_chase() {
    drop_partial_chase(false).await;
}

#[tokio::test]
async fn dropping_an_unpolled_consumer_wakes_a_partial_rar_chase() {
    drop_partial_chase(true).await;
}

#[tokio::test]
async fn rar_chase_rejects_unacceptable_extension_before_writing_payload() {
    for gate in [DirectStoreGate::Disabled, DirectStoreGate::Enabled] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(gate);
        pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
            gate: DirectUnpackGate::Enabled,
        });
        pipeline
            .db
            .save_post_processing_settings(&crate::post_processing::model::PostProcessingSettings {
                unacceptable_extensions: vec!["bin".into()],
                ..Default::default()
            })
            .unwrap();
        let volumes = vec![(
            "mixed.rar".into(),
            rar5_fixture_bytes("rar5_multifile_lz.rar"),
        )];
        let job_id = JobId(41952);
        let par2_bytes = par2_index_over_volumes(&volumes);
        let (spec, _) = par2_bearing_job_spec("Blocked chase", &volumes, &par2_bytes);
        insert_active_job(&mut pipeline, job_id, spec).await;
        for segment in 0..2 {
            submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
        }
        if gate == DirectStoreGate::Enabled {
            pipeline.update_mixed_rar_chase(job_id, 0);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                pipeline.reap_direct_unpack().await;
                if pipeline.direct_unpack.outcome(job_id, "mixed").is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let outcome = pipeline.direct_unpack.outcome(job_id, "mixed").unwrap();
        assert!(
            outcome
                .result
                .as_ref()
                .err()
                .expect("chase must reject the blocked member")
                .contains("unacceptable extension 'bin'")
        );
        assert!(!outcome.staging_dir.join("zeros_64k.bin").exists());
    }
}

#[tokio::test]
async fn rar_chase_settle_ignores_partial_topology_but_fences_reordered_parts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    // Only the first article is published. The worker remains behind coverage;
    // the test exercises roster identity, not the contents of the second part.
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let volumes = vec![
        ("fixture.part1.rar".into(), bytes.clone()),
        ("fixture.part2.rar".into(), bytes),
    ];
    let job_id = JobId(41954);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Roster", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "fixture")
        .unwrap();
    assert_eq!(coverage.part_count(), 2);
    let armed = pipeline.direct_unpack.counters().armed;
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([("fixture.part1.rar".into(), 0)]),
        complete_volumes: Default::default(),
        expected_volume_count: Some(2),
        members: Vec::new(),
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("fixture".into(), topology);
    pipeline.settle_direct_unpack_after_download(job_id);
    let survived = pipeline
        .direct_unpack
        .armed_coverage(job_id, "fixture")
        .is_some_and(|current| std::sync::Arc::ptr_eq(&current, &coverage));
    assert_eq!(pipeline.direct_unpack.counters().armed, armed);

    // A true rebind changes ordered identities even when the count is equal.
    pipeline
        .rar_sets
        .entry((job_id, "fixture".into()))
        .or_default()
        .volume_files = std::collections::BTreeMap::from([
        (0, "fixture.part2.rar".into()),
        (1, "fixture.part1.rar".into()),
    ]);
    pipeline.settle_direct_unpack_after_download(job_id);
    let fenced = coverage.abort_reason().is_some();
    pipeline.direct_unpack_shutdown("test teardown").await;
    assert!(
        survived,
        "an incomplete topology must preserve the original chase"
    );
    assert!(
        fenced,
        "same-count reordering must invalidate the original mapping"
    );
}

async fn drop_partial_chase(transfer_to_consumer: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let volumes = vec![(
        "mixed.rar".to_string(),
        rar5_fixture_bytes("rar5_multifile_lz.rar"),
    )];
    let job_id = JobId(41951);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Partial chase", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .unwrap();
    assert!(!coverage.part_is_complete(0));
    if transfer_to_consumer {
        let disposition = pipeline.take_direct_unpack_disposition(job_id, "mixed");
        assert!(matches!(
            disposition,
            crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(_)
        ));
        drop(disposition);
    }
    drop(pipeline);
    assert_eq!(
        coverage.abort_reason().as_deref(),
        Some(if transfer_to_consumer {
            "direct-unpack consumer dropped"
        } else {
            "direct-unpack owner dropped"
        })
    );
}

async fn run_chase(gate: DirectStoreGate, invalidate_for_repair: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(bytes.clone())).unwrap();
    let mut expected = std::collections::BTreeMap::new();
    for index in 0..archive.len() {
        let name = archive.member_info(index).unwrap().name;
        let mut output = Vec::new();
        archive
            .by_index(index)
            .unwrap()
            .copy_to(&mut output)
            .unwrap();
        expected.insert(name, output);
    }
    let volumes = vec![("mixed.rar".to_string(), bytes)];
    let par2_bytes = par2_index_over_volumes(&volumes);
    let job_id = JobId(41950);
    let (spec, index_file_index) = par2_bearing_job_spec("Mixed chase", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for segment in 0..2 {
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
    }
    let file = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    let set_name = pipeline
        .classified_archive_set_name_for_file(job_id, file)
        .unwrap();
    if gate == DirectStoreGate::Enabled {
        assert!(!pipeline.direct_store.set(job_id, 0).unwrap().is_demoted());
        pipeline.update_mixed_rar_chase(job_id, 0);
    } else {
        pipeline.try_arm_rar_chase(job_id, &set_name);
    }
    for _ in 0..3000 {
        pipeline.reap_direct_unpack().await;
        if pipeline.direct_unpack.outcome(job_id, &set_name).is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let outcome = pipeline
        .direct_unpack
        .outcome(job_id, &set_name)
        .unwrap_or_else(|| panic!(
            "RAR chase must finish while the PAR2 index is queued: armed={} latch={:?} coverage={:?}",
            pipeline.direct_unpack.is_armed(job_id, &set_name),
            pipeline.direct_unpack.latched_reason(job_id, &set_name),
            pipeline.direct_unpack.armed_coverage(job_id, &set_name).map(|coverage| (coverage.part_progress(0), coverage.abort_reason())),
        ));
    let chased: std::collections::BTreeSet<_> = outcome
        .result
        .as_ref()
        .unwrap()
        .extracted
        .iter()
        .cloned()
        .collect();
    if gate == DirectStoreGate::Enabled {
        assert_eq!(
            chased,
            std::collections::BTreeSet::from(["zeros_64k.bin".to_string()])
        );
    } else {
        assert_eq!(chased, expected.keys().cloned().collect());
    }
    let chased_bytes: u64 = chased.iter().map(|name| expected[name].len() as u64).sum();
    assert_eq!(outcome.total_bytes, chased_bytes);
    assert_eq!(outcome.completed_bytes, chased_bytes);
    assert_eq!(
        std::fs::read(outcome.staging_dir.join("zeros_64k.bin")).unwrap(),
        expected["zeros_64k.bin"]
    );
    assert_eq!(
        outcome.staging_dir.join("hello.txt").exists(),
        gate == DirectStoreGate::Disabled
    );
    assert_eq!(
        outcome.staging_dir.join("second.txt").exists(),
        gate == DirectStoreGate::Disabled
    );
    assert_eq!(
        working_dir.join("mixed.rar").exists(),
        gate == DirectStoreGate::Disabled
    );

    if invalidate_for_repair {
        pipeline.prepare_direct_unpack_for_par3_repair(job_id);
        pipeline.release_direct_unpack_after_repair(job_id);
        assert!(
            pipeline
                .direct_unpack
                .outcome(job_id, &set_name)
                .unwrap()
                .tainted,
            "repair must make speculative output unusable"
        );
    } else if gate == DirectStoreGate::Enabled {
        // A completed, verified chase no longer needs a decoder. Reducing
        // the allowance now must not make the tolerated ticket reopen and
        // reject the archive's dictionary instead of installing its output.
        let mut limits = (*pipeline.extraction_limits).clone();
        limits.max_memory_bytes = 1;
        pipeline.extraction_limits = std::sync::Arc::new(limits);
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = crate::DownloadQueue::new();
    state.recovery_queue = crate::DownloadQueue::new();
    pipeline.check_job_completion(job_id).await;
    drain_rar_refreshes(&mut pipeline).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_extractions_to_terminal(&mut pipeline, job_id, 64),
    )
    .await
    .unwrap_or_else(|_| panic!("{}", debug_job_state(&pipeline, job_id)));
    let output_root = complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Mixed chase"));
    for (name, bytes) in expected {
        assert_eq!(
            std::fs::read(output_root.join(&name)).unwrap(),
            bytes,
            "{name}"
        );
    }
    assert_eq!(
        pipeline.direct_unpack.counters().consumed,
        u64::from(!invalidate_for_repair)
    );
    assert_eq!(
        pipeline.direct_unpack.counters().discarded,
        u64::from(invalidate_for_repair)
    );
    assert!(
        !pipeline
            .direct_unpack_staging_dir(job_id, &set_name)
            .exists()
    );
}
