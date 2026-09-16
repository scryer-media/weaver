use super::*;

#[tokio::test]
async fn new_chases_cannot_join_a_repair_after_the_vouching_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(41922);
    let set = "generated_split_store_plain.7z";
    let files = sevenz_fixture_bytes(set);
    insert_active_job(&mut pipeline, job, rar_job_spec("Late Chase", &files)).await;
    pipeline.decide_direct_unpack_before_repair(job, None);
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job, index as u32, name, bytes).await;
    }
    assert!(!pipeline.direct_unpack.is_armed(job, set));
    assert_eq!(pipeline.direct_unpack.counters().armed, 0);
    pipeline.settle_direct_unpack_after_repair(job, true, &Ok(repaired_outcome()));
    pipeline.try_arm_direct_unpack_for_file(
        job,
        NzbFileId {
            job_id: job,
            file_index: 0,
        },
    );
    assert!(pipeline.direct_unpack.is_armed(job, set));
    pipeline
        .direct_unpack_shutdown("late chase test teardown")
        .await;
}

#[tokio::test]
async fn repair_decision_freezes_consumption_until_handback() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(41920);
    let set = "generated_split_store_plain.7z";
    armed_chase_with_real_par2(&mut pipeline, job, set, Clone::clone).await;
    let coverage = pipeline.direct_unpack.armed_coverage(job, set).unwrap();
    let generation = coverage.part_progress(0).unwrap().rewritten;
    pipeline.decide_direct_unpack_before_repair(job, None);
    assert!(pipeline.direct_unpack.is_armed(job, set));
    assert!(!coverage.commit_read(0, 0, 1, generation).unwrap());
    pipeline.settle_direct_unpack_after_repair(job, true, &Ok(repaired_outcome()));
    let generation = coverage.part_progress(0).unwrap().rewritten;
    assert!(coverage.commit_read(0, 0, 1, generation).unwrap());
    pipeline
        .direct_unpack_shutdown("repair barrier test teardown")
        .await;
}

#[tokio::test]
async fn repair_geometry_change_taints_running_and_completed_chases_even_with_vouched_bytes() {
    for finished in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        enable_direct_unpack(&mut pipeline);
        let job = JobId(41921);
        let set = "generated_split_store_plain.7z";
        let mut files = sevenz_fixture_bytes(set);
        // End the first part on a complete PAR2 block. Redistribute the tail
        // into the next part without changing the valid concatenated archive.
        let mut tail = files[0].1.split_off(2 * VOUCH_SLICE as usize);
        tail.extend_from_slice(&files[1].1);
        files[1].1 = tail;
        insert_active_job(&mut pipeline, job, rar_job_spec("Repair Geometry", &files)).await;
        for (index, (name, bytes)) in files.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job, index as u32, name, bytes).await;
        }
        if finished {
            reap_until_outcome(&mut pipeline, job, set).await;
        } else {
            assert!(pipeline.direct_unpack.is_armed(job, set));
        }
        let mut described = files.clone();
        // An additional block changes the true length without invalidating
        // the verified blocks covering every byte of the posted first part.
        // A prefix-only vouch incorrectly accepts both kinds of outcome.
        described[0].1.push(0);
        let inputs: Vec<_> = described
            .iter()
            .map(|(n, b)| (n.as_str(), b.as_slice()))
            .collect();
        let par2 = build_repairable_par2_set_for_files(&inputs, VOUCH_SLICE, 4);
        install_test_par2_runtime(&mut pipeline, job, par2, &[]);
        let plan = pipeline.par2_checkpoint_plan(job);
        for (index, (_, bytes)) in files.iter().enumerate() {
            let id = NzbFileId {
                job_id: job,
                file_index: index as u32,
            };
            feed_block_aligned_articles(&mut pipeline, id, bytes, &plan);
            assert!(pipeline.in_stream_intact_prefix(id).unwrap() >= bytes.len() as u64);
        }
        pipeline.decide_direct_unpack_before_repair(job, None);
        assert!(!pipeline.direct_unpack.is_armed(job, set));
        if finished {
            assert!(pipeline.direct_unpack.outcome(job, set).unwrap().tainted);
        } else {
            assert_eq!(pipeline.direct_unpack.counters().demoted_repair_rewrote, 1);
        }
        pipeline
            .direct_unpack_shutdown("repair geometry test teardown")
            .await;
    }
}
