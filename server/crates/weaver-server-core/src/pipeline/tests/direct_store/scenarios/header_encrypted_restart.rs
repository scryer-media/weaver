// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A header-encrypted (`-hp`) set across a restart.
//
// Restoring a set rebuilds its layout from cached volume facts, and that
// rebuild re-runs the header parse — which, for `-hp`, is where admission
// happens and where the archive key is proved. The password itself is never
// persisted, so the restore seam has to hand the set's header gate the same
// candidates the live seam would have: the job spec's password and the job's
// harvest (NZB meta, file-name convention, the global list). A gate with an
// empty candidate ring refuses under a sticky `NoPassword`, and the set throws
// its checkpoint away and downloads every volume again.

use super::*;

const HP_RESTART_PASSWORD: &str = "cobalt-lighthouse";

const HP_RESTART_ARTICLES: usize = 2;

// The "before" half with the job's persisted NZB chosen, so the harvest the
// restore reads back out of the database carries whatever the NZB carries.
async fn hp_before_restart(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
) -> std::path::PathBuf {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec =
        direct_store_job_spec_with_articles("Silver Horizon", volumes, HP_RESTART_ARTICLES);
    spec.password = spec_password.map(str::to_owned);
    let working_dir =
        insert_active_job_with_persisted_nzb_named(&mut pipeline, job_id, spec, nzb_zstd, None)
            .await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            volumes,
            *file_index,
            *segment_number,
            HP_RESTART_ARTICLES,
        )
        .await;
    }
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    retire_pipeline_database(pipeline).await;
    working_dir
}

#[tokio::test]
async fn header_restore_uses_a_recorded_password_before_installing_the_actor_spec() {
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = header_encrypted_store_set(
        "payload.bin",
        &payload,
        3,
        HP_RESTART_PASSWORD,
        HeaderCheck::For(HP_RESTART_PASSWORD),
    );
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45193);
    let working_dir = hp_before_restart(
        &temp_dir,
        job_id,
        &volumes,
        &[(0, 0), (0, 1), (1, 0)],
        Some(HP_RESTART_PASSWORD),
        sample_nzb_zstd(),
    )
    .await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec =
        direct_store_job_spec_with_articles("Silver Horizon", &volumes, HP_RESTART_ARTICLES);
    spec.password = Some("stale-restore-key".into());
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: Default::default(),
            file_progress: Default::default(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            extracted_members: Default::default(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();
    handle
        .update_job(
            job_id,
            crate::JobUpdate {
                password: crate::jobs::FieldUpdate::Set(HP_RESTART_PASSWORD.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!pipeline.jobs.contains_key(&job_id));

    pipeline.apply_queue_intents().await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("restored direct set");
    assert!(
        !set.is_demoted(),
        "recorded password must re-prove the header key: {set:?}"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "the correct password must preserve the direct checkpoint"
    );
    assert!(
        !peek_queued_segments(&mut pipeline, job_id)
            .iter()
            .any(|(file_index, _)| *file_index == 0)
    );
}

// Restarts a `-hp` job with volume 0 complete and asserts the checkpoint
// survived: the set is live, seeded from its coverage, and nothing of volume
// 0 is fetched again.
async fn assert_hp_set_survives_restart(
    job_id: JobId,
    member_name: &str,
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
) {
    let payload: Vec<u8> = (0..6000u32).map(|index| (index % 247) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        HP_RESTART_PASSWORD,
        HeaderCheck::For(HP_RESTART_PASSWORD),
    );
    let temp_dir = tempfile::tempdir().unwrap();
    // Volume 0 whole, volume 1 half: a restart that honours the checkpoint
    // skips the first entirely.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];
    let working_dir = hp_before_restart(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        spec_password,
        nzb_zstd,
    )
    .await;

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        HP_RESTART_ARTICLES,
        &working_dir,
        spec_password,
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored `-hp` job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that still has the password must re-admit the `-hp` set, got {set:?}"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "the `-hp` header parse must admit at restore, so the checkpoint is kept rather \
         than refused under `NoPassword`, got {set:?}"
    );
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, \
         got {queued:?}"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            HP_RESTART_ARTICLES,
        )
        .await;
    }
    // The restored coverage is re-read off the pipeline task before its
    // member gates can compose; settle that read the way the select loop does.
    settle_direct_post_repair_work(&mut pipeline).await;
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a restarted `-hp` set must finish byte-identical to an uninterrupted one"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "a restarted `-hp` set must never materialize a source volume"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_keyed_from_the_job_spec_survives_a_restart() {
    assert_hp_set_survives_restart(
        JobId(46101),
        "Silver.Horizon.S05E01.mkv",
        Some(HP_RESTART_PASSWORD),
        sample_nzb_zstd(),
    )
    .await;
}

#[tokio::test]
async fn a_header_encrypted_set_keyed_from_nzb_meta_survives_a_restart() {
    // No spec password on either side of the restart: the key lives only in
    // the persisted NZB's `<meta type="password">`, which the restore has to
    // harvest itself because the job state it would otherwise cache it on does
    // not exist yet.
    assert_hp_set_survives_restart(
        JobId(46102),
        "Silver.Horizon.S05E02.mkv",
        None,
        sample_nzb_zstd_with_password(HP_RESTART_PASSWORD),
    )
    .await;
}

#[tokio::test]
async fn a_header_encrypted_set_whose_spec_password_lost_to_nzb_meta_survives_a_restart() {
    // The spec holds an operator's guess and the NZB meta holds the key. The
    // live parse proves the meta candidate and binds it as the file key; a
    // restore that re-admitted members against the spec alone would refuse
    // them as a wrong password and redownload the set.
    assert_hp_set_survives_restart(
        JobId(46103),
        "Silver.Horizon.S05E03.mkv",
        Some("not-the-password"),
        sample_nzb_zstd_with_password(HP_RESTART_PASSWORD),
    )
    .await;
}
