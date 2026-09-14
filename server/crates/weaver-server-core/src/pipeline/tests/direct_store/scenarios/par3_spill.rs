use super::*;
use crate::pipeline::direct_store::{DirectStoreSettings, wiring::DirectStoreRuntime};
use crate::pipeline::repair::par3::work::Coordinator;
use par3_rs::source::SourceId;

#[tokio::test]
async fn par3_spill_preserves_held_articles_and_sparse_holes_without_a_layout() {
    spill_held_article(false).await;
}

#[tokio::test]
async fn par3_spill_reports_insufficient_disk_without_discarding_received_bytes() {
    spill_held_article(true).await;
}

#[tokio::test]
async fn par3_spill_preserves_encrypted_boundary_holds_as_posted_bytes() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(41972);
    let payload: Vec<u8> = (0..2405u32).map(|i| (i % 197) as u8).collect();
    let volumes = encrypted_store_set(
        "cipher.mkv",
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let par2 = par2_index_over_volumes(&volumes);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        gate: DirectStoreGate::Enabled,
        holds_disk_reserve_bytes: 0,
        ..Default::default()
    });
    let (mut spec, _) = par2_bearing_job_spec("PAR3 cipher spill", &volumes, &par2);
    spec.password = Some("moonlit-harbour".into());
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0, 0), (0, 1), (1, 1)] {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(set.router.routes_encrypted());
    assert_ne!(
        set.volume_coverage(0),
        set.volume_coverage_with_holds(0),
        "an encrypted edge must still be held across the lost article"
    );
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        std::sync::Arc::clone(&pipeline.metrics),
    );
    coordinator.force_spill(job_id, SourceId(0));
    pipeline.par3_runtime = Some(Box::new(coordinator));
    assert!(pipeline.spill_par3_source(job_id).await);
    settle_direct_post_repair_work(&mut pipeline).await;
    assert_eq!(
        std::fs::read(working.join(&volumes[0].0)).unwrap(),
        volumes[0].1
    );
    let (start, end) = article_extent(volumes[1].1.len(), 1, 2);
    let second = std::fs::read(working.join(&volumes[1].0)).unwrap();
    assert_eq!(&second[start..end], &volumes[1].1[start..end]);
    let queued = queued_segments(&mut pipeline, job_id);
    assert!(queued.contains(&(1, 0)));
    assert!(!queued.contains(&(0, 0)) && !queued.contains(&(0, 1)) && !queued.contains(&(1, 1)));
}

async fn spill_held_article(deny_disk: bool) {
    let root = TempDir::new().unwrap();
    let job_id = JobId(41971);
    let volumes = demotion_fixture_volumes("held.mkv");
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        gate: DirectStoreGate::Enabled,
        holds_disk_reserve_bytes: if deny_disk { u64::MAX } else { 0 },
        ..Default::default()
    });
    let spec = direct_store_job_spec("PAR3 held spill", &volumes);
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    let segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 1,
    };
    take_queued_segment(&mut pipeline, job_id, segment);
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(set.router.member_partials().is_empty());
    let (start, end) = article_extent(volumes[0].1.len(), 1, 2);
    assert_eq!(
        set.volume_coverage_with_holds(0).ranges(),
        &[(start as u64, end as u64)]
    );
    assert!(!working.join(&volumes[0].0).exists());
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        std::sync::Arc::clone(&pipeline.metrics),
    );
    coordinator.force_spill(job_id, SourceId(0));
    pipeline.par3_runtime = Some(Box::new(coordinator));
    assert!(pipeline.spill_par3_source(job_id).await);
    if deny_disk {
        assert!(matches!(job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { error }) if error.contains("disk fallback")));
        assert!(!working.join(&volumes[0].0).exists());
        assert!(!pipeline.direct_demotion_in_flight.contains_key(&job_id));
        return;
    }
    settle_direct_post_repair_work(&mut pipeline).await;
    let restored = std::fs::read(working.join(&volumes[0].0)).unwrap();
    assert_eq!(&restored[start..end], &volumes[0].1[start..end]);
    assert!(restored[..start].iter().all(|byte| *byte == 0));
    let queued = queued_segments(&mut pipeline, job_id);
    assert!(
        queued.contains(&(0, 0)),
        "the missing header remains a hole"
    );
    assert!(
        !queued.contains(&(0, 1)),
        "received held bytes must not be refetched"
    );
    assert!(
        !pipeline.spill_par3_source(job_id).await,
        "one refusal must not retry unchanged admission"
    );
}
