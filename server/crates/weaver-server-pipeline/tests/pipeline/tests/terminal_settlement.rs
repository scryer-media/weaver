// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Delivery claims, including terminal classifications written by older versions.

use weaver_server_composition::{JobId, NzbFileId, SegmentId};
use weaver_server_pipeline::jobs::model::TerminalDiscardKind;
use weaver_server_pipeline::pipeline::tests::{
    control_fixture::start_move_to_complete, delivery_fixture::*, insert_active_job,
    new_direct_pipeline, segmented_job_spec, yenc_fixture::mark_missing,
};

fn file_id(job_id: JobId, file_index: u32) -> NzbFileId {
    NzbFileId { job_id, file_index }
}

fn segment(file_id: NzbFileId, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id,
        segment_number,
    }
}

#[test]
fn legacy_unfetchable_duplicate_terminal_records_remain_readable() {
    let json =
        r#"{"file_index":1,"filename":"legacy.bin","kind":"unfetchable_duplicate","bytes":56000}"#;
    let discard: weaver_server_pipeline::jobs::model::TerminalDiscard =
        serde_json::from_str(json).unwrap();
    assert_eq!(discard.kind, TerminalDiscardKind::UnfetchableDuplicate);
    assert_eq!(discard.bytes, 56000);
    assert_eq!(
        serde_json::from_str::<weaver_server_pipeline::jobs::model::TerminalDiscard>(
            &serde_json::to_string(&discard).unwrap()
        )
        .unwrap(),
        discard
    );
}

// The job-10220 pin. Every article of the payload and of its recovery set is
// missing, so nothing describes anything and nothing claims anything. A post
// that delivered none of itself must not archive as a success.
#[tokio::test]
async fn a_post_whose_every_article_is_missing_is_refused_at_the_delivery_gate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40013);
    let segment_sizes = [4_000u32; 10];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    cover_with_par2(&mut pipeline, job_id);
    let payload = file_id(job_id, 0);
    for segment_number in 0..segment_sizes.len() as u32 {
        mark_missing(&mut pipeline, segment(payload, segment_number));
    }

    let refusal = reconcile_delivery(&mut pipeline, job_id)
        .expect_err("a payload nothing delivered and nothing claims is not a delivery");
    assert!(
        refusal.contains("silver-horizon.mkv"),
        "the refusal names the files it is refusing over: {refusal}"
    );
    assert!(
        !is_reconciled(&pipeline, job_id),
        "a refused delivery leaves no settled record behind"
    );

    let error = start_move_to_complete(&mut pipeline, job_id)
        .await
        .expect_err("the delivery gate must refuse the move");
    assert!(error.contains("silver-horizon.mkv"));
    assert_eq!(
        terminal_cause(&pipeline, job_id),
        Some(&weaver_server_pipeline::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
}

// A job that fails while a store it gave up still has volumes to read back,
// its graph kept unstopped, drops the rest of the give-up: nothing holds the
// loop for it, the wake rung before the failure reads nothing back, and the
// job ends Failed.
#[tokio::test]
async fn a_job_failed_between_volumes_of_a_give_up_is_not_held_by_it() {
    use weaver_server_pipeline::pipeline::tests::engines::give_up_failure::fail_mid_give_up;
    let failed = fail_mid_give_up().await;
    assert!(failed.giving_up_at_failure, "the give-up had volumes left");
    assert!(
        failed.kept_unstopped,
        "the failure kept the graph unstopped"
    );
    assert!(!failed.giving_up_after, "the failure ended the give-up");
    assert!(
        !failed.held_after,
        "nothing holds the loop for the failed job"
    );
    assert!(
        matches!(
            failed.status,
            Some(weaver_server_composition::JobStatus::Failed { .. })
        ),
        "{:?}",
        failed.status
    );
}
