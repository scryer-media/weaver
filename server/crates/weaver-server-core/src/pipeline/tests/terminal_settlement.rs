// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Delivery claims, including terminal classifications written by older versions.

use super::*;
use crate::jobs::model::TerminalDiscardKind;
use crate::pipeline::SegmentTerminalState;

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
    let discard: crate::jobs::model::TerminalDiscard = serde_json::from_str(json).unwrap();
    assert_eq!(discard.kind, TerminalDiscardKind::UnfetchableDuplicate);
    assert_eq!(discard.bytes, 56000);
    assert_eq!(
        serde_json::from_str::<crate::jobs::model::TerminalDiscard>(
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
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.par2_bytes = state.spec.total_bytes;
    }
    let payload = file_id(job_id, 0);
    for segment_number in 0..segment_sizes.len() as u32 {
        pipeline.book_terminal_segment(
            segment(payload, segment_number),
            SegmentTerminalState::Missing,
        );
    }

    let refusal = pipeline
        .reconcile_terminal_delivery(job_id)
        .expect_err("a payload nothing delivered and nothing claims is not a delivery");
    assert!(
        refusal.contains("silver-horizon.mkv"),
        "the refusal names the files it is refusing over: {refusal}"
    );
    assert!(
        !pipeline.terminal_reconciliations.contains_key(&job_id),
        "a refused delivery leaves no settled record behind"
    );

    let error = pipeline
        .start_move_to_complete(job_id)
        .await
        .expect_err("the delivery gate must refuse the move");
    assert!(error.contains("silver-horizon.mkv"));
    assert_eq!(
        pipeline.semantic_terminal_causes.get(&job_id),
        Some(&crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
}
