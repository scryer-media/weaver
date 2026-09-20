//! Delivery claims, including terminal classifications written by older versions.

use super::*;
use crate::jobs::model::TerminalDiscardKind;
use crate::pipeline::SegmentTerminalState;

/// A two-file payload: a canonical file and the collided repost beside it.
fn two_payload_files_spec(
    name: &str,
    canonical: (&str, &[u32]),
    duplicate: (&str, &[u32]),
) -> JobSpec {
    let file = |filename: &str, sizes: &[u32], prefix: &str| FileSpec {
        filename: filename.to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: sizes
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                segment_spec! {
                    number: index as u32,
                    bytes: *bytes,
                    message_id: format!("{prefix}-{index}@example.com"),
                }
            })
            .collect(),
    };
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: canonical
            .1
            .iter()
            .chain(duplicate.1.iter())
            .map(|bytes| *bytes as u64)
            .sum(),
        category: None,
        metadata: vec![],
        files: vec![
            file(canonical.0, canonical.1, "canonical"),
            file(duplicate.0, duplicate.1, "duplicate"),
        ],
    }
}

fn file_id(job_id: JobId, file_index: u32) -> NzbFileId {
    NzbFileId { job_id, file_index }
}

fn segment(file_id: NzbFileId, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id,
        segment_number,
    }
}

fn mark_file_delivered(pipeline: &mut Pipeline, file_id: NzbFileId, segment_sizes: &[u32]) {
    let state = pipeline.jobs.get_mut(&file_id.job_id).unwrap();
    let file = state.assembly.file_mut(file_id).unwrap();
    for (ordinal, bytes) in segment_sizes.iter().enumerate() {
        file.commit_segment(ordinal as u32, *bytes).unwrap();
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

/// Real damage stays real. An unprotected file delivered short keeps its
/// failure, and the job reports the honest fraction it delivered.
#[tokio::test]
async fn an_unprotected_file_delivered_short_keeps_an_honest_partial_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40011);
    let segment_sizes = [1_000u32; 10];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }
    let payload = file_id(job_id, 0);

    // Nine of the ten segments land; the tenth never does.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(payload).unwrap();
        for ordinal in 0..9u32 {
            file.commit_segment(ordinal, 1_000).unwrap();
        }
    }
    pipeline.book_terminal_segment(segment(payload, 9), SegmentTerminalState::Missing);

    pipeline
        .reconcile_terminal_delivery(job_id)
        .expect("a file delivered nine tenths of the way is a delivery with damage");

    let reconciliation = &pipeline.terminal_reconciliations[&job_id];
    assert_eq!(reconciliation.failed_bytes, 1_000);
    assert_eq!(reconciliation.health, 900);
    assert!(reconciliation.discards.is_empty());
}

/// The reconciliation is bidirectional. Bytes that read complete while segments
/// of theirs are terminally lost were completed by *something*, and if no
/// verdict, proof or discard says what, the failure stays on the record.
#[tokio::test]
async fn bytes_that_read_complete_without_a_claim_are_not_forgiven() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40012);
    let segment_sizes = [1_000u32; 10];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }
    let payload = file_id(job_id, 0);

    pipeline.book_terminal_segment(segment(payload, 9), SegmentTerminalState::Missing);
    // Something marked the file whole afterwards without leaving a verdict
    // behind it — the reconciliation gap this arm exists to refuse.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.file_mut(payload).unwrap().mark_complete();
    }

    pipeline
        .reconcile_terminal_delivery(job_id)
        .expect("bytes that are present are still delivered, they are just unexplained");

    let reconciliation = &pipeline.terminal_reconciliations[&job_id];
    assert_eq!(
        reconciliation.failed_bytes, 1_000,
        "an unclaimed file keeps its failure contribution rather than being zeroed"
    );
    assert_eq!(reconciliation.health, 900);
    assert!(reconciliation.discards.is_empty());
}

/// The job-10220 pin. Every article of the payload and of its recovery set is
/// missing, so nothing describes anything and nothing claims anything. A post
/// that delivered none of itself must not archive as a success.
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
        state.next_health_probe_failed_bytes = u64::MAX;
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

/// A dead file beside a real delivery, without the breaker's positive
/// evidence: the duplicate's articles are simply missing everywhere. Nothing
/// claims it, so its failure stays in the record — but a job that delivered
/// its canonical payload completes with that honest damage rather than being
/// refused over the hole beside it.
#[tokio::test]
async fn a_dead_duplicate_with_missing_articles_does_not_refuse_a_delivered_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40020);
    let sizes = [4_000u32; 14];
    insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_files_spec(
            "Silver Horizon",
            ("silver-horizon.mkv", &sizes),
            ("silver-horizon.mkv.1", &sizes),
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }
    let canonical = file_id(job_id, 0);
    let duplicate = file_id(job_id, 1);

    mark_file_delivered(&mut pipeline, canonical, &sizes);
    for segment_number in 0..sizes.len() as u32 {
        pipeline.book_terminal_segment(
            segment(duplicate, segment_number),
            SegmentTerminalState::Missing,
        );
    }

    pipeline
        .reconcile_terminal_delivery(job_id)
        .expect("a delivered canonical beside a dead file is a delivery with damage");

    let duplicate_bytes: u64 = sizes.iter().map(|bytes| *bytes as u64).sum();
    let reconciliation = &pipeline.terminal_reconciliations[&job_id];
    assert_eq!(
        reconciliation.failed_bytes, duplicate_bytes,
        "the hole is not forgiven — nothing claimed it"
    );
    assert_eq!(reconciliation.health, 500);
    assert!(
        reconciliation.discards.is_empty(),
        "without positive could-never-arrive evidence nothing is discarded"
    );
}
