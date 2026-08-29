//! The claim census and the foreign-layout breaker: what a delivered job's
//! terminal record says, and what makes a file stop being fetched at all.

use super::*;
use crate::jobs::model::TerminalDiscardKind;
use crate::pipeline::SegmentTerminalState;
use crate::pipeline::decode::YencLayoutMismatch;

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

/// One article of a coherent *other* file, offered under this segment's
/// message id: a part count that is not the one the NZB declared.
fn foreign_article(
    segment_number: u32,
    served_total: u32,
    served_file_size: u64,
) -> YencLayoutAssertions {
    YencLayoutAssertions {
        file_size: served_file_size,
        part: Some(segment_number + 1),
        total: Some(served_total),
        begin: Some(1),
        end: Some(1),
    }
}

fn mark_file_delivered(pipeline: &mut Pipeline, file_id: NzbFileId, segment_sizes: &[u32]) {
    let state = pipeline.jobs.get_mut(&file_id.job_id).unwrap();
    let file = state.assembly.file_mut(file_id).unwrap();
    for (ordinal, bytes) in segment_sizes.iter().enumerate() {
        file.commit_segment(ordinal as u32, *bytes).unwrap();
    }
}

fn trip_breaker(pipeline: &mut Pipeline, file_id: NzbFileId, served_total: u32, served_size: u64) {
    for segment_number in 0..14u32 {
        pipeline.note_yenc_layout_refusal(
            segment(file_id, segment_number),
            YencLayoutMismatch::Total,
            foreign_article(segment_number, served_total, served_size),
        );
    }
}

/// The job-10206 shape. A canonical payload file is delivered; beside it sits a
/// repost that collided on message ids and could never have arrived. The
/// terminal record must describe the delivery, not the wire counters the
/// discard already answered.
#[tokio::test]
async fn a_discarded_unfetchable_duplicate_leaves_the_delivery_whole() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40010);
    let canonical_sizes = [4_000u32; 14];
    let duplicate_sizes = [4_000u32; 14];
    insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_files_spec(
            "Silver Horizon",
            ("silver-horizon.mkv", &canonical_sizes),
            ("silver-horizon.mkv.1", &duplicate_sizes),
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

    mark_file_delivered(&mut pipeline, canonical, &canonical_sizes);
    trip_breaker(&mut pipeline, duplicate, 1_525, 1_093_084_655);

    assert!(
        pipeline.foreign_layout_watches[&duplicate].tripped,
        "a consistent foreign geometry across twelve segments retires the file"
    );
    let duplicate_bytes: u64 = duplicate_sizes.iter().map(|bytes| *bytes as u64).sum();
    assert_eq!(
        pipeline.jobs[&job_id].failed_bytes, duplicate_bytes,
        "while the job is live the ledger still reports what the wire could not fetch"
    );

    pipeline
        .reconcile_terminal_delivery(job_id)
        .expect("a delivered canonical file beside a discarded duplicate is a delivery");

    let reconciliation = &pipeline.terminal_reconciliations[&job_id];
    assert_eq!(reconciliation.failed_bytes, 0);
    assert_eq!(reconciliation.health, 1000);
    assert_eq!(reconciliation.discards.len(), 1);
    let discard = &reconciliation.discards[0];
    assert_eq!(discard.file_index, 1);
    assert_eq!(discard.kind, TerminalDiscardKind::UnfetchableDuplicate);
    assert_eq!(discard.bytes, duplicate_bytes);
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

/// A tripped breaker retires the file's queued segments through the terminal
/// ledger — once each, at their declared sizes — and leaves everything else in
/// the job's queues alone.
#[tokio::test]
async fn a_tripped_breaker_retires_only_its_own_files_queued_segments() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40014);
    let canonical_sizes = [500u32; 4];
    let duplicate_sizes = [1_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_files_spec(
            "Silver Horizon",
            ("silver-horizon.mkv", &canonical_sizes),
            ("silver-horizon.mkv.1", &duplicate_sizes),
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.next_health_probe_failed_bytes = u64::MAX;
        state.par2_bytes = state.spec.total_bytes;
    }
    let duplicate = file_id(job_id, 1);

    trip_breaker(&mut pipeline, duplicate, 1_525, 1_093_084_655);

    let state = &pipeline.jobs[&job_id];
    assert_eq!(
        state.download_queue.len(),
        canonical_sizes.len(),
        "only the abandoned file's work leaves the queue"
    );
    let duplicate_bytes: u64 = duplicate_sizes.iter().map(|bytes| *bytes as u64).sum();
    assert_eq!(state.failed_bytes, duplicate_bytes);
    assert_eq!(pipeline.derived_failed_bytes(job_id), duplicate_bytes);
    assert_eq!(
        pipeline.segment_terminal_states.len(),
        duplicate_sizes.len(),
        "each segment is booked exactly once"
    );
    assert!(
        pipeline
            .segment_terminal_states
            .values()
            .all(|terminal_state| *terminal_state == SegmentTerminalState::ForeignLayout)
    );

    // Firing again changes nothing: the breaker is per-file and fires once.
    trip_breaker(&mut pipeline, duplicate, 1_525, 1_093_084_655);
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, duplicate_bytes);
    assert_eq!(
        pipeline.segment_terminal_states.len(),
        duplicate_sizes.len()
    );
}

/// Refusals that disagree with each other are corruption of a real file, not a
/// collision with a coherent other one. Keep fetching.
#[tokio::test]
async fn refusals_that_disagree_with_each_other_never_retire_a_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40015);
    let segment_sizes = [1_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    let payload = file_id(job_id, 0);

    for segment_number in 0..20u32 {
        pipeline.note_yenc_layout_refusal(
            segment(payload, segment_number),
            YencLayoutMismatch::Total,
            foreign_article(
                segment_number,
                1_500 + segment_number,
                1_000 + segment_number as u64,
            ),
        );
    }

    assert!(!pipeline.foreign_layout_watches[&payload].tripped);
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
    assert_eq!(
        pipeline.jobs[&job_id].download_queue.len(),
        segment_sizes.len(),
        "a file that may still be damaged rather than absent keeps its queued work"
    );
}

/// A run of refusals that agree only about the `=ybegin size=` header proves
/// nothing: real posters misstate it. Part geometry is what may retire a file.
#[tokio::test]
async fn a_misstated_size_header_alone_never_retires_a_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40016);
    let segment_sizes = [1_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    let payload = file_id(job_id, 0);

    for segment_number in 0..20u32 {
        pipeline.note_yenc_layout_refusal(
            segment(payload, segment_number),
            YencLayoutMismatch::FileSizeAboveDeclared,
            YencLayoutAssertions {
                file_size: 99_999_999,
                part: Some(segment_number + 1),
                total: Some(segment_sizes.len() as u32),
                begin: Some(1),
                end: Some(1),
            },
        );
    }

    assert!(!pipeline.foreign_layout_watches[&payload].tripped);
    assert!(pipeline.segment_terminal_states.is_empty());
}

/// One article that decodes into the declared layout proves the declared file
/// exists on the wire, and no amount of later foreign evidence may retire it.
#[tokio::test]
async fn one_declared_layout_decode_permanently_disarms_the_breaker() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40017);
    let segment_sizes = [1_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    let payload = file_id(job_id, 0);

    pipeline.note_yenc_layout_refusal(
        segment(payload, 0),
        YencLayoutMismatch::Total,
        foreign_article(0, 1_525, 1_093_084_655),
    );
    pipeline.disarm_foreign_layout_watch(payload);
    trip_breaker(&mut pipeline, payload, 1_525, 1_093_084_655);

    assert!(!pipeline.foreign_layout_watches[&payload].tripped);
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.jobs[&job_id].download_queue.len(),
        segment_sizes.len()
    );
}

/// The job-10220 shape taken through the breaker's door instead of the
/// missing-article one: every payload file's message ids collided with a
/// repost, everything tripped, and nothing delivered. The discard claim must
/// not launder the whole payload out of the record — with no delivery behind
/// it, an unfetchable file is the payload, and the census refuses the job.
#[tokio::test]
async fn a_job_whose_entire_payload_is_foreign_is_refused_not_discarded() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40019);
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

    trip_breaker(&mut pipeline, canonical, 1_525, 1_093_084_655);
    trip_breaker(&mut pipeline, duplicate, 1_525, 1_093_084_655);
    assert!(pipeline.foreign_layout_watches[&canonical].tripped);
    assert!(pipeline.foreign_layout_watches[&duplicate].tripped);

    let refusal = pipeline
        .reconcile_terminal_delivery(job_id)
        .expect_err("a payload that is foreign end to end is not a delivery");
    assert!(
        refusal.contains("silver-horizon.mkv"),
        "the refusal names what never arrived: {refusal}"
    );
    assert!(!pipeline.terminal_reconciliations.contains_key(&job_id));
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

/// The escape hatch keeps a file fetching whatever its articles claim to be.
#[tokio::test]
async fn the_escape_hatch_keeps_the_breaker_from_retiring_anything() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40018);
    let segment_sizes = [1_000u32; 20];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    pipeline.foreign_layout_breaker_override = Some(false);
    let payload = file_id(job_id, 0);

    trip_breaker(&mut pipeline, payload, 1_525, 1_093_084_655);

    assert!(!pipeline.foreign_layout_watches.contains_key(&payload));
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.jobs[&job_id].download_queue.len(),
        segment_sizes.len()
    );
}
