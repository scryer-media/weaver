use super::*;

/// A job of `files` files, each of `segments` articles.
fn multi_file_job_spec(name: &str, files: usize, segments: u32) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (files as u64) * (segments as u64) * 512,
        category: None,
        metadata: vec![],
        files: (0..files)
            .map(|file_index| FileSpec {
                filename: format!("copper-meridian.part{file_index:02}.bin"),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..segments)
                    .map(|ordinal| {
                        segment_spec! {
                            number: ordinal,
                            bytes: 512u32,
                            message_id: format!("copper-{file_index}-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Serves the job's queue in dispatch order, answering every article with
/// "no such article", and stops as soon as the job is no longer running.
/// Returns the articles that were handed out, in order.
fn serve_until_the_job_stops(pipeline: &mut Pipeline, job_id: JobId) -> Vec<SegmentId> {
    let mut dispatched = Vec::new();
    loop {
        let Some(work) = pipeline
            .jobs
            .get_mut(&job_id)
            .and_then(|state| state.download_queue.pop())
        else {
            return dispatched;
        };
        dispatched.push(work.segment_id);
        pipeline.book_terminal_segment(work.segment_id, SegmentTerminalState::Missing);
    }
}

fn job_failed(pipeline: &Pipeline, job_id: JobId) -> Option<String> {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => Some(error),
        _ => None,
    }
}

/// A post whose every article is gone is answered for by one article per file.
///
/// The articles of a job are served file by file, so a post with tens of
/// thousands of articles would otherwise have to work through a whole file
/// before it even asked about the second one. One article of each of the
/// leading files, served first, says the same thing in a handful of round
/// trips — and it rides the ordinary dispatch order, so it costs no lane of
/// its own.
#[tokio::test]
async fn a_post_whose_first_articles_are_all_missing_fails_on_the_sample() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41701);
    let files = 12;
    let segments = 8;
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Dead On Arrival", files, segments),
    )
    .await;

    let dispatched = serve_until_the_job_stops(&mut pipeline, job_id);

    let error = job_failed(&pipeline, job_id).expect("the post must be refused");
    assert!(
        error.contains("first articles are missing"),
        "the failure must say what it read: {error}"
    );
    assert!(
        error.contains(&format!("{files} of {files}")),
        "and how much of the sample it read: {error}"
    );
    assert_eq!(
        dispatched.len(),
        files,
        "nothing beyond the sample is asked for: {dispatched:?}"
    );
    assert!(
        dispatched
            .iter()
            .all(|segment_id| segment_id.segment_number == 0),
        "and every article asked for is a file's first: {dispatched:?}"
    );
    let sampled: std::collections::HashSet<u32> = dispatched
        .iter()
        .map(|segment_id| segment_id.file_id.file_index)
        .collect();
    assert_eq!(sampled.len(), files, "one article per file, not one file");
}

/// A handful of files is not a sample. A small post that loses its first file
/// is an ordinary damaged post, and the health arithmetic rules on it.
#[tokio::test]
async fn a_small_post_is_not_judged_on_its_first_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41702);
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Too Few To Judge", 6, 4),
    )
    .await;

    // Only the first articles are answered for; the rest of the queue is left
    // alone, so nothing but the gate could have ruled here.
    let first_articles: Vec<SegmentId> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .first_articles()
        .collect();
    assert_eq!(first_articles.len(), 6);
    for segment_id in first_articles {
        pipeline.book_terminal_segment(segment_id, SegmentTerminalState::Missing);
    }

    assert!(
        job_failed(&pipeline, job_id).is_none(),
        "a six-file post is not a post the sample can speak for"
    );
}

/// A post that is merely damaged keeps going. The sample only speaks when
/// almost none of it answered.
#[tokio::test]
async fn a_sample_that_mostly_arrives_leaves_the_job_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41703);
    let files = 12;
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Damaged But Alive", files, 4),
    )
    .await;

    let mut first_articles: Vec<SegmentId> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .first_articles()
        .collect();
    first_articles.sort_by_key(|segment_id| segment_id.file_id.file_index);

    // Nine of twelve missing is three quarters of the sample: short of the
    // share that makes a post unrecoverable rather than damaged.
    for segment_id in first_articles.iter().take(9) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }
    for segment_id in first_articles.iter().skip(9) {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(segment_id.file_id)
            .unwrap()
            .commit_segment(segment_id.segment_number, 512)
            .unwrap();
        pipeline.note_first_article_settled(*segment_id);
    }

    assert!(
        job_failed(&pipeline, job_id).is_none(),
        "three quarters missing is damage, not a dead post"
    );
}

/// A restart in the middle of the sample finishes it rather than forgetting it.
#[tokio::test]
async fn a_restart_mid_sample_still_reaches_the_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41704);
    let files = 12;
    let spec = multi_file_job_spec("Interrupted Sample", files, 4);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    // Half the sample is answered for, then the process goes away.
    let mut first_articles: Vec<SegmentId> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .first_articles()
        .collect();
    first_articles.sort_by_key(|segment_id| segment_id.file_id.file_index);
    for segment_id in first_articles.iter().take(6) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }
    assert!(job_failed(&pipeline, job_id).is_none());
    drop(pipeline);

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
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

    let dispatched = serve_until_the_job_stops(&mut restored, job_id);

    let error = job_failed(&restored, job_id).expect("the resumed post must still be refused");
    assert!(error.contains("first articles are missing"), "{error}");
    assert!(
        dispatched
            .iter()
            .all(|segment_id| segment_id.segment_number == 0),
        "the resumed job re-reads the sample before anything else: {dispatched:?}"
    );
}
