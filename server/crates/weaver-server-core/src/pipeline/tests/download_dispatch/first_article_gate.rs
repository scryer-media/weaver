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

    // Ten of twelve is the failure share, and the verdict is taken the moment
    // it is certain: the last two articles of the sample are never asked for.
    let decided_at = 10;
    let error = job_failed(&pipeline, job_id).expect("the post must be refused");
    assert!(
        error.contains("first articles are missing"),
        "the failure must say what it read: {error}"
    );
    assert!(
        error.contains(&format!("{decided_at} of {files}")),
        "and how much of the sample it read: {error}"
    );
    assert_eq!(
        dispatched.len(),
        decided_at,
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
    assert_eq!(
        sampled.len(),
        decided_at,
        "one article per file, not one file"
    );
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

/// The first articles of `job_id`, in file order.
fn sample_in_file_order(pipeline: &Pipeline, job_id: JobId) -> Vec<SegmentId> {
    let mut sample: Vec<SegmentId> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .download_queue
        .first_articles()
        .collect();
    sample.sort_by_key(|segment_id| segment_id.file_id.file_index);
    sample
}

/// Books the job's ordinary articles missing, in file order, until the job
/// stops running, then answers any health probe that armed along the way —
/// the health arithmetic's own confirmation, which is what lets it abort. The
/// probe answer leaves one sample present so that the probe does not rule on
/// the job itself. Nothing of the first-article sample is touched.
fn fail_ordinary_articles_until_the_job_stops(pipeline: &mut Pipeline, job_id: JobId) {
    book_ordinary_articles_missing(pipeline, job_id);
    let Some(state) = pipeline.jobs.get(&job_id) else {
        return;
    };
    if job_failed(pipeline, job_id).is_some() || !state.health_probing {
        return;
    }
    let probe_round = state.health_probe_round.wrapping_sub(1);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round,
        total: 10,
        missed: 9,
        unverified: 0,
        done: true,
        inconclusive: false,
    });
}

fn book_ordinary_articles_missing(pipeline: &mut Pipeline, job_id: JobId) {
    let sample = sample_in_file_order(pipeline, job_id);
    let spec = pipeline.jobs.get(&job_id).unwrap().spec.clone();
    for (file_index, file) in spec.files.iter().enumerate() {
        for segment in &file.segments {
            let segment_id = SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: file_index as u32,
                },
                segment_number: segment.ordinal,
            };
            if sample.contains(&segment_id) {
                continue;
            }
            pipeline.book_terminal_segment(segment_id, SegmentTerminalState::Missing);
            if job_failed(pipeline, job_id).is_some() {
                return;
            }
        }
    }
}

/// The verdict is taken the moment it is certain, not when the sample is
/// complete: once the articles ruled missing reach the failure share of the
/// whole sample, the ones still outstanding cannot change the answer.
#[tokio::test]
async fn a_certain_sample_decides_before_it_is_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41705);
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Certain Early", 12, 4),
    )
    .await;
    let sample = sample_in_file_order(&pipeline, job_id);
    assert_eq!(sample.len(), 12);

    // Nine of twelve is short of the share; the sample waits.
    for segment_id in sample.iter().take(9) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }
    assert!(
        job_failed(&pipeline, job_id).is_none(),
        "nine missing with three outstanding is not yet certain"
    );

    // The tenth makes it certain with two articles still outstanding.
    pipeline.book_terminal_segment(sample[9], SegmentTerminalState::Missing);
    let error = job_failed(&pipeline, job_id).expect("the certain verdict must be taken");
    assert_eq!(
        error,
        "aborted: 10 of 12 first articles are missing, the post cannot complete"
    );
}

/// A health abort with the sample short of the failure share is a health
/// failure, and says so.
#[tokio::test]
async fn a_health_abort_below_the_share_keeps_the_health_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41706);
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Short Of The Share", 12, 8),
    )
    .await;
    let sample = sample_in_file_order(&pipeline, job_id);

    // Nine of the sample are ruled missing, three are still outstanding.
    for segment_id in sample.iter().take(9) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }
    assert!(job_failed(&pipeline, job_id).is_none());

    fail_ordinary_articles_until_the_job_stops(&mut pipeline, job_id);

    let error = job_failed(&pipeline, job_id).expect("the health arithmetic must fail the job");
    assert!(
        error.starts_with("health ") && error.contains("below critical"),
        "a sample short of the share leaves the health error standing: {error}"
    );
}

/// A health abort with the sample already past the failure share fails the
/// job with the sample's diagnosis, worded as the complete sample words it.
///
/// The sample's rulings are recorded here without the gate being read, which
/// is the state a health abort can meet: the ruling that made the sample
/// certain landed, and the health arithmetic ran before anything read the
/// sample again.
#[tokio::test]
async fn a_health_abort_past_the_share_fails_with_the_sample_diagnosis() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41707);
    insert_active_job(
        &mut pipeline,
        job_id,
        multi_file_job_spec("Past The Share", 12, 8),
    )
    .await;
    let sample = sample_in_file_order(&pipeline, job_id);

    for segment_id in sample.iter().take(10) {
        pipeline
            .segment_terminal_states
            .insert(*segment_id, SegmentTerminalState::Missing);
    }
    assert!(job_failed(&pipeline, job_id).is_none());

    fail_ordinary_articles_until_the_job_stops(&mut pipeline, job_id);

    let error = job_failed(&pipeline, job_id).expect("the health arithmetic must fail the job");
    assert_eq!(
        error,
        "aborted: 10 of 12 first articles are missing, the post cannot complete"
    );
}

/// A job whose payload files are `payload_segments` articles each (one entry
/// per file), followed by one PAR2 recovery volume of `recovery_segments`
/// articles.
fn job_spec_with_recovery(name: &str, payload_segments: &[u32], recovery_segments: u32) -> JobSpec {
    let file = |filename: String, role: FileRole, segments: u32, tag: String| FileSpec {
        filename,
        role,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: (0..segments)
            .map(|ordinal| {
                segment_spec! {
                    number: ordinal,
                    bytes: 512u32,
                    message_id: format!("{tag}-{ordinal}@example.com"),
                }
            })
            .collect(),
    };
    let mut files: Vec<FileSpec> = payload_segments
        .iter()
        .enumerate()
        .map(|(file_index, &segments)| {
            file(
                format!("amber-lattice.part{file_index:02}.bin"),
                FileRole::Standalone,
                segments,
                format!("amber-{file_index}"),
            )
        })
        .collect();
    files.push(file(
        "amber-lattice.vol00+32.par2".to_string(),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 32,
        },
        recovery_segments,
        "amber-par2".to_string(),
    ));
    let total_segments: u64 = files.iter().map(|file| file.segments.len() as u64).sum();
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: total_segments * 512,
        category: None,
        metadata: vec![],
        files,
    }
}

/// The sample counts files, not bytes. Small files ruled missing beside a
/// recovery volume that covers all of them are damage the repair can undo,
/// and the gate leaves the job to the health arithmetic.
#[tokio::test]
async fn missing_small_files_the_recovery_covers_leave_the_job_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41708);
    // Ten one-article files and two of sixteen: 5 KiB of the sample's files
    // could be lost outright, against 8 KiB of recovery.
    let mut payload = vec![1u32; 10];
    payload.extend([16, 16]);
    insert_active_job(
        &mut pipeline,
        job_id,
        job_spec_with_recovery("Small Losses Covered", &payload, 16),
    )
    .await;
    let sample = sample_in_file_order(&pipeline, job_id);
    assert_eq!(sample.len(), 12, "the recovery volume is not sampled");

    for segment_id in sample.iter().take(10) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }

    assert!(
        job_failed(&pipeline, job_id).is_none(),
        "ten of twelve first articles missing, all of them covered by recovery, is not a dead post"
    );
}

/// Recovery that cannot cover the files the sample rules missing does not
/// hold the gate back.
#[tokio::test]
async fn missing_files_beyond_the_recovery_still_fail_on_the_sample() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41709);
    // Twelve files of eight articles: 40 KiB of the sample's files could be
    // lost outright, against 2 KiB of recovery.
    insert_active_job(
        &mut pipeline,
        job_id,
        job_spec_with_recovery("Losses Past The Recovery", &[8; 12], 4),
    )
    .await;
    let sample = sample_in_file_order(&pipeline, job_id);
    assert_eq!(sample.len(), 12);

    for segment_id in sample.iter().take(10) {
        pipeline.book_terminal_segment(*segment_id, SegmentTerminalState::Missing);
    }

    let error = job_failed(&pipeline, job_id).expect("the post must be refused");
    assert_eq!(
        error,
        "aborted: 10 of 12 first articles are missing, the post cannot complete"
    );
}
