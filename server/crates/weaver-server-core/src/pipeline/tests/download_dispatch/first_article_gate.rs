// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A job of `files` files, each of `segments` articles.
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

fn job_failed(pipeline: &Pipeline, job_id: JobId) -> Option<String> {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => Some(error),
        _ => None,
    }
}

// A handful of files is not a sample. A small post that loses its first file
// is an ordinary damaged post, and the health arithmetic rules on it.
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
        .articles
        .active()
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
