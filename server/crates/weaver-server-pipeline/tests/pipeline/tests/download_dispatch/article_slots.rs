// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One article slot per part number: the slot fetches its first message-id and
// moves to the next only once the id in use is spent.

use std::sync::atomic::Ordering;
use weaver_server_pipeline::JobId;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::{
    DispatchFixture,
    article_slots::{finish_slot_body, finish_slot_missing, lease_slot, slot_body, slot_pipeline},
};

const FIRST: &str = "first@example.invalid";
const SECOND: &str = "second@example.invalid";

fn articles_not_found(pipeline: &DispatchFixture) -> u64 {
    pipeline
        .metrics()
        .articles_not_found
        .load(Ordering::Relaxed)
}

#[tokio::test]
async fn a_crc_failure_on_the_first_id_fetches_the_second() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40302);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);

    // One body byte changed: the article decodes to the wrong bytes and
    // fails its part CRC.
    let mut body = slot_body().to_vec();
    let lines_end = body
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| *pair == b"\r\n")
        .nth(1)
        .map(|(at, _)| at + 2)
        .unwrap();
    body[lines_end] = if body[lines_end] == b'a' { b'b' } else { b'a' };
    finish_slot_body(&mut pipeline, job_id, body.into()).await;
    pipeline.drain_decode(1).await;

    // The second id goes out to every server, the one that served the bad
    // body included: the id is new, the server is not at fault.
    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
}

#[tokio::test]
async fn a_first_id_missing_on_every_server_fetches_the_second() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40303);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);

    finish_slot_missing(&mut pipeline, job_id).await;

    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    assert_eq!(articles_not_found(&pipeline), 0);
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
}

// Every id spent is a lost article, booked exactly as a single-id article
// that every server is missing.
#[tokio::test]
async fn a_slot_with_every_id_spent_is_lost_like_a_single_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40304);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);
    finish_slot_missing(&mut pipeline, job_id).await;
    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    finish_slot_missing(&mut pipeline, job_id).await;

    let single_dir = tempfile::tempdir().unwrap();
    let single_id = JobId(40305);
    let mut single = slot_pipeline(&single_dir, single_id, &[FIRST]).await;
    lease_slot(&mut single, single_id);
    finish_slot_missing(&mut single, single_id).await;

    for (pipeline, job_id) in [(&mut pipeline, job_id), (&mut single, single_id)] {
        assert!(lease_slot(pipeline, job_id).is_empty());
        assert_eq!(articles_not_found(pipeline), 1);
        assert!(
            pipeline
                .job_if_present(job_id)
                .is_none_or(|state| state.failed_bytes == 128),
            "the lost article's bytes are booked failed (the job may already be archived)"
        );
    }
}
