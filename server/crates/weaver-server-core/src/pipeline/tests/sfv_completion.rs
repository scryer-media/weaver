//! SFV verification fallback: the completion-gate arm that rules on a job with
//! no PAR2 set.
//!
//! The read split (`sfv_verify_read_splits`) is `(files composed from verified
//! article CRCs, files streamed off disk)` for each pass, and it is what
//! distinguishes "the zero-I/O arm answered" from "the arm refused and the disk
//! was read" — two outcomes that are otherwise indistinguishable from the
//! verdict alone.

use super::*;

/// One classic `.sfv` line: name, then the file's CRC32 as eight hex digits.
fn sfv_line(name: &str, bytes: &[u8]) -> String {
    format!("{name} {:08x}\n", par2_rs::checksum::crc32(bytes))
}

fn sfv_line_with_crc(name: &str, crc32: u32) -> String {
    format!("{name} {crc32:08x}\n")
}

/// Puts the job in the shape the completion gate sees once the download pass is
/// over: nothing queued, status back to `Downloading`.
fn settle_download_state(pipeline: &mut Pipeline, job_id: JobId) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
}

fn assert_not_failed(pipeline: &Pipeline, job_id: JobId) {
    if let Some(JobStatus::Failed { error }) = job_status_for_assert(pipeline, job_id) {
        panic!("job failed unexpectedly: {error}");
    }
}

fn failure_error(pipeline: &Pipeline, job_id: JobId) -> String {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error }) => error,
        other => panic!("expected a failed job, got {other:?}"),
    }
}

/// A file whose every article arrived with a wire-verified CRC32 over a proven
/// contiguous tiling is verified by *composing* those CRCs — the file is never
/// read back. The split is what witnesses it: `(1, 0)`.
#[tokio::test]
async fn sfv_verifies_a_wire_proven_file_without_reading_it_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30801);
    let payload_filename = "silver.horizon.bin";
    let payload: Vec<u8> = (0..96u32).map(|value| (value % 251) as u8).collect();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Wire Proven",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let listing_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };

    submit_decoded_segment(
        &mut pipeline,
        listing_file_id,
        0,
        0,
        &listing_bytes,
        "silver.horizon.sfv",
        Some(par2_rs::checksum::crc32(&listing_bytes)),
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        payload_file_id,
        0,
        0,
        &payload,
        payload_filename,
        Some(par2_rs::checksum::crc32(&payload)),
    )
    .await;

    // The premise of the zero-I/O arm, stated rather than assumed: the
    // download retained a whole-file CRC32 composed from article CRCs the wire
    // verified.
    let checksum = pipeline
        .par2_runtime(job_id)
        .and_then(|runtime| runtime.completed_checksums.get(&payload_file_id))
        .copied()
        .expect("completed checksum recorded for the payload");
    assert!(
        checksum.all_parts_crc_verified,
        "test premise: every article of the payload was wire-verified"
    );

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(1usize, 0usize)],
        "the payload's CRC32 must be composed from verified article CRCs, not read back"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// A uuencode file has no wire checksum at all — every one of its segments
/// commits with `part_crc_verified: false` — so the zero-I/O arm must refuse it
/// and the disk arm must answer. The split witnesses the refusal: `(0, 1)`.
#[tokio::test]
async fn sfv_reads_back_a_file_the_wire_could_not_vouch_for() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30802);
    let payload_filename = "silver.horizon.bin";
    let payload: Vec<u8> = (0..96u32).map(|value| (value % 253) as u8).collect();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Uuencoded",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let listing_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };

    submit_decoded_segment(
        &mut pipeline,
        listing_file_id,
        0,
        0,
        &listing_bytes,
        "silver.horizon.sfv",
        Some(par2_rs::checksum::crc32(&listing_bytes)),
    )
    .await;
    // The uuencode shape: no aggregate whole-file CRC, and the part CRC is not
    // vouched for by anything on the wire.
    submit_decoded_segment_with_part_crc_verified(
        &mut pipeline,
        payload_file_id,
        0,
        0,
        &payload,
        payload_filename,
        None,
        false,
    )
    .await;

    let checksum = pipeline
        .par2_runtime(job_id)
        .and_then(|runtime| runtime.completed_checksums.get(&payload_file_id))
        .copied()
        .expect("completed checksum recorded for the payload");
    assert!(
        !checksum.all_parts_crc_verified,
        "test premise: a uuencode file carries no verified article CRCs"
    );

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 1usize)],
        "a file the wire could not vouch for must be streamed off disk"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// A listing that disagrees with the bytes is terminal: no PAR2 set means no
/// repair path, so the job fails and says which file.
#[tokio::test]
async fn sfv_mismatch_fails_the_job_with_a_named_cause() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30803);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let listing = sfv_line_with_crc(payload_filename, 0xdead_beef);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Mismatch",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        failure_error(&pipeline, job_id),
        format!("sfv mismatch: {payload_filename}")
    );
}

/// A file the listing names, that the job holds, and that is not on disk when
/// the gate asks for it. There is nothing to measure, so the job fails and
/// names it.
#[tokio::test]
async fn sfv_missing_file_fails_the_job_with_a_named_cause() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30804);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Missing",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;
    tokio::fs::remove_file(working_dir.join(payload_filename))
        .await
        .unwrap();

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        failure_error(&pipeline, job_id),
        format!("sfv missing: {payload_filename}")
    );
}

/// Recovery data outranks a poster's checksum listing. A job with a parsed PAR2
/// set never consults the `.sfv` at all — here the listing is deliberately
/// wrong about a file PAR2 finds intact, and the job still passes.
#[tokio::test]
async fn a_par2_covered_job_never_consults_a_disagreeing_sfv() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30805);
    let payload_filename = "silver.horizon.bin";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 241) as u8).collect();
    let listing = sfv_line_with_crc(payload_filename, 0x0bad_0bad);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Par2 Wins",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;
    persist_completed_file_hash(&pipeline, job_id, 1, "silver.horizon.sfv", &listing_bytes).await;

    settle_download_state(&mut pipeline, job_id);
    // The first pass is the PAR2 verdict itself, which returns before the
    // fallback's seam. The second is the one that reaches it — a verified job
    // owes no further PAR2 evaluation — and is where a missing scope guard
    // would let the listing overrule recovery data.
    pipeline.check_job_completion(job_id).await;
    assert!(pipeline.par2_verified.contains(&job_id));
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert!(
        pipeline.sfv_verify_read_splits.is_empty(),
        "a PAR2-covered job must not run the SFV arm at all"
    );
}

/// The behaviour every PAR2-less job without a listing keeps: nothing runs, and
/// the job's route to completion is untouched.
#[tokio::test]
async fn a_par2_less_job_with_no_sfv_is_unchanged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30806);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon No Listing",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert!(pipeline.sfv_verify_read_splits.is_empty());

    pump_pipeline_runtime_queues(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

/// A named listing is not necessary when its content is unobfuscated. SAB's
/// fallback recognises this completed file as a listing, then the normal SFV
/// path verifies the payload without inventing a new name or file identity.
#[tokio::test]
async fn an_obfuscated_sfv_listing_with_comments_and_blank_lines_is_used() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30813);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let listing_filename = "oBfU5CaTeD-0001";
    let listing = format!(
        "; Generated by a checksum tool\n\n{}",
        sfv_line(payload_filename, &payload)
    );

    let spec = standalone_job_spec(
        "Silver Horizon Obfuscated SFV",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            (listing_filename.to_string(), listing.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        listing_filename,
        listing.as_bytes(),
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(pipeline.sfv_verify_read_splits, vec![(0usize, 1usize)]);
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// Discovery is not limited to the first matching obfuscated listing: a
/// release can publish independent listing files without retaining `.sfv`
/// extensions on either of them.
#[tokio::test]
async fn multiple_obfuscated_sfv_listings_are_combined() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30814);
    let first_payload_filename = "silver.horizon.part01.bin";
    let second_payload_filename = "silver.horizon.part02.bin";
    let first_payload = b"first payload".to_vec();
    let second_payload = b"second payload".to_vec();
    let first_listing_filename = "5wXn1TLm";
    let second_listing_filename = "aC3f9pQ2";
    let first_listing = sfv_line(first_payload_filename, &first_payload);
    let second_listing = sfv_line(second_payload_filename, &second_payload);

    let spec = standalone_job_spec(
        "Silver Horizon Multiple Obfuscated SFVs",
        &[
            (
                first_payload_filename.to_string(),
                first_payload.len() as u32,
            ),
            (
                second_payload_filename.to_string(),
                second_payload.len() as u32,
            ),
            (
                first_listing_filename.to_string(),
                first_listing.len() as u32,
            ),
            (
                second_listing_filename.to_string(),
                second_listing.len() as u32,
            ),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        first_payload_filename,
        &first_payload,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        second_payload_filename,
        &second_payload,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        first_listing_filename,
        first_listing.as_bytes(),
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        3,
        second_listing_filename,
        second_listing.as_bytes(),
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(pipeline.sfv_verify_read_splits, vec![(0usize, 2usize)]);
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// A conventional `.sfv` deliberately suppresses fallback probing. The hidden
/// candidate is valid but wrong; if it were added to the named listing, the
/// conflicting entry would be withdrawn and no verification result recorded.
#[tokio::test]
async fn a_named_sfv_suppresses_obfuscated_discovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30815);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let named_listing_filename = "silver.horizon.sfv";
    let named_listing = sfv_line(payload_filename, &payload);
    let hidden_listing_filename = "n0t-a-listing-name";
    let hidden_listing = sfv_line_with_crc(payload_filename, 0xdead_beef);

    let spec = standalone_job_spec(
        "Silver Horizon Named SFV Wins",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            (
                named_listing_filename.to_string(),
                named_listing.len() as u32,
            ),
            (
                hidden_listing_filename.to_string(),
                hidden_listing.len() as u32,
            ),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        named_listing_filename,
        named_listing.as_bytes(),
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        hidden_listing_filename,
        hidden_listing.as_bytes(),
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(pipeline.sfv_verify_read_splits, vec![(0usize, 1usize)]);
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// A listing routinely covers a wider release than the NZB fetched. Entries
/// naming nothing this job holds are noted and skipped; the entries that do
/// match are still verified.
#[tokio::test]
async fn sfv_entries_naming_no_job_file_do_not_fail_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30807);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let listing = format!(
        "{}{}",
        sfv_line(payload_filename, &payload),
        sfv_line_with_crc("silver.horizon.sample.bin", 0x1234_5678),
    );
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Wider Listing",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 1usize)],
        "only the entry that matched a job file may be measured"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// The converse: a payload file no listing names is left unverified, and the
/// job still completes. A `.sfv` covers what it covers.
#[tokio::test]
async fn job_files_absent_from_the_sfv_stay_unverified_and_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30808);
    let listed = b"silver horizon listed payload".to_vec();
    let unlisted = b"silver horizon unlisted payload".to_vec();
    let listing = sfv_line("silver.horizon.part01.bin", &listed);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Partial Listing",
        &[
            ("silver.horizon.part01.bin".to_string(), listed.len() as u32),
            (
                "silver.horizon.part02.bin".to_string(),
                unlisted.len() as u32,
            ),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        "silver.horizon.part01.bin",
        &listed,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.part02.bin",
        &unlisted,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 1usize)],
        "only the listed file may be measured"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// Two listings disagreeing about one name are a contradiction, not evidence.
/// That name is withdrawn — even though one of the two values is the wrong one
/// and would fail the job — while the name they agree on is still verified.
#[tokio::test]
async fn conflicting_sfv_entries_leave_that_file_unverified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30809);
    let disputed = b"silver horizon disputed payload".to_vec();
    let agreed = b"silver horizon agreed payload".to_vec();
    let first = format!(
        "{}{}",
        sfv_line("silver.horizon.part01.bin", &disputed),
        sfv_line("silver.horizon.part02.bin", &agreed),
    );
    // The same name, a different checksum — and the wrong one, so a listing
    // that failed to withdraw it would fail this job.
    let second = sfv_line_with_crc("silver.horizon.part01.bin", 0x0bad_f00d);
    let first_bytes = first.as_bytes().to_vec();
    let second_bytes = second.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Conflicting Listings",
        &[
            (
                "silver.horizon.part01.bin".to_string(),
                disputed.len() as u32,
            ),
            ("silver.horizon.part02.bin".to_string(), agreed.len() as u32),
            ("silver.horizon.sfv".to_string(), first_bytes.len() as u32),
            (
                "silver.horizon.extra.sfv".to_string(),
                second_bytes.len() as u32,
            ),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        "silver.horizon.part01.bin",
        &disputed,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.part02.bin",
        &agreed,
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 2, "silver.horizon.sfv", &first_bytes).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        3,
        "silver.horizon.extra.sfv",
        &second_bytes,
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 1usize)],
        "the disputed name is withdrawn; only the agreed name is measured"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// The gate is re-entered many times per job. The fallback runs once — a second
/// pass must not re-read the payload.
#[tokio::test]
async fn the_sfv_arm_runs_once_per_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30810);
    let payload_filename = "silver.horizon.bin";
    let payload = b"silver horizon payload".to_vec();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon One Shot",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;
    // The first pass hands the job to the final move, which is where a real
    // job would leave the gate's working status. Put it back — the shape a RAR
    // job returning from an extraction round arrives in — and ask again.
    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 1usize)],
        "the fallback must rule once per job, not once per completion check"
    );
}

/// Incremental RAR extraction deletes a volume as soon as it owns every member
/// that reads from it, which happens long before the completion gate runs. The
/// listing still names that volume, and the download still holds a whole-file
/// CRC32 composed from its verified article CRCs — so the zero-I/O arm answers
/// for a file that no longer exists.
#[tokio::test]
async fn a_volume_extraction_already_consumed_is_verified_from_its_wire_crcs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30811);
    let payload_filename = "silver.horizon.part01.bin";
    let payload: Vec<u8> = (0..96u32).map(|value| (value % 239) as u8).collect();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Consumed Volume",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // Held out of the gate while the fixture is built: the decode worker checks
    // completion the moment the last file lands, and this test's subject is the
    // state *after* extraction consumed the volume, not before.
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let listing_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    submit_decoded_segment(
        &mut pipeline,
        listing_file_id,
        0,
        0,
        &listing_bytes,
        "silver.horizon.sfv",
        Some(par2_rs::checksum::crc32(&listing_bytes)),
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        payload_file_id,
        0,
        0,
        &payload,
        payload_filename,
        Some(par2_rs::checksum::crc32(&payload)),
    )
    .await;

    // What eager deletion leaves behind: no file, and the name recorded as
    // weaver's own removal.
    tokio::fs::remove_file(working_dir.join(payload_filename))
        .await
        .unwrap();
    pipeline
        .eagerly_deleted
        .entry(job_id)
        .or_default()
        .insert(payload_filename.to_string());

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(1usize, 0usize)],
        "a consumed volume must be answered from its retained article CRCs"
    );
    assert!(pipeline.jobs_with_verification_outcome.contains(&job_id));
}

/// The same absence with no retained CRC to compose from. Neither arm can
/// measure the file, but weaver deleted it — that is not the listing finding a
/// hole in the download, so the job is left unverified rather than failed.
#[tokio::test]
async fn a_consumed_volume_without_wire_evidence_is_left_unverified_not_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30812);
    let payload_filename = "silver.horizon.part01.bin";
    let payload = b"silver horizon consumed payload".to_vec();
    let listing = sfv_line(payload_filename, &payload);
    let listing_bytes = listing.as_bytes().to_vec();

    let spec = standalone_job_spec(
        "Silver Horizon Consumed Unprovable",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            ("silver.horizon.sfv".to_string(), listing_bytes.len() as u32),
        ],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        "silver.horizon.sfv",
        &listing_bytes,
    )
    .await;
    tokio::fs::remove_file(working_dir.join(payload_filename))
        .await
        .unwrap();
    pipeline
        .eagerly_deleted
        .entry(job_id)
        .or_default()
        .insert(payload_filename.to_string());

    settle_download_state(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    assert_not_failed(&pipeline, job_id);
    assert_eq!(
        pipeline.sfv_verify_read_splits,
        vec![(0usize, 0usize)],
        "neither arm may run for a consumed volume with no retained CRC"
    );
}
