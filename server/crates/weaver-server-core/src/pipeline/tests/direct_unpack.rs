//! Controller tests: what gets admitted, what gets refused, what a chase
//! produces, and what ends one.
//!
//! These drive the real controller — the same `try_arm_direct_unpack_for_file`
//! the download path calls when a part completes, and the same
//! `direct_unpack_note_commit` the decode worker calls when bytes land — rather
//! than reaching into its state.

use std::path::PathBuf;

use super::*;
use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
use crate::pipeline::direct_unpack::start_header::MAGIC;
use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;

/// Turn the feature on for one pipeline, the way config would.
fn enable_direct_unpack(pipeline: &mut Pipeline) {
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
}

/// A well-formed 32-byte signature header declaring the given end header.
fn signature_header(next_header_offset: u64, next_header_size: u64) -> [u8; 32] {
    let mut header = [0u8; 32];
    header[..6].copy_from_slice(&MAGIC);
    header[7] = 4;
    header[12..20].copy_from_slice(&next_header_offset.to_le_bytes());
    header[20..28].copy_from_slice(&next_header_size.to_le_bytes());
    let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &header[12..32]) as u32;
    header[8..12].copy_from_slice(&crc.to_le_bytes());
    header
}

/// Land a whole split set and run its chase to completion.
///
/// Returns the fixture files so a caller can corrupt or inspect them.
async fn chase_a_complete_split_set(
    pipeline: &mut Pipeline,
    job_id: JobId,
    set_name: &str,
) -> Vec<(String, Vec<u8>)> {
    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(pipeline, job_id, spec).await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    reap_until_outcome(pipeline, job_id, set_name).await;
    files
}

/// Replace every part on disk with garbage.
///
/// The sentinel for "did conventional extraction run?": with the sources
/// destroyed, only an installed chase can produce members, so a successful
/// extraction proves the chase was consumed rather than the archive re-decoded.
fn destroy_parts(pipeline: &Pipeline, job_id: JobId, files: &[(String, Vec<u8>)]) {
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, bytes) in files {
        std::fs::write(working_dir.join(filename), vec![0xAB; bytes.len()]).unwrap();
    }
}

/// Mark a file complete without touching its bytes on disk.
///
/// [`write_and_complete_file`] rewrites the whole file, which truncates it
/// first — harmless when nothing is reading, but a chase running against a
/// drip-fed part would briefly see the file shrink out from under it. Real
/// downloads only ever append, which is the invariant the gated reader is built
/// on, so the drip test appends and then settles the bookkeeping here.
async fn complete_already_written_file(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    len: usize,
) {
    let file_id = NzbFileId { job_id, file_index };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(file_id).unwrap();
        file.record_placement(0, 0, len as u32);
        file.commit_segment(0, len as u32).unwrap();
    }
    pipeline
        .refresh_archive_state_for_completed_file(job_id, file_id, true)
        .await;
}

/// A single-file 7z job whose archive is `bytes`.
async fn insert_single_7z_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    bytes: Vec<u8>,
) -> Vec<(String, Vec<u8>)> {
    let files = vec![("silver_horizon.7z".to_string(), bytes)];
    let spec = rar_job_spec("Silver Horizon", &files);
    insert_active_job(pipeline, job_id, spec).await;
    files
}

/// Run the controller's reaper until `job_id`/`set` has an outcome.
///
/// The budget is deliberately generous. The chase is a real decode on a
/// blocking thread, and this suite runs alongside the rest of the pipeline
/// tests on a shared pool, so a tight deadline measures machine load rather
/// than the controller. Panics with what the controller actually thinks rather
/// than returning a bare bool, so a timeout says why.
async fn reap_until_outcome(pipeline: &mut Pipeline, job_id: JobId, set_name: &str) {
    for _ in 0..3_000 {
        pipeline.reap_direct_unpack().await;
        if pipeline.direct_unpack.outcome(job_id, set_name).is_some() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "chase never produced an outcome (armed={}, latched={:?})",
        pipeline.direct_unpack.is_armed(job_id, set_name),
        pipeline.direct_unpack.latched_reason(job_id, set_name),
    );
}

#[tokio::test]
async fn a_disabled_gate_leaves_no_controller_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // Deliberately not enabling the gate: this is the shipped posture.
    let job_id = JobId(41001);
    let files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let counters = pipeline.direct_unpack.counters();
    assert_eq!(counters.armed, 0);
    assert_eq!(counters.completed, 0);
    assert!(
        !pipeline
            .direct_unpack
            .is_armed(job_id, "generated_split_store_plain.7z")
    );
    assert!(
        !pipeline
            .direct_unpack_staging_dir(job_id, "generated_split_store_plain.7z")
            .exists(),
        "a dark feature must not create directories"
    );
}

#[tokio::test]
async fn a_malformed_signature_header_is_refused() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41002);

    // Right length, wrong magic.
    let mut bytes = vec![0u8; 4096];
    bytes[..6].copy_from_slice(b"NOT7Z!");
    let files = insert_single_7z_job(&mut pipeline, job_id, bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    assert_eq!(
        pipeline
            .direct_unpack
            .latched_reason(job_id, "silver_horizon.7z"),
        Some("header_malformed")
    );
    assert_eq!(
        pipeline.direct_unpack.counters().refused_header_malformed,
        1
    );
    assert_eq!(pipeline.direct_unpack.counters().armed, 0);
}

#[tokio::test]
async fn an_empty_end_header_is_refused() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41003);

    let mut bytes = signature_header(0, 0).to_vec();
    bytes.resize(4096, 0);
    let files = insert_single_7z_job(&mut pipeline, job_id, bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    assert_eq!(
        pipeline
            .direct_unpack
            .latched_reason(job_id, "silver_horizon.7z"),
        Some("empty_end_header")
    );
    assert_eq!(
        pipeline.direct_unpack.counters().refused_empty_end_header,
        1
    );
}

#[tokio::test]
async fn an_oversized_end_header_is_refused_before_any_decoder_sees_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41004);

    // An end header far larger than any extraction budget would allow to be
    // buffered. This is the attacker-controlled allocation the gate exists for.
    let mut bytes = signature_header(64, u64::MAX / 4).to_vec();
    bytes.resize(4096, 0);
    let files = insert_single_7z_job(&mut pipeline, job_id, bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    assert_eq!(
        pipeline
            .direct_unpack
            .latched_reason(job_id, "silver_horizon.7z"),
        Some("end_header_too_large")
    );
    assert_eq!(
        pipeline
            .direct_unpack
            .counters()
            .refused_end_header_too_large,
        1
    );
    assert_eq!(pipeline.direct_unpack.counters().armed, 0);
}

#[tokio::test]
async fn a_refusal_latches_and_is_not_reconsidered() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41005);

    let mut bytes = vec![0u8; 4096];
    bytes[..6].copy_from_slice(b"NOT7Z!");
    let files = insert_single_7z_job(&mut pipeline, job_id, bytes).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    // Offer the same file again, as a re-refresh would.
    for _ in 0..3 {
        pipeline.try_arm_direct_unpack_for_file(job_id, file_id);
    }

    assert_eq!(
        pipeline.direct_unpack.counters().refused_header_malformed,
        1,
        "a latched refusal must be counted once, not on every re-offer"
    );
}

#[tokio::test]
async fn a_drip_fed_split_set_is_chased_to_byte_identical_members() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41010);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    assert!(files.len() > 2, "fixture should be a multi-part set");
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();

    // Part one lands whole: that is what builds the topology and arms the set.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the set should arm once its first part is on disk"
    );
    assert_eq!(pipeline.direct_unpack.counters().armed, 1);
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");

    // The rest arrive in pieces, each commit published the way the decode
    // worker publishes one.
    for (file_index, (filename, bytes)) in files.iter().enumerate().skip(1) {
        let path = working_dir.join(filename);
        let chunk = (bytes.len() / 4).max(1);
        let mut written = 0usize;
        while written < bytes.len() {
            let end = (written + chunk).min(bytes.len());
            // Deliberately `std::fs`, not `tokio::fs`: the watermark must not
            // be published until the bytes are actually on disk, and a
            // `tokio::fs::File` buffers internally, so `write_all` alone can
            // leave the last bytes unwritten. The download path advances its
            // floor only after the write syscall has returned, and this
            // mirrors that.
            use std::io::Write;
            if written == 0 {
                std::fs::write(&path, &bytes[..end]).unwrap();
            } else {
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                file.write_all(&bytes[written..end]).unwrap();
            }
            written = end;
            pipeline.direct_unpack_note_commit(
                NzbFileId {
                    job_id,
                    file_index: file_index as u32,
                },
                filename,
                written as u64,
                false,
            );
            tokio::task::yield_now().await;
        }
        // Completion is what settles the part's exact length.
        complete_already_written_file(&mut pipeline, job_id, file_index as u32, bytes.len()).await;
    }

    reap_until_outcome(&mut pipeline, job_id, set_name).await;

    let outcome = pipeline.direct_unpack.outcome(job_id, set_name).unwrap();
    let extracted = outcome.result.as_ref().unwrap_or_else(|error| {
        let state: Vec<String> = (0..files.len())
            .map(|index| format!("{index}:{:?}", coverage.part_progress(index)))
            .collect();
        panic!(
            "direct unpack failed: {error}\n  total_len={:?}\n  parts={}\n  sum_on_disk={}",
            coverage.total_len(),
            state.join(" "),
            files.iter().map(|(_, b)| b.len()).sum::<usize>(),
        )
    });
    assert_eq!(extracted.extracted.len(), 1);
    assert_eq!(pipeline.direct_unpack.counters().completed, 1);

    // Byte-identical to the material the fixture was built from.
    let member = extracted.extracted[0].clone();
    let staged = pipeline
        .direct_unpack_staging_dir(job_id, set_name)
        .join(&member);
    let original = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sevenz/originals")
        .join(&member);
    assert_eq!(
        std::fs::read(&staged).unwrap(),
        std::fs::read(&original).unwrap(),
        "chased member differs from the original"
    );

    // The conventional path's staging is somewhere else entirely, which is what
    // lets a demotion delete the chase's output without the conventional
    // extractor ever having seen it.
    let conventional = pipeline.extraction_staging_dir(job_id);
    let chased = pipeline.direct_unpack_staging_dir(job_id, set_name);
    assert!(
        !chased.starts_with(&conventional) && !conventional.starts_with(&chased),
        "staging trees must be disjoint: conventional {} vs chased {}",
        conventional.display(),
        chased.display()
    );
    assert!(
        !conventional.join(member).exists(),
        "the chase must not write members into the conventional staging dir"
    );
}

/// Under write-backlog pressure the decode path evicts parked segments to their
/// true offsets, which leaves a sparse hole below the furthest byte written. A
/// watermark seeded from the file's *length* would present that hole as
/// committed and feed the decoder zeros; the seed must come from the contiguous
/// progress floor instead.
#[tokio::test]
async fn an_out_of_order_tail_does_not_inflate_the_seeded_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41060);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();

    // Part two, arranged BEFORE the set arms: a contiguous prefix, then an
    // out-of-order tail at its true offset, leaving a hole between them. The
    // file is now longer than the verified prefix.
    let floor = 8_192u64;
    let hole_end = 40_960u64;
    let part_two = working_dir.join(&files[1].0);
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::File::create(&part_two).unwrap();
        file.write_all(&files[1].1[..floor as usize]).unwrap();
        file.seek(SeekFrom::Start(hole_end)).unwrap();
        file.write_all(&files[1].1[hole_end as usize..hole_end as usize + 4_096])
            .unwrap();
    }
    let part_two_len = std::fs::metadata(&part_two).unwrap().len();
    assert!(
        part_two_len > floor,
        "the fixture must leave a hole: len {part_two_len} vs floor {floor}"
    );
    // Only the contiguous prefix is recorded as the floor, which is what the
    // real drain records.
    pipeline.pending_file_progress.insert(
        NzbFileId {
            job_id,
            file_index: 1,
        },
        floor,
    );

    // Part one lands whole, which is what arms the set.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    let seeded = coverage.part_progress(1).unwrap();
    // The hole is inside the part, so the byte at the floor is not readable
    // while the byte before it is.
    let readable_below = coverage.readable_at(1, floor - 1).unwrap();

    // Shut the chase down before asserting: its worker is parked on this
    // coverage, and a panic with a parked blocking task would hang the
    // runtime's drop rather than fail the test.
    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;

    assert_eq!(
        seeded.watermark, floor,
        "the seed must be the contiguous floor, not the file length {part_two_len}"
    );
    assert_eq!(seeded.len, None, "an unfinished part has no settled length");
    assert_eq!(
        readable_below, 1,
        "the verified prefix ends at the floor, so exactly one byte is readable below it"
    );
}

#[tokio::test]
async fn cancelling_a_job_aborts_the_chase_and_removes_its_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41020);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Arm, then leave the rest of the set undelivered so the worker parks.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    let staging = pipeline.direct_unpack_staging_dir(job_id, set_name);

    // The job-removal seam every cancel and purge reaches.
    pipeline.direct_unpack_forget_job(job_id);
    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "the set must no longer be armed"
    );

    // The parked worker was woken by the abort; the reaper joins it.
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        if !staging.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !staging.exists(),
        "aborted staging must be removed once the worker is joined"
    );
}

#[tokio::test]
async fn a_renamed_part_demotes_the_chase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41030);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // A part renamed under the chase: the seam that notices is the one the
    // deobfuscation and PAR2 identity paths both go through.
    pipeline.direct_unpack_abort_sets_containing(job_id, &files[1].0, "archive part renamed");

    assert!(!pipeline.direct_unpack.is_armed(job_id, set_name));
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_download_ended,
        1,
        "a rename demotes the chase rather than failing it obscurely later"
    );
}

/// The genuine end of the road: parts that never arrived and never will.
///
/// It takes both passes. The lenient one deliberately holds its fire — at that
/// moment it cannot tell a part whose commit is still in flight from one that
/// has none — and the strict one, which runs from the completion check after
/// decode has drained, is what ends the chase.
#[tokio::test]
async fn a_download_that_ends_with_a_part_missing_aborts_the_chase_at_the_strict_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41040);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // Every other part never arrives; the download pass ends anyway.
    pipeline.settle_direct_unpack_after_download(job_id);
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the lenient pass must not end a chase on parts whose commits could still be in flight"
    );

    // The strict pass declines while the job still has queued work, so a
    // completion check taken mid-download cannot end anything.
    pipeline.settle_direct_unpack_at_completion(job_id);
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "and the strict pass must decline while download work is still queued"
    );

    // Now the queue is genuinely empty and decode has drained. Nothing is coming
    // for those parts, and this is the pass that says so.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }
    pipeline.settle_direct_unpack_after_download(job_id);
    pipeline.settle_direct_unpack_at_completion(job_id);

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "a chase waiting on bytes that stopped coming must be ended"
    );
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        Some("download_ended")
    );
}

#[tokio::test]
async fn a_contradictory_part_length_aborts_the_chase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41050);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    // Part 0's length was settled by its completion. A second, different
    // declaration would move every later part's offset.
    coverage.note_part_len(0, files[0].1.len() as u64 + 1);

    assert!(
        coverage.abort_reason().is_some(),
        "a contradictory part length must abort the set"
    );
    assert!(
        coverage.readable_at(0, 0).is_err(),
        "and every subsequent read must fail"
    );
}

// ---------------------------------------------------------------------------
// Consumption
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_finished_chase_is_installed_instead_of_re_extracting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41100);
    let set_name = "generated_split_store_plain.7z";

    let files = chase_a_complete_split_set(&mut pipeline, job_id, set_name).await;
    let chase_staging = pipeline.direct_unpack_staging_dir(job_id, set_name);
    assert!(chase_staging.exists(), "the chase staged its members");

    // Destroy the sources. From here only an installed chase can succeed.
    destroy_parts(&pipeline, job_id, &files);

    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("installed members, so the destroyed sources never mattered");
    assert_eq!(outcome.extracted.len(), 1);

    let member = &outcome.extracted[0];
    let installed = pipeline.extraction_staging_dir(job_id).join(member);
    let original = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sevenz/originals")
        .join(member);
    assert_eq!(
        std::fs::read(&installed).unwrap(),
        std::fs::read(&original).unwrap(),
        "installed member differs from the original"
    );

    assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
    assert_eq!(pipeline.direct_unpack.counters().discarded, 0);
    assert!(
        !chase_staging.exists(),
        "the chase's staging dir is removed once its members are installed"
    );
}

#[tokio::test]
async fn consumption_attributes_the_chase_bytes_to_the_extracting_phase_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41110);
    let set_name = "generated_split_store_plain.7z";

    chase_a_complete_split_set(&mut pipeline, job_id, set_name).await;
    let expected_total = pipeline
        .direct_unpack
        .outcome(job_id, set_name)
        .expect("an outcome")
        .total_bytes;
    assert!(expected_total > 0, "the chase decoded real bytes");

    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let _ = next_extraction_done(&mut pipeline).await;

    let counters = pipeline
        .phase_progress
        .get(&(job_id, JobPhase::Extracting))
        .map(|runtime| Arc::clone(&runtime.counters))
        .expect("the Extracting phase exists");
    assert_eq!(
        counters.total_bytes.load(Ordering::Relaxed),
        expected_total,
        "the phase must report the chase's real bytes, attributed exactly once"
    );
}

#[tokio::test]
async fn a_tainted_outcome_is_discarded_and_the_set_is_extracted_conventionally() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41120);
    let set_name = "generated_split_store_plain.7z";

    chase_a_complete_split_set(&mut pipeline, job_id, set_name).await;
    let chase_staging = pipeline.direct_unpack_staging_dir(job_id, set_name);

    // Repair rewrote the sources after the chase read them.
    pipeline.taint_direct_unpack_set(job_id, set_name);
    assert_eq!(pipeline.direct_unpack.counters().demoted_repair_rewrote, 1);

    // Sources are intact here, so conventional extraction must produce the
    // same members from scratch.
    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("conventional extraction of intact sources");
    assert_eq!(outcome.extracted.len(), 1);

    let member = &outcome.extracted[0];
    let installed = pipeline.extraction_staging_dir(job_id).join(member);
    let original = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sevenz/originals")
        .join(member);
    assert_eq!(
        std::fs::read(&installed).unwrap(),
        std::fs::read(&original).unwrap()
    );

    assert_eq!(pipeline.direct_unpack.counters().consumed, 0);
    assert_eq!(pipeline.direct_unpack.counters().discarded, 1);
    assert!(
        !chase_staging.exists(),
        "a discarded chase's staging is removed"
    );
}

#[tokio::test]
async fn a_repair_rewrite_mid_chase_taints_the_running_chase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41130);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    // Arm with the rest of the set still outstanding, so the chase is running.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // The remaining parts reach disk but are never marked complete, so the
    // chase stays parked at the frontier while the conventional path — which
    // reads the files, not the coverage — has a whole archive to fall back to.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, bytes) in files.iter().skip(1) {
        std::fs::write(working_dir.join(filename), bytes).unwrap();
    }

    let chase_staging = pipeline.direct_unpack_staging_dir(job_id, set_name);

    // Repair touches one of its parts while it is still being chased.
    pipeline.taint_direct_unpack_for_file(job_id, &files[1].0);
    assert_eq!(pipeline.direct_unpack.counters().demoted_repair_rewrote, 1);
    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "tainting a running chase must end it, not merely flag it"
    );

    // The worker was parked on the coverage; the taint woke it, so it reaps
    // here rather than being held until extraction dispatch gets to it.
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        if !chase_staging.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !chase_staging.exists(),
        "the aborted chase should have unparked, joined, and had its staging removed"
    );

    // Consumption then finds nothing to consume and extracts conventionally.
    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    assert!(
        result.is_ok(),
        "the intact sources still extract conventionally: {:?}",
        result.as_ref().err()
    );
    assert_eq!(pipeline.direct_unpack.counters().consumed, 0);
}

// ---------------------------------------------------------------------------
// Tail prefetch
// ---------------------------------------------------------------------------

/// The chase cannot list an archive until the end header — the last bytes of
/// the last part — is on disk, so those segments join the head wave at queue
/// birth. Planned at birth rather than reprioritized later because a
/// reprioritization only reaches work still queued.
#[tokio::test]
async fn a_7z_split_set_boosts_both_ends_of_every_part_at_queue_birth() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41200);

    // Three segments per part, so head, middle and tail are distinguishable.
    let parts = ["silver_horizon.7z.001", "silver_horizon.7z.002"];
    let spec = JobSpec {
        name: "Silver Horizon Split".to_string(),
        password: None,
        total_bytes: 6 * 1_000,
        category: None,
        metadata: vec![],
        files: parts
            .iter()
            .enumerate()
            .map(|(file_index, filename)| FileSpec {
                filename: filename.to_string(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..3u32)
                    .map(|ordinal| {
                        segment_spec! {
                            number: ordinal,
                            bytes: 1_000,
                            message_id: format!("du-{file_index}-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    let works = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .drain_all();

    for file_index in 0..parts.len() as u32 {
        let priority_of = |ordinal: u32| {
            works
                .iter()
                .find(|work| {
                    work.segment_id.file_id.file_index == file_index
                        && work.segment_id.segment_number == ordinal
                })
                .unwrap_or_else(|| panic!("segment {file_index}/{ordinal} should be queued"))
                .priority
        };

        assert_eq!(priority_of(0), 2, "part {file_index}: head is boosted");
        assert_eq!(
            priority_of(2),
            2,
            "part {file_index}: tail carries the end header and is boosted"
        );
        assert_ne!(
            priority_of(1),
            2,
            "part {file_index}: the middle is ordinary payload"
        );
    }
}

/// Extraction can reach a set whose chase has not been reaped yet. Every part
/// is complete by then, so the chase is finishing at disk speed and awaiting it
/// is cheaper than decoding the archive again — but it must be awaited inside
/// the extraction task, never on the orchestrator loop.
#[tokio::test]
async fn a_chase_still_running_at_extraction_is_awaited_and_installed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41140);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    // Deliberately NOT reaped: the chase is still in flight, so extraction
    // takes the pending arm.
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the chase should still be armed and unreaped"
    );

    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("the awaited chase produced members");
    assert_eq!(outcome.extracted.len(), 1);

    let member = &outcome.extracted[0];
    let installed = pipeline.extraction_staging_dir(job_id).join(member);
    let original = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sevenz/originals")
        .join(member);
    assert_eq!(
        std::fs::read(&installed).unwrap(),
        std::fs::read(&original).unwrap()
    );
    assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
    assert!(
        !pipeline
            .direct_unpack_staging_dir(job_id, set_name)
            .exists(),
        "the chase's staging is consumed by the install"
    );
}

/// Stage 2 of the tail prefetch: at arming, the segments covering the end of
/// the archive are pulled forward even when they are not a part's *last*
/// segment, which is all the birth-time boost can reach.
///
/// The distinguishing assertion is the second-to-last segment of the last part:
/// birth-time never touches it, and the tail window does.
#[tokio::test]
async fn arming_pulls_the_tail_window_forward_in_a_deep_queue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41210);
    let set_name = "silver_horizon.7z";

    const SEGMENT_BYTES: u32 = 1024 * 1024;
    const SEGMENTS_PER_TAIL_PART: u32 = 4;
    let head_bytes = 4_096u64;
    let tail_part_bytes = SEGMENT_BYTES as u64 * SEGMENTS_PER_TAIL_PART as u64;
    let total_len = head_bytes + tail_part_bytes * 2;

    // Part one is real: arming reads its 32-byte signature header off disk. The
    // declared end header is tiny, so the window is essentially the 1 MiB of
    // slack — comfortably inside the last part.
    let next_header_size = 122u64;
    let next_header_offset = total_len - 32 - next_header_size;
    let mut head = signature_header(next_header_offset, next_header_size).to_vec();
    head.resize(head_bytes as usize, 0);

    let mut files = vec![FileSpec {
        filename: "silver_horizon.7z.001".to_string(),
        role: FileRole::from_filename("silver_horizon.7z.001"),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: head_bytes as u32,
            message_id: "du-tail-0@example.com".to_string(),
        }],
    }];
    for part in 1..3u32 {
        files.push(FileSpec {
            filename: format!("silver_horizon.7z.{:03}", part + 1),
            role: FileRole::from_filename(&format!("silver_horizon.7z.{:03}", part + 1)),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..SEGMENTS_PER_TAIL_PART)
                .map(|ordinal| {
                    segment_spec! {
                        number: ordinal,
                        bytes: SEGMENT_BYTES,
                        message_id: format!("du-tail-{part}-{ordinal}@example.com"),
                    }
                })
                .collect(),
        });
    }

    let spec = JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        total_bytes: total_len,
        category: None,
        metadata: vec![],
        files,
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    let priority_of = |pipeline: &Pipeline, file_index: u32, segment_number: u32| {
        let mut found = None;
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .download_queue
            .count_matching(|work| {
                if work.segment_id.file_id.file_index == file_index
                    && work.segment_id.segment_number == segment_number
                {
                    found = Some(work.priority);
                }
                false
            });
        found.expect("segment should be queued")
    };

    // Before arming: only the birth wave has boosted anything, and it never
    // reaches a part's second-to-last segment.
    assert_ne!(
        priority_of(&pipeline, 2, SEGMENTS_PER_TAIL_PART - 2),
        2,
        "birth-time boost must not have touched the second-to-last segment"
    );

    // Landing part one arms the set, which is when the window is computed.
    write_and_complete_file(&mut pipeline, job_id, 0, "silver_horizon.7z.001", &head).await;
    let armed = pipeline.direct_unpack.is_armed(job_id, set_name);
    let tail_window_segment = priority_of(&pipeline, 2, SEGMENTS_PER_TAIL_PART - 2);
    // Segment 0 of every part is already boosted at birth, so the segment that
    // proves the window's *edge* is the one neither reaches: the last part's
    // second segment, ahead of the window and behind the head.
    let last_part_middle = priority_of(&pipeline, 2, 1);
    let middle_part = priority_of(&pipeline, 1, 1);

    // The rest of the set never arrives, so the worker is parked. Shut it down
    // before asserting: a panic with a parked blocking task hangs the runtime's
    // drop instead of failing the test.
    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;

    assert!(armed, "the set should have armed once its header landed");
    assert_eq!(
        tail_window_segment, 2,
        "the tail window must pull the last part's closing segments forward"
    );
    assert_ne!(
        last_part_middle, 2,
        "bytes ahead of the window stay ordinary even in the last part"
    );
    assert_ne!(
        middle_part, 2,
        "a middle part is outside the window entirely"
    );
}

// ---------------------------------------------------------------------------
// Single-file .7z early arming
// ---------------------------------------------------------------------------

/// A bare `.7z` is a one-part set, so it has no order to discover and no reason
/// to wait for a topology. It arms as soon as its signature header is on disk —
/// 32 bytes — which is the whole overlap an unsplit archive can win.
#[tokio::test]
async fn a_single_7z_arms_from_its_first_32_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41300);

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();
    let files = vec![("silver_horizon.7z".to_string(), archive.clone())];
    let spec = rar_job_spec("Silver Horizon", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    let path = working_dir.join("silver_horizon.7z");

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // One byte short of a signature header: nothing to decide on yet.
    std::fs::write(&path, &archive[..31]).unwrap();
    pipeline.direct_unpack_note_commit(file_id, "silver_horizon.7z", 31, false);
    assert!(
        !pipeline.direct_unpack.is_armed(job_id, "silver_horizon.7z"),
        "31 bytes is not a signature header"
    );

    // The 32nd byte is enough.
    std::fs::write(&path, &archive[..64 * 1024]).unwrap();
    pipeline.direct_unpack_note_commit(file_id, "silver_horizon.7z", 64 * 1024, false);
    assert!(
        pipeline.direct_unpack.is_armed(job_id, "silver_horizon.7z"),
        "a single .7z must arm from its header, not from its completion"
    );

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "silver_horizon.7z")
        .expect("armed");
    assert_eq!(
        coverage.total_len().unwrap(),
        archive.len() as u64,
        "the header's declared total is the archive's real length"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The header's declared total is taken on trust while the file is still
/// arriving. Completion is when it gets checked, and a file that settles at a
/// different length is not the archive the header described.
#[tokio::test]
async fn a_single_7z_whose_length_contradicts_its_header_aborts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41310);

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();
    let files = vec![("silver_horizon.7z".to_string(), archive.clone())];
    let spec = rar_job_spec("Silver Horizon", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    std::fs::write(working_dir.join("silver_horizon.7z"), &archive[..64 * 1024]).unwrap();
    pipeline.direct_unpack_note_commit(file_id, "silver_horizon.7z", 64 * 1024, false);
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "silver_horizon.7z")
        .expect("armed");

    // The file settles shorter than the header promised.
    let short = archive.len() as u64 - 4_096;
    coverage.advance_watermark(0, short);
    coverage.mark_part_complete(0);

    let reason = coverage.abort_reason().expect("a contradiction aborts");
    assert!(
        reason.contains("declared") && reason.contains(&archive.len().to_string()),
        "unexpected abort reason: {reason}"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The commit hook is on the download's hot path, so a job that carries no
/// unsplit 7z must leave it costing one `is_empty` and nothing else — no
/// registration, no lookup, no arming attempt.
#[tokio::test]
async fn a_job_without_a_single_7z_registers_no_arming_candidates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41320);

    // A split set: every part is a SevenZipSplit, none is a bare archive.
    let files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    assert!(
        pipeline.direct_unpack.no_pending_single_arm(),
        "a split set must not register single-file arming candidates"
    );

    // And with the gate off, not even a bare .7z registers.
    let dark_job = JobId(41321);
    pipeline.direct_unpack = DirectUnpackRuntime::default();
    let single = vec![("silver_horizon.7z".to_string(), vec![0u8; 4_096])];
    let spec = rar_job_spec("Silver Horizon", &single);
    insert_active_job(&mut pipeline, dark_job, spec).await;
    assert!(
        pipeline.direct_unpack.no_pending_single_arm(),
        "a dark pipeline must register nothing"
    );
}

/// The repair-time decision is allowed to spare a chase only when the recovery
/// set positively vouches for every byte it consumed. With no binding and no
/// grid verdicts — which is the state of any set the recovery data does not
/// describe — nothing is vouched, and the behaviour must be exactly the taint
/// it was before repair-resume existed.
///
/// This is the guard on the fallback. Everything above it is an optimisation;
/// this is the thing that must not regress.
#[tokio::test]
async fn a_repair_over_an_unvouched_chase_taints_exactly_as_before() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41400);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // The rest of the set reaches disk so the conventional fallback has a whole
    // archive to read, but no PAR2 set binds these files: there are no verdicts
    // to vouch with.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, bytes) in files.iter().skip(1) {
        std::fs::write(working_dir.join(filename), bytes).unwrap();
    }

    // The real repair-time entry point.
    pipeline.decide_direct_unpack_before_repair(job_id, None);

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "an unvouched chase must be ended by the repair decision"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_repair_rewrote,
        1,
        "the fallback must still book a repair-rewrote demotion"
    );
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        Some("repair_rewrote")
    );

    // And the set extracts conventionally, correctly.
    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("conventional extraction of intact sources");
    assert_eq!(outcome.extracted.len(), 1);
    assert_eq!(pipeline.direct_unpack.counters().consumed, 0);
    // Nothing is left for dispatch to *discard*: tainting a running chase ends
    // it on the spot, so the demotion is the whole accounting. `discarded`
    // counts outcomes thrown away at consumption, which this never reached.
    assert_eq!(pipeline.direct_unpack.counters().discarded, 0);

    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
}

/// A repair that fails leaves nothing to lift the damage caps, so the sets it
/// was allowed to run underneath have to be ended rather than left parked.
#[tokio::test]
async fn a_failed_repair_releases_the_chases_it_was_parked_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41410);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");

    // Stand in for the vouched decision: the set is parked through a repair.
    pipeline.park_direct_unpack_through_repair_for_test(job_id, set_name);

    pipeline.fail_direct_unpack_after_repair(job_id, "PAR2 repair failed");

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "a stranded chase must be ended when its repair fails"
    );
    // Its own reason. A failed repair rewrote nothing, so booking this as
    // `repair_rewrote` would name something that did not happen — and the
    // download did not end either, which is what it used to claim.
    assert_eq!(pipeline.direct_unpack.counters().demoted_repair_failed, 1);
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_repair_rewrote,
        0,
        "nothing was rewritten"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_download_ended,
        0,
        "and the download did not end"
    );
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        Some("repair_failed")
    );
    let reason = coverage
        .abort_reason()
        .expect("the parked reader must be woken");
    assert!(reason.contains("PAR2 repair failed"), "reason: {reason}");

    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
}

/// The seam every exit from the repairer funnels through. A parked chase is
/// waiting on a frontier only this call advances, so both directions matter —
/// and so does the analysis case, which parks nothing and must release nothing.
#[tokio::test]
async fn the_repair_settle_seam_releases_on_success_and_ends_on_failure() {
    async fn armed_and_parked(
        temp_dir: &tempfile::TempDir,
        job_id: JobId,
    ) -> (
        Pipeline,
        std::sync::Arc<crate::pipeline::direct_unpack::SetCoverage>,
    ) {
        let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
        enable_direct_unpack(&mut pipeline);
        let set_name = "generated_split_store_plain.7z";
        let files = sevenz_fixture_bytes(set_name);
        let spec = rar_job_spec("Silver Horizon Split", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;
        write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
        let coverage = pipeline
            .direct_unpack
            .armed_coverage(job_id, set_name)
            .expect("armed");
        pipeline.park_direct_unpack_through_repair_for_test(job_id, set_name);
        (pipeline, coverage)
    }

    let set_name = "generated_split_store_plain.7z";

    // An analysis pass parks nothing, so it must leave the set exactly as it is.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, coverage) = armed_and_parked(&temp_dir, JobId(41500)).await;
        pipeline.settle_direct_unpack_after_repair(JobId(41500), false, &Err("unused".to_string()));
        assert!(pipeline.direct_unpack.is_armed(JobId(41500), set_name));
        assert!(coverage.abort_reason().is_none());
        pipeline.direct_unpack_forget_job(JobId(41500));
        for _ in 0..600 {
            pipeline.reap_direct_unpack().await;
            tokio::task::yield_now().await;
        }
    }

    // A failed repair ends the parked chase and wakes its reader.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, coverage) = armed_and_parked(&temp_dir, JobId(41510)).await;
        pipeline.settle_direct_unpack_after_repair(
            JobId(41510),
            true,
            &Err("evidence candidates failed".to_string()),
        );
        assert!(!pipeline.direct_unpack.is_armed(JobId(41510), set_name));
        let reason = coverage.abort_reason().expect("the parked reader is woken");
        assert!(
            reason.contains("evidence candidates failed"),
            "reason: {reason}"
        );
        for _ in 0..600 {
            pipeline.reap_direct_unpack().await;
            tokio::task::yield_now().await;
        }
    }
}

/// What the `direct-unpack-repair` e2e failure actually measured.
///
/// The chase does not read a byte at a time: it reads through a 128 KiB
/// `BufReader`, and the 7z decoder's first move is a probe at the *end* of the
/// archive for the header. So within microseconds of arming, the consumed
/// high-water is 128 KiB into the first part and the whole of the last one —
/// long before any part has completed and therefore long before any damage cap
/// exists to hold it back.
///
/// That is not a bug in the accounting: consumed-includes-readahead only ever
/// over-demands vouching, which is the safe direction. It does mean a chase can
/// consume bytes that a later verdict calls damaged, and such a chase can never
/// vouch itself. This test pins the measurement rather than a wish, so the
/// number is on the record when the semantics are revisited.
#[tokio::test]
async fn a_chase_consumes_readahead_and_the_archive_tail_within_microseconds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41600);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    let observed_head = coverage.consumed_high_water(0);
    let observed_tail = coverage.consumed_high_water(files.len() - 1);

    // Pin the consumption rather than race the decode thread. How far the chase
    // has actually run by this instant depends on scheduling — a set that has
    // consumed *nothing* vouches trivially and correctly, because there is
    // nothing repair could invalidate — so the case under test has to be stated
    // outright: bytes were read, and no verdict covers them.
    coverage.note_consumed(0, 64 * 1024);

    // With no grid verdicts behind it — the state of any set the recovery data
    // has not independently claimed — nothing vouches, and the fallback taint
    // is the correct outcome.
    pipeline.decide_direct_unpack_before_repair(job_id, None);
    let tainted = !pipeline.direct_unpack.is_armed(job_id, set_name);

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;

    // No assertion on the magnitudes: how far the chase has run by this point
    // depends on how much of its blocking decode has been scheduled, and
    // pinning it would be pinning the scheduler. Measured values on this
    // fixture have reached 131072 into the head part and the whole of the tail
    // one within microseconds of arming — the numbers are reported here rather
    // than asserted, and the deterministic half is what gets checked.
    assert!(
        tainted,
        "a chase that consumed unvouched bytes must taint; observed head={observed_head} tail={observed_tail}"
    );
}

/// Reproduces the e2e `direct-unpack-solid-split` failure: the set armed 22
/// microseconds before the download-end settle ran, and the settle decided its
/// parts were incomplete and killed it.
///
/// Deterministic by construction rather than by timing: every part is landed
/// first, so arming happens with the whole set already on disk — exactly the
/// state the racing arm found itself in — and then the settle runs.
#[tokio::test]
async fn a_set_armed_after_its_download_ended_is_not_killed_by_the_settle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41610);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the set arms as its last part lands"
    );

    // The download pass ends. Every part is complete on disk, so there is
    // nothing here the settle should object to.
    pipeline.settle_direct_unpack_after_download(job_id);

    let armed = pipeline.direct_unpack.is_armed(job_id, set_name);
    let latched = pipeline.direct_unpack.latched_reason(job_id, set_name);
    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;

    assert!(
        armed,
        "a set whose parts are all complete must survive the download-end settle, got latch {latched:?}"
    );
}

/// The panic, at the seam that caused it.
///
/// The download drains while a part's writes are still flushing, so the file on
/// disk measures far short of what the part will actually be. The settle used to
/// read that length and declare it — and the real completion commit, arriving
/// moments later with the true and larger length, then had no truthful move
/// left: it advanced a watermark past a declared length and tripped an assert
/// that killed the whole pipeline task.
///
/// The settle must not look at the file at all. It leaves the part alone, the
/// commit lands, and the part settles at its real length.
#[tokio::test]
async fn a_settle_that_races_a_flush_leaves_the_part_for_its_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41620);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Only the first part is reconciled — that is what builds the topology and
    // arms the set.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // Part 1 is mid-flush: a prefix of it is on disk, and the assembly does not
    // describe it yet. This is exactly the state that produced a 12,288,000-byte
    // length for a 21,097,033-byte part.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    let short_prefix = files[1].1.len() / 3;
    std::fs::write(working_dir.join(&files[1].0), &files[1].1[..short_prefix]).unwrap();

    pipeline.settle_direct_unpack_after_download(job_id);

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("the lenient settle must not end this chase");
    assert!(
        coverage.abort_reason().is_none(),
        "the lenient settle must not have contradicted anything"
    );
    let progress = coverage.part_progress(1).expect("part 1 exists");
    assert!(
        !progress.complete && progress.len.is_none(),
        "the settle must take no length from a file that is still being written, got {progress:?}"
    );

    // Now the commit that was in flight lands, with the whole truth.
    write_and_complete_file(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    let progress = coverage.part_progress(1).expect("part 1 exists");
    assert_eq!(
        (progress.complete, progress.len),
        (true, Some(files[1].1.len() as u64)),
        "the completion commit must settle the part at its real length"
    );
    assert!(
        coverage.abort_reason().is_none(),
        "and it must not have contradicted a fabricated one"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_download_ended,
        0,
        "nothing here is a demotion"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The `direct-unpack-solid-split` abort: the settle ran 1ms after arming, while
/// a part's writes were still buffered and its file did not exist on disk at
/// all. Nothing about that says the part is not coming.
#[tokio::test]
async fn a_settle_before_a_parts_file_exists_does_not_end_the_chase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41630);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // No file for parts 1.. exists yet, and the download pass reports drained.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, _) in files.iter().skip(1) {
        assert!(!working_dir.join(filename).exists());
    }
    pipeline.settle_direct_unpack_after_download(job_id);

    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "a chase must survive a settle that merely ran ahead of the decode stage"
    );

    // The buffered writes drain and every part settles.
    for (file_index, (filename, bytes)) in files.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("still armed");
    assert!(
        (0..files.len()).all(|index| coverage
            .part_progress(index)
            .is_ok_and(|part| part.complete)),
        "every part settles from its own commit"
    );

    // And the strict pass, which runs after all that, finds nothing to object to.
    pipeline.settle_direct_unpack_at_completion(job_id);
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the strict pass must be a no-op once every part has settled"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// A successful repair outcome, for driving the settle seam directly.
fn repaired_outcome() -> par2_rs::Par2RepairOutcome {
    par2_rs::Par2RepairOutcome {
        status: par2_rs::Par2RepairStatus::Repaired,
        files_complete: 3,
        files_renamed: 0,
        files_damaged: 0,
        files_missing: 0,
        available_blocks: 1,
        missing_blocks: 0,
        recovery_blocks_available: 1,
        recovery_blocks_used: 1,
        bytes_copied: 0,
        bytes_reconstructed: 1024,
        packets: par2_rs::PacketDiagnostics::default(),
        scan: par2_rs::ScanDiagnostics::default(),
        carry: par2_rs::repairer::CarryDiagnostics::default(),
        verification: par2_rs::VerificationResult {
            files: Vec::new(),
            recovery_blocks_available: 1,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        },
    }
}

/// The starvation escape. A gated set whose claims never arrive — the
/// straddled-slice geometry, where no block is ever independently claimed — is
/// parked on evidence that does not exist. Once the repair has concluded there
/// is nothing left in the system that could unpark it, so it must demote by
/// name rather than hold a blocking thread for ever.
#[tokio::test]
async fn a_gated_chase_with_no_vouching_evidence_demotes_after_the_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41700);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");

    // Damage is reported, so the set gates — but nothing ever vouches for
    // anything, which is exactly what straddled articles produce.
    coverage.cap_at_damage(1, 0);
    assert!(coverage.is_gated());

    // The repair concludes. Nothing released this set, and no claim is coming.
    pipeline.settle_direct_unpack_after_repair(job_id, true, &Ok(repaired_outcome()));

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "a stalled gated chase must not survive the repair that could have freed it"
    );
    // One demotion, counted once, under its own name. The abort used to record
    // a `DownloadEnded` of its own on top of this, so a single stalled chase
    // booked two demotions and latched under a reason that had not happened.
    assert_eq!(pipeline.direct_unpack.counters().demoted_gated_stall, 1);
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_download_ended,
        0,
        "the abort must not also book a download-ended demotion"
    );
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        Some("gated_stall"),
        "and the latch must name what actually happened"
    );
    let reason = coverage
        .abort_reason()
        .expect("the parked reader must be woken");
    assert!(reason.contains("gated chase stalled"), "reason: {reason}");

    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// The real vouch path: PAR2 binding, grid, verdicts
//
// These stand up an actual recovery set over the actual part files and feed the
// actual article-evidence call the decode worker makes. The coverage-layer
// tests above can prove the frontier arithmetic but not this: whether the
// evidence the arithmetic runs on exists at all.
// ---------------------------------------------------------------------------

/// The block size the recovery sets in this section describe their files on.
const VOUCH_SLICE: u64 = 65_536;

/// Offer one article per recovery-set block, through the same entry point the
/// decode worker uses.
///
/// `plan` is the checkpoint geometry the decoder actually applied, which is the
/// whole point: an article decoded before the PAR2 packets were parsed was cut
/// on no grid at all, and no later knowledge can retroactively cut it.
fn feed_block_aligned_articles(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    bytes: &[u8],
    plan: &weaver_yenc::CheckpointPlan,
) {
    let mut offset = 0usize;
    while offset < bytes.len() {
        let end = (offset + VOUCH_SLICE as usize).min(bytes.len());
        let chunk = &bytes[offset..end];
        let crc32 = par2_rs::checksum::crc32(chunk);
        pipeline.note_block_crc_segments_for_plan(
            file_id,
            plan,
            offset as u64,
            chunk.len() as u64,
            crc32,
            true,
            false,
            &[weaver_yenc::Segment {
                file_offset: offset as u64,
                len: chunk.len() as u64,
                crc32,
            }],
        );
        offset = end;
    }
    // What the decode worker does once the file's last write lands: the length
    // is what makes the short final block's extent decidable, and without it
    // that block stays unclaimed and the intact prefix stops one block early.
    pipeline
        .block_crcs
        .note_file_len(file_id, bytes.len() as u64);
}

/// Stand up a job whose 7z parts are described by a real recovery set, with the
/// chase armed on part 0.
///
/// `plan_for_articles` decides whether the article evidence lands on the
/// recovery set's grid or on no grid at all.
async fn armed_chase_with_real_par2(
    pipeline: &mut Pipeline,
    job_id: JobId,
    set_name: &str,
    plan_for_articles: impl Fn(&weaver_yenc::CheckpointPlan) -> weaver_yenc::CheckpointPlan,
) -> Vec<(String, Vec<u8>)> {
    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(pipeline, job_id, spec).await;

    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let par2_set = build_repairable_par2_set_for_files(&described, VOUCH_SLICE, 4);
    install_test_par2_runtime(pipeline, job_id, par2_set, &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let registered = pipeline.par2_checkpoint_plan(job_id);
    let plan = plan_for_articles(&registered);
    for (file_index, (_, bytes)) in files.iter().enumerate() {
        feed_block_aligned_articles(
            pipeline,
            NzbFileId {
                job_id,
                file_index: file_index as u32,
            },
            bytes,
            &plan,
        );
    }

    files
}

/// The control: articles cut on the recovery set's own grid produce verdicts,
/// the verdicts produce a vouched prefix, and a chase inside it survives repair.
///
/// This is the shape the vouching rule was designed against, and it has to hold
/// before the failure below means anything.
#[tokio::test]
async fn a_chase_inside_a_populated_grids_intact_prefix_is_vouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41800);
    let set_name = "generated_split_store_plain.7z";

    let files = armed_chase_with_real_par2(&mut pipeline, job_id, set_name, |registered| {
        registered.clone()
    })
    .await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    // Part 0 is 163,840 bytes: two full 64 KiB blocks and a 32,768-byte tail.
    // All three vouch — the short final block included, because the completed
    // file's length has been recorded and its extent is therefore decidable.
    // The prefix is quantised to whole blocks, so it reaches the block boundary
    // above the file rather than the file's own length.
    let described_blocks = files[0].1.len().div_ceil(VOUCH_SLICE as usize) as u64;
    assert_eq!(
        pipeline.in_stream_intact_prefix(file_id),
        Some(described_blocks * VOUCH_SLICE),
        "a grid cut on the recovery set's own slice size must vouch the whole part"
    );

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    // The chase is a real decode; pin what it has read so the decision is about
    // the evidence rather than about timing.
    coverage.note_consumed(0, 2 * VOUCH_SLICE);

    pipeline.decide_direct_unpack_before_repair(job_id, None);

    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "a chase inside the vouched prefix must be parked through the repair, not tainted"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The round-8 failure, reproduced.
///
/// The recovery set binds the part and describes every one of its blocks. What
/// is missing is the *grid*: these articles were decoded before the PAR2
/// packets were parsed, so the decoder cut them on no checkpoint geometry, and
/// `note_article_on_grids` will not retroactively claim them — doing so would
/// imply cuts the decoder never made. `verdicts_against` therefore returns an
/// empty map, the intact prefix walks to zero, and every byte the chase read is
/// past it.
///
/// Locally the tiny `.par2` articles race the data volumes, so whichever data
/// articles decode inside that window are grid-less for good — which is why the
/// e2e failure was intermittent rather than reproducible.
#[tokio::test]
async fn a_chase_over_a_part_whose_grid_never_formed_cannot_be_vouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41810);
    let set_name = "generated_split_store_plain.7z";

    // The articles are offered with the plan that was current *before* the
    // recovery set registered: none.
    armed_chase_with_real_par2(&mut pipeline, job_id, set_name, |_| {
        weaver_yenc::CheckpointPlan::None
    })
    .await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_some(),
        "the part is bound: this is not a binding failure"
    );
    assert!(
        pipeline.block_crc_verdicts(file_id).is_none(),
        "and it has no verdicts at all, because it has no grid"
    );
    assert_eq!(
        pipeline.in_stream_intact_prefix(file_id),
        Some(0),
        "so the in-stream prefix vouches for nothing"
    );

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    coverage.note_consumed(0, 700_000.min(2 * VOUCH_SLICE));

    pipeline.decide_direct_unpack_before_repair(job_id, None);

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "with no grid evidence the chase is refused — this is the failure under test"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_repair_rewrote,
        1,
        "and it is booked as a repair-rewrote taint"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// One analysis verdict per part, as the analysis pass would report it.
fn analysis_verification(
    par2_set: &par2_rs::Par2FileSet,
    statuses: &[(&str, par2_rs::verify::FileStatus)],
) -> par2_rs::VerificationResult {
    let files = statuses
        .iter()
        .map(|(filename, status)| {
            let (file_id, _) = par2_set
                .files
                .iter()
                .find(|(_, description)| description.filename == *filename)
                .expect("the recovery set describes this file");
            par2_rs::verify::FileVerification {
                file_id: *file_id,
                filename: (*filename).to_string(),
                status: status.clone(),
                valid_slices: Vec::new(),
                missing_slice_count: 0,
            }
        })
        .collect();
    par2_rs::VerificationResult {
        files,
        recovery_blocks_available: 4,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

/// The fix for the failure above: a part with no grid claim falls back to the
/// analysis pass's own file-level verdict.
///
/// `Complete` there is a full-MD5 match read from disk — strictly stronger than
/// any run of CRC32 block claims — and a file the analysis found complete is not
/// one the repair is going to write to. Same setup as the failing test, same
/// missing grid; the only addition is the evidence the analysis already had.
#[tokio::test]
async fn a_part_with_no_grid_is_vouched_by_the_analysis_file_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41820);
    let set_name = "generated_split_store_plain.7z";

    let files = armed_chase_with_real_par2(&mut pipeline, job_id, set_name, |_| {
        weaver_yenc::CheckpointPlan::None
    })
    .await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert_eq!(
        pipeline.in_stream_intact_prefix(file_id),
        Some(0),
        "the grid still vouches for nothing — the fallback is what has to carry this"
    );

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    coverage.note_consumed(0, 2 * VOUCH_SLICE);

    let par2_set = pipeline.par2_set(job_id).expect("installed").clone();
    let statuses: Vec<(&str, par2_rs::verify::FileStatus)> = files
        .iter()
        .map(|(name, _)| (name.as_str(), par2_rs::verify::FileStatus::Complete))
        .collect();
    let verification = analysis_verification(&par2_set, &statuses);

    pipeline.decide_direct_unpack_before_repair(job_id, Some(&verification));

    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "a part the analysis verified complete must be vouched despite having no grid"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().demoted_repair_rewrote,
        0,
        "and nothing is tainted"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The fallback must never vouch a file the repair intends to rewrite.
///
/// This is the whole safety condition. A damaged part with no grid claim looks
/// exactly like a clean one to the in-stream evidence — both vouch for nothing —
/// so if the fallback were keyed on anything looser than `Complete` it would
/// hand a chase a file that is about to change underneath it.
#[tokio::test]
async fn the_analysis_fallback_refuses_a_part_the_repair_will_rewrite() {
    for damaged_status in [
        par2_rs::verify::FileStatus::Damaged(3),
        par2_rs::verify::FileStatus::Missing,
        par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from("elsewhere.7z.001")),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        enable_direct_unpack(&mut pipeline);
        let job_id = JobId(41830);
        let set_name = "generated_split_store_plain.7z";

        let files = armed_chase_with_real_par2(&mut pipeline, job_id, set_name, |_| {
            weaver_yenc::CheckpointPlan::None
        })
        .await;

        let coverage = pipeline
            .direct_unpack
            .armed_coverage(job_id, set_name)
            .expect("armed");
        coverage.note_consumed(0, 2 * VOUCH_SLICE);

        let par2_set = pipeline.par2_set(job_id).expect("installed").clone();
        // Every part complete except the one the chase has actually read.
        let statuses: Vec<(&str, par2_rs::verify::FileStatus)> = files
            .iter()
            .enumerate()
            .map(|(index, (name, _))| {
                let status = if index == 0 {
                    damaged_status.clone()
                } else {
                    par2_rs::verify::FileStatus::Complete
                };
                (name.as_str(), status)
            })
            .collect();
        let verification = analysis_verification(&par2_set, &statuses);

        pipeline.decide_direct_unpack_before_repair(job_id, Some(&verification));

        assert!(
            !pipeline.direct_unpack.is_armed(job_id, set_name),
            "the fallback must refuse a part whose analysis status is {damaged_status:?}"
        );

        // Join every worker before the test returns. Dropping the runtime with a
        // blocking task still live blocks forever, and a chase parked on a
        // coverage nothing will advance is exactly that.
        pipeline.direct_unpack_shutdown("test teardown").await;
    }
}

/// A part the analysis never classified stays unvouched, and so does one whose
/// analysis result is absent entirely.
#[tokio::test]
async fn an_unclassified_part_is_not_vouched_by_the_fallback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41840);
    let set_name = "generated_split_store_plain.7z";

    let files = armed_chase_with_real_par2(&mut pipeline, job_id, set_name, |_| {
        weaver_yenc::CheckpointPlan::None
    })
    .await;

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    coverage.note_consumed(0, 2 * VOUCH_SLICE);

    // The analysis reported on every part except the one that was read.
    let par2_set = pipeline.par2_set(job_id).expect("installed").clone();
    let statuses: Vec<(&str, par2_rs::verify::FileStatus)> = files
        .iter()
        .skip(1)
        .map(|(name, _)| (name.as_str(), par2_rs::verify::FileStatus::Complete))
        .collect();
    let verification = analysis_verification(&par2_set, &statuses);

    pipeline.decide_direct_unpack_before_repair(job_id, Some(&verification));

    assert!(
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "silence from the analysis is not a verdict"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

// ---------------------------------------------------------------------------
// The topology eraser
//
// PAR2 identity application used to delete every non-RAR topology in a job. The
// search for a file's "old RAR set name" fell through to a map built from ALL
// archive topologies, so a 7z volume answered with its own 7z set; that set was
// then marked stale and swept by a retirement path whose every test is
// RAR-shaped, so it could neither be busy nor referenced, and was torn down.
// File completions rebuilt it, which is why this only killed the jobs whose
// last topology-building event came before the last PAR2 registration.
// ---------------------------------------------------------------------------

/// A 7z split set whose parts are described by a real recovery set, with the
/// topology built and the PAR2 runtime installed.
async fn sevenz_job_with_par2_and_topology(
    pipeline: &mut Pipeline,
    job_id: JobId,
    set_name: &str,
) -> Vec<(String, Vec<u8>)> {
    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(pipeline, job_id, spec).await;

    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let par2_set = build_repairable_par2_set_for_files(&described, VOUCH_SLICE, 4);
    install_test_par2_runtime(pipeline, job_id, par2_set, &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    files
}

fn has_topology(pipeline: &Pipeline, job_id: JobId, set_name: &str) -> bool {
    pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .is_some()
}

/// Applying PAR2 identity must not delete a 7z topology — not on the first
/// application, and not on a second one that rebinds nothing at all.
///
/// The second call is the one that mattered in production: the stale-set
/// bookkeeping ran above the "nothing changed, skip this file" check, so every
/// registration swept sets no rebind had touched, with the rebind counter at
/// zero and nothing logged.
#[tokio::test]
async fn applying_par2_identity_does_not_erase_a_7z_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41900);
    let set_name = "generated_split_store_plain.7z";

    sevenz_job_with_par2_and_topology(&mut pipeline, job_id, set_name).await;
    assert!(
        has_topology(&pipeline, job_id, set_name),
        "the fixture must start with a 7z topology"
    );

    pipeline.retry_par2_authoritative_identity(job_id).await;
    assert!(
        has_topology(&pipeline, job_id, set_name),
        "the first PAR2 identity application must not delete the 7z topology"
    );

    // Nothing left to rebind. This pass used to sweep anyway.
    pipeline.retry_par2_authoritative_identity(job_id).await;
    assert!(
        has_topology(&pipeline, job_id, set_name),
        "an identity application that rebinds nothing must not delete anything"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The deterministic kill sequence, end to end.
///
/// Every data file completes — which is the last thing that would have rebuilt
/// the topology — and only then does more recovery metadata register. In the
/// gate run that ordering came from targeted recovery downloads arriving after
/// the last data volume, and it killed both repair scenarios every time: the
/// chase could no longer resolve its own part paths, so the repair decision
/// refused with "no topology for set", 7 ms after the merge.
#[tokio::test]
async fn a_late_par2_registration_leaves_the_chase_able_to_resolve_its_parts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41910);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_job_with_par2_and_topology(&mut pipeline, job_id, set_name).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // The late registration: recovery metadata applied after the last data
    // file has already completed, with nothing left to rebuild the topology.
    pipeline.retry_par2_authoritative_identity(job_id).await;

    assert!(
        has_topology(&pipeline, job_id, set_name),
        "the topology must survive a registration that lands after the data does"
    );
    assert!(
        pipeline.sevenz_set_part_paths(job_id, set_name).is_ok(),
        "and the chase must still be able to resolve its parts"
    );

    // The repair decision now has something to reason about. With every part
    // verified complete it parks rather than refusing.
    let par2_set = pipeline.par2_set(job_id).expect("installed").clone();
    let statuses: Vec<(&str, par2_rs::verify::FileStatus)> = files
        .iter()
        .map(|(name, _)| (name.as_str(), par2_rs::verify::FileStatus::Complete))
        .collect();
    let verification = analysis_verification(&par2_set, &statuses);
    pipeline.decide_direct_unpack_before_repair(job_id, Some(&verification));

    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the chase must park through the repair, not be refused for a missing topology"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// Queue one piece of ordinary download work, so the job reads as still fetching.
fn queue_download_work(pipeline: &mut Pipeline, job_id: JobId, message_id: &str) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue.push(DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 1,
        },
        message_id: MessageId::new(message_id),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 128,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    });
}

/// A drained download *pass* is not a drained download.
///
/// `maybe_finish_download_pass` fires at every pass boundary, and a job with
/// tens of megabytes still queued crosses several. Stamping "settled" there and
/// then letting the next completion check act on the stamp is how two chases
/// were ended 5 ms and 14 ms after arming, mid-download, with all their volumes
/// still to come.
#[tokio::test]
async fn the_strict_settle_ignores_a_stamp_left_by_an_earlier_pass_boundary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41950);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // An early pass boundary stamps the job as settled.
    pipeline.settle_direct_unpack_after_download(job_id);

    // The download is nowhere near over: more work queues, and the set arms as
    // its first part lands.
    queue_download_work(&mut pipeline, job_id, "still-coming-1@example.invalid");
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    pipeline.settle_direct_unpack_at_completion(job_id);

    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "a stale stamp must not let the strict pass end a chase mid-download"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The 10127 shape: the set arms after an earlier pass boundary, and the rest of
/// its parts arrive normally. Nothing here is an error, so nothing must die.
#[tokio::test]
async fn a_set_armed_after_an_early_pass_boundary_survives_and_settles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41960);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.settle_direct_unpack_after_download(job_id);
    queue_download_work(&mut pipeline, job_id, "more-volumes@example.invalid");

    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    pipeline.settle_direct_unpack_at_completion(job_id);
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "the chase must survive a completion check taken mid-download"
    );

    // The remaining volumes land, and the queue drains for real.
    for (file_index, (filename, bytes)) in files.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }
    pipeline.settle_direct_unpack_after_download(job_id);
    pipeline.settle_direct_unpack_at_completion(job_id);

    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("the chase must still be armed after a genuine drain");
    assert!(
        (0..files.len()).all(|index| coverage
            .part_progress(index)
            .is_ok_and(|part| part.complete)),
        "and every part must have settled"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// A single-file 7z chase, driven all the way to an outcome.
///
/// This shape was untested: `a_single_7z_arms_from_its_first_32_bytes` proves
/// the arming and stops there, and every other end-to-end chase test uses a
/// split set. In the round-9 gate run every single-file 7z scenario timed out,
/// and no chase of any shape ever logged a completion — so "does a single-file
/// chase finish at all" had no answer in-process either way.
///
/// It drips the archive the way the download does — append, then publish the
/// new watermark — and then completes the file, which is the only thing that
/// tells the coverage the part is finished.
#[tokio::test]
async fn a_single_7z_chase_runs_to_an_outcome() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41970);
    let set_name = "silver_horizon.7z";

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();
    let files = vec![(set_name.to_string(), archive.clone())];
    let spec = rar_job_spec("Silver Horizon", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    let path = working_dir.join(set_name);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Arm from the header, exactly as the commit path does.
    std::fs::write(&path, &archive[..64 * 1024]).unwrap();
    pipeline.direct_unpack_note_commit(file_id, set_name, 64 * 1024, false);
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    // The rest of the bytes arrive by appending, never by rewriting: a chase is
    // reading this file, and a truncate-and-rewrite would pull committed bytes
    // out from under it.
    {
        use std::io::Write;
        let mut handle = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        handle.write_all(&archive[64 * 1024..]).unwrap();
        handle.flush().unwrap();
    }
    pipeline.direct_unpack_note_commit(file_id, set_name, archive.len() as u64, false);
    complete_already_written_file(&mut pipeline, job_id, 0, archive.len()).await;

    reap_until_outcome(&mut pipeline, job_id, set_name).await;

    let outcome = pipeline
        .direct_unpack
        .outcome(job_id, set_name)
        .expect("the chase must produce an outcome");
    assert!(
        outcome.result.is_ok(),
        "a single-file chase over an intact archive must succeed: {:?}",
        outcome.result.as_ref().err()
    );
}

/// The round-9 wedge: a parked chase must not stop other jobs' extraction.
///
/// Both the chase and conventional 7z extraction reserve
/// `budget.max_memory_bytes()`, and that is also the limit of the
/// `ProcessMemoryBudget` they draw from — so one permit is the whole pool. The
/// chase takes it before it opens the archive and holds it until it returns,
/// across every park the gated reader does while waiting on the download.
///
/// While chases drew from the *shared* pool, a chase whose download never
/// finished never released it and no other 7z work in the process could start.
/// In the gate run 39 chases armed and not one ever logged a completion; every
/// 7z scenario after the first wedge timed out in batch-sized groups, and
/// completions burst the moment the harness cancelled the jobs holding the pool.
///
/// Chases now have their own pool, so this is bounded: a parked chase can starve
/// other *chases*, which are speculative, but never the extractions on a job's
/// critical path. This test drives exactly that — an independent, fully
/// downloaded job finishing while another job's chase sits parked forever.
#[tokio::test]
async fn a_parked_chase_does_not_block_another_jobs_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let set_name = "silver_horizon.7z";

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();

    // Job A arms and then stops receiving bytes: its chase parks inside the
    // decoder, holding the permit.
    let parked_job = JobId(41980);
    let files = vec![(set_name.to_string(), archive.clone())];
    insert_active_job(
        &mut pipeline,
        parked_job,
        rar_job_spec("Silver Horizon Parked", &files),
    )
    .await;
    let parked_dir = pipeline.jobs.get(&parked_job).unwrap().working_dir.clone();
    std::fs::write(parked_dir.join(set_name), &archive[..64 * 1024]).unwrap();
    pipeline.direct_unpack_note_commit(
        NzbFileId {
            job_id: parked_job,
            file_index: 0,
        },
        set_name,
        64 * 1024,
        false,
    );
    assert!(pipeline.direct_unpack.is_armed(parked_job, set_name));

    // Job B has every byte it needs and nothing to wait for.
    let ready_job = JobId(41981);
    insert_active_job(
        &mut pipeline,
        ready_job,
        rar_job_spec("Silver Horizon Ready", &files),
    )
    .await;
    let ready_dir = pipeline.jobs.get(&ready_job).unwrap().working_dir.clone();
    std::fs::write(ready_dir.join(set_name), &archive).unwrap();
    pipeline.direct_unpack_note_commit(
        NzbFileId {
            job_id: ready_job,
            file_index: 0,
        },
        set_name,
        archive.len() as u64,
        false,
    );
    complete_already_written_file(&mut pipeline, ready_job, 0, archive.len()).await;

    // Job B's own chase is demoted, exactly as the two wedged jobs in the gate
    // run were: they aborted, reported "extraction ready", and then produced
    // nothing at all. Conventional extraction is now job B's critical path, and
    // it must not be waiting on another job's parked chase for decoder memory.
    pipeline.direct_unpack_abort_set(
        ready_job,
        set_name,
        "test demotes this chase to force the conventional path",
        crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
        crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
    );

    pipeline.extract_7z_set(ready_job, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("conventional extraction must not wait on a parked chase");
    assert_eq!(outcome.extracted.len(), 1);

    // And the parked chase really is still parked, holding its own permit: the
    // property under test is isolation, not that the holder went away.
    assert!(
        pipeline.direct_unpack.is_armed(parked_job, set_name),
        "the parked chase must still be holding its permit"
    );
    assert!(
        pipeline
            .direct_unpack
            .outcome(parked_job, set_name)
            .is_none(),
        "and must still not have produced an outcome"
    );

    // Join every worker before the test returns. Dropping the runtime with a
    // blocking task still live blocks forever, and a chase parked on a
    // coverage nothing will advance is exactly that.
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// Admission is capped at the number of chase workers.
///
/// A chase occupies one `chase_pool` worker for its entire life, parks included.
/// Arming more chases than there are workers produces chases that are armed but
/// never start — and extraction awaits a still-running chase with no deadline,
/// so those become wedges rather than merely slow overlaps.
///
/// The refusal is counted but not latched: having no free worker right now says
/// nothing about the archive, so the set must be free to arm later.
#[tokio::test]
async fn arming_stops_at_the_number_of_chase_workers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let set_name = "silver_horizon.7z";
    let workers = pipeline.chase_pool.current_num_threads();

    // Hold every chase-pool thread for the length of the assertions, so each
    // chase's closure stays queued inside `install`. A queued closure has not
    // touched its coverage, so an abort cannot reach it and its worker cannot
    // return — which is exactly the state the capacity count below is about.
    // Without the hold, the aborted worker wakes off its coverage and races the
    // next `note_commit` to the finish: if it unwinds first its slot frees, the
    // over-the-line set arms, and the assertion fails.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let release = Arc::new(std::sync::Barrier::new(workers + 1));
    for _ in 0..workers {
        let started_tx = started_tx.clone();
        let release = Arc::clone(&release);
        pipeline.chase_pool.spawn(move || {
            started_tx.send(()).unwrap();
            release.wait();
        });
    }
    for _ in 0..workers {
        started_rx
            .recv()
            .expect("every chase-pool thread reports that it is held");
    }

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();
    let files = vec![(set_name.to_string(), archive.clone())];

    // Arm one more job than there are workers, none of them ever completing.
    let mut jobs = Vec::new();
    for index in 0..=workers {
        let job_id = JobId(42000 + index as u64);
        jobs.push(job_id);
        insert_active_job(
            &mut pipeline,
            job_id,
            rar_job_spec("Silver Horizon Capacity", &files),
        )
        .await;
        let dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
        std::fs::write(dir.join(set_name), &archive[..64 * 1024]).unwrap();
        pipeline.direct_unpack_note_commit(
            NzbFileId {
                job_id,
                file_index: 0,
            },
            set_name,
            64 * 1024,
            false,
        );
    }

    let armed = jobs
        .iter()
        .filter(|job_id| pipeline.direct_unpack.is_armed(**job_id, set_name))
        .count();
    assert_eq!(
        armed, workers,
        "exactly as many chases as there are workers may be armed"
    );

    // Aborting a set moves it out of `armed` but its worker keeps its
    // `chase_pool` slot until it actually returns — and one still queued inside
    // `install` cannot even see the abort, because the abort only pokes a
    // coverage that closure has not touched. Counting `armed` alone made those
    // workers invisible and let new sets arm past true capacity, which is how
    // the pool filled with chases that could never start.
    let first = jobs[0];
    pipeline.direct_unpack_abort_set(
        first,
        set_name,
        "test moves this chase into draining",
        crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
        crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
    );
    assert!(!pipeline.direct_unpack.is_armed(first, set_name));

    let over_the_line = *jobs.last().unwrap();
    pipeline.direct_unpack_note_commit(
        NzbFileId {
            job_id: over_the_line,
            file_index: 0,
        },
        set_name,
        64 * 1024,
        false,
    );
    assert!(
        !pipeline.direct_unpack.is_armed(over_the_line, set_name),
        "a draining worker still occupies its slot, so nothing new may arm"
    );
    assert_eq!(
        pipeline.direct_unpack.counters().refused_no_chase_capacity,
        2,
        "refused by name both before and after the abort"
    );
    let last = *jobs.last().unwrap();
    assert_eq!(
        pipeline.direct_unpack.latched_reason(last, set_name),
        None,
        "a capacity refusal must not latch: it says nothing about the archive"
    );

    // Let the queued closures run: the aborted one reads its coverage, sees the
    // abort and returns; the live one parks in its header read until shutdown
    // aborts it. Both must be able to finish or the join below never does.
    release.wait();
    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// Consumption must not wait forever on a chase that never finishes.
///
/// A chase can be handed to extraction still running — normally it is finishing
/// at disk speed, because every part is complete by then. But "normally" is not
/// a guarantee: a worker still queued behind occupied chase workers cannot even
/// see its own set's abort, because the abort only pokes a coverage that
/// closure has not touched yet. The await used to have no deadline, so such a
/// chase left extraction never returning, the job in Extracting forever, and a
/// global extraction slot held until the job was cancelled.
///
/// Here the chase is parked on a coverage nothing will ever advance. The
/// deadline must fire, abort the coverage, and let conventional extraction
/// produce the members.
#[tokio::test]
async fn consumption_gives_up_on_a_chase_that_never_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(42100);
    let set_name = "generated_split_store_plain.7z";

    *crate::pipeline::direct_unpack::wiring::PENDING_CHASE_DEADLINE_OVERRIDE
        .lock()
        .unwrap() = Some(std::time::Duration::from_millis(250));

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Silver Horizon Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Arm on the first part, then land the rest on disk WITHOUT telling the
    // coverage: the chase parks at part 1's boundary and never returns, while
    // the conventional path has a whole archive to read.
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, bytes) in files.iter().skip(1) {
        std::fs::write(working_dir.join(filename), bytes).unwrap();
    }

    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected a full-set extraction result");
    };
    let outcome = result.expect("conventional extraction must finish after the deadline");
    assert_eq!(outcome.extracted.len(), 1);

    let reason = coverage
        .abort_reason()
        .expect("the deadline must end the chase's coverage");
    assert!(
        reason.contains("consumption deadline"),
        "and say why: {reason}"
    );

    *crate::pipeline::direct_unpack::wiring::PENDING_CHASE_DEADLINE_OVERRIDE
        .lock()
        .unwrap() = None;
}

// ---------------------------------------------------------------------------
// Decoder memory: a chase reserves what its archive needs, when it needs it
// ---------------------------------------------------------------------------

/// An in-process LZMA2 archive with a known dictionary, so the reservation a
/// chase takes for it can be computed and compared exactly.
fn sized_lzma2_archive(dictionary: u32, members: &[(String, Vec<u8>)]) -> Vec<u8> {
    use sevenz_rust2::encoder_options::Lzma2Options;
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter, EncoderConfiguration};

    let mut writer = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).expect("writer");
    let mut options = Lzma2Options::from_level(5);
    options.set_dictionary_size(dictionary);
    writer.set_content_methods(vec![EncoderConfiguration::from(options)]);
    for (name, bytes) in members {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(std::io::Cursor::new(bytes.clone())),
            )
            .expect("entry");
    }
    writer.finish().expect("finish").into_inner()
}

/// Deterministic member bytes that compress a little, so the packed stream is
/// neither trivially small nor a copy of the input.
fn sized_member(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|index| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            if index % 7 == 0 {
                (state >> 24) as u8
            } else {
                b'a' + (index % 13) as u8
            }
        })
        .collect()
}

/// Cut `archive` into parts on disk at `bounds`, returning the part paths.
fn write_parts(dir: &std::path::Path, archive: &[u8], bounds: &[usize]) -> Vec<PathBuf> {
    bounds
        .windows(2)
        .enumerate()
        .map(|(index, window)| {
            let path = dir.join(format!("silver_horizon.7z.{:03}", index + 1));
            std::fs::write(&path, &archive[window[0]..window[1]]).expect("write part");
            path
        })
        .collect()
}

/// A budget over `pool` with limits that do not depend on the machine the
/// test runs on.
fn chase_budget_over(
    pool: &std::sync::Arc<crate::pipeline::extraction::ProcessMemoryBudget>,
    staging: &std::path::Path,
) -> std::sync::Arc<crate::pipeline::extraction::JobExtractionBudget> {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    let limits = crate::pipeline::extraction::ExtractionLimits {
        max_job_bytes: 2048 * GIB,
        max_member_bytes: 1024 * GIB,
        max_entries: 100_000,
        max_ratio: 100,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes: GIB,
    };
    crate::pipeline::extraction::JobExtractionBudget::new_with_process_memory(
        std::sync::Arc::new(limits),
        std::sync::Arc::clone(pool),
        staging.to_path_buf(),
        1,
        0,
        0,
        PipelineMetrics::new(),
    )
    .expect("budget")
}

/// Run the shared 7z extraction body the way a chase does, on its own thread.
fn spawn_chase_extraction(
    context: crate::pipeline::completion::finalize::extract::SevenZipExtractionContext,
    paths: Vec<PathBuf>,
    coverage: std::sync::Arc<crate::pipeline::direct_unpack::SetCoverage>,
) -> std::thread::JoinHandle<Result<FullSetExtractionOutcome, String>> {
    std::thread::spawn(move || {
        crate::pipeline::completion::finalize::extract::extract_7z_stream(&context, || {
            crate::pipeline::direct_unpack::GatedSplitReader::open(
                &paths,
                std::sync::Arc::clone(&coverage),
            )
            .map(|reader| std::io::BufReader::with_capacity(128 * 1024, reader))
            .map_err(|error| format!("open: {error}"))
        })
    })
}

fn chase_context(
    set_name: &str,
    output_dir: PathBuf,
    budget: std::sync::Arc<crate::pipeline::extraction::JobExtractionBudget>,
    end_header_bytes: u64,
) -> crate::pipeline::completion::finalize::extract::SevenZipExtractionContext {
    use crate::pipeline::completion::finalize::extract::{
        SevenZipDecodeMemory, SevenZipExtractionContext,
    };
    let (events, _) = tokio::sync::broadcast::channel(1);
    SevenZipExtractionContext {
        job_id: JobId(41995),
        set_name: set_name.to_string(),
        root: std::sync::Arc::new(
            crate::pipeline::extraction::ExtractionRoot::open(&output_dir).expect("root"),
        ),
        output_dir,
        budget,
        password: sevenz_rust2::Password::empty(),
        event_tx: events,
        phase_counters: std::sync::Arc::new(crate::jobs::PhaseCounters::default()),
        decode_memory: SevenZipDecodeMemory::ReservedPerPass { end_header_bytes },
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool, what: &str) {
    for _ in 0..3_000 {
        if condition() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// A chase's decode pass holds its archive's decoders, not the ceiling.
///
/// Two chases over the same archive park mid-block with their permits held,
/// and what the pool shows reserved is exactly two dictionaries plus the
/// fixed allowances — a number small enough that both fit side by side in a
/// pool that could not have held even one whole-ceiling reservation. When
/// the parts they wait on land, both finish and every member matches.
#[tokio::test]
async fn a_chase_reserves_its_decoders_not_the_ceiling() {
    use crate::pipeline::completion::finalize::extract::CHASE_DECODE_ALLOWANCE_BYTES;
    use crate::pipeline::direct_unpack::SetCoverage;
    use crate::pipeline::direct_unpack::start_header::StartHeader;
    const MIB: u64 = 1024 * 1024;

    let dictionary = 4u32 << 20;
    let members = vec![
        (
            "silver_horizon/reel_01.bin".to_string(),
            sized_member(768 * 1024, 11),
        ),
        (
            "silver_horizon/reel_02.bin".to_string(),
            sized_member(768 * 1024, 12),
        ),
    ];
    let archive = sized_lzma2_archive(dictionary, &members);
    let header = StartHeader::parse(&archive[..32]).expect("signature header");
    let parsed = sevenz_rust2::ArchiveReader::new(
        std::io::Cursor::new(archive.clone()),
        sevenz_rust2::Password::empty(),
    )
    .expect("parse");
    let decoders =
        crate::pipeline::direct_unpack::decode_memory::decoder_memory_bytes(parsed.archive())
            .expect("sized");
    assert_eq!(decoders, u64::from(dictionary) + MIB, "one LZMA2 block");
    let expected_each = decoders + header.next_header_size + CHASE_DECODE_ALLOWANCE_BYTES;

    // Four parts. Part three is only half there, and its half is where the
    // decode parks; every length is declared, so the listing pass can reach
    // the tail without parking.
    let dir = tempfile::tempdir().unwrap();
    let quarter = archive.len() / 4;
    let bounds = [0, quarter, 2 * quarter, 3 * quarter, archive.len()];
    let paths = write_parts(dir.path(), &archive, &bounds);
    let short_part = 2usize;
    let short_len = (bounds[3] - bounds[2]) as u64;
    let short_written = short_len / 2;
    std::fs::write(
        &paths[short_part],
        &archive[bounds[2]..bounds[2] + short_written as usize],
    )
    .unwrap();

    let coverage_for = || {
        let coverage = std::sync::Arc::new(SetCoverage::new(paths.len()));
        coverage.set_total_len(archive.len() as u64);
        for (index, window) in bounds.windows(2).enumerate() {
            let len = (window[1] - window[0]) as u64;
            coverage.note_part_len(index, len);
            if index == short_part {
                coverage.advance_watermark(index, short_written);
            } else {
                coverage.advance_watermark(index, len);
                coverage.mark_part_complete(index);
            }
        }
        coverage
    };

    // A pool one whole-ceiling reservation could never fit in.
    let pool = std::sync::Arc::new(crate::pipeline::extraction::ProcessMemoryBudget::new(
        64 * MIB,
    ));
    assert!(
        2 * expected_each < 64 * MIB,
        "the two right-sized permits must fit"
    );

    let mut runs = Vec::new();
    for index in 0..2 {
        let staging = dir.path().join(format!("staging_{index}"));
        std::fs::create_dir_all(&staging).unwrap();
        let output = staging.join("out");
        std::fs::create_dir_all(&output).unwrap();
        let budget = chase_budget_over(&pool, &staging);
        let coverage = coverage_for();
        let context = chase_context(
            "silver_horizon.7z",
            output.clone(),
            budget,
            header.next_header_size,
        );
        let handle =
            spawn_chase_extraction(context, paths.clone(), std::sync::Arc::clone(&coverage));
        runs.push((coverage, handle, output));
    }

    wait_until(
        || {
            runs.iter()
                .all(|(coverage, _, _)| coverage.park_count() > 0)
        },
        "both chases to park inside the decode",
    )
    .await;
    assert_eq!(
        pool.reserved_bytes(),
        2 * expected_each,
        "two parked chases hold two dictionaries plus allowances, nothing more"
    );

    // The missing half lands; both chases finish.
    std::fs::write(&paths[short_part], &archive[bounds[2]..bounds[3]]).unwrap();
    for (coverage, _, _) in &runs {
        coverage.advance_watermark(short_part, short_len);
        coverage.mark_part_complete(short_part);
    }
    for (_, handle, output) in runs {
        let outcome = handle
            .join()
            .expect("no panic")
            .expect("chase decode succeeds");
        assert_eq!(outcome.extracted.len(), members.len());
        for (name, bytes) in &members {
            assert_eq!(&std::fs::read(output.join(name)).unwrap(), bytes, "{name}");
        }
    }
    assert_eq!(pool.reserved_bytes(), 0, "every permit is returned");
}

/// While a chase lists the archive it holds a header-sized permit and nothing
/// else — and that is the pass that parks for the tail, sometimes for most of
/// a download.
#[tokio::test]
async fn a_chase_listing_the_archive_holds_a_header_sized_permit() {
    use crate::pipeline::completion::finalize::extract::CHASE_HEADER_PASS_ALLOWANCE_BYTES;
    use crate::pipeline::direct_unpack::SetCoverage;
    use crate::pipeline::direct_unpack::start_header::StartHeader;
    const MIB: u64 = 1024 * 1024;

    let members = vec![(
        "silver_horizon/reel_01.bin".to_string(),
        sized_member(256 * 1024, 21),
    )];
    let archive = sized_lzma2_archive(1 << 20, &members);
    let header = StartHeader::parse(&archive[..32]).expect("signature header");

    // Two parts; only the first exists. The second's length is unknown, so
    // the tail cannot be located and the listing pass parks.
    let dir = tempfile::tempdir().unwrap();
    let half = archive.len() / 2;
    let paths = write_parts(dir.path(), &archive, &[0, half, archive.len()]);
    std::fs::remove_file(&paths[1]).unwrap();
    let coverage = std::sync::Arc::new(SetCoverage::new(2));
    coverage.set_total_len(archive.len() as u64);
    coverage.note_part_len(0, half as u64);
    coverage.advance_watermark(0, half as u64);
    coverage.mark_part_complete(0);

    let pool = std::sync::Arc::new(crate::pipeline::extraction::ProcessMemoryBudget::new(
        64 * MIB,
    ));
    let staging = dir.path().join("staging");
    let output = staging.join("out");
    std::fs::create_dir_all(&output).unwrap();
    let budget = chase_budget_over(&pool, &staging);
    let context = chase_context("silver_horizon.7z", output, budget, header.next_header_size);
    let handle = spawn_chase_extraction(context, paths, std::sync::Arc::clone(&coverage));

    wait_until(|| coverage.park_count() > 0, "the listing pass to park").await;
    assert_eq!(
        pool.reserved_bytes(),
        header.next_header_size + CHASE_HEADER_PASS_ALLOWANCE_BYTES,
        "the listing pass holds the end header plus its allowance"
    );

    coverage.abort("test ends the chase");
    let result = handle.join().expect("no panic");
    assert!(result.is_err(), "an aborted listing pass fails the chase");
    assert_eq!(pool.reserved_bytes(), 0);
}

/// The round-10 wedge, at the controller: chases no longer single-file through
/// the pool. Two jobs' chases park side by side in a pool that could not have
/// held one whole-ceiling reservation, both still armed, neither failed.
#[tokio::test]
async fn parked_chases_share_the_decoder_memory_pool() {
    const MIB: u64 = 1024 * 1024;
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let pool = std::sync::Arc::new(crate::pipeline::extraction::ProcessMemoryBudget::new(
        256 * MIB,
    ));
    pipeline.direct_unpack_process_memory = std::sync::Arc::clone(&pool);
    let set_name = "silver_horizon.7z";

    let archive = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sevenz/generated_bcj2_silver_horizon.7z"),
    )
    .unwrap();
    let files = vec![(set_name.to_string(), archive.clone())];
    let jobs = [
        (JobId(41996), "Silver Horizon Parked One"),
        (JobId(41997), "Silver Horizon Parked Two"),
    ];
    for (job_id, name) in jobs {
        insert_active_job(&mut pipeline, job_id, rar_job_spec(name, &files)).await;
        let dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
        std::fs::write(dir.join(set_name), &archive[..64 * 1024]).unwrap();
        pipeline.direct_unpack_note_commit(
            NzbFileId {
                job_id,
                file_index: 0,
            },
            set_name,
            64 * 1024,
            false,
        );
        assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    }

    let coverages: Vec<_> = jobs
        .iter()
        .map(|(job_id, _)| {
            pipeline
                .direct_unpack
                .armed_coverage(*job_id, set_name)
                .expect("armed")
        })
        .collect();
    wait_until(
        || coverages.iter().all(|coverage| coverage.park_count() > 0),
        "both chases to open their readers and park",
    )
    .await;

    pipeline.reap_direct_unpack().await;
    for (job_id, _) in jobs {
        assert!(
            pipeline.direct_unpack.is_armed(job_id, set_name),
            "a parked chase must still be armed"
        );
        assert!(
            pipeline.direct_unpack.outcome(job_id, set_name).is_none(),
            "and must not have failed for want of the whole pool"
        );
    }
    let reserved = pool.reserved_bytes();
    assert!(reserved > 0, "parked chases hold something");
    assert!(
        reserved < 256 * MIB,
        "two parked chases fit in a pool one ceiling would not: reserved {reserved}"
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// Finalize can see a chase that is gated on recovery-reported damage — and
/// only while it is armed.
#[tokio::test]
async fn a_gated_chase_is_reported_to_finalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(41998);
    let other_job = JobId(41999);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Gated", &files),
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    assert!(pipeline.direct_unpack_gated_sets(job_id).is_empty());

    coverage.cap_at_damage(files.len() - 1, 0);
    assert!(coverage.is_gated());
    assert_eq!(
        pipeline.direct_unpack_gated_sets(job_id),
        vec![set_name.to_string()]
    );
    assert!(
        pipeline.direct_unpack_gated_sets(other_job).is_empty(),
        "another job's sets are not this job's evidence"
    );

    pipeline.direct_unpack_abort_set(
        job_id,
        set_name,
        "test ends the chase",
        crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
        crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
    );
    assert!(
        pipeline.direct_unpack_gated_sets(job_id).is_empty(),
        "a draining set is no longer evidence: it will be decoded conventionally"
    );

    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
}

/// A split 7z job with a loaded, intact PAR2 set, its chase armed on part one
/// and every other part landed afterwards. `gate` decides whether the chase is
/// held on a damage cap before those parts arrive.
///
/// Returns the pipeline and the set name. The job's archive topology is a
/// multi-volume 7z, so the clean-PAR2 integrity gate reads `StrongDecode` —
/// the claim the gated chase has to be able to override.
async fn split_7z_job_with_par2(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    gate: bool,
) -> (Pipeline, &'static str) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let set_name = "generated_split_store_plain.7z";
    let parts = sevenz_fixture_bytes(set_name);
    let index_filename = "silver_horizon.par2";
    let recovery_filename = "silver_horizon.vol00+01.par2";
    let slice_size = 65_536;
    let described: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let par2_bytes = build_test_par2_index_for_files(&described, slice_size);
    let recovery_bytes = vec![0xAA; 64];

    let mut files = parts.clone();
    files.push((index_filename.to_string(), par2_bytes.clone()));
    files.push((recovery_filename.to_string(), recovery_bytes.clone()));
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Gated Verify", &files),
    )
    .await;

    // Part one lands and the chase arms off the topology it creates.
    write_and_complete_file(&mut pipeline, job_id, 0, &parts[0].0, &parts[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed on part one");
    if gate {
        // Recovery data reports damage in the last part: the chase will park
        // there and stay parked until a repair releases it.
        coverage.cap_at_damage(parts.len() - 1, 0);
        assert!(coverage.is_gated());
    }
    for (file_index, (filename, bytes)) in parts.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    let index_file = parts.len() as u32;
    let recovery_file = index_file + 1;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        index_file,
        index_filename,
        &par2_bytes,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        recovery_file,
        recovery_filename,
        &recovery_bytes,
    )
    .await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set_for_files(&described, slice_size, 1),
        &[
            (index_file, index_filename, 0, false),
            (recovery_file, recovery_filename, 1, true),
        ],
    );
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    (pipeline, set_name)
}

/// A chase gated on recovery-reported damage forces the authoritative PAR2
/// pass, over the strong-decode claim that would otherwise skip it.
///
/// This is the seam that left a gated chase waiting out its whole deadline:
/// finalize settled the set as clean on the strength of the archive type, no
/// repair was ever summoned, and the chase sat on vouches that were never
/// coming.
#[tokio::test]
async fn a_gated_chase_forces_the_authoritative_par2_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42000);
    let (mut pipeline, set_name) = split_7z_job_with_par2(&temp_dir, job_id, true).await;
    let mut verify_events = pipeline.event_tx.subscribe();
    assert_eq!(
        pipeline.direct_unpack_gated_sets(job_id),
        vec![set_name.to_string()]
    );

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        1,
        "the authoritative pass must run while a chase is gated on damage"
    );
    // The pass read every part back and found them complete, which
    // contradicts the gate: it is lifted, and nothing is left parked on a
    // repair that will never come.
    assert!(
        pipeline.direct_unpack_gated_sets(job_id).is_empty(),
        "a clean authoritative verdict lifts the gate it contradicts"
    );
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "status = {:?}",
        job_status_for_assert(&pipeline, job_id)
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// A clean verdict lifts only the gates it speaks for. A recovery set that
/// does not describe the chased parts has nothing to say about them.
#[tokio::test]
async fn a_clean_verdict_for_another_set_leaves_the_gate_up() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(42002);
    let set_name = "generated_split_store_plain.7z";
    let parts = sevenz_fixture_bytes(set_name);
    let unrelated_name = "silver_horizon_sample.mkv";
    let unrelated_bytes: Vec<u8> = (0..4096u32).map(|value| (value % 253) as u8).collect();
    let index_filename = "silver_horizon_sample.par2";
    let recovery_filename = "silver_horizon_sample.vol00+01.par2";
    let par2_bytes = build_test_par2_index_for_files(&[(unrelated_name, &unrelated_bytes)], 1024);
    let recovery_bytes = vec![0xAA; 64];
    let mut files = parts.clone();
    files.push((unrelated_name.to_string(), unrelated_bytes.clone()));
    files.push((index_filename.to_string(), par2_bytes.clone()));
    files.push((recovery_filename.to_string(), recovery_bytes.clone()));
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Other Set", &files),
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 0, &parts[0].0, &parts[0].1).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, set_name)
        .expect("armed");
    coverage.cap_at_damage(parts.len() - 1, 0);
    for (file_index, (filename, bytes)) in parts.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    let unrelated_file = parts.len() as u32;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        unrelated_file,
        unrelated_name,
        &unrelated_bytes,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        unrelated_file + 1,
        index_filename,
        &par2_bytes,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        unrelated_file + 2,
        recovery_filename,
        &recovery_bytes,
    )
    .await;
    let par2_set =
        build_repairable_par2_set_for_files(&[(unrelated_name, &unrelated_bytes)], 1024, 1);
    let set_id = par2_set.recovery_set_id;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        par2_set,
        &[
            (unrelated_file + 1, index_filename, 0, false),
            (unrelated_file + 2, recovery_filename, 1, true),
        ],
    );

    pipeline.release_direct_unpack_after_clean_verification(job_id, set_id);
    assert!(
        coverage.is_gated(),
        "a verdict about other files must not lift this set's gate"
    );
    assert_eq!(
        pipeline.direct_unpack_gated_sets(job_id),
        vec![set_name.to_string()]
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The control: with nothing gated, the strong-decode skip stands exactly as
/// it did — the new term is evidence-only, never a blanket veto for 7z jobs.
#[tokio::test]
async fn an_ungated_chase_leaves_the_strong_decode_skip_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42001);
    let (mut pipeline, set_name) = split_7z_job_with_par2(&temp_dir, job_id, false).await;
    let mut verify_events = pipeline.event_tx.subscribe();
    reap_until_outcome(&mut pipeline, job_id, set_name).await;
    assert!(pipeline.direct_unpack_gated_sets(job_id).is_empty());

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        0,
        "an intact job with a clean chase takes the strong-decode skip"
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}
