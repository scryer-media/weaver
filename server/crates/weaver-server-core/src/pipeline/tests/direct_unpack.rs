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
    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }

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

#[tokio::test]
async fn a_download_that_ends_with_a_part_missing_aborts_the_chase() {
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
        !pipeline.direct_unpack.is_armed(job_id, set_name),
        "a chase waiting on bytes that stopped coming must be ended"
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
    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }

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

    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
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

    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
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
    pipeline.decide_direct_unpack_before_repair(job_id);

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
    pipeline.decide_direct_unpack_before_repair(job_id);
    let tainted = !pipeline.direct_unpack.is_armed(job_id, set_name);

    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }

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
    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }

    assert!(
        armed,
        "a set whose parts are all complete must survive the download-end settle, got latch {latched:?}"
    );
}

/// The tighter form of the `direct-unpack-solid-split` race: the set arms as
/// its first part completes and the download ends before the rest have been
/// reconciled into the assembly. The bytes are all on disk by then — no more
/// are coming — so the settle must finish the coverage from what is there
/// rather than kill a chase that is about to succeed.
#[tokio::test]
async fn a_settle_finishes_parts_that_are_on_disk_but_not_yet_reconciled() {
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

    // The remaining parts land on disk while the assembly has not caught up.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (filename, bytes) in files.iter().skip(1) {
        std::fs::write(working_dir.join(filename), bytes).unwrap();
    }

    pipeline.settle_direct_unpack_after_download(job_id);

    let armed = pipeline.direct_unpack.is_armed(job_id, set_name);
    let coverage = pipeline.direct_unpack.armed_coverage(job_id, set_name);
    let settled = coverage.as_ref().map(|coverage| {
        (0..files.len()).all(|index| {
            coverage
                .part_progress(index)
                .map(|part| part.complete)
                .unwrap_or(false)
        })
    });

    pipeline.direct_unpack_forget_job(job_id);
    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }

    assert!(
        armed,
        "a set whose parts are all on disk must survive the download-end settle"
    );
    assert_eq!(
        settled,
        Some(true),
        "every part must be settled from what is on disk"
    );
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
    assert_eq!(pipeline.direct_unpack.counters().demoted_gated_stall, 1);
    let reason = coverage
        .abort_reason()
        .expect("the parked reader must be woken");
    assert!(reason.contains("gated chase stalled"), "reason: {reason}");

    for _ in 0..600 {
        pipeline.reap_direct_unpack().await;
        tokio::task::yield_now().await;
    }
}
