use super::*;
use crate::pipeline::completion::finalize::rar::{ArchiveSetRetirement, UnmaterializedArchiveSet};

/// A four-volume set whose two file members sanitize to one destination
/// on a case-folding filesystem: `CERTIFICATE/BACKUP/id.bdmv` and
/// `CERTIFICATE/backup/id.bdmv`.
fn build_case_colliding_multivolume_rar_set() -> Vec<(String, Vec<u8>)> {
    let first = b"first-of-two-names-for-one-path";
    let second = b"second-of-two-names-for-one-path";
    let members = [
        (
            "CERTIFICATE/BACKUP/id.bdmv",
            &first[..],
            &first[..12],
            &first[12..],
        ),
        (
            "CERTIFICATE/backup/id.bdmv",
            &second[..],
            &second[..12],
            &second[12..],
        ),
    ];
    let mut volumes = Vec::new();
    for (member_index, (name, whole, head, tail)) in members.iter().enumerate() {
        let crc = checksum::crc32(whole);
        let open_volume = member_index * 2;
        let close_volume = open_volume + 1;

        let mut open = Vec::new();
        open.extend_from_slice(&TEST_RAR5_SIG);
        open.extend_from_slice(&build_test_rar_main_header(
            if open_volume == 0 {
                0x0001
            } else {
                0x0001 | 0x0002
            },
            (open_volume > 0).then_some(open_volume as u64),
        ));
        open.extend_from_slice(&build_test_rar_file_header(
            name,
            0x0010,
            head.len() as u64,
            whole.len() as u64,
            None,
        ));
        open.extend_from_slice(head);
        open.extend_from_slice(&build_test_rar_end_header(true));
        volumes.push((format!("show.part{:02}.rar", open_volume + 1), open));

        let last = member_index + 1 == members.len();
        let mut close = Vec::new();
        close.extend_from_slice(&TEST_RAR5_SIG);
        close.extend_from_slice(&build_test_rar_main_header(
            0x0001 | 0x0002,
            Some(close_volume as u64),
        ));
        close.extend_from_slice(&build_test_rar_file_header(
            name,
            0x0008,
            tail.len() as u64,
            whole.len() as u64,
            Some(crc),
        ));
        close.extend_from_slice(tail);
        close.extend_from_slice(&build_test_rar_end_header(!last));
        volumes.push((format!("show.part{:02}.rar", close_volume + 1), close));
    }
    volumes
}

/// A job whose four-volume RAR set is fully downloaded, complete, and already
/// carries a derived plan — the state every post-repair refresh finds.
async fn complete_rar_set_with_plan(
    pipeline: &mut Pipeline,
    job_id: JobId,
    files: &[(String, Vec<u8>)],
    name: &str,
) {
    insert_active_job(pipeline, job_id, rar_job_spec(name, files)).await;
    pause_job_for_rar_fixture_setup(pipeline, job_id);
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    assert!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_ref())
            .is_some(),
        "the fixture must reproduce the post-repair precondition: a plan already exists"
    );
}

/// Every file `Complete` at its canonical name, which is what a repaired volume
/// re-verifies as — never `Renamed`.
fn all_complete_verification(files: &[(String, Vec<u8>)]) -> par2_rs::VerificationResult {
    par2_rs::VerificationResult {
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, _))| par2_rs::verify::FileVerification {
                file_id: par2_rs::FileId::from_bytes([index as u8; 16]),
                filename: filename.clone(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: Vec::new(),
                missing_slice_count: 0,
            })
            .collect(),
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

/// A job whose RAR volumes are complete in the ledger but were never written
/// under the names its live identities classify them into.
///
/// Nothing is written to the working directory on purpose: this is the state a
/// direct set leaves behind, where the bytes were routed straight to their
/// members and no volume file ever existed to reopen.
async fn claimed_alias_set_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    name: &str,
    alias: &str,
    volumes: &[String],
) {
    let files: Vec<(String, Vec<u8>)> = volumes
        .iter()
        .map(|filename| (filename.clone(), vec![0u8; 64]))
        .collect();
    insert_active_job(pipeline, job_id, rar_job_spec(name, &files)).await;
    set_job_status_for_test(pipeline, job_id, JobStatus::Downloading);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        let file_id = NzbFileId {
            job_id,
            file_index: file_index as u32,
        };
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            let file = state.assembly.file_mut(file_id).unwrap();
            file.record_placement(0, 0, bytes.len() as u32);
            file.commit_segment(0, bytes.len() as u32).unwrap();
        }
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index: file_index as u32,
                    source_filename: filename.clone(),
                    current_filename: filename.clone(),
                    canonical_filename: Some(filename.clone()),
                    classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                        kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                        set_name: alias.to_string(),
                        volume_index: Some(file_index as u32),
                    }),
                    classification_source: crate::jobs::record::FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }
}

/// Turns the completion loop the way the run loop does: pop everything the
/// queue holds, check it, and let each check re-arm whatever it wants to.
async fn drain_completion_checks(pipeline: &mut Pipeline, rounds: usize) -> usize {
    let mut checks = 0;
    for _ in 0..rounds {
        let queued = pipeline.pending_completion_checks.len();
        if queued == 0 {
            break;
        }
        for _ in 0..queued {
            let Some(job_id) = pipeline.pending_completion_checks.pop_front() else {
                break;
            };
            pipeline.check_job_completion(job_id).await;
            checks += 1;
        }
    }
    checks
}

/// A single stored member split across an **old-numbering** RAR4 set —
/// `<base>.rar`, `<base>.r00`, `<base>.r01`, … — the one multi-volume RAR
/// shape whose headers state no per-volume number anywhere: RAR4's main header
/// has no number field (that is RAR5's), and these end records carry no
/// `VOLUME_NUMBER` flag (a numbered `ENDARC` is what modern `.partNN` writers
/// emit). Every volume of such a set parses as volume 0; only the filename
/// says which volume a file is.
fn single_member_rar4_old_numbering_set(
    base: &str,
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);
    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;
            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            // VOLUME only — no NEW_NUMBERING, exactly like the old scheme.
            bytes.extend_from_slice(&build_test_rar4_block(0x73, 0x0001, &[0u8; 6]));
            bytes.extend_from_slice(&build_test_rar4_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header(!is_last));
            let filename = if is_first {
                format!("{base}.rar")
            } else {
                format!("{base}.r{:02}", volume - 1)
            };
            (filename, bytes)
        })
        .collect()
}

/// Fill in the CRC16 of every RAR4 block in `bytes`. The shared fixture
/// builders leave it zero because weaver's own parser only warns on a
/// mismatch; the recovery restorer's validity probe does not, and a set
/// written to exercise it has to be one it would accept.
fn with_rar4_header_crcs(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut offset = TEST_RAR4_SIG.len();
    while offset + 7 <= bytes.len() {
        let flags = u16::from_le_bytes([bytes[offset + 3], bytes[offset + 4]]);
        let header_size = u16::from_le_bytes([bytes[offset + 5], bytes[offset + 6]]) as usize;
        let data_size = if flags & 0x8000 != 0 {
            u32::from_le_bytes([
                bytes[offset + 7],
                bytes[offset + 8],
                bytes[offset + 9],
                bytes[offset + 10],
            ]) as usize
        } else {
            0
        };
        let crc = (checksum::crc32(&bytes[offset + 2..offset + header_size]) & 0xFFFF) as u16;
        bytes[offset..offset + 2].copy_from_slice(&crc.to_le_bytes());
        offset += header_size + data_size;
    }
    bytes
}

/// A RAR3 standalone recovery volume for `volumes`: the set's RS parity over
/// every data volume, padded to the longest, followed by the 7-byte footer
/// (`data-1`, `rec-1`, `position-1`, CRC32) a new-style `.rev` carries and the
/// restorer reads its geometry from.
fn single_recovery_volume_for_rar4_set(volumes: &[&[u8]]) -> Vec<u8> {
    let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
    let coder = reedsolomon_rs::rar3::Rar3RsCoder::new(1).expect("one recovery volume");
    let mut rev = Vec::with_capacity(width + 7);
    let mut column = vec![0u8; volumes.len()];
    let mut parity = [0u8; 1];
    for position in 0..width {
        for (slot, volume) in column.iter_mut().zip(volumes) {
            *slot = volume.get(position).copied().unwrap_or(0);
        }
        coder.encode(&column, &mut parity);
        rev.push(parity[0]);
    }
    rev.push((volumes.len() - 1) as u8);
    rev.push(0);
    rev.push(0);
    let crc = checksum::crc32(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

/// An old-numbering RAR4 set of four volumes whose volume 2 was never posted,
/// plus one `.rev` that can rebuild it. The job's files are volumes 0, 1 and 3
/// and the recovery volume, every one of them downloaded and complete, and
/// the set is left waiting on the hole. Returns the member's payload.
async fn stage_old_numbering_rar4_set_with_a_recovery_volume(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> Vec<u8> {
    let payload: Vec<u8> = (0..4096u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let volumes: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", "silver.horizon.mkv", &payload, 4)
            .into_iter()
            .map(|(filename, bytes)| (filename, with_rar4_header_crcs(bytes)))
            .collect();
    let volume_bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = single_recovery_volume_for_rar4_set(&volume_bytes);

    let mut files: Vec<(String, Vec<u8>)> = volumes
        .iter()
        .enumerate()
        .filter(|(volume, _)| *volume != 2)
        .map(|(_, file)| file.clone())
        .collect();
    files.push(("silver.horizon.rev".to_string(), rev));
    insert_active_job(pipeline, job_id, rar_job_spec(job_name, &files)).await;
    for (index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(pipeline, job_id, index as u32, filename, bytes).await;
    }

    let state = pipeline
        .rar_sets
        .get(&(job_id, "silver.horizon".to_string()))
        .expect("the downloaded volumes register the set");
    assert!(
        !state.volume_files.contains_key(&2),
        "volume 2 is the hole this fixture exists for"
    );
    assert!(
        state
            .plan
            .as_ref()
            .is_some_and(|plan| plan.waiting_on_volumes.contains(&2)),
        "the set waits on the hole before the restore: {:?}",
        state.plan
    );
    payload
}

/// Stands up the fixture both extraction-ordering tests share: a five-volume
/// old-numbering RAR4 set in the NZB, with a recovery set installed over it
/// **before** any volume completes — the order a real job has, where the small
/// index lands long before the payload.
///
/// The volumes are returned rather than completed, because the ordering under
/// test is exactly what a completing volume triggers.
async fn rar_set_with_recovery_awaiting_volumes(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> (String, Vec<(String, Vec<u8>)>) {
    let payload: Vec<u8> = (0..5120u32)
        .map(|index| (index.wrapping_mul(1_597_334_677) >> 11) as u8)
        .collect();
    let volumes: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", "silver.horizon.mkv", &payload, 5)
            .into_iter()
            .map(|(filename, bytes)| (filename, with_rar4_header_crcs(bytes)))
            .collect();
    insert_active_job(pipeline, job_id, rar_job_spec(job_name, &volumes)).await;
    install_test_par2_runtime(pipeline, job_id, placement_par2_file_set(&volumes), &[]);
    assert!(
        pipeline.par2_set(job_id).is_some(),
        "the fixture's recovery set must be the thing a verdict can come from"
    );
    ("silver.horizon".to_string(), volumes)
}

async fn complete_rar_volumes(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) {
    for (index, (filename, bytes)) in volumes.iter().enumerate() {
        write_and_complete_rar_volume(pipeline, job_id, index as u32, filename, bytes).await;
    }
}

fn extraction_dispatched(pipeline: &Pipeline, job_id: JobId, set_name: &str) -> bool {
    let set_busy = pipeline
        .rar_sets
        .get(&(job_id, set_name.to_string()))
        .is_some_and(|state| state.active_workers > 0 || !state.in_flight_members.is_empty());
    set_busy
        || pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| !sets.is_empty())
}

mod extraction_reserves_member_totals;
mod no_par2_retry_reclassifies;
mod par2_metadata_immediately_rebinds;
mod repaired_rar_set_holds;
